import json
import logging
import sys
from pathlib import Path
from pathlib import PurePath

import boto3
import dask
import numpy as np
import openpyxl
import parallel_functions
import rioxarray
import xarray as xr
from dask.distributed import Client
from dask.distributed import LocalCluster

TESTING = True

DATA_PATH = Path("/data")

CROPLANDS_S3_BUCKET = "trends.earth-private"
CROPLANDS_S3_PREFIX = "cropland"
CCI_S3_BUCKET = "trends.earth-private"
OUT_S3_BUCKET = "trends.earth-private"
OUT_S3_PREFIX = "esa-cci/transitions"

INITIAL_YEAR = 2011
FINAL_YEAR = 2019
CROPLANDS_INITIAL_FILE = "Croplands_300m_2011.tif"
CROPLANDS_FINAL_FILE = "Croplands_300m_2019.tif"
CCI_TRANSITIONS_FILE = "ESACCI-LC-L4-LCCS-Map-300m-P1Y-Transitions_2011-2019.tif"
CCI_INITIAL_FILE = "ESACCI-LC-L4-LCCS-Map-300m-P1Y-2011-v2.0.7.tif"

formatter = "[%(levelname)s] %(asctime)s - %(message)s [%(funcName)s %(lineno)d]"
logging.basicConfig(level=logging.INFO, format=formatter)

logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


with open(PurePath("/data/aws_credentials.json"), "r") as f:
    aws_creds = json.load(f)


def put_to_s3(filename: Path, bucket: str, prefix: str):
    client = boto3.client("s3")
    key = f"{prefix}/{filename.name}"
    logger.info(f"Uploading {filename} to s3 at {key}")
    client.upload_file(str(filename), bucket, key)


def get_from_s3(bucket, prefix, filename, out_path):
    client = boto3.client("s3")
    logger.info(f"Downloading {filename} from s3 to {out_path}")
    client.download_file(bucket, f"{prefix}/{filename}", out_path)


def ds_to_cogs(ds):
    ds.rio.write_crs("EPSG:4326", inplace=True)

    if TESTING:
        testing_string = "_TEST"
    else:
        testing_string = ""

    for name, array in ds.data_vars.items():
        out_file = (
            DATA_PATH
            / f"natural-conversion_300m_{INITIAL_YEAR}-{FINAL_YEAR}_{name}{testing_string}.tif"
        )
        array.rio.to_raster(out_file, tiled=True, compress="LZW")
        put_to_s3(out_file, OUT_S3_BUCKET, OUT_S3_PREFIX)
        out_file.unlink()


def get_trans_codes(
    xl_file,
    initial_class_column,
    final_class_column,
    first_data_row,
    last_data_row,
):
    wb = openpyxl.load_workbook(xl_file)
    sheet = wb["Legend"]

    initial_class_codes = [
        val[0]
        for val in sheet.iter_rows(
            min_row=first_data_row,
            max_row=last_data_row,
            min_col=initial_class_column,
            max_col=initial_class_column,
            values_only=True,
        )
    ]

    final_class_codes = [
        val[0]
        for val in sheet.iter_rows(
            min_row=first_data_row,
            max_row=last_data_row,
            min_col=final_class_column,
            max_col=final_class_column,
            values_only=True,
        )
    ]

    return initial_class_codes, final_class_codes


def main():
    # Download data
    DATA_PATH.mkdir(parents=True, exist_ok=True)

    crops_in_files = []
    for in_file in [CROPLANDS_INITIAL_FILE, CROPLANDS_FINAL_FILE]:
        local_crop_file_path = DATA_PATH / in_file
        crops_in_files.append(local_crop_file_path)

        if not local_crop_file_path.exists():
            get_from_s3(
                CROPLANDS_S3_BUCKET,
                CROPLANDS_S3_PREFIX,
                in_file,
                str(local_crop_file_path),
            )

    local_trans_file_path = DATA_PATH / CCI_TRANSITIONS_FILE
    if not local_trans_file_path.exists():
        get_from_s3(
            CCI_S3_BUCKET,
            "esa-cci/transitions",
            CCI_TRANSITIONS_FILE,
            str(local_trans_file_path),
        )

    local_initial_cover_file_path = DATA_PATH / CCI_INITIAL_FILE
    if not local_initial_cover_file_path.exists():
        get_from_s3(
            CCI_S3_BUCKET,
            "esa-cci",
            CCI_INITIAL_FILE,
            str(local_initial_cover_file_path),
        )

    logger.info("Loading data")

    with dask.config.set(**{"array.slicing.split_large_chunks": True}):
        trans = rioxarray.open_rasterio(local_trans_file_path, chunks=True)
        # for trans band 1 is transition code, band 2 is meaning
        trans = trans.rename("trans").sel(band=2).drop("band")

        initial_cover = rioxarray.open_rasterio(
            local_initial_cover_file_path, chunks=True
        )
        initial_cover = initial_cover.rename("lc_initial").sel(band=1).drop("band")

        crops_initial = rioxarray.open_rasterio(crops_in_files[0], chunks=True)
        crops_initial = crops_initial.rename("crops_initial").sel(band=1).drop("band")
        crops_final = rioxarray.open_rasterio(crops_in_files[-1], chunks=True)
        crops_final = crops_final.rename("crops_final").sel(band=1).drop("band")

        # logger.info("********** trans %s", trans)
        # logger.info("********** initial_cover %s", initial_cover)
        # logger.info("********** crops_initial %s", crops_initial)
        # logger.info("********** crops_final %s", crops_final)

        # Crop data for testing
        if TESTING:
            logger.warning("****** Cropping data for testing ******")
            trans = trans[12000:22000, 12000:22000]
            initial_cover = initial_cover[12000:22000, 12000:22000]
            crops_initial = crops_initial[12000:22000, 12000:22000]
            crops_final = crops_final[12000:22000, 12000:22000]

        in_data = xr.merge(
            [trans, initial_cover, crops_initial, crops_final],
            join="override",
            combine_attrs="drop",
        ).unify_chunks()

    trans_codes, trans_meanings = get_trans_codes(
        "ESA_CCI_Natural_Conversion_Coding_v2.xlsx",
        initial_class_column=1,
        final_class_column=3,
        first_data_row=3,
        last_data_row=40,
    )

    kwargs = {
        "trans_codes": trans_codes,
        "trans_meanings": trans_meanings,
        "x_res": float((in_data.x[1] - in_data.x[0]).values),
        "y_res": float((in_data.y[0] - in_data.y[1]).values),
    }

    ###########################################################################
    # Compute transitions

    logger.info("Calculating transitions...")

    nat_conv = xr.map_blocks(
        parallel_functions.compute_natural_conversion, in_data, kwargs=kwargs
    )
    logger.debug("nat_conv %s", nat_conv)

    nat_conv = nat_conv.compute()

    logger.info("Writing geotiff to S3")
    ds_to_cogs(nat_conv)


if __name__ == "__main__":
    cluster = LocalCluster()
    client = Client(cluster)

    main()
