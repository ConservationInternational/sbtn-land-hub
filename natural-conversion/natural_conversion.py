import json
import logging
from pathlib import Path
from pathlib import PurePath

import boto3
import openpyxl
import requests
import rioxarray
import xarray as xr
from dask.distributed import Client
from dask.distributed import LocalCluster

import natural_conversion_parallel_functions

N_WORKERS = 60
CROP_DATA_FOR_TESTING = False

DATA_PATH = Path('/data')

IN_S3_BUCKET = 'trends.earth-private'
IN_S3_PREFIX = 'esa-cci'
OUT_S3_BUCKET = 'trends.earth-private'
OUT_S3_PREFIX = 'esa-cci/transitions'

INITIAL_YEAR = 2000
FINAL_YEAR = 2019
CCI_FILE_INITIAL = 'ESACCI-LC-L4-LCCS-Map-300m-P1Y-2000-v2.0.7.tif'
CCI_FILE_FINAL = 'C3S-LC-L4-LCCS-Map-300m-P1Y-2019-v2.1.1.tif'

formatter = '[%(levelname)s] %(asctime)s - %(message)s [%(funcName)s %(lineno)d]'
logging.basicConfig(level=logging.INFO, format=formatter)

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def log_progress(fraction, message=None, data=None):
    logger.info('%s - %.2f%%', message, 100 * fraction)


with open(PurePath('/data/aws_credentials.json'), 'r') as f:
    aws_creds = json.load(f)


def put_to_s3(filename: Path, bucket: str, prefix: str):
    client = boto3.client('s3')
    key = f'{prefix}/{filename.name}'
    logger.info(f'Uploading {filename} to s3 at {key}')
    client.upload_file(str(filename), bucket, key)


def download_file(url, local_gz_file):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_gz_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def get_from_s3(bucket, prefix, filename, out_path):
    client = boto3.client('s3')
    logger.info(f'Downloading {filename} from s3 to {out_path}')
    client.download_file(bucket, f'{prefix}/{filename}', out_path)


def ds_to_cog(ds, cloud='s3'):
    ds.rio.write_crs('EPSG:4326', inplace=True)

    out_file = (
        DATA_PATH /
        f'ESACCI-LC-L4-LCCS-Map-300m-P1Y-Transitions_{INITIAL_YEAR}-{FINAL_YEAR}.tif'
    )
    ds.rio.to_raster(out_file, tiled=True, compress='LZW', dtype='int32')

    put_to_s3(out_file, OUT_S3_BUCKET, OUT_S3_PREFIX)
    out_file.unlink()


def get_trans_codes(xl_file, header_column, first_data_column,
                    last_data_column, first_data_row, last_data_row):
    wb = openpyxl.load_workbook(xl_file)
    sheet = wb[wb.sheetnames[0]]

    header_codes = [
        val[0] for val in sheet.iter_rows(min_row=first_data_row,
                                          max_row=last_data_row,
                                          min_col=header_column,
                                          max_col=header_column,
                                          values_only=True)
    ]

    trans_codes = []
    trans_meanings = []

    for cells in sheet.iter_rows(min_row=first_data_row,
                                 max_row=last_data_row,
                                 min_col=first_data_column,
                                 max_col=last_data_column):

        for cell in cells:
            initial_class = header_codes[cell.row - first_data_row]
            final_class = header_codes[cell.column - first_data_column]
            trans_codes.append(initial_class * 1000 + final_class)
            trans_meanings.append(cell.value)

    return trans_codes, trans_meanings


def main():

    ###############################################################################
    # Download ESA data if not already present
    DATA_PATH.mkdir(parents=True, exist_ok=True)

    in_files = []
    for in_file in [CCI_FILE_INITIAL, CCI_FILE_FINAL]:
        local_file_path = DATA_PATH / in_file
        in_files.append(local_file_path)

        if not local_file_path.exists():
            get_from_s3(IN_S3_BUCKET, IN_S3_PREFIX, in_file,
                        str(local_file_path))
    #TODO: Check md5s of downloads vs etags

    ###########################################################################
    # Load data

    logger.info('Loading data')

    lc_initial = rioxarray.open_rasterio(in_files[0], chunks=True)
    lc_initial = lc_initial.rename('lc_initial').sel(band=1).drop('band')
    lc_final = rioxarray.open_rasterio(in_files[-1], chunks=True)
    lc_final = lc_final.rename('lc_final').sel(time=lc_final['time'][0]).drop('time')

    # Crop data for testing
    if CROP_DATA_FOR_TESTING:
        logger.warning('****** Cropping data for testing ******')
        lc_initial = lc_initial[48000:96000, 48000:96000]
        lc_final = lc_final[48000:96000, 48000:96000]

    lc = xr.merge([lc_initial, lc_final], combine_attrs='drop').unify_chunks()
    logger.info(f'lc {lc}')

    logger.debug('lc %s', lc)
    attrs_to_copy = ['_FillValue', 'scale_factor', 'add_offset']
    global_attrs = {
        key: value
        for (key, value) in lc.attrs.items() if key in attrs_to_copy
    }

    ###########################################################################
    # Compute transitions

    trans_codes, trans_meanings = get_trans_codes(
        'ESA_CCI_Natural_Conversion_Coding_v2.xlsx',
        header_column=2,
        first_data_column=4,
        last_data_column=41,
        first_data_row=4,
        last_data_row=41)
    logger.debug('trans_codes are %s', trans_codes)
    logger.debug('trans_meanings are %s', trans_meanings)

    logger.info('Calculating transitions...')

    kwargs = {
        'trans_codes': trans_codes,
        'trans_meanings': trans_meanings,
        'global_attrs': global_attrs
    }

    trans = xr.map_blocks(
        natural_conversion_parallel_functions.compute_transitions,
        lc,
        kwargs=kwargs)
    logger.debug('transition %s', trans)

    trans = trans.compute()

    logger.info('Writing geotiff to S3')
    ds_to_cog(trans, cloud='s3')


if __name__ == "__main__":
    cluster = LocalCluster(n_workers=N_WORKERS, threads_per_worker=1)
    client = Client(cluster)

    main()
