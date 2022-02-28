import json
import logging
import os
import tempfile
from pathlib import Path, PurePath

import botocore
import boto3
import requests
from osgeo import gdal

DATA_PATH = Path('/data')

OUT_S3_BUCKET = 'trends.earth-private'
OUT_S3_PREFIX = 'cropland/250m'

OUT_TILE_WIDTH_DEG = 10
OUT_TILE_HEIGHT_DEG = 10
MIN_TILE_X = -180
MAX_TILE_X = 180
MIN_TILE_Y = -90
MAX_TILE_Y = 90

CCI_IN_S3_BUCKET = 'trends.earth-private'
CCI_IN_S3_PREFIX = 'esa-cci'
CCI_INITIAL_YEAR = 2000
CCI_IN_FILES = [
    f'ESACCI-LC-L4-LCCS-Map-300m-P1Y-{year}-v2.0.7.tif'
    for year in [CCI_INITIAL_YEAR]
]

CROP_YEARS = [2003, 2015]
CROP_QUADRANTS = ["NW", "SW", "NE", "SE"]
CROP_S3_BUCKET = 'trends.earth-private'
CROP_S3_PREFIX = 'cropland'

formatter = '[%(levelname)s] %(asctime)s - %(message)s [%(funcName)s %(lineno)d]'

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


def warp_croplands(in_file, out_file, x_res, y_res, bounds):
    gdal.Warp(
        out_file,
        in_file,
        xRes=x_res,
        yRes=y_res,
        outputBounds=bounds,
        outputType=gdal.GDT_Float32,
        workingType=gdal.GDT_Float32,
        resampleAlg=gdal.GRA_Average,
        format='COG',
        multithread=True,
        warpMemoryLimit=1000,
        warpOptions=[
            'NUM_THREADS=ALL_CPUS',
            'GDAL_CACHEMAX=2000',
        ],
        creationOptions=[
            'COMPRESS=LZW',
            'BIGTIFF=YES',
            'NUM_THREADS=ALL_CPUS',
        ],
        callback=log_progress
    )

def get_tile_info(n):
    tile_uls = []
    crop_years = []
    for crop_year in CROP_YEARS:
        for ul_x in range(MIN_TILE_X, MAX_TILE_X, OUT_TILE_WIDTH_DEG):
            for ul_y in range(MAX_TILE_Y, MIN_TILE_Y, -OUT_TILE_HEIGHT_DEG):
                tile_uls.append((ul_x, ul_y))
                crop_years.append(crop_year)
    logger.info(f'Selecting tile {n} from list of {len(tile_uls)}...')
    if n > len(tile_uls):
        raise Exception("tile index is greater than length of tile list")
    else:
        ul_x, ul_y = tile_uls[n]
        bounds = (
            ul_x,
            ul_y - OUT_TILE_HEIGHT_DEG,
            ul_x + OUT_TILE_WIDTH_DEG,
            ul_y
        )  # yapf: disable
        return crop_year, bounds


def x_coord_to_str(x):
    return f'{abs(x)}W' if x < 0 else f'{x}E'


def y_coord_to_str(y):
    return f'{abs(y)}S' if y < 0 else f'{y}N'


def key_exists(bucket, prefix, file):
    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket, f'{prefix}/{file}').load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Something else has gone wrong.
            logger.exception('Error accessing s3')
            raise
    else:
        return True


def main():

    ###############################################################################
    # Download ESA data if not already present
    DATA_PATH.mkdir(parents=True, exist_ok=True)

    logger.info('Downloading data...')
    for cci_in_file in CCI_IN_FILES:
        local_file_path = DATA_PATH / cci_in_file

        if not local_file_path.exists():
            get_from_s3(CCI_IN_S3_BUCKET, CCI_IN_S3_PREFIX, cci_in_file,
                        str(local_file_path))
        #TODO: Check md5s of downloads vs etags

    # CROP_IN_FILES = [
    #     f'Global_cropland_{quadrant}_{year}.tif'
    #     for year in CROP_YEARS for quadrant in CROP_QUADRANTS
    # ]
    # for crop_in_file in CROP_IN_FILES:
    #     local_file_path = DATA_PATH / crop_in_file
    #
    #     if not local_file_path.exists():
    #         get_from_s3(CROP_S3_BUCKET, CROP_S3_PREFIX, crop_in_file,
    #                     str(local_file_path))
    #     #TODO: Check md5s of downloads vs etags
    # crop_in_files = [*DATA_PATH.glob(f'Global_cropland_*{year}.tif')]
    # logger.info('Making VRT of %s crop files', len(crop_in_files))

    CROP_IN_URLS = [
        (
            f'/vsicurl/https://glad.geog.umd.edu/Potapov/Global_Crop/Data/' +
            f'Global_cropland_{quadrant}_{year}.tif'
        )
        for year in CROP_YEARS for quadrant in CROP_QUADRANTS
    ]

    tile_index = int(os.getenv("AWS_BATCH_JOB_ARRAY_INDEX", 280))
    year, bounds = get_tile_info(tile_index)

    logger.info(f'Processing data for {year} and bounds {bounds}...')
    cci_in_files = [*DATA_PATH.glob('ESACCI-LC-L4-LCCS-Map-300m-P1Y-*.tif')]

    crop_in_vrt = Path(tempfile.NamedTemporaryFile(suffix='.vrt').name)
    gdal.BuildVRT(str(crop_in_vrt), [str(f) for f in CROP_IN_URLS])

    cci_ds = gdal.Open(str(cci_in_files[0]))
    _, x_res, _, _, _, y_res = cci_ds.GetGeoTransform()

    logger.info('Warping...')
    crop_out_file = crop_in_vrt.parent / (
        f'Croplands_250m_{year}_{x_coord_to_str(bounds[0])}_{y_coord_to_str(bounds[3])}.tif')
    if key_exists(crop_out_file.name, CROP_S3_BUCKET, OUT_S3_PREFIX):
        logger.info('Key already exists - skipping')
    else:
        warp_croplands(str(crop_in_vrt), str(crop_out_file), x_res, y_res, bounds)

    put_to_s3(crop_out_file, CROP_S3_BUCKET, OUT_S3_PREFIX)

if __name__ == "__main__":
    main()
