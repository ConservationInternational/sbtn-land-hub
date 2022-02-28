# Based on https://stackoverflow.com/a/62041888/871101

import logging

import numba
import numpy as np
import xarray as xr
from numba.pycc import CC

cc = CC('natural_conversion_parallel_functions')

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# logger = logging.getLogger('parallel logger')
# fh = logging.FileHandler('{}_{:%Y%m%d_%H%M%S}.log'.format(
#     multiprocessing.current_process().name, datetime.now()))
# logger.addHandler(fh)

logger = logging.getLogger(__name__)

NODATA_VALUE = 0


@numba.jit(nopython=True)
@cc.export('calc_deg_lc', 'i2[:,:](i4[:,:], i4[:,:], i2[:], i4)')
def calc_natural_conversion(trans, trans_codes, trans_meanings):
    '''calculate land cover degradation'''
    shp = trans.shape
    trans = trans.ravel()
    out = np.zeros(trans.shape, dtype=np.int16)

    for code, meaning in zip(trans_codes, trans_meanings):
        out[trans == code] = meaning
    out[trans == NODATA_VALUE] = NODATA_VALUE

    return np.reshape(out, shp)


@numba.jit(nopython=True)
@cc.export('calc_lc_trans', 'i4[:,:](u1[:,:], u1[:,:], i4)')
def calc_lc_trans(lc_bl, lc_tg, multiplier):
    shp = lc_bl.shape
    lc_bl = lc_bl.ravel().astype(np.int32)
    lc_tg = lc_tg.ravel().astype(np.int32)
    a_trans_bl_tg = lc_bl * multiplier + lc_tg
    a_trans_bl_tg[np.logical_or(lc_bl < 1, lc_tg < 1)] = NODATA_VALUE

    return np.reshape(a_trans_bl_tg, shp)


def compute_transitions(lc: xr.DataArray, trans_codes: list, trans_meanings: list,
                        global_attrs: dict) -> xr.DataArray:
    gamma_coords = {"y": lc.y, "x": lc.x}
    out = xr.Dataset(coords=gamma_coords, attrs=global_attrs)

    trans = calc_lc_trans(lc['lc_initial'].values, lc['lc_final'].values, 1000)
    meaning = calc_natural_conversion(trans, trans_codes, trans_meanings)

    out['transition'] = (('y', 'x'), trans)
    out['meaning'] = (('y', 'x'), meaning)

    return out
