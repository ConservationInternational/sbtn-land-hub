# Based on https://stackoverflow.com/a/62041888/871101
import logging

import numba
import numpy as np
import xarray as xr
from numba.pycc import CC

cc = CC("parallel_functions")

logger = logging.getLogger(__name__)

NODATA_VALUE = 0


@numba.jit(nopython=True, nogil=True)
@cc.export("slice_area", "f8(f8)")
def slice_area(f):
    """
    Calculate the area of a slice of the globe from the equator to the parallel
    at latitude f (on WGS84 ellipsoid). Based on:
    https://gis.stackexchange.com/questions/127165/more-accurate-way-to-calculate-area-of-rasters
    """
    a = 6378137.0  # in meters
    b = 6356752.3142  # in meters,
    e = np.sqrt(1 - pow(b / a, 2))
    zp = 1 + e * np.sin(f)
    zm = 1 - e * np.sin(f)

    return (
        np.pi
        * pow(b, 2)
        * ((2 * np.arctanh(e * np.sin(f))) / (2 * e) + np.sin(f) / (zp * zm))
    )


@numba.jit(nopython=True, nogil=True)
@cc.export("calc_cell_area", "f8(f8, f8, f8)")
def calc_cell_area(y, x_res, y_res):
    """
    Returns cell area in hectares

    Use formula to calculate area of a raster cell on WGS84 ellipsoid, following
    https://gis.stackexchange.com/questions/127165/more-accurate-way-to-calculate-area-of-rasters

    y_min: minimum latitude
    y_max: maximum latitude
    x_res: width of cell in degrees
    """

    shp = y.shape
    out = np.zeros(shp, dtype=np.float32)

    y = y.copy().ravel()
    y_min = y - y_res / 2
    y_max = y + y_res / 2

    out = (
        (slice_area(np.deg2rad(y_max)) - slice_area(np.deg2rad(y_min)))
        * (x_res / 360.0)
        * 0.0001
    )

    return np.reshape(out, shp)


@numba.jit(nopython=True, nogil=True)
@cc.export("calc_natural_conversion", "i2[:,:](i4[:,:], i4[:,:], i4[:,:], i4[:,:])")
def calc_natural_conversion(trans, initial_cover, crops_initial, crops_final):
    """calculate land cover degradation"""
    shp = trans.shape
    trans = trans.ravel()
    initial_cover = initial_cover.ravel()
    crops_initial = crops_initial.ravel()
    crops_final = crops_final.ravel()
    out = np.zeros(trans.shape, dtype=np.int8)

    # Code natural conversion as indicated by CCI as conversion
    out[trans == 1] = 1

    # Code cropland increase co-occurring with esa-indicated conversion as 2
    crop_increase = (crops_initial <= 0.5) & (crops_final > 0.5)
    out[(out == 1) & crop_increase] = 2

    # Code cropland increase occurring on what was initially natural, but where change
    # was not in ESA, as 3
    out[(out == 0) & crop_increase & (initial_cover == 1)] = 3

    ##
    # Below are not natural conversion, but are coded in the output for future use

    # Code other cropland increase occurring on land that esa indicated was forest as 4
    out[(out == 0) & crop_increase & (initial_cover == 2)] = 4
    # Code other cropland increase occurring on land that esa indicated was urban as 5
    out[(out == 0) & crop_increase & (initial_cover == 4)] = 5
    # Code other cropland increase occurring on land that esa indicated was other as 6
    out[(out == 0) & crop_increase & (initial_cover == 5)] = 6

    return np.reshape(out, shp)


def compute_natural_conversion(
    data: xr.DataArray,
    trans_codes: list,
    trans_meanings: list,
    x_res: float,
    y_res: float,
) -> xr.DataArray:

    coords = {"y": data.y, "x": data.x}
    out = xr.Dataset(coords=coords)

    initial_natural = calc_trans_meaning(
        data.lc_initial.values,
        numba.typed.List(trans_codes),
        numba.typed.List(trans_meanings),
    )
    meaning = calc_natural_conversion(
        data.trans.values,
        initial_natural,
        data.crops_initial.values,
        data.crops_final.values,
    )

    cell_areas = np.expand_dims(calc_cell_area(data.y.values, x_res, y_res), axis=1)
    cell_areas = np.repeat(cell_areas, repeats=data.x.size, axis=1)

    area_natural_conversion = ((meaning >= 1) & (meaning <= 3)).astype(
        np.float32
    ) * cell_areas

    out["transition"] = (("y", "x"), meaning)
    out["area_pixel"] = (("y", "x"), cell_areas)
    out["area_natural_conversion"] = (("y", "x"), area_natural_conversion)

    return out


def compute_cell_areas(
    data: xr.DataArray,
    x_res: float,
    y_res: float,
) -> xr.DataArray:
    coords = {"y": data.y, "x": data.x}
    out = xr.Dataset(coords=coords)

    cell_areas = np.expand_dims(calc_cell_area(data.y.values, x_res, y_res), axis=1)
    cell_areas = np.repeat(cell_areas, repeats=data.x.size, axis=1)

    out["area_pixel"] = (("y", "x"), cell_areas)

    return out


def compute_natural_conv_transitions(
    data: xr.DataArray, trans_codes: list, trans_meanings: list
) -> xr.DataArray:
    coords = {"y": data.y, "x": data.x}
    out = xr.Dataset(coords=coords)

    initial_natural = calc_trans_meaning(
        data.lc_initial.values,
        numba.typed.List(trans_codes),
        numba.typed.List(trans_meanings),
    )
    meaning = calc_natural_conversion(
        data.trans.values,
        initial_natural,
        data.crops_initial.values,
        data.crops_final.values,
    )

    out["transition"] = (("y", "x"), meaning)

    return out


def compute_natural_conv_areas(data: xr.DataArray) -> xr.DataArray:
    coords = {"y": data.y, "x": data.x}
    out = xr.Dataset(coords=coords)

    area_natural_conversion = (
        (data.transition.data >= 1) & (data.transition.data <= 3)
    ).astype(np.float32) * data.area_pixel.data

    out["area_natural_conversion"] = (("y", "x"), area_natural_conversion)

    return out


@numba.jit(nopython=True, nogil=True)
@cc.export("calc_trans_meaning", "i4[:,:](i4[:,:], i4[:,:], i2[:], i4)")
def calc_trans_meaning(trans, trans_codes, trans_meanings):
    """calculate meaning of land cover transition"""
    shp = trans.shape
    trans = trans.ravel()
    out = np.zeros(trans.shape, dtype=np.int32)

    for code, meaning in zip(trans_codes, trans_meanings):
        out[trans == code] = meaning
    out[trans == NODATA_VALUE] = NODATA_VALUE

    return np.reshape(out, shp)


@numba.jit(nopython=True, nogil=True)
@cc.export("calc_lc_trans", "i4[:,:](u1[:,:], u1[:,:], i4)")
def calc_lc_trans(lc_bl, lc_tg, multiplier):
    shp = lc_bl.shape
    lc_bl = lc_bl.ravel().astype(np.int32)
    lc_tg = lc_tg.ravel().astype(np.int32)
    a_trans_bl_tg = lc_bl * multiplier + lc_tg
    a_trans_bl_tg[np.logical_or(lc_bl < 1, lc_tg < 1)] = NODATA_VALUE

    return np.reshape(a_trans_bl_tg, shp)


def compute_transitions(
    lc: xr.DataArray, trans_codes: list, trans_meanings: list, global_attrs: dict
) -> xr.DataArray:
    coords = {"y": lc.y, "x": lc.x}
    out = xr.Dataset(coords=coords, attrs=global_attrs)

    trans = calc_lc_trans(lc.lc_initial.values, lc.lc_final.values, 1000)
    meaning = calc_trans_meaning(
        trans, numba.typed.List(trans_codes), numba.typed.List(trans_meanings)
    )

    out["transition"] = (("y", "x"), trans)
    out["meaning"] = (("y", "x"), meaning)

    return out
