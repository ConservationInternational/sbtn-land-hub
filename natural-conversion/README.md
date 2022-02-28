# Natural conversion calculation

This folder contains the code used to calculate a conversion of natural vegetation layer
or use by the Science Based Targets Network (SBTN) Land Hub as part of its work piloting
indicators that may be used in the target setting process.

## Input datasets

- [European Space Agency Climate Change Initiative (ESA-CCI) land cover data](https://www.esa-landcover-cci.org/) for 2000 and 2015.
- Cropland extent layer from [Potapov et al. 2022](https://www.nature.com/articles/s43016-021-00429-z)

## Processing steps

1. The ESA CCI land cover data is approximately 300m spatial resolution, while the cropland extent layer from Potapov et al. is at 30m. Therefore the cropland extent layer must be resampled to match the ESA CCI. This is done via the `cropland_match_to_esa.py` script, which produced 10x10 degree tiles at 300m resolution.

- The cropland extent layer is a binary 0/1 (no cropland / cropland) map
- The code converts the map to floating point and each cell at 300m is the percentage of that cell that was cropland during a particular year

2. The
