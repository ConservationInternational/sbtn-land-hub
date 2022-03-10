# Conversion of native vegetation (non-forest) calculation

This folder contains the code used to calculate a layer indicating conversion of native
vegetation (other than deforestation) for use by the Science Based Targets Network
(SBTN) Land Hub as part of its work piloting indicators that may be used in the target
setting process.

## Input datasets

- [European Space Agency Climate Change Initiative (ESA-CCI) land cover data](https://www.esa-landcover-cci.org/) for 2011 and 2019.
- Cropland extent layer from [Potapov et al. 2022](https://www.nature.com/articles/s43016-021-00429-z) for 2011 and 2019

## Processing steps

1. The ESA CCI land cover data is approximately 300m spatial resolution, while the
   cropland extent layer from Potapov et al. is at 30m. Therefore the cropland extent
   layer must be aggregated to match the ESA CCI. This is done via the
   `cropland_match_to_esa.py` script, which produces 10x10 degree tiles at 300m
   resolution.

   - The cropland extent layer is a binary 0/1 (no cropland / cropland) map for each
     4-year period

   - The code calculates from the cropland extent layers, for each 300m cell in the ESA
     CCI, the percentage of that cell that was cropland during a particular year

   - The final output is a series of 10x10 degree tiles at 300m where each cell is
     percent coverage by croplands for that period

2. The ESA CCI contains 36 classes. The map is a land cover - not a land use - product.
   Therefore assumptions need to be made on which types of transitions are likely to
   constitute "natural conversion". This conversion is done by the
   `esa_cci_transitions.py` script, using the following process:

   - The Excel file `ESA_CCI_Natural_Conversion_Coding_v2.xlsx` contains the rules used
     to map transitions to "natural conversion" (coded as a 1). All other transitions
     are coded as zero.

   - The final output map from this analysis is a global 300m grid representing changes
     between two time points (2011 and 2019). The final output contains two layers: the
     first is the transition code indicating the particular transition a pixel made,
     while the second is the coding (natural conversion / not natural conversion).

3. The final conversion of non-native vegetation (non-forest) layer is produced by the
   `natural_conversion.py` script by combining the cropland extents data with the
   ESA-CCI transitions data. There are three different outputs from this process: 1) a layer
   indicating transition types with numeric codes, 2) a layer indicating the area of
   each cell in hectares, and 3) a layer indicating area (in hectares) of non-native
   vegetation conversion (exclusive of deforestation). The layers are produced using the
   following rules:

   | Code | Initial Land Cover Type | ESA CCI Conversion Layer        | Cropland Layer             |
   | ---- | ----------------------- | ------------------------------- | -------------------------- |
   | 1    | Native vegetation       | Native vegetation conversion    | No change or cropland loss |
   | 2    | Native vegetation       | Native vegetation conversion    | Conversion to cropland     |
   | 3    | Native vegetation       | No native vegetation conversion | Conversion to cropland     |
   | 4    | Forest                  | No native vegetation conversion | Conversion to cropland     |
   | 5    | Urban                   | No native vegetation conversion | Conversion to cropland     |
   | 6    | Other                   | No native vegetation conversion | Conversion to cropland     |

   - Note that for the cropland layer the assumption is made that an change in cropland
     extent from a value less than 50% to a value greater than 50% constitutes a
     conversion to cropland within that pixel.

   - The final conversion of non-native vegetation (non-forest) layer (the actual
     indicator), is produced by calculating the area of the first three rows of the
     above table. In other words, the final indicator is the area of those areas that
     were indicated as initially being native vegetation via the CCI, and that then
     experienced change from native vegetation as indicated by either ESA CCI or the
     Potapov croplands layer.

## License

<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative
Commons License" style="border-width:0"
src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br />This work is licensed
under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative
Commons Attribution 4.0 International License</a>.
