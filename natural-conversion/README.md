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

2. The ESA CCI contains 36 classes. The map is a land cover - not a land use - product. Therefore assumptions need to be made on which types of transitions are likely to constitute "natural conversion". The Excel file `ESA_CCI_Natural_Conversion_Coding_v2.xlsx` contains the rules used to map transitions to "natural conversion" (coded as a 1). All other transitions are coded as zero. The final output map from this analysis is a global 300m grid representing changes between two time points (2000 and 2015). The final output contains two layers: the first is the transition code indicating the particular transition a pixel made, while the second is the coding (natural conversion / not natural conversion).

3. To catch conversion to cropland that may have been missed by the ESA CCI product, the Potapov et al. map is overlaid on top of the output that comes from analyzing the ESA CCI transitions. The assumption is made that an change in cropland extent from a value less than 50% to a value greater than 50% constitutes a conversion of natural vegetation within that pixel.

## License

<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative
Commons License" style="border-width:0"
src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br />This work is licensed
under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative
Commons Attribution 4.0 International License</a>.
