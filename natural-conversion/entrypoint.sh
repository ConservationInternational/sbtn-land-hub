#!/bin/bash

# nohup dask-scheduler --host localhost --port 8786 --dashboard-address 8787 &
# nohup dask-worker --host localhost --dashboard-address 8786 --nprocs 8 --nthreads 1 &

set -e

case "$1" in
    cropland_match)
        echo "Resampling cropland to match ESA CCI"
        exec python cropland_match_to_esa.py "${@:2}"
		;;
    esa_cci)
        echo "Starting esa_cci_transitions calculations"
        exec python esa_cci_transitions.py "${@:2}"
		;;
    natural_conversion)
        echo "Starting natural_conversion calculations"
        exec python natural_conversion.py "${@:2}"
		;;
    *)
        exec "$@"
esac
