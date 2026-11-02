#!/bin/bash
echo "Starting GEDI Pipeline Setup..."

# 1. Catch positional arguments passed by MAAP
export EARTHDATA_USERNAME=$1
export EARTHDATA_PASSWORD=$2
START_DATE=$3
END_DATE=$4
BBOX=$5

echo "Job working directory is: $PWD"
mkdir -p output

PYTHON_SCRIPT=$(find /app -name "gedi_pipeline.py" | head -n 1)
echo "Found Python script at: $PYTHON_SCRIPT"

echo "Installing standard dependencies..."
pip install --quiet earthaccess geopandas rioxarray rasterio geocube dask distributed pyarrow fastparquet

echo "Installing GEDI-H3 library from GitHub zip..."
pip install --quiet https://github.com/tiagodc/GEDI-H3/archive/refs/heads/main.zip

echo "Executing Python Pipeline..."
# Pass the variables to Python
python "$PYTHON_SCRIPT" --start "$START_DATE" --end "$END_DATE" --bbox "$BBOX"

echo "Job Complete!"