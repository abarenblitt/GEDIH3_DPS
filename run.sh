#!/bin/bash
echo "Starting GEDI Pipeline Setup..."

# Capture the credentials passed by MAAP and set them as environment variables
export EARTHDATA_USERNAME=$1
export EARTHDATA_PASSWORD=$2

echo "Job working directory is: $PWD"
mkdir -p output

PYTHON_SCRIPT=$(find /app -name "gedi_pipeline.py" | head -n 1)
echo "Found Python script at: $PYTHON_SCRIPT"

echo "Installing standard dependencies..."
pip install earthaccess geopandas rioxarray rasterio geocube dask distributed pyarrow fastparquet

echo "Installing GEDI-H3 library from GitHub zip..."
pip install https://github.com/tiagodc/GEDI-H3/archive/refs/heads/main.zip

echo "Executing Python Pipeline..."
python "$PYTHON_SCRIPT"

echo "Job Complete!"