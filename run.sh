#!/bin/bash
echo "Starting GEDI Pipeline Setup..."

echo "Job working directory is: $PWD"
mkdir -p output

PYTHON_SCRIPT=$(find /app -name "gedi_pipeline.py" | head -n 1)
echo "Found Python script at: $PYTHON_SCRIPT"

echo "Installing standard dependencies..."
# Removed --quiet so we can actually see errors if they happen!
pip install earthaccess geopandas rioxarray rasterio geocube dask distributed pyarrow fastparquet

echo "Installing GEDI-H3 library from GitHub zip..."
# Bypasses the need for 'git' to be installed on the container
pip install https://github.com/tiagodc/GEDI-H3/archive/refs/heads/main.zip

echo "Executing Python Pipeline..."
python "$PYTHON_SCRIPT"

echo "Job Complete!"