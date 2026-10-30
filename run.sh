#!/bin/bash
# =============================================================================
# MAAP DPS Execution Script for GEDI Pipeline
# =============================================================================

echo "Starting GEDI Pipeline Setup..."

# 1. DPS runs in a specific workspace. Ensure we are in the right directory.
cd $PWD

# 2. Create the standard MAAP output directory
# MAAP automatically preserves EVERYTHING in this folder when the job finishes.
mkdir -p output

# 3. Install required libraries into the container
# (Assuming the base container is a standard python/conda environment)
echo "Installing dependencies..."
pip install --quiet earthaccess geopandas rioxarray rasterio geocube dask distributed
# Make sure gedih3 is installed from its specific source if it's not in standard pip
# pip install git+https://github.com/tiagodc/GEDI-H3.git (if needed)

# 4. Run the Python Pipeline
echo "Executing Python Pipeline..."
python gedi_pipeline.py

echo "Job Complete! Check the output/ directory for results."