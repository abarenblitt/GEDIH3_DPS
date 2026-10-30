#!/bin/bash
# =============================================================================
# MAAP DPS Execution Script for GEDI Pipeline
# =============================================================================

echo "Starting GEDI Pipeline Setup..."

# 1. Find exactly where this run.sh script lives (the algorithm folder)
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Algorithm code is located at: ${SCRIPT_DIR}"

# 2. We stay in the job's scratch directory ($PWD) to create the output folder
echo "Job working directory is: $PWD"
mkdir -p output

# 3. Install required libraries into the container
echo "Installing dependencies..."
pip install --quiet earthaccess geopandas rioxarray rasterio geocube dask distributed

# 4. Run the Python Pipeline using the explicit path to the script
echo "Executing Python Pipeline..."
python "${SCRIPT_DIR}/gedi_pipeline.py"

echo "Job Complete!"