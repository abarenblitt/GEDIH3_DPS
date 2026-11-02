#!/usr/bin/env python3
import os
import shutil
import earthaccess
import subprocess
import argparse
from pathlib import Path

from dask.distributed import Client
import dask.dataframe as dd
import geopandas as gpd

import gedih3.gh3driver as gh3
from gedih3.gh3builder import build_h3db
from gedih3.gedidriver import gedi_vars_expand

try:
    from geocube.api.core import make_geocube
except ImportError:
    print("Installing 'geocube'...")
    subprocess.check_call(["pip", "install", "geocube", "rioxarray", "rasterio"])
    from geocube.api.core import make_geocube

def main():
    # =============================================================================
    # 1. Parse Dynamic Arguments
    # =============================================================================
    parser = argparse.ArgumentParser(description="GEDI Pipeline")
    parser.add_argument('--start', type=str, required=True, help="Start Date (YYYY-MM-DD)")
    parser.add_argument('--end', type=str, required=True, help="End Date (YYYY-MM-DD)")
    parser.add_argument('--bbox', type=str, required=True, help="Comma-separated Bounding Box: min_lon,min_lat,max_lon,max_lat")
    args = parser.parse_args()

    DATE_START = args.start
    DATE_END = args.end
    # Convert "w,s,e,n" string into a list of floats
    STUDY_AREA = [float(x.strip()) for x in args.bbox.split(',')]

    print(f"--- DYNAMIC JOB PARAMS ---")
    print(f"Dates: {DATE_START} to {DATE_END}")
    print(f"BBox: {STUDY_AREA}")
    print(f"--------------------------")

    # =============================================================================
    # 2. Configuration & Speed Tuning
    # =============================================================================
    BATCH_SIZE = 6          
    DOWNLOAD_WORKERS = 2    
    DASK_WORKERS = 4        

    BASE_DIR = Path.cwd() / "gedih3_run"
    SOC_DIR = BASE_DIR / "soc_data"
    H3_DIR = BASE_DIR / "h3_database"
    OUTPUT_DIR = Path.cwd() / "output"

    if H3_DIR.exists():
        shutil.rmtree(H3_DIR)

    for d in [SOC_DIR, H3_DIR, OUTPUT_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    product_vars = gedi_vars_expand({'L2A': ['default', 'rh']})

    # =============================================================================
    # 3. Authenticate and Search
    # =============================================================================
    print("Authenticating with Earthdata...")
    auth = earthaccess.login()

    results = earthaccess.search_data(
        short_name='GEDI02_A', version='003',
        bounding_box=(STUDY_AREA[0], STUDY_AREA[1], STUDY_AREA[2], STUDY_AREA[3]),
        temporal=(DATE_START, DATE_END)
    )
    print(f"Found {len(results)} granules to process.")

    # =============================================================================
    # 4. The Optimized BATCHED Loop
    # =============================================================================
    if len(results) > 0:
        batches = [results[i:i + BATCH_SIZE] for i in range(0, len(results), BATCH_SIZE)]
        with Client(n_workers=DASK_WORKERS, threads_per_worker=2, memory_limit='auto', local_directory=str(BASE_DIR)) as client:
            
            for batch_idx, batch in enumerate(batches):
                print(f"\n=== Processing Batch {batch_idx + 1} of {len(batches)} ===")
                
                download_paths = []
                for granule in batch:
                    granule_name = granule['umm']['DataGranule']['ArchiveAndDistributionInformation'][0]['Name']
                    time_str = granule_name.split('_')[2] 
                    granule_dir = SOC_DIR / time_str[0:4] / time_str[4:7]
                    granule_dir.mkdir(parents=True, exist_ok=True)
                    target_path = granule_dir / granule_name
                    download_paths.append((granule, target_path, granule_dir))

                to_download = [g for g, p, _ in download_paths if not p.exists()]
                if to_download:
                    earthaccess.download(to_download, local_path=str(SOC_DIR), threads=DOWNLOAD_WORKERS)
                    for g, p, g_dir in download_paths:
                        root_path = SOC_DIR / p.name
                        if root_path.exists() and not p.exists():
                            shutil.move(str(root_path), str(p))

                valid_files = 0
                for _, p, _ in download_paths:
                    if p.exists():
                        size_mb = os.path.getsize(p) / (1024 * 1024)
                        if size_mb < 500:
                            os.remove(p)
                        else:
                            valid_files += 1

                if valid_files > 0:
                    try:
                        build_h3db(
                            product_vars=product_vars, res=12, part=3,
                            spatial=STUDY_AREA, soc_source=str(SOC_DIR), h3_dir=str(H3_DIR)
                        )
                    except Exception as e:
                        print(f"   -> DB Build Error on Batch {batch_idx+1}: {e}")

                for _, p, _ in download_paths:
                    if p.exists():
                        os.remove(p)

        # =============================================================================
        # 5. GeoCube Direct Rasterization (2km)
        # =============================================================================
        print("\nAll batches processed! Loading database to aggregate...")
        parquet_files = [str(p) for p in H3_DIR.rglob("*.parquet")]

        if len(parquet_files) > 0:
            with Client(n_workers=DASK_WORKERS, threads_per_worker=2, memory_limit='auto', local_directory=str(BASE_DIR)) as client:
                print("Loading files directly into Dask...")
                ddf = dd.read_parquet(parquet_files)
                
                rh_cols = [c for c in ddf.columns if 'rh' in c and '098' in c]
                target_col = rh_cols[0] if rh_cols else [c for c in ddf.columns if 'rh' in c][0]
                qual_cols = [c for c in ddf.columns if 'quality' in c.lower() and 'flag' in c.lower()]
                
                if qual_cols:
                    ddf = ddf[ddf[qual_cols[0]] == 1]
                    
                print("Aggregating to native EGI grid Level 6 (~1km)...")
                egi_agg = gh3.egi_aggregate(
                    ddf, target_level=6, agg='mean', columns=[target_col], add_geometry=True
                )
                egi_gdf = egi_agg.compute()
                
                if len(egi_gdf) > 0:
                    var_name = f"{target_col}_mean" if f"{target_col}_mean" in egi_gdf.columns else target_col
                    if egi_gdf.crs is None:
                        egi_gdf.set_crs("EPSG:6933", inplace=True)
                        
                    print("Reprojecting vector data directly to EPSG:5070...")
                    egi_gdf_5070 = egi_gdf.to_crs("EPSG:5070")
                    
                    print("Rasterizing directly to a clean 2km grid...")
                    cube = make_geocube(
                        vector_data=egi_gdf_5070,
                        measurements=[var_name],
                        resolution=(-2000, 2000)
                    )
                    
                    # Dynamically name the output file based on the run dates!
                    output_tif = OUTPUT_DIR / f"rh98_map_{DATE_START}_{DATE_END}.tif"
                    cube[var_name].rio.to_raster(str(output_tif), compress='LZW')
                    
                    print(f"✅ FINAL SUCCESS! Complete map saved to: {output_tif}")
                else:
                    print("❌ ERROR: The dataframe is empty.")
        else:
            print("❌ ERROR: No database Parquet files found.")
    else:
        print("No granules found for those dates.")

if __name__ == "__main__":
    main()