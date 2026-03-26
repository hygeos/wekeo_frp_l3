from wekeo_frp_l3 import config
from wekeo_frp_l3.download import get_FRP_products
from wekeo_frp_l3.log_event_accumulator import accumulate_events_to_grid
from wekeo_frp_l3.reader import read_FRP_product
import xarray as xr

from wekeo_frp_l3.hygeos_core import log
from wekeo_frp_l3.hygeos_core import env
from datetime import date
    
def  get_log_event(day: date, save_result: bool = False, use_cache: bool = False) -> xr.Dataset:
    """Download and Compile multiple FRP files into a single xarray Dataset."""
    l3_output_file = config.log_event_dir / f"FRP_SLSTR_log_event_{day.strftime('%Y_%m_%d')}.nc"
    
    if l3_output_file.exists() and use_cache:
        log.info(f"FRP log event dataset already exists for date {day}, loading from {l3_output_file}")
        return xr.open_dataset(l3_output_file)
    
    log.info(f"Downloading FRP products for date: {day}")
    
    eday = day.strftime("%Y-%m-%d")
    sday = day.strftime("%Y-%m-%d") # same day for single day query

    files = get_FRP_products(
        start_date = sday,
        end_date = eday,
    )

    # Read specific variables
    variables = [
        'latitude', 
        'longitude',
        'FRP_SWIR',
        'FRP_uncertainty_SWIR',
        'FRP_MWIR', 
        'FRP_uncertainty_MWIR',
        'confidence_SWIR_SAA',  # SAA flag for quality control
        'solar_zenith',
        # 'S8_Fire_pixel_BT',  # This is from a different dimension
    ]
    
    log.info(f"Compiling {len(files)} FRP products into a Log Event dataset")
    datasets = []
    
    for f in files:
        ds = read_FRP_product(f, variables=variables).compute()
        datasets.append(ds)
        
    log_event = xr.concat(datasets, dim='merged_MWIR1kmStandard_SWIR1km')
    log_event = log_event.rename_dims({"merged_MWIR1kmStandard_SWIR1km": "nb_detection"})
    log_event = log_event.rename_vars({"solar_zenith": "sza"})
    
    log_event.attrs['source_files'] = [str(f.name) for f in files]
    log_event.attrs['date'] = eday
    
    if save_result:
        log.info(f"Saving FRP log event dataset to {l3_output_file}")
        log_event.to_netcdf(l3_output_file, mode="w")
        
    return log_event.compute()
    

def grid_log_event(
    log_event: xr.Dataset,
    width: int = 3272,     
    lat_name: str = "latitude",
    lon_name: str = "longitude",
    min_count: int = 1,
    save_result: bool = False,
    use_cache: bool = False
) -> xr.Dataset:
    """
    Accumulate the Log Event dataset into a gridded level 3 dataset.
    """
    day = log_event.attrs['date']
    gridded_l3_output_file = config.gridded_log_event_dir / f"FRP_SLSTR_grid{width}_{day}.nc"
    
    if gridded_l3_output_file.exists() and use_cache:
        log.info(f"Gridded FRP dataset already exists for date {day}, grid {width}, loading from {gridded_l3_output_file}")
        return xr.open_dataset(gridded_l3_output_file)

    ds = accumulate_events_to_grid(
        log_event, 
        width=width, 
        lat_name=lat_name, 
        lon_name=lon_name, 
        min_count=min_count
    ) # gridded level3
    
    if save_result:
        log.info(f"Saving gridded FRP dataset to {gridded_l3_output_file}")
        ds.to_netcdf(gridded_l3_output_file, mode="w")
    
    return ds