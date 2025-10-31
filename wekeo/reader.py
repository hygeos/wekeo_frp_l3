from pathlib import Path
from pprint import pprint
import xarray as xr
import numpy as np
from typing import List, Optional, Union


def read_FRP_product(
    product_path: Union[str, Path],
    variables: Optional[List[str]] = None,
    decode_times: bool = True,
    chunks: dict = None,
) -> xr.Dataset:
    """
    Read Sentinel-3 SLSTR L2 FRP product into an xarray Dataset.
    
    This reader specifically handles the FRP (Fire Radiative Power) product structure
    with its multiple NetCDF files and different dimension systems.
    
    Args:
        product_path: Path to the .SEN3 product directory
        variables: List of variables to read. If None, reads all standard FRP variables.
                  Available variables:
                  - latitude, longitude
                  - FRP_SWIR, FRP_uncertainty_SWIR
                  - FRP_MWIR, FRP_uncertainty_MWIR
                  - solar_zenith, solar_azimuth
                  - sat_zenith, sat_azimuth
                  - time
                  - S8_Fire_pixel_BT (from standard MWIR)
                  - transmittance_SWIR, transmittance_MWIR
                  - confidence_* variables
                  - and more...
        decode_times: Whether to decode time coordinates
        chunks: Chunking specification for dask arrays
        
    Returns:
        xarray.Dataset: Dataset with requested FRP variables
        
    Example:
        >>> ds = read_slstr_frp(
        ...     "S3A_SL_2_FRP____20250714T144720.SEN3",
        ...     variables=['latitude', 'longitude', 'FRP_SWIR', 'FRP_MWIR']
        ... )
    """
    # path adjustments
    product_path = Path(product_path)
    
    sub = product_path / product_path.name
    if sub.exists():
        product_path = sub
    
    if not product_path.exists():
        raise FileNotFoundError(f"Product path does not exist: {product_path}")
    
    # Define default variables if none specified
    if variables is None:
        variables = [
            'latitude', 
            'longitude', 
            'time',
            'FRP_SWIR', 
            'FRP_uncertainty_SWIR',
            'FRP_MWIR', 
            'FRP_uncertainty_MWIR',
            'confidence_SWIR_SAA',  # SAA flag for quality control
            'solar_zenith',
        ]
    
    # Mapping of variables to their source files and dimensions
    variable_map = {
        # Merged MWIR/SWIR 1km variables (main dataset)
        'merged': {
            'file': 'FRP_Merged_MWIR1kmStandard_SWIR1km.nc',
            'dimension': 'merged_MWIR1kmStandard_SWIR1km',
            'variables': [
                'latitude',
                'longitude', 
                'time', 
                'i', 
                'j',
                'FRP_SWIR',
                'FRP_uncertainty_SWIR',
                'FRP_MWIR',
                'FRP_uncertainty_MWIR',
                'confidence_SWIR_SAA',
                'confidence_MWIR',
                'transmittance_SWIR',
                'transmittance_MWIR',
                'n_SWIR_fire',
                'used_channel',
                'confidence_Rel_BT_MWIR_spectral',
                'confidence_clear_sky_freezing',
                'confidence_clear_sky_split_window',
                'solar_zenith',
                'solar_azimuth',
                'sat_zenith',
                'sat_azimuth',
            ]
        },
        # Standard MWIR 1km variables
        'mwir_standard': {
            'file': 'FRP_MWIR1km_standard.nc',
            'dimension': 'fires_MWIR1km_standard',
            'variables': [
                'S8_Fire_pixel_BT', 
                'MWIR_Fire_pixel_BT',
                'MWIR_Fire_pixel_radiance', 
                'Radiance_window',
                'classification', 
                'Glint_angle', 
                'IFOV_area',
                'TCWV', 
                'n_window', 
                'n_water', 
                'n_cloud',
                'eff_across_track_pixel_size', 
                'eff_along_track_pixel_size',
                'convert_f3',
                'confidence_Abs_BT_MWIR', 
                'confidence_Rel_BT_MWIR_spatial',
                'confidence_ncloud_vicinity', 
                'confidence_nwater_vicinity',
            ]
        },
        # Alternative MWIR 1km variables
        'mwir_alternative': {
            'file': 'FRP_MWIR1km_alternative.nc',
            'dimension': 'fires_MWIR1km_alternative',
            'variables': [
                # Similar to standard but for alternative detections
            ]
        },
        # SWIR 500m variables
        'swir_500m': {
            'file': 'FRP_SWIR500m.nc',
            'dimension': 'fires_SWIR500m',
            'variables': [
                'time_SWIR_500m', 'latitude_SWIR_500m', 'longitude_SWIR_500m',
                'FRP_SWIR_500m', 'FRP_uncertainty_SWIR_500m',
                'S6_Fire_pixel_radiance', 'confidence_SWIR_SAA_500m',
                'transmittance_SWIR_500m', 'IFOV_area_500m',
                'eff_across_track_pixel_size_SWIR_500m',
                'eff_along_track_pixel_size_SWIR_500m',
            ]
        },
        # Geodetic coordinates (grid)
        'geodetic': {
            'file': 'geodetic_in.nc',
            'dimension': ('rows', 'columns'),
            'variables': ['latitude_in', 'longitude_in']
        }
    }
    
    datasets = []
    
    # Determine which files to read based on requested variables
    files_to_read = set()
    for var in variables:
        found = False
        for group_name, group_info in variable_map.items():
            if var in group_info['variables']:
                files_to_read.add(group_name)
                found = True
                break
        if not found and var != 'all':
            print(f"Warning: Variable '{var}' not found in known variables")
    
    # If no specific variables found, default to merged dataset
    if not files_to_read:
        files_to_read = {'merged'}
    
    # Read requested datasets
    for group_name in files_to_read:
        group_info = variable_map[group_name]
        nc_file = product_path / group_info['file']
        
        if not nc_file.exists():
            print(f"Warning: File {group_info['file']} not found, skipping")
            continue
        
        try:
            ds = xr.open_dataset(nc_file, decode_times=decode_times, chunks=chunks)
            
            # Filter to requested variables (if they exist in this file)
            available_vars = [v for v in variables if v in ds.variables]
            if available_vars:
                # Keep the dimension coordinate plus requested variables
                dim_name = group_info['dimension']
                if isinstance(dim_name, tuple):
                    coords_to_keep = list(dim_name)
                else:
                    coords_to_keep = [dim_name] if dim_name in ds.coords else []
                
                vars_to_keep = list(set(available_vars + coords_to_keep))
                ds = ds[vars_to_keep]
                
                # Add metadata
                ds.attrs['source_file'] = group_info['file']
                ds.attrs['dimension'] = str(dim_name)
                
                datasets.append(ds)
        
        except Exception as e:
            print(f"Error reading {nc_file}: {e}")
            continue
    
    if not datasets:
        raise ValueError("No valid datasets could be read")
    
    # Combine datasets
    # Note: Different dimension systems will remain separate in the output
    if len(datasets) == 1:
        combined_ds = datasets[0]
    else:
        # Merge datasets - variables with different dimensions will coexist
        combined_ds = xr.merge(datasets, compat='override', join='outer')
    
    # Add global attributes
    combined_ds.attrs.update({
        'product_path': str(product_path),
        'product_type': 'Sentinel-3 SLSTR L2 FRP',
        'reader_version': '1.0',
        'description': 'Fire Radiative Power product from SLSTR instrument'
    })
    
    return combined_ds