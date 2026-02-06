from pathlib import Path
from pprint import pprint
import xarray as xr
import numpy as np
from typing import List, Optional, Union


"""
Sentinel-3 SLSTR L2 FRP Product Reader

This module provides readers for Sentinel-3 SLSTR Fire Radiative Power (FRP) products.
It supports multiple product format versions with automatic detection:

- v2 format (2022 and earlier): Single FRP_in.nc file structure
- v3 format (2024+): Multiple specialized NetCDF files (merged, standard, alternative)

The main function read_FRP_product() automatically detects the format version and
dispatches to the appropriate sub-reader.
"""


def read_FRP_product(
    product_path: Union[str, Path],
    variables: Optional[List[str]] = None,
    decode_times: bool = True,
    chunks: dict = None,
) -> xr.Dataset:
    """
    Read Sentinel-3 SLSTR L2 FRP product into an xarray Dataset.
    
    This reader automatically detects the product version and uses the appropriate
    sub-reader for different FRP product formats (v2 for 2022 and earlier, v3 for 2024+).
    
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
        >>> ds = read_FRP_product(
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
    
    # Detect product version by checking which files exist
    has_merged_file = (product_path / 'FRP_Merged_MWIR1kmStandard_SWIR1km.nc').exists()
    has_frp_in_file = (product_path / 'FRP_in.nc').exists()
    
    if has_merged_file:
        # Newer format
        return _read_FRP_product_v3(product_path, variables, decode_times, chunks)
    elif has_frp_in_file:
        # Older format
        return _read_FRP_product_v2(product_path, variables, decode_times, chunks)
    else:
        raise ValueError(
            f"Unknown FRP product format in {product_path}. "
            "Could not find 'FRP_Merged_MWIR1kmStandard_SWIR1km.nc' (v3) or 'FRP_in.nc' (v2)."
        )


def _read_FRP_product_v2(
    product_path: Path,
    variables: Optional[List[str]] = None,
    decode_times: bool = True,
    chunks: dict = None,
) -> xr.Dataset:
    """
    Read older format (v2) Sentinel-3 SLSTR L2 FRP product (2022 and earlier).
    
    This format uses a single FRP_in.nc file with dimensions:
    - fires: main dimension for standard MWIR/SWIR detections
    - fires_MWIR_alternative: alternative MWIR detections
    - fires_SWIR_500m: high-resolution SWIR detections
    
    Args:
        product_path: Path to the .SEN3 product directory (already validated)
        variables: List of variables to read
        decode_times: Whether to decode time coordinates
        chunks: Chunking specification for dask arrays
        
    Returns:
        xarray.Dataset: Dataset with requested FRP variables
    """
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
            'confidence_SWIR_SAA',
            'solar_zenith',
        ]
    
    # Main FRP file
    frp_file = product_path / 'FRP_in.nc'
    
    if not frp_file.exists():
        raise FileNotFoundError(f"FRP file does not exist: {frp_file}")
    
    try:
        # Open the main FRP dataset
        ds = xr.open_dataset(frp_file, decode_times=decode_times, chunks=chunks)
        
        # Filter to requested variables if specified
        if variables:
            available_vars = [v for v in variables if v in ds.variables]
            if available_vars:
                # Keep dimension coordinates
                coords_to_keep = ['fires']
                if 'fires_MWIR_alternative' in ds.dims:
                    coords_to_keep.append('fires_MWIR_alternative')
                if 'fires_SWIR_500m' in ds.dims:
                    coords_to_keep.append('fires_SWIR_500m')
                
                vars_to_keep = list(set(available_vars + coords_to_keep))
                ds = ds[vars_to_keep]
            else:
                print(f"Warning: None of the requested variables found in {frp_file}")
        
        # Rename dimensions to match v3 format (2024) for consistency
        # This conforms the older v2 format to the newer, more structured v3 naming
        dim_mapping = {
            'fires': 'merged_MWIR1kmStandard_SWIR1km',  # Main merged detections
            'fires_MWIR_alternative': 'fires_MWIR1km_alternative',  # Alternative MWIR
            'fires_SWIR_500m': 'fires_SWIR500m'  # High-res SWIR
        }
        ds = ds.rename({k: v for k, v in dim_mapping.items() if k in ds.dims})
        
        # Add global attributes
        ds.attrs.update({
            'product_path': str(product_path),
            'product_type': 'Sentinel-3 SLSTR L2 FRP',
            'product_format_version': 'v2',
            'reader_version': '1.0',
            'description': 'Fire Radiative Power product from SLSTR instrument (v2 format, conformed to v3 naming)'
        })
        
        return ds
        
    except Exception as e:
        raise RuntimeError(f"Error reading FRP product v2 from {frp_file}: {e}")


def _read_FRP_product_v3(
    product_path: Path,
    variables: Optional[List[str]] = None,
    decode_times: bool = True,
    chunks: dict = None,
) -> xr.Dataset:
    """
    Read newer format (v3) Sentinel-3 SLSTR L2 FRP product (2024+).
    
    This format uses multiple NetCDF files:
    - FRP_Merged_MWIR1kmStandard_SWIR1km.nc: merged MWIR/SWIR detections
    - FRP_MWIR1km_standard.nc: standard MWIR detections
    - FRP_MWIR1km_alternative.nc: alternative MWIR detections
    - FRP_SWIR500m.nc: high-resolution SWIR detections
    - geodetic_in.nc: grid coordinates
    
    Args:
        product_path: Path to the .SEN3 product directory (already validated)
        variables: List of variables to read
        decode_times: Whether to decode time coordinates
        chunks: Chunking specification for dask arrays
        
    Returns:
        xarray.Dataset: Dataset with requested FRP variables
    """
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
        
        # try:
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
        
        # except Exception as e:
        #     print(f"Error reading {nc_file}: {e}")
        #     continue
    
    if not datasets:
        raise ValueError("No valid datasets could be read")
    
    # Combine datasets
    # Note: Different dimension systems will remain separate in the output
    if len(datasets) == 1:
        combined_ds = datasets[0]
    else:
        # Merge datasets - variables with different dimensions will coexist
        combined_ds = xr.merge(datasets, compat='override', join='outer')
    
    # Keep v3 dimension names as they are (the standard going forward)
    # v2 files will be conformed to match these names
    
    # Add dimension coordinates if not present
    # This allows for easier concatenation and indexing
    if 'merged_MWIR1kmStandard_SWIR1km' in combined_ds.dims and 'merged_MWIR1kmStandard_SWIR1km' not in combined_ds.coords:
        combined_ds = combined_ds.assign_coords(merged_MWIR1kmStandard_SWIR1km=np.arange(combined_ds.sizes['merged_MWIR1kmStandard_SWIR1km']))
    if 'fires_MWIR1km_standard' in combined_ds.dims and 'fires_MWIR1km_standard' not in combined_ds.coords:
        combined_ds = combined_ds.assign_coords(fires_MWIR1km_standard=np.arange(combined_ds.sizes['fires_MWIR1km_standard']))
    if 'fires_MWIR1km_alternative' in combined_ds.dims and 'fires_MWIR1km_alternative' not in combined_ds.coords:
        combined_ds = combined_ds.assign_coords(fires_MWIR1km_alternative=np.arange(combined_ds.sizes['fires_MWIR1km_alternative']))
    if 'fires_SWIR500m' in combined_ds.dims and 'fires_SWIR500m' not in combined_ds.coords:
        combined_ds = combined_ds.assign_coords(fires_SWIR500m=np.arange(combined_ds.sizes['fires_SWIR500m']))
    
    # Add global attributes
    combined_ds.attrs.update({
        'product_path': str(product_path),
        'product_type': 'Sentinel-3 SLSTR L2 FRP',
        'product_format_version': 'v3',
        'reader_version': '1.0',
        'description': 'Fire Radiative Power product from SLSTR instrument (v3 format)'
    })
    
    return combined_ds