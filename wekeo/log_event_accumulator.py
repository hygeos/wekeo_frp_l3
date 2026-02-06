import xarray as xr
import numpy as np




def accumulate_events_to_grid(
    dataset: xr.Dataset,
    width: int,
    lat_name: str = "latitude",
    lon_name: str = "longitude",
    min_count: int = 1,
) -> xr.Dataset:
    """
    Accumulate event data (e.g., firepower detections) into a 2D geographic grid
    and compute statistics per grid cell for multiple data fields.
    
    Parameters
    ----------
    dataset : xr.Dataset
        Input dataset containing the event data
    width : int
        Width of the output grid (longitude bins)
    lat_name : str, optional
        Name of the latitude variable in the dataset (default: "latitude")
    lon_name : str, optional
        Name of the longitude variable in the dataset (default: "longitude")
    min_count : int, optional
        Minimum number of observations required per grid cell to compute statistics
    
    Returns
    -------
    xr.Dataset
        Dataset with dimensions (latitude, longitude) containing for each field:
        - {field}_mean: mean value per grid cell
        - {field}_std: standard deviation per grid cell
        - {field}_min: minimum value per grid cell
        - {field}_max: maximum value per grid cell
        - {field}_count: number of observations per grid cell
    """
    
    height = width // 2
    assert width == 2 * height, "Expected width to be 2*height for lat/lon grid"
    
    variables = ["FRP_SWIR", "FRP_MWIR", "FRP_SWIR_no_SAA"]
    assert len(variables) > 0, "Must provide at least one variable to accumulate"
    
    # Extract lat/lon arrays from dataset
    lat = dataset[lat_name].values
    lon = dataset[lon_name].values
    
    # Create coordinate arrays
    lat_coords = np.linspace(-90, 90, height, endpoint=True)   # latitude is not circular
    lon_coords = np.linspace(-180, 180, width, endpoint=False) # longitude is circular
    
    # Convert lat/lon to grid indices (same for all fields)
    lat_idx = np.uint32(
        np.round((lat + 90.0) * ((height - 1) / 180.0))
    )
    
    lon_idx = np.uint32(
        np.round((lon + 180.0) * (width / 360.0)) % width
    )
    
    # Build the dataset by accumulating each field
    data_vars = {}
    
    for variable in variables:
        
        name = variable
        if "no_SAA" in variable:
            name = variable.replace("_no_SAA", "")
            
        # Extract data and filter out NaNs
        data = dataset[name].values
        filt = ~np.isnan(data) & (data > 0)
        
        if "no_SAA" in variable: # only consider values where confidence_SWIR_SAA <= 0 then
            filt &= (dataset["confidence_SWIR_SAA"].values <= 0)
            
        data_valid = data[filt]
        lat_idx_valid = lat_idx[filt]
        lon_idx_valid = lon_idx[filt]
        
        # Initialize accumulators
        sum_grid = np.zeros((height, width), dtype=np.float64)
        sum_sq_grid = np.zeros((height, width), dtype=np.float64)
        count_grid = np.zeros((height, width), dtype=np.int32)
        
        # Accumulate using np.add.at (fast)
        np.add.at(sum_grid, (lat_idx_valid, lon_idx_valid), data_valid)
        np.add.at(sum_sq_grid, (lat_idx_valid, lon_idx_valid), data_valid**2)
        np.add.at(count_grid, (lat_idx_valid, lon_idx_valid), 1)

        # select min/max raster by raster 
        # Initialize with appropriate values
        min_grid = np.full((height, width), np.inf, dtype=np.float32)
        max_grid = np.full((height, width), -np.inf, dtype=np.float32)

        # Accumulate min/max values directly
        np.minimum.at(min_grid, (lat_idx_valid, lon_idx_valid), data_valid)
        np.maximum.at(max_grid, (lat_idx_valid, lon_idx_valid), data_valid)
        
        # Compute mean and std
        mask = count_grid > 0
        mean_grid = np.full((height, width), np.nan, dtype=np.float32)
        std_grid = np.full((height, width), np.nan, dtype=np.float32)
        mean_grid[mask] = (sum_grid[mask] / count_grid[mask]).astype(np.float32)
        
        # std = sqrt(E[X^2] - E[X]^2)
        mean_sq = sum_sq_grid[mask] / count_grid[mask]
        std_grid[mask] = np.sqrt(np.maximum(0, mean_sq - mean_grid[mask]**2)).astype(np.float32)
        
        # Filter cells with insufficient data
        insufficient_data_mask = count_grid < min_count
        mean_grid[insufficient_data_mask] = np.nan
        std_grid[insufficient_data_mask] = np.nan
        min_grid[insufficient_data_mask] = np.nan
        max_grid[insufficient_data_mask] = np.nan
        count_grid[insufficient_data_mask] = -1
        
        # remove infs from min/max grids
        min_grid[np.isinf(min_grid)] = np.nan
        max_grid[np.isinf(max_grid)] = np.nan
        
        # Add to data_vars dictionary
        data_vars[f'{variable}_mean'] = (('latitude', 'longitude'), mean_grid)
        data_vars[f'{variable}_std'] = (('latitude', 'longitude'), std_grid)
        data_vars[f'{variable}_count'] = (('latitude', 'longitude'), count_grid)
        data_vars[f'{variable}_min'] = (('latitude', 'longitude'), min_grid)
        data_vars[f'{variable}_max'] = (('latitude', 'longitude'), max_grid)
    
    # Create xarray Dataset
    result_ds = xr.Dataset(
        data_vars,
        coords={
            'latitude': lat_coords,
            'longitude': lon_coords,
        },
        attrs={
            'description': 'Accumulated FRP event data on an equirectangular grid',
            'grid_width': width,
            'grid_height': height,
            'date': dataset.attrs.get('date', 'unknown'),
            'source_files': "\n".join(dataset.attrs.get('source_files', [])),
        }
    )
    
    return result_ds
