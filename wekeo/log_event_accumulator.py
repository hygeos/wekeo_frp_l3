import xarray as xr
import numpy as np




def accumulate_events_to_grid(
    dataset: xr.Dataset,
    variables: list[str],
    width: int,
    height: int,
    lat_name: str = "latitude",
    lon_name: str = "longitude"
) -> xr.Dataset:
    """
    Accumulate event data (e.g., firepower detections) into a 2D geographic grid
    and compute statistics per grid cell for multiple data fields.
    
    Parameters
    ----------
    dataset : xr.Dataset
        Input dataset containing the event data
    variables : list[str]
        List of variable names to accumulate from the dataset
    width : int
        Width of the output grid (longitude bins)
    height : int
        Height of the output grid (latitude bins)
    lat_name : str, optional
        Name of the latitude variable in the dataset (default: "latitude")
    lon_name : str, optional
        Name of the longitude variable in the dataset (default: "longitude")
    
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
    
    assert width == 2 * height, "Expected width to be 2*height for lat/lon grid"
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
        np.round(((lon + 180.0) * ((width - 1) / 360.0)) % width)
    )
    
    # insert both lat_idx and lon_idx into dataset for easier grouping
    dataset = dataset.assign_coords({
        'lat_idx': (('nb_detection'), lat_idx),
        'lon_idx': (('nb_detection'), lon_idx),
    })
    
    # Create a multi-index for grouping by both lat and lon indices (do this once)
    # dataset = dataset.assign({'grid_cell': (('nb_detection',), lat_idx * width + lon_idx)})
    
    # Build the dataset by accumulating each field
    data_vars = {}
    
    for variable in variables:
        # Group by grid cell and compute statistics
        grouped = dataset[variable].groupby(["lat_idx", "lon_idx"])
        
        # Compute statistics
        mean_vals = grouped.mean()
        std_vals = grouped.std()
        min_vals = grouped.min()
        max_vals = grouped.max()
        count_vals = grouped.count()
        
        # Create 2D grids initialized with NaN
        mean_grid = np.full((height, width), np.nan, dtype=np.float32)
        std_grid = np.full((height, width), np.nan, dtype=np.float32)
        min_grid = np.full((height, width), np.nan, dtype=np.float32)
        max_grid = np.full((height, width), np.nan, dtype=np.float32)
        count_grid = np.zeros((height, width), dtype=np.uint32)
        
        # Fill the grids using lat_idx and lon_idx
        for lat_i in mean_vals.lat_idx.values:
            for lon_i in mean_vals.lon_idx.values:
                try:
                    mean_grid[lat_i, lon_i] = mean_vals.sel(lat_idx=lat_i, lon_idx=lon_i).values
                    std_grid[lat_i, lon_i] = std_vals.sel(lat_idx=lat_i, lon_idx=lon_i).values
                    min_grid[lat_i, lon_i] = min_vals.sel(lat_idx=lat_i, lon_idx=lon_i).values
                    max_grid[lat_i, lon_i] = max_vals.sel(lat_idx=lat_i, lon_idx=lon_i).values
                    count_grid[lat_i, lon_i] = count_vals.sel(lat_idx=lat_i, lon_idx=lon_i).values
                except KeyError:
                    # No data for this combination
                    pass
        
        # Add to data_vars dictionary
        data_vars[f'{variable}_mean'] = (('latitude', 'longitude'), mean_grid)
        data_vars[f'{variable}_std'] = (('latitude', 'longitude'), std_grid)
        data_vars[f'{variable}_min'] = (('latitude', 'longitude'), min_grid)
        data_vars[f'{variable}_max'] = (('latitude', 'longitude'), max_grid)
        data_vars[f'{variable}_count'] = (('latitude', 'longitude'), count_grid)
    
    # Create xarray Dataset
    result_ds = xr.Dataset(
        data_vars,
        coords={
            'latitude': lat_coords,
            'longitude': lon_coords,
        }
    )
    
    return result_ds
