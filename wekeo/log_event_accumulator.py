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
        np.round(((lon + 180.0) * (width / 360.0)) % width)
    )
    
    # insert both lat_idx and lon_idx into dataset for easier grouping
    dataset = dataset.assign_coords({
        'lat_idx': (('nb_detection'), lat_idx),
        'lon_idx': (('nb_detection'), lon_idx),
    }) 
    
    # Build the dataset by accumulating each field
    data_vars = {}
    
    for variable in variables:
        
        pass
    
        # Accumulate statistics for this field
        # dataset = _accumulate_field(
            # dataset,
            # variable,
        # )
    
    # Create xarray Dataset
    result_ds = xr.Dataset(
        data_vars,
        coords={
            'latitude': lat_coords,
            'longitude': lon_coords,
        }
    )
    
    return result_ds
