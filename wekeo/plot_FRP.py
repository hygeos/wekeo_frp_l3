import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
from matplotlib.colors import LogNorm
from pathlib import Path

from wekeo.reader import read_FRP_product

# ===== CONFIGURATION PARAMETERS =====
BACKGROUND_COLOR = 'black'
FOREGROUND_COLOR = 'white'
LAND_COLOR = '#99AD8D'
OCEAN_COLOR = '#8795CC'
COASTLINE_COLOR = 'black'
BORDERS_COLOR = 'black'
STATES_COLOR = 'black'
GRIDLINES_COLOR = 'black'
USE_LOG_SCALE = True

VARIABLES = [
    'latitude', 
    'longitude',
    'FRP_SWIR',
    'FRP_uncertainty_SWIR',
    'FRP_MWIR', 
    'FRP_uncertainty_MWIR',
    'confidence_SWIR_SAA',
    'solar_zenith',
]
# ====================================

def expand(coord: float, direction: str, amount: float):
    """
    Expand or contract a geographic coordinate.
    Parameters:
    -----------
    coord : float
        The original coordinate (longitude or latitude)
    direction : str
        Direction to expand/contract ('west', 'east', 'south', 'north')
    amount : float
        Amount to expand/contract the coordinate
    Returns:
    --------
    float
        The modified coordinate
    """
    
    if direction == 'west' or direction == 'south':
        coord -= amount
    elif direction == 'east' or direction == 'north':
        coord += amount

    if direction == 'west' and coord < -180:
        coord = -180
    elif direction == 'east' and coord > 180:
        coord = 180
    elif direction == 'south' and coord < -90:
        coord = -90
    elif direction == 'north' and coord > 90:
        coord = 90
    
    return coord

def _read_and_combine_FRP_data(inputs: list[Path], field: str = 'FRP_MWIR'):
    """
    Core routine to read and combine FRP data from multiple files.
    
    Parameters:
    -----------
    inputs : list[Path]
        List of file paths to FRP product files
    field : str
        Field to extract ('FRP_MWIR' or 'FRP_SWIR')
    
    Returns:
    --------
    dict
        Dictionary containing combined data arrays:
        - 'lats': Latitude coordinates
        - 'lons': Longitude coordinates
        - 'data': FRP MWIR or SWIR values
    """
    all_lats = []
    all_lons = []
    all_data = []

    for file in inputs:
        ds_file = read_FRP_product(file, variables=VARIABLES)
        
        # Extract and flatten data
        lats = ds_file['latitude'].values.flatten()
        lons = ds_file['longitude'].values.flatten()
        data = ds_file[field].values.flatten()
        
        # Remove NaN values and filter out negative values
        mask = ~np.isnan(lats) & ~np.isnan(lons) & ~np.isnan(data) & (data >= 0.0)
        
        all_lats.extend(lats[mask])
        all_lons.extend(lons[mask])
        all_data.extend(data[mask])

    # Convert to numpy arrays
    return {
        'lats': np.array(all_lats),
        'lons': np.array(all_lons),
        'data': np.array(all_data),
    }


def _plot_FRP_core(lats, lons, frp_values, frp_type: str, extent, area_name: str = '', 
                   start_date: str = '', end_date: str = ''):
    """
    Core plotting routine for FRP data visualization.
    
    Parameters:
    -----------
    lats : np.ndarray
        Latitude coordinates
    lons : np.ndarray
        Longitude coordinates
    frp_values : np.ndarray
        FRP values to plot
    frp_type : str
        Type of FRP data ('MWIR' or 'SWIR')
    extent : tuple
        Map extent (west, east, south, north)
    area_name : str, optional
        Name of the geographic area
    start_date : str, optional
        Start date of the data
    end_date : str, optional
        End date of the data
    """
    fig = plt.figure(figsize=(14, 10), facecolor=BACKGROUND_COLOR)
    ax = plt.axes(projection=ccrs.PlateCarree(), facecolor=BACKGROUND_COLOR)

    ax.set_extent(extent, crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.LAND, facecolor=LAND_COLOR, alpha=0.8)
    ax.add_feature(cfeature.OCEAN, facecolor=OCEAN_COLOR, alpha=0.8)
    ax.add_feature(cfeature.COASTLINE, linewidth=0.5, edgecolor=COASTLINE_COLOR)
    ax.add_feature(cfeature.BORDERS, linewidth=0.5, linestyle=':', edgecolor=BORDERS_COLOR)
    ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor=STATES_COLOR)

    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color=GRIDLINES_COLOR, alpha=0.3, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False
    gl.xlabel_style = {'color': FOREGROUND_COLOR}
    gl.ylabel_style = {'color': FOREGROUND_COLOR}

    # Use log scale if enabled
    if USE_LOG_SCALE:
        # Avoid log(0) by setting a minimum value
        vmin = max(frp_values[frp_values > 0].min(), 0.01)
        vmax = frp_values.max()
        scatter = ax.scatter(lons, lats, c=frp_values, 
                             cmap='plasma', s=50, alpha=0.9, 
                             transform=ccrs.PlateCarree(),
                             edgecolors='none',
                             norm=LogNorm(vmin=vmin, vmax=vmax))
    else:
        scatter = ax.scatter(lons, lats, c=frp_values, 
                             cmap='plasma', s=50, alpha=0.9, 
                             transform=ccrs.PlateCarree(),
                             edgecolors='none')

    cbar = plt.colorbar(scatter, ax=ax, orientation='vertical', pad=0.05, shrink=0.5)
    cbar.set_label(f'FRP {frp_type} (MW)' + (' [log scale]' if USE_LOG_SCALE else ''), 
                   fontsize=12, color=FOREGROUND_COLOR)
    cbar.ax.tick_params(colors=FOREGROUND_COLOR)

    title = f'Fire Radiative Power ({frp_type})'
    
    if area_name != '':
        title += f' over {area_name}'
    
    if start_date != '':
        if start_date == end_date:
            date_str = start_date
        else:
            date_str = f'from {start_date} to {end_date}'
    
        title += f' - {date_str}'
        
    title += f'\n({len(lats)} fire points)'
    plt.title(title, fontsize=14, fontweight='bold', color=FOREGROUND_COLOR)

    plt.tight_layout()
    plt.show()


def plot_FRP_MWIR(inputs: list[Path], area: dict = None, area_name: str = '', 
                  start_date: str = '', end_date: str = ''):
    """
    Plot Fire Radiative Power (FRP) MWIR data from SLSTR products.
    
    Parameters:
    -----------
    inputs : list[Path]
        List of file paths to FRP product files
    area : dict, optional
        Dictionary with geographic extent containing 'west', 'east', 'south', 'north' keys.
        If None, defaults to global extent.
        Example: {'west': -140.51, 'east': -48.97, 'south': 33.65, 'north': 66.79}
    area_name : str, optional
        Name of the geographic area
    start_date : str, optional
        Start date of the data
    end_date : str, optional
        End date of the data
    """
    # Set default area if not provided
    if area is None:
        area = {
            'west': -180,
            'east':  180,
            'south': -90,
            'north':  90,
        }
    
    dilation_factor = 5.0  # degrees
    
    # Create extent tuple from area dict
    extent = (
        expand(area['west'],   'west', dilation_factor), 
        expand(area['east'],   'east', dilation_factor), 
        expand(area['south'], 'south', dilation_factor), 
        expand(area['north'], 'north', dilation_factor)
    )
    
    # Read and combine data
    data = _read_and_combine_FRP_data(inputs, field='FRP_MWIR')
    
    print(f"Total number of fire points across all files: {len(data['lats'])}")
    print(f"FRP_MWIR range: {data['data'].min():.2f} - {data['data'].max():.2f} MW")
    
    # Plot MWIR data
    _plot_FRP_core(data['lats'], data['lons'], data['data'], 'MWIR', 
                   extent, area_name, start_date, end_date)


def plot_FRP_SWIR(inputs: list[Path], area: dict = None, area_name: str = '', 
                  start_date: str = '', end_date: str = ''):
    """
    Plot Fire Radiative Power (FRP) SWIR data from SLSTR products.
    
    Parameters:
    -----------
    inputs : list[Path]
        List of file paths to FRP product files
    area : dict, optional
        Dictionary with geographic extent containing 'west', 'east', 'south', 'north' keys.
        If None, defaults to global extent.
        Example: {'west': -140.51, 'east': -48.97, 'south': 33.65, 'north': 66.79}
    area_name : str, optional
        Name of the geographic area
    start_date : str, optional
        Start date of the data
    end_date : str, optional
        End date of the data
    """
    # Set default area if not provided
    if area is None:
        area = {
            'west': -180,
            'east':  180,
            'south': -90,
            'north':  90,
        }
    
    dilation_factor = 5.0  # degrees
    
    # Create extent tuple from area dict
    extent = (
        expand(area['west'],   'west', dilation_factor), 
        expand(area['east'],   'east', dilation_factor), 
        expand(area['south'], 'south', dilation_factor), 
        expand(area['north'], 'north', dilation_factor)
    )
    
    # Read and combine data
    data = _read_and_combine_FRP_data(inputs, field='FRP_SWIR')
    
    print(f"Total number of fire points across all files: {len(data['lats'])}")
    print(f"FRP_SWIR range: {data['data'].min():.2f} - {data['data'].max():.2f} MW")
    
    # Plot SWIR data
    _plot_FRP_core(data['lats'], data['lons'], data['data'], 'SWIR', 
                   extent, area_name, start_date, end_date)


def plot_FRP(inputs: list[Path], area: dict = None, area_name: str = '', start_date: str = '', end_date: str = ''):
    """
    Plot both Fire Radiative Power (FRP) MWIR and SWIR data from SLSTR products.
    
    Uses configuration parameters defined at the top of the file.
    
    Parameters:
    -----------
    inputs : list[Path]
        List of file paths to FRP product files
    area : dict, optional
        Dictionary with geographic extent containing 'west', 'east', 'south', 'north' keys.
        If None, defaults to global extent.
        Example: {'west': -140.51, 'east': -48.97, 'south': 33.65, 'north': 66.79}
    area_name : str, optional
        Name of the geographic area
    start_date : str, optional
        Start date of the data
    end_date : str, optional
        End date of the data
    """
    # Set default area if not provided
    if area is None:
        area = {
            'west': -180,
            'east':  180,
            'south': -90,
            'north':  90,
        }
    
    dilation_factor = 5.0  # degrees
    
    # Create extent tuple from area dict
    extent = (
        expand(area['west'],   'west', dilation_factor), 
        expand(area['east'],   'east', dilation_factor), 
        expand(area['south'], 'south', dilation_factor), 
        expand(area['north'], 'north', dilation_factor)
    )
    
    # Read and combine data
    data = _read_and_combine_FRP_data(inputs)
    
    print(f"Total number of fire points across all files: {len(data['lats'])}")
    print(f"FRP_MWIR range: {data['frp_mwir'].min():.2f} - {data['frp_mwir'].max():.2f} MW")
    print(f"FRP_SWIR range: {data['frp_swir'].min():.2f} - {data['frp_swir'].max():.2f} MW")
    
    # Plot both MWIR and SWIR
    _plot_FRP_core(data['lats'], data['lons'], data['frp_mwir'], 'MWIR', 
                   extent, area_name, start_date, end_date)
    _plot_FRP_core(data['lats'], data['lons'], data['frp_swir'], 'SWIR', 
                   extent, area_name, start_date, end_date)


