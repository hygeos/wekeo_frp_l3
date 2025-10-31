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
DATE_STR = '2025-07-24'

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


def plot_FRP(inputs: list[Path], area: dict = None):
    """
    Plot Fire Radiative Power (FRP) data from SLSTR products.
    
    Uses configuration parameters defined at the top of the cell.
    
    Parameters:
    -----------
    inputs : list[Path]
        List of file paths to FRP product files
    area : dict, optional
        Dictionary with geographic extent containing 'west', 'east', 'south', 'north' keys.
        If None, defaults to North America region.
        Example: {'west': -140.51, 'east': -48.97, 'south': 33.65, 'north': 66.79}
    """
    # Set default area if not provided
    if area is None:
        area = {
            'west': -140.51,
            'east': -48.97,
            'south': 33.65,
            'north': 66.79
        }
    
    # Create extent tuple from area dict
    extent = (area['west'], area['east'], area['south'], area['north'])
    # Read all files and combine the data
    all_lats = []
    all_lons = []
    all_frp_mwir = []
    all_frp_swir = []

    for file in inputs:
        ds_file = read_FRP_product(file, variables=VARIABLES)
        
        # Extract and flatten data
        lats = ds_file['latitude'].values.flatten()
        lons = ds_file['longitude'].values.flatten()
        frp_mwir = ds_file['FRP_MWIR'].values.flatten()
        frp_swir = ds_file['FRP_SWIR'].values.flatten()
        
        # Remove NaN values
        mask = ~np.isnan(frp_mwir) & ~np.isnan(frp_swir)
        
        all_lats.extend(lats[mask])
        all_lons.extend(lons[mask])
        all_frp_mwir.extend(frp_mwir[mask])
        all_frp_swir.extend(frp_swir[mask])

    # Convert to numpy arrays
    all_lats = np.array(all_lats)
    all_lons = np.array(all_lons)
    all_frp_mwir = np.array(all_frp_mwir)
    all_frp_swir = np.array(all_frp_swir)

    print(f"Total number of fire points across all files: {len(all_lats)}")
    print(f"FRP_MWIR range: {all_frp_mwir.min():.2f} - {all_frp_mwir.max():.2f} MW")
    print(f"FRP_SWIR range: {all_frp_swir.min():.2f} - {all_frp_swir.max():.2f} MW")

    # --- Plot 1: FRP_MWIR ---
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
        vmin = max(all_frp_mwir[all_frp_mwir > 0].min(), 0.01)
        vmax = all_frp_mwir.max()
        scatter = ax.scatter(all_lons, all_lats, c=all_frp_mwir, 
                             cmap='plasma', s=50, alpha=0.9, 
                             transform=ccrs.PlateCarree(),
                             edgecolors='none',
                             norm=LogNorm(vmin=vmin, vmax=vmax))
    else:
        scatter = ax.scatter(all_lons, all_lats, c=all_frp_mwir, 
                             cmap='plasma', s=50, alpha=0.9, 
                             transform=ccrs.PlateCarree(),
                             edgecolors='none')

    cbar = plt.colorbar(scatter, ax=ax, orientation='vertical', pad=0.05, shrink=0.5)
    cbar.set_label('FRP MWIR (MW)' + (' [log scale]' if USE_LOG_SCALE else ''), fontsize=12, color=FOREGROUND_COLOR)
    cbar.ax.tick_params(colors=FOREGROUND_COLOR)

    title = f'Fire Radiative Power (MWIR) over North America - All Files'
    if DATE_STR:
        title += f'\n{DATE_STR} ({len(all_lats)} fire points)'
    else:
        title += f'\n({len(all_lats)} fire points)'
    plt.title(title, fontsize=14, fontweight='bold', color=FOREGROUND_COLOR)

    plt.tight_layout()
    plt.show()

    # --- Plot 2: FRP_SWIR ---
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
        vmin = max(all_frp_swir[all_frp_swir > 0].min(), 0.01)
        vmax = all_frp_swir.max()
        scatter = ax.scatter(all_lons, all_lats, c=all_frp_swir, 
                             cmap='plasma', s=50, alpha=0.9, 
                             transform=ccrs.PlateCarree(),
                             edgecolors='none',
                             norm=LogNorm(vmin=vmin, vmax=vmax))
    else:
        scatter = ax.scatter(all_lons, all_lats, c=all_frp_swir, 
                             cmap='plasma', s=50, alpha=0.9, 
                             transform=ccrs.PlateCarree(),
                             edgecolors='none')

    cbar = plt.colorbar(scatter, ax=ax, orientation='vertical', pad=0.05, shrink=0.5)
    cbar.set_label('FRP SWIR (MW)' + (' [log scale]' if USE_LOG_SCALE else ''), fontsize=12, color=FOREGROUND_COLOR)
    cbar.ax.tick_params(colors=FOREGROUND_COLOR)

    title = f'Fire Radiative Power (SWIR) over North America - All Files'
    if DATE_STR:
        title += f'\n{DATE_STR} ({len(all_lats)} fire points)'
    else:
        title += f'\n({len(all_lats)} fire points)'
    plt.title(title, fontsize=14, fontweight='bold', color=FOREGROUND_COLOR)

    plt.tight_layout()
    plt.show()


