import zipfile
from datetime import datetime
from pathlib import Path

from hda import Client, Configuration

from wekeo import env


def unzip(archive: Path, to: Path|None):
    """
    Extract a zip archive to a specified directory. Keep the same name as the archive
    without the .zip extension.
        
        Args:
            archive: Path to the zip file to extract
            to: Path to the directory where contents will be extracted.
                If None, extracts to the same directory as the archive.
        
        Returns:
            None
    """
    if to is None: to = archive.parent # Extract to same directory as archive
    
    archive = Path(archive)
    to = Path(to)
    target = to / archive.stem
    
    try:
        with zipfile.ZipFile(archive, 'r') as zip_ref:
            zip_ref.extractall(target)
    except:
        print(f"Failed to extract {archive} to {target}")
        return


def download(query, archive_dir: Path, extract_dir: Path|None = None, rm_archive: bool = False):
    """
    Download files from query results, skipping files that already exist locally.
        
        Args:
            query: SearchResults object containing items to download
            archive_dir: Path where archive files will be downloaded
            extract_dir: Path where archives files will be extracted
            remove_archive: bool, whether to remove archive after extraction (default: False)
        
        Returns:
            None
    """
    
    if extract_dir is None: extract_dir=archive_dir
    
    missing = [] # archives to download
    extract = [] # archives to extract after download
    results = [] # all extracted paths
    
    for item in query.results:
        
        archive_path = archive_dir / f"{item['id']}.zip"
        extract_path = extract_dir / item['id']
        results.append(extract_path)
        
        if extract_path.exists() == True:
            continue                        # already extracted, skip
            
        elif archive_path.exists() == True: # archive exists locally but not extracted
            extract.append(archive_path)    # queue archive for extraction after download
            
        else:
            missing.append(item)            # archive missing, queue for download   
            extract.append(archive_path)    # queue archive for extraction after download
            
    # Download missing archives
    if missing: # query only if missing files
        print(f"Downloading {len(missing)} files...")
        query.results = missing
        query.download(download_dir=archive_dir)
    else:
        print("All files already present locally, skipping download.")
    
    # Extract downloaded archives
    if extract:
        for archive in extract:
            unzip(archive, to=extract_dir)
            if rm_archive:
                archive.unlink()  # remove archive after extraction
                
    return results

def format_query(
    start_date: datetime,
    end_date: datetime,
    area: dict[float] = {'west': -180.0, 'south': -90.0, 'east': 180.0, 'north': 90.0},
):

    # convert from string to datetime
    start_date = datetime.strptime(start_date, "%Y-%m-%d") 
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    json_query = {
        "dataset_id": "EO:EUM:DAT:SENTINEL-3:0417",
        "startdate": start_date.strftime("%Y-%m-%dT00:00:00.000Z"),
        "enddate":   end_date.strftime("%Y-%m-%dT23:59:59.999Z"),
        "bbox": [
            area['west'],
            area['south'],
            area['east'],
            area['north'],
        ],
        "fire": "true",
        "itemsPerPage": 99999,
        "startIndex": 0
    }
    
    return json_query


def get_storage_path():
    """
    Retrieves and prepares the storage path for FRP products.
    Gets the base path from the DIR_ANCILLARY environment variable,
    Returns:
        Path: The SLSTR_FRP storage path.
    Raises:
        ValueError: If DIR_ANCILLARY is not set or the path does not exist.
    """
    
    storage_path = Path(env.getvar("DIR_ANCILLARY"))
    if not storage_path.exists():
        raise ValueError("Environment variable DIR_ANCILLARY not set or path does not exist")
        
    storage_path = Path(storage_path) / "SLSTR_FRP"
    if not storage_path.exists():
        storage_path.mkdir(exist_ok=True)
        
    return storage_path

def get_FRP_products(
    start_date: datetime,
    end_date: datetime,
    area: dict[float] = {'west': -180.0, 'south': -90.0, 'east': 180.0, 'north': 90.0},
):
    """
    Query and download Sentinel-3 SLSTR FRP products from WEkEO.
    Args:
        start_date: Start date for the query (datetime)
        end_date: End date for the query (datetime)
        area: Dictionary defining the bounding box with keys 'west', 'south', 'east', 'north'
    Returns:
        List[Path]: List of paths to the extracted FRP product directories
    """

    hda_client = Client()

    json_query = format_query(start_date, end_date, area)
    query = hda_client.search(json_query)
    
    results = download(query, archive_dir=get_storage_path(), rm_archive=True)
    return results
    