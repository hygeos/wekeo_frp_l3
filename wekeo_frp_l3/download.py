import zipfile
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

from hda import Client, Configuration

from wekeo_frp_l3 import env
from wekeo_frp_l3.stubs import log

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
    except Exception as e:
        log.error(f"Failed to extract {archive} to {target}: {e}", e=None)
        return


def download(query, archive_dir: Path, extract_dir: Path|None = None, rm_archive: bool = False, recursive_try = 0, max_recursive_try = 3):
    """
    Download files from query results, skipping files that already exist locally.
        
        Args:
            query: SearchResults object containing items to download
            archive_dir: Path where archive files will be downloaded (and stored if not removed)
            extract_dir: Path where archives files will be extracted
            rm_archive: bool, whether to remove archive after extraction (default: False)
        
        Returns:
            list[Path]: List of all target paths (extracted directories)
    """
    
    if extract_dir is None: extract_dir=archive_dir
    extract_dir = Path(extract_dir)
    archive_dir = Path(archive_dir)
    
    missing = [] # items to download
    local_archives = [] # archives already present to extract
    results = [] # all extracted paths
    
    # Create a copy of items to iterate safely
    all_items = list(query.results)
    
    for item in all_items:
        extract_path = extract_dir / item['id']
        archive_path = archive_dir / f"{item['id']}.zip"
        results.append(extract_path)
        
        if extract_path.exists():
            continue
            
        elif archive_path.exists():
            local_archives.append(archive_path)
        else:
            missing.append(item)
            
    # Process local archives first
    for archive in local_archives:
        unzip(archive, to=extract_dir)
        
        # Verify extraction success for local archives
        expected_extracted_path = extract_dir / archive.stem
        if not expected_extracted_path.exists():
            log.info(f"Failed to extract existing archive {archive.name}. Deleting it to re-download.")
            try:
                archive.unlink()
            except Exception as e:
                log.error(f"Failed to delete corrupt archive {archive}: {e}")
        elif rm_archive:
            try: archive.unlink()
            except: pass

    # Download missing items with temp folder strategy
    if missing:
        log.info(f"Downloading {len(missing)} missing files...")
        
        # Configure query to download only missing items
        query.results = missing
        
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            
            try:
                query.download(download_dir=tmp_dir)
            except Exception as e:
                log.error(f"Download error: {e}")
                
            # Process downloaded files in temp dir
            for item in missing:
                tmp_archive = tmp_dir / f"{item['id']}.zip"
                
                if not tmp_archive.exists():
                    log.error(f"File {tmp_archive.name} not found after download.")
                    continue
                
                # Unzip in temp
                unzip(tmp_archive, to=tmp_dir)
                
                tmp_extracted = tmp_dir / item['id']
                final_extracted = extract_dir / item['id']
                final_archive = archive_dir / f"{item['id']}.zip"
                
                if tmp_extracted.exists():
                    # Move extracted content to final destination
                    if final_extracted.exists():
                        shutil.rmtree(final_extracted)
                    shutil.move(str(tmp_extracted), str(final_extracted))
                    
                    # Move archive to final destination if keeping it
                    if not rm_archive:
                        if final_archive.exists():
                            final_archive.unlink()
                        shutil.move(str(tmp_archive), str(final_archive))
                else:
                    log.error(f"Extraction failed for {tmp_archive.name}")
    else:
        if not local_archives and recursive_try == 0:
             log.info("All files already present locally, skipping download.")

    # Check for failures and recurse if needed
    failed_items = []
    for item in all_items:
        extract_path = extract_dir / item['id']
        if not extract_path.exists():
            failed_items.append(item)

    if failed_items:
        if recursive_try >= max_recursive_try:
            raise RuntimeError(f"Error: Maximum recursive download attempts ({max_recursive_try}) reached. {len(failed_items)} files failed.")
            
        log.info(f"Warning: {len(failed_items)} files failed processing. Retrying...")
        query.results = failed_items
        download(query, archive_dir, extract_dir, rm_archive, recursive_try + 1, max_recursive_try)

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
    
    results = download(query, archive_dir=get_storage_path(), rm_archive=False)
    return results
    