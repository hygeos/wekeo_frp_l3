import zipfile
import shutil
from datetime import datetime
from pathlib import Path

from hda import Client

from wekeo_frp_l3 import env
from wekeo_frp_l3 import config
from wekeo_frp_l3.hygeos_core import log

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
        return target
    except Exception as e:
        log.error(f"Failed to extract {archive} to {target}: {e}", e=None)
        return None


def save_failed_archive(archive: Path):
    """Copy a failed archive to the configured failed folder for diagnostics."""
    try:
        archive = Path(archive)
        if not archive.exists():
            return

        destination = config.failed_fpr_dir / archive.name
        if destination.exists():
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
            destination = config.failed_fpr_dir / f"{archive.stem}__{ts}{archive.suffix}"

        shutil.copy2(archive, destination)
        log.info(f"Copied failed archive {archive.name} to {destination}")
    except Exception as e:
        log.error(f"Failed to copy failed archive {archive} to {config.failed_fpr_dir}: {e}")


def download(
    query,
    archive_dir: Path,
    extract_dir: Path | None = None,
    rm_archive: bool = False,
    recursive_try=0,
    max_recursive_try=3,
):
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
    
    incoming_query_results = list(query.results)
    try:
        if extract_dir is None:
            extract_dir = archive_dir
        extract_dir = Path(extract_dir)
        archive_dir = Path(archive_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        archive_dir.mkdir(parents=True, exist_ok=True)

        # Always return paths for the original request scope.
        requested_items = list(query.results)
        requested_ids = [item['id'] for item in requested_items]
        results = [extract_dir / item_id for item_id in requested_ids]

        attempt = recursive_try
        items_to_process = requested_items

        while True:
            missing = []  # items to download
            local_archives = []  # archives already present to extract

            for item in items_to_process:
                extract_path = extract_dir / item['id']
                archive_path = archive_dir / f"{item['id']}.zip"

                if extract_path.exists():
                    continue

                if archive_path.exists():
                    local_archives.append(archive_path)
                else:
                    missing.append(item)

            # Process local archives first.
            for archive in local_archives:
                extracted = unzip(archive, to=extract_dir)

                # Verify extraction success for local archives.
                expected_extracted_path = extract_dir / archive.stem
                if extracted is None or not expected_extracted_path.exists():
                    log.info(f"Failed to extract existing archive {archive.name}. Deleting it to re-download.")
                    save_failed_archive(archive)
                    try:
                        archive.unlink()
                    except Exception as e:
                        log.error(f"Failed to delete corrupt archive {archive}: {e}")
                elif rm_archive:
                    try:
                        archive.unlink()
                    except Exception:
                        pass

            # Download missing items one by one so successful files are persisted immediately.
            if missing:
                log.info(f"Downloading {len(missing)} missing files...")
                for item in missing:
                    archive_path = archive_dir / f"{item['id']}.zip"
                    extract_path = extract_dir / item['id']

                    # Temporarily scope query to a single item for download, then restore it.
                    previous_results = list(query.results)
                    query.results = [item]

                    try:
                        query.download(download_dir=archive_dir)
                    except Exception as e:
                        log.error(f"Download error for {item['id']}: {e}")
                        continue
                    finally:
                        query.results = previous_results

                    if not archive_path.exists():
                        log.error(f"File {archive_path.name} not found after download.")
                        continue

                    extracted = unzip(archive_path, to=extract_dir)
                    if extracted is None or not extract_path.exists():
                        log.error(f"Extraction failed for {archive_path.name}")
                        save_failed_archive(archive_path)
                        continue

                    if rm_archive:
                        try:
                            archive_path.unlink()
                        except Exception as e:
                            log.error(f"Failed to delete archive {archive_path}: {e}")
            elif not local_archives and attempt == 0:
                log.info("All files already present locally, skipping download.")

            # Check for failures and retry if needed.
            failed_items = []
            for item in items_to_process:
                extract_path = extract_dir / item['id']
                if not extract_path.exists():
                    failed_items.append(item)

            if not failed_items:
                break

            if attempt >= max_recursive_try:
                raise RuntimeError(f"Error: Maximum recursive download attempts ({max_recursive_try}) reached. {len(failed_items)} files failed.")

            log.info(f"Warning: {len(failed_items)} files failed processing. Retrying...")
            items_to_process = failed_items
            attempt += 1

        return results
    finally:
        query.results = incoming_query_results

def format_query(
    start_date: str | datetime,
    end_date: str | datetime,
    area: dict[str, float] = {'west': -180.0, 'south': -90.0, 'east': 180.0, 'north': 90.0},
):

    # Convert date strings when needed; keep datetime inputs untouched.
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
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
    area: dict[str, float] = {'west': -180.0, 'south': -90.0, 'east': 180.0, 'north': 90.0},
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
    assert len(query.results) == len(results), (
        "Mismatch between number of queried items and returned result paths: "
        f"query.results={len(query.results)}, results={len(results)}"
    )
    print(f"Successfully downloaded and extracted {len(results)} FRP products.")
    return results
    