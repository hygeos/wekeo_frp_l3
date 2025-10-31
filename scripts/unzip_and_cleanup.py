import zipfile
from pathlib import Path


def unzip(archive: Path, to: Path|None):
    """Extract file_one.zip to file_one/ and remove archive if successful."""
    if to is None: to = archive.parent # Extract to same directory as archive
    
    archive = Path(archive)
    to = Path(to)
    target = to / archive.stem
    
    try:
        with zipfile.ZipFile(archive, 'r') as zip_ref:
            zip_ref.extractall(target)
    except:
        print(f"Failed to extract {archive} to {target}")