from pathlib import Path

from utils.config import SOURCE_DIR
from utils.logger import get_logger

logger = get_logger(__name__)


def discover_files():
    source_dir = Path(SOURCE_DIR).resolve()

    if not source_dir.is_dir():
        raise FileNotFoundError(f"CSV source directory does not exist: {source_dir}")

    files = sorted(source_dir.rglob("*.csv"))

    logger.info("Discovered %d CSV file(s) under %s", len(files), source_dir)
    
    return [str(file) for file in files]
