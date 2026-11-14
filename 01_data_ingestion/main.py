import sys
from pathlib import Path

# Allows running: uv run python 01_data_ingestion/main.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from check_connection import check_adls_connection
from discover_files import discover_files
from local_to_adls import ingest_file_to_adls
from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    logger.info("========== Local CSV to ADLS Gen2 ingestion started ==========")
    check_adls_connection()
    files = discover_files()
    if not files:
        logger.warning("No CSV files found. Nothing to ingest.")
        return
    uploaded = skipped = failed = 0
    for file in files:
        try:
            result = ingest_file_to_adls(Path(file))
            if result["status"] == "UPLOADED":
                uploaded += 1
            else:
                skipped += 1
        except Exception:
            failed += 1
            logger.exception("Continuing after file failure: %s", file)
    logger.info("Ingestion finished | discovered=%d uploaded=%d skipped=%d failed=%d",
                len(files), uploaded, skipped, failed)
    if failed:
        raise RuntimeError(f"Ingestion finished with {failed} failed file(s)")


if __name__ == "__main__":
    main()
