from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from utils.azure_connection import get_adls_service_client
from utils.config import AZURE_STORAGE_FILESYSTEM, SOURCE_DIR
from utils.logger import get_logger

logger = get_logger(__name__)


def file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as input_file:
        for block in iter(lambda: input_file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ingest_file_to_adls(csv_path: Path) -> dict:
    path = Path(csv_path).resolve()
    source_dir = SOURCE_DIR.resolve()
    if not path.is_file() or not path.is_relative_to(source_dir):
        raise ValueError(f"Invalid source file: {path}")
    if path.suffix.lower() != ".csv":
        raise ValueError(f"Expected CSV file: {path}")

    relative_path = path.relative_to(source_dir)
    if len(relative_path.parts) != 3:
        raise ValueError(
            "Expected source_data/<source_name>/<table_name>/<file>.csv; "
            f"received {relative_path}"
        )
    source_name, table_name, _ = relative_path.parts
    delivery_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Content-based filename makes reruns skip the same file even after a restart.
    # Changed CSV content produces a different new Parquet file in the landing zone.
    content_id = file_sha256(path)[:16]
    adls_path = (
        f"{source_name}/{table_name}/{delivery_date}/"
        f"{path.stem}_{content_id}.parquet"
    )
    service_client = get_adls_service_client()
    try:
        filesystem_client = service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM)
        file_client = filesystem_client.get_file_client(adls_path)
        try:
            file_client.get_file_properties()
        except ResourceNotFoundError:
            pass
        else:
            logger.info("SKIPPED existing: %s", adls_path)
            return {"status": "SKIPPED", "source_file": str(path), "adls_path": adls_path}

        rows = 0
        columns = 0
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / f"{path.stem}.parquet"
            writer = None
            try:
                # Store Bronze landing data as strings: source-preserving, stable
                # schema across chunks; staging can apply typed conversions.
                for chunk in pd.read_csv(
                    path, chunksize=100_000, dtype=str, keep_default_na=False
                ):
                    if chunk.empty:
                        continue
                    arrow_table = pa.Table.from_pandas(chunk, preserve_index=False)
                    if writer is None:
                        writer = pq.ParquetWriter(parquet_path, arrow_table.schema)
                    writer.write_table(arrow_table)
                    rows += len(chunk)
                    columns = len(chunk.columns)
            finally:
                if writer is not None:
                    writer.close()

            if rows == 0:
                raise ValueError(f"CSV has no data rows: {path}")

            directory_client = filesystem_client.get_directory_client(
                f"{source_name}/{table_name}/{delivery_date}"
            )
            try:
                directory_client.create_directory()
            except ResourceExistsError:
                pass

            # Never replace existing landing objects.
            with parquet_path.open("rb") as payload:
                file_client.upload_data(
                    payload, overwrite=True, length=parquet_path.stat().st_size
                )
        logger.info("UPLOADED %s rows=%d columns=%d", adls_path, rows, columns)
        return {
            "status": "UPLOADED", "source_file": str(path),
            "adls_path": adls_path, "rows": rows, "columns": columns,
        }
    finally:
        service_client.close()
