import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyodbc
from contextlib import contextmanager

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
class IngestionConfig:
    FOLDERS        = ["data", "logs"]
    CHUNK_SIZE     = 10_000
    LOG_FILE       = "logs/ingestion.log"
    CONN_STRING    = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=DESKTOP-5RU5DHA\\SQLEXPRESS;"
        "DATABASE=inventory_db;"
        "Trusted_Connection=yes;"
    )
    FILE_EXTENSION = ".csv"
    LOG_LEVEL      = logging.DEBUG
    ENCODING       = "utf-8"


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
def setup_logging(
    log_file: str = IngestionConfig.LOG_FILE,
    level: int = IngestionConfig.LOG_LEVEL
) -> None:
    """
    Configure file + console logging once.
    """
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # File handler
    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(formatter)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(level)
        root.addHandler(fh)
        root.addHandler(ch)


# -------------------------------------------------------------------
# Database Connection
# -------------------------------------------------------------------
@contextmanager
def get_db_connection(
    conn_string: str = IngestionConfig.CONN_STRING,
    autocommit: bool = False
):
    """
    Yields a pyodbc cursor with fast_executemany enabled.
    Caller must commit if autocommit=False.
    """
    conn = pyodbc.connect(conn_string, autocommit=autocommit)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    try:
        yield cursor
        if not autocommit:
            conn.commit()
    finally:
        cursor.close()
        conn.close()


# -------------------------------------------------------------------
# Table Management
# -------------------------------------------------------------------
def ensure_table(
    cursor: pyodbc.Cursor,
    table_name: str,
    columns: List[str],
    column_types: Optional[List[str]] = None
) -> None:
    """
    Drop & recreate table with the given schema.
    """
    logging.info(f"Recreating table `{table_name}`")
    cursor.execute(f"IF OBJECT_ID(N'{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
    if column_types is None:
        column_types = ["VARCHAR(MAX) NULL"] * len(columns)
    col_defs = ", ".join(f"[{col}] {col_type}" for col, col_type in zip(columns, column_types))
    cursor.execute(f"CREATE TABLE {table_name} ({col_defs});")


# -------------------------------------------------------------------
# File Discovery
# -------------------------------------------------------------------
def list_csv_files(
    data_folder: str = IngestionConfig.FOLDERS[0]
) -> List[Path]:
    """
    Recursively find all CSV files under `data_folder`.
    """
    base = Path(data_folder)
    return sorted(base.rglob(f"*{IngestionConfig.FILE_EXTENSION}"))


# -------------------------------------------------------------------
# Ingestion
# -------------------------------------------------------------------
def ingest_file(
    cursor: pyodbc.Cursor,
    file_path: Path,
    table_name: str,
    chunk_size: int = IngestionConfig.CHUNK_SIZE,
    column_types: Optional[List[str]] = None
) -> None:
    """
    Stream a CSV into `table_name` in chunks.
    """
    logging.info(f"Ingesting `{file_path}` → `{table_name}`")

    # Read header-only to sanitize column names
    header_df = pd.read_csv(
        file_path,
        nrows=0,
        encoding=IngestionConfig.ENCODING
    )
    columns = [col.strip().replace(" ", "_") for col in header_df.columns]
    ensure_table(cursor, table_name, columns, column_types)

    # Pre-build insert SQL
    cols_esc = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = f"INSERT INTO {table_name} ({cols_esc}) VALUES ({placeholders})"

    # Stream data in chunks
    for batch_idx, df_chunk in enumerate(pd.read_csv(
        file_path,
        header=0,
        names=columns,
        chunksize=chunk_size,
        dtype=str,
        encoding=IngestionConfig.ENCODING,
        na_filter=False
    )):
        rows = [tuple(row) for row in df_chunk.itertuples(index=False, name=None)]
        cursor.executemany(insert_sql, rows)
        logging.info(f"  • Batch {batch_idx + 1}: inserted {len(rows)} rows")


# -------------------------------------------------------------------
# Orchestration
# -------------------------------------------------------------------
def ingest_all(
    data_folder: str = IngestionConfig.FOLDERS[0],
    conn_string: str = IngestionConfig.CONN_STRING,
    column_types: Optional[List[str]] = None
) -> None:
    """
    Ingest every CSV in `data_folder` into its own table.
    """
    setup_logging()
    files = list_csv_files(data_folder)
    if not files:
        logging.warning(f"No CSV files under `{data_folder}`")
        return

    with get_db_connection(conn_string, autocommit=False) as cursor:
        for file_path in files:
            table_name = file_path.stem
            ingest_file(cursor, file_path, table_name, column_types=column_types)

    logging.info("All files ingested successfully.")


if __name__ == "__main__":
    ingest_all()
