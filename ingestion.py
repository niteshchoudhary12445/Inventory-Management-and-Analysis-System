import os
import logging
from typing import List, Optional, Iterator

import pandas as pd
import pyodbc

from utils import (
    IngestionConfig,
    setup_logging,
    get_db_connection,
    ensure_table,
)
from aggregate import aggregate_and_store


def read_csv_in_chunks(
    file_path: str,
    columns: List[str],
    encoding: str = IngestionConfig.ENCODING,
    chunk_size: int = IngestionConfig.CHUNK_SIZE,
) -> Iterator[pd.DataFrame]:
    """
    Yield DataFrame chunks from a CSV file, with sanitized column names.
    """
    # pandas will take care of skipping empty lines, quoting, etc.
    return pd.read_csv(
        file_path,
        encoding=encoding,
        names=columns,
        header=0,
        dtype=str,
        chunksize=chunk_size,
        na_filter=False,  # empty strings instead of NaN
    )


def ingest_table_from_csv(
    cursor: pyodbc.Cursor,
    folder_path: str,
    fname: str,
    column_types: Optional[List[str]]
) -> None:
    """
    Ingest a single CSV file into its corresponding SQL table.
    """
    table_name = os.path.splitext(fname)[0]
    file_path = os.path.join(folder_path, fname)

    if os.path.getsize(file_path) == 0:
        logging.warning(f"Skipping empty file: {file_path}")
        return

    # Read just the header line to get the columns
    with open(file_path, encoding=IngestionConfig.ENCODING) as f:
        raw_cols = next(f).strip().split(",")
    # sanitize
    columns = [c.strip().replace(" ", "_") for c in raw_cols]

    logging.info(f"Ingesting `{file_path}` → table `{table_name}`")
    ensure_table(cursor, table_name, columns, column_types)

    # Pre-build the INSERT statement
    cols_escaped = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = (
        f"INSERT INTO {table_name} ({cols_escaped}) VALUES ({placeholders})"
    )
    cursor.fast_executemany = True

    # Loop over pandas chunks
    for chunk_df in read_csv_in_chunks(
        file_path, columns, chunk_size=IngestionConfig.CHUNK_SIZE
    ):
        # convert all columns to string to match VARCHAR or actual types
        rows = [tuple(row) for row in chunk_df.itertuples(index=False, name=None)]
        cursor.executemany(insert_sql, rows)
        logging.info(f"  → inserted {len(rows)} rows into `{table_name}`")


def ingest_folder(
    folder_path: str,
    cursor: pyodbc.Cursor,
    file_extension: str = IngestionConfig.FILE_EXTENSION,
    column_types: Optional[List[str]] = None,
) -> None:
    """
    Ingest every CSV in a folder into its own table.
    """
    if not os.path.isdir(folder_path):
        logging.error(f"Folder not found: {folder_path}")
        return

    for fname in os.listdir(folder_path):
        if not fname.lower().endswith(file_extension.lower()):
            continue
        ingest_table_from_csv(cursor, folder_path, fname, column_types)

    logging.info(f"Finished ingesting all files in `{folder_path}`")


def main(
    folders: List[str] = IngestionConfig.FOLDERS,
    conn_string: str = IngestionConfig.CONN_STRING,
    file_extension: str = IngestionConfig.FILE_EXTENSION,
    column_types: Optional[List[str]] = None,
    log_file: str = IngestionConfig.LOG_FILE,
    log_level: int = IngestionConfig.LOG_LEVEL,
) -> None:
    """
    Main entrypoint: ingest all folders, then run aggregate.
    """
    setup_logging(log_file, log_level)
    logging.info("Starting ingestion process")

    with get_db_connection(conn_string) as cursor:
        for folder in folders:
            ingest_folder(
                folder_path=folder,
                cursor=cursor,
                file_extension=file_extension,
                column_types=column_types,
            )
        # commit after all folders
        cursor.connection.commit()

    logging.info("Ingestion complete — now running aggregation")
    aggregate_and_store(
        conn_string=conn_string,
        log_file=log_file,
        log_level=log_level,
    )


if __name__ == "__main__":
    main()
