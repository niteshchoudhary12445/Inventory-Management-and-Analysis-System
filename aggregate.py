import logging
import time
from typing import List, Tuple

import pandas as pd
import pyodbc

from utils import IngestionConfig, setup_logging, get_db_connection, ensure_table

class AggregateConfig:
    TARGET_TABLE = "final_summary"
    LOG_FILE      = "logs/aggregate.log"
    QUERY = f"""
    WITH FreightSummary AS (
      SELECT
        TRY_CAST(VendorNumber AS BIGINT) AS VendorNumber,
        SUM(TRY_CAST(Freight AS DECIMAL(18,2))) AS FreightCost
      FROM vendor_invoice
      WHERE TRY_CAST(VendorNumber AS BIGINT) IS NOT NULL
      GROUP BY TRY_CAST(VendorNumber AS BIGINT)
    ),
    PurchaseSummary AS (
      SELECT
        TRY_CAST(p.VendorNumber AS BIGINT)           AS VendorNumber,
        LTRIM(RTRIM(p.VendorName))                   AS VendorName,
        TRY_CAST(p.Brand AS BIGINT)                  AS Brand,
        p.Description,
        TRY_CAST(p.PurchasePrice AS DECIMAL(18,2))   AS PurchasePrice,
        TRY_CAST(pp.Price AS DECIMAL(18,2))          AS ActualPrice,
        TRY_CAST(pp.Volume AS BIGINT)                AS Volume,
        SUM(TRY_CAST(p.Quantity AS BIGINT))          AS TotalPurchaseQuantity,
        SUM(TRY_CAST(p.Dollars AS DECIMAL(18,2)))    AS TotalPurchaseDollars
      FROM purchases p
      JOIN purchase_prices pp
        ON p.Brand = pp.Brand
      WHERE
        TRY_CAST(p.VendorNumber AS BIGINT)   IS NOT NULL
        AND TRY_CAST(p.Brand AS BIGINT)       IS NOT NULL
        AND TRY_CAST(p.PurchasePrice AS DECIMAL(18,2)) > 0
      GROUP BY
        TRY_CAST(p.VendorNumber AS BIGINT),
        LTRIM(RTRIM(p.VendorName)),
        TRY_CAST(p.Brand AS BIGINT),
        p.Description,
        TRY_CAST(p.PurchasePrice AS DECIMAL(18,2)),
        TRY_CAST(pp.Price AS DECIMAL(18,2)),
        pp.Volume
    ),
    SalesSummary AS (
      SELECT
        TRY_CAST(s.VendorNo AS BIGINT)              AS VendorNumber,
        TRY_CAST(s.Brand AS BIGINT)                 AS Brand,
        SUM(TRY_CAST(s.SalesQuantity AS BIGINT))    AS TotalSalesQuantity,
        SUM(TRY_CAST(s.SalesDollars AS DECIMAL(18,2))) AS TotalSalesDollars,
        SUM(TRY_CAST(s.SalesPrice AS DECIMAL(18,2))) AS TotalSalesPrice,
        SUM(TRY_CAST(s.ExciseTax AS DECIMAL(18,2))) AS TotalExciseTax
      FROM sales s
      WHERE
        TRY_CAST(s.VendorNo AS BIGINT) IS NOT NULL
        AND TRY_CAST(s.Brand AS BIGINT) IS NOT NULL
      GROUP BY
        TRY_CAST(s.VendorNo AS BIGINT),
        TRY_CAST(s.Brand AS BIGINT)
    )
    SELECT
      ps.VendorNumber,
      ps.VendorName,
      ps.Brand,
      ps.Description,
      ps.PurchasePrice,
      ps.ActualPrice,
      ps.Volume,
      ps.TotalPurchaseQuantity,
      ps.TotalPurchaseDollars,
      COALESCE(ss.TotalSalesQuantity, 0)    AS TotalSalesQuantity,
      COALESCE(ss.TotalSalesDollars, 0)     AS TotalSalesDollars,
      COALESCE(ss.TotalSalesPrice, 0)       AS TotalSalesPrice,
      COALESCE(ss.TotalExciseTax, 0)        AS TotalExciseTax,
      COALESCE(fs.FreightCost, 0)           AS FreightCost,
      -- Compute profits & ratios here:
      (COALESCE(ss.TotalSalesDollars,0) - ps.TotalPurchaseDollars) AS GrossProfit,
      CASE 
        WHEN COALESCE(ss.TotalSalesDollars,0) = 0 THEN 0 
        ELSE (COALESCE(ss.TotalSalesDollars,0) - ps.TotalPurchaseDollars)
             / COALESCE(ss.TotalSalesDollars,1) * 100 
      END AS ProfitMargin,
      CASE
        WHEN ps.TotalPurchaseQuantity = 0 THEN 0
        ELSE COALESCE(ss.TotalSalesQuantity,0) * 1.0 / ps.TotalPurchaseQuantity
      END AS StockTurnover,
      CASE
        WHEN ps.TotalPurchaseDollars = 0 THEN 0
        ELSE COALESCE(ss.TotalSalesDollars,0) / ps.TotalPurchaseDollars
      END AS SalesToPurchaseRatio
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary    ss ON ps.VendorNumber = ss.VendorNumber AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC;
    """

def aggregate_and_store(
    conn_string: str = IngestionConfig.CONN_STRING,
    target_table: str = AggregateConfig.TARGET_TABLE,
    log_file: str = AggregateConfig.LOG_FILE,
    log_level: int = IngestionConfig.LOG_LEVEL,
    batch_size: int = 5_000
) -> None:
    """
    Run server‐side aggregation, pull results, and bulk‑insert into target_table.
    """
    setup_logging(log_file, log_level)
    logging.info("Starting aggregation and storage…")
    t0 = time.time()

    try:
        # Single connection for both read & write
        with get_db_connection(conn_string) as cursor:
            conn = cursor.connection

            # 1) Read aggregated result into DataFrame
            logging.info("Executing aggregation query")
            df = pd.read_sql_query(AggregateConfig.QUERY, conn)

            # 2) Light Python clean‑up
            logging.info("Cleaning DataFrame")
            df.fillna(0, inplace=True)
            # VendorName was already trimmed in SQL

            # 3) Ensure target schema
            columns = [
                "VendorNumber", "VendorName", "Brand", "Description", "PurchasePrice",
                "ActualPrice", "Volume", "TotalPurchaseQuantity", "TotalPurchaseDollars",
                "TotalSalesQuantity", "TotalSalesDollars", "TotalSalesPrice", "TotalExciseTax",
                "FreightCost", "GrossProfit", "ProfitMargin", "StockTurnover", "SalesToPurchaseRatio"
            ]
            types = [
                "BIGINT NULL", "VARCHAR(MAX) NULL", "BIGINT NULL", "VARCHAR(MAX) NULL",
                "DECIMAL(18,2) NULL", "DECIMAL(18,2) NULL", "BIGINT NULL",
                "BIGINT NULL", "DECIMAL(18,2) NULL", "BIGINT NULL",
                "DECIMAL(18,2) NULL", "DECIMAL(18,2) NULL", "DECIMAL(18,2) NULL",
                "DECIMAL(18,2) NULL", "DECIMAL(18,2) NULL", "DECIMAL(18,2) NULL",
                "DECIMAL(18,2) NULL", "DECIMAL(18,2) NULL"
            ]
            logging.info(f"Recreating target table `{target_table}`")
            ensure_table(cursor, target_table, columns, types)

            # 4) Bulk insert in batches
            placeholders = ", ".join("?" for _ in columns)
            col_list = ", ".join(f"[{c}]" for c in columns)
            insert_sql = f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholders})"
            cursor.fast_executemany = True

            total_rows = len(df)
            inserted = 0
            logging.info(f"Inserting {total_rows} rows in batches of {batch_size}")
            while inserted < total_rows:
                batch = df.iloc[inserted : inserted + batch_size]
                cursor.executemany(insert_sql, batch.values.tolist())
                inserted += len(batch)

            conn.commit()
            elapsed = time.time() - t0
            logging.info(f"Inserted {total_rows} rows into `{target_table}` in {elapsed:.2f}s")

    except Exception:
        logging.exception("Failed during aggregation/storage")
        raise


if __name__ == "__main__":
    aggregate_and_store()
