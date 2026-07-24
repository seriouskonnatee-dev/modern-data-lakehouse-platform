"""
Silver job entrypoint: Bronze `sales_events` -> Silver `sales_events_clean`.

Reads every Bronze sales_events Parquet partition (all ingest_date= folders present),
applies the transformation pipeline in `transforms.py`, and writes:
  - data_lake/silver/sales_events_clean/  (partitioned by event_date)
  - data_lake/silver/_quarantine/sales_events/  (rows that failed validation)

Run: `python clean_transactions.py [--lake-root PATH]`
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from spark_session import get_spark
from transforms import clean_sales_events

DEFAULT_LAKE_ROOT = Path(__file__).resolve().parent.parent / "data_lake"


def run(lake_root: Path, bronze_root: Path | None = None, silver_root: Path | None = None) -> None:
    spark = get_spark("silver-clean-transactions")

    bronze_path = str((bronze_root or lake_root / "bronze") / "sales_events")
    silver_root = silver_root or (lake_root / "silver")
    silver_path = str(silver_root / "sales_events_clean")
    quarantine_path = str(silver_root / "_quarantine" / "sales_events")

    raw_df = spark.read.parquet(bronze_path)
    print(f"[silver] read {raw_df.count()} raw sales events from {bronze_path}")

    clean_df, quarantine_df = clean_sales_events(raw_df)
    clean_df = clean_df.withColumn("event_date", F.to_date("event_ts"))

    n_clean = clean_df.count()
    n_quarantine = quarantine_df.count()
    print(f"[silver] clean={n_clean} quarantined={n_quarantine}")

    (
        clean_df.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(silver_path)
    )
    if n_quarantine > 0:
        quarantine_df.write.mode("overwrite").parquet(quarantine_path)

    print(f"[silver] wrote clean events -> {silver_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake-root", type=str, default=str(DEFAULT_LAKE_ROOT))
    parser.add_argument(
        "--bronze-root", type=str, default=None,
        help="Override Bronze read root (defaults to <lake-root>/bronze).",
    )
    parser.add_argument(
        "--silver-root", type=str, default=None,
        help="Override Silver write root (defaults to <lake-root>/silver).",
    )
    args = parser.parse_args()
    run(
        Path(args.lake_root),
        bronze_root=Path(args.bronze_root) if args.bronze_root else None,
        silver_root=Path(args.silver_root) if args.silver_root else None,
    )
