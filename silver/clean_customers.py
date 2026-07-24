"""
Silver job entrypoint: Bronze `customer_profile_snapshots` -> Silver
`customer_profiles_clean`.

Run: `python clean_customers.py [--lake-root PATH]`
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from spark_session import get_spark
from transforms import clean_customer_profiles

DEFAULT_LAKE_ROOT = Path(__file__).resolve().parent.parent / "data_lake"


def run(lake_root: Path, bronze_root: Path | None = None, silver_root: Path | None = None) -> None:
    spark = get_spark("silver-clean-customers")

    bronze_path = str((bronze_root or lake_root / "bronze") / "customer_profile_snapshots")
    silver_path = str((silver_root or lake_root / "silver") / "customer_profiles_clean")

    raw_df = spark.read.parquet(bronze_path)
    print(f"[silver] read {raw_df.count()} raw customer snapshots from {bronze_path}")

    clean_df = clean_customer_profiles(raw_df)
    # Partitioned by snapshot_date (not just written as one flat file) so that, like
    # sales_events_clean, re-running the job with dynamic partition overwrite mode only
    # replaces the day(s) being reprocessed instead of the entire dataset.
    clean_df = clean_df.withColumn("snapshot_date", F.to_date("snapshot_ts"))
    n_clean = clean_df.count()
    print(f"[silver] clean customer snapshots={n_clean}")

    clean_df.write.mode("overwrite").partitionBy("snapshot_date").parquet(silver_path)
    print(f"[silver] wrote clean customer profiles -> {silver_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake-root", type=str, default=str(DEFAULT_LAKE_ROOT))
    parser.add_argument("--bronze-root", type=str, default=None)
    parser.add_argument("--silver-root", type=str, default=None)
    args = parser.parse_args()
    run(
        Path(args.lake_root),
        bronze_root=Path(args.bronze_root) if args.bronze_root else None,
        silver_root=Path(args.silver_root) if args.silver_root else None,
    )
