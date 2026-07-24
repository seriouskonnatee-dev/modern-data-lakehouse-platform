"""
Pure PySpark transformation logic for the Silver layer, split out from the job
entrypoints (`clean_sales_events.py`, `clean_customer_profiles.py`) specifically so it
can be unit tested without spinning up file I/O — `tests/test_silver_transforms.py`
imports these functions directly and asserts against small in-memory DataFrames built
with a local SparkSession fixture.

Implements the Bronze -> Silver contract in docs/design.md §3.3/3.4.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LATE_ARRIVAL_THRESHOLD_HOURS = 1
VALID_EVENT_TYPES = ["sale", "refund", "void"]
VALID_LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]


def dedupe_sales_events(df: DataFrame) -> DataFrame:
    """
    Keep exactly one row per event_id: the one with the latest ingest_ts, mirroring
    at-least-once delivery semantics from an upstream broker (see ADR 0002).
    """
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def quarantine_malformed_sales_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Splits sales events into (valid, quarantined) based on Silver's validation rules:
    non-empty product_id/store_id, non-zero quantity, non-negative unit_price.
    Returns (valid_df, quarantined_df) so callers can persist rejects for observability
    instead of silently dropping them.
    """
    is_valid = (
        (F.col("product_id") != "")
        & (F.col("store_id") != "")
        & (F.col("quantity") != 0)
        & (F.col("unit_price") >= 0)
        & (F.col("event_type").isin(VALID_EVENT_TYPES))
    )
    valid_df = df.filter(is_valid)
    quarantined_df = df.filter(~is_valid)
    return valid_df, quarantined_df


def enrich_sales_events(df: DataFrame) -> DataFrame:
    """Derives line_amount and is_late_arrival, coalesces nullable discount_amount."""
    df = df.withColumn(
        "discount_amount", F.coalesce(F.col("discount_amount"), F.lit(0.0))
    )
    df = df.withColumn(
        "line_amount",
        F.round(
            F.col("quantity") * F.col("unit_price") - F.col("discount_amount"), 2
        ),
    )
    df = df.withColumn("event_ts", F.to_timestamp("event_ts"))
    df = df.withColumn("ingest_ts", F.to_timestamp("ingest_ts"))
    df = df.withColumn(
        "is_late_arrival",
        (F.col("ingest_ts").cast("long") - F.col("event_ts").cast("long"))
        > (LATE_ARRIVAL_THRESHOLD_HOURS * 3600),
    )
    df = df.withColumn("silver_loaded_at", F.current_timestamp())
    return df


def clean_sales_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Full Bronze -> Silver pipeline for sales_events. Returns (clean_df, quarantine_df)."""
    deduped = dedupe_sales_events(df)
    valid, quarantined = quarantine_malformed_sales_events(deduped)
    enriched = enrich_sales_events(valid)
    select_cols = [
        "event_id",
        "event_type",
        "customer_id",
        "product_id",
        "store_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "line_amount",
        "event_ts",
        "silver_loaded_at",
        "is_late_arrival",
    ]
    return enriched.select(*select_cols), quarantined


def dedupe_customer_profiles(df: DataFrame) -> DataFrame:
    """One row per (customer_id, snapshot date): latest snapshot_ts wins for that day."""
    df = df.withColumn("_snapshot_date", F.to_date("snapshot_ts"))
    w = Window.partitionBy("customer_id", "_snapshot_date").orderBy(
        F.col("snapshot_ts").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_snapshot_date")
    )


def clean_customer_profiles(df: DataFrame) -> DataFrame:
    """Full Bronze -> Silver pipeline for customer_profile_snapshots."""
    deduped = dedupe_customer_profiles(df)
    valid = deduped.filter(F.col("loyalty_tier").isin(VALID_LOYALTY_TIERS))
    cleaned = (
        valid.withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("full_name", F.trim(F.col("full_name")))
        .withColumn("snapshot_ts", F.to_timestamp("snapshot_ts"))
        .withColumn("silver_loaded_at", F.current_timestamp())
    )
    select_cols = [
        "customer_id",
        "full_name",
        "email",
        "loyalty_tier",
        "home_store_id",
        "snapshot_ts",
        "silver_loaded_at",
    ]
    return cleaned.select(*select_cols)
