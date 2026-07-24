"""
Unit tests for silver/transforms.py -- the pure PySpark transformation functions,
tested directly against small in-memory DataFrames (no file I/O, no Bronze/Silver
fixtures on disk). This is the "GitHub Actions running pytest unit tests on the
PySpark transformation logic" requirement for the capstone.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "silver"))

from transforms import (  # noqa: E402
    clean_customer_profiles,
    clean_sales_events,
    dedupe_sales_events,
    enrich_sales_events,
    quarantine_malformed_sales_events,
)

def _sales_row(**overrides):
    base = dict(
        event_id="evt-1",
        event_type="sale",
        customer_id="CUST-001",
        product_id="PROD-001",
        store_id="STORE-01",
        quantity=2,
        unit_price=100.0,
        discount_amount=0.0,
        event_ts="2026-07-20T10:00:00",
        ingest_ts="2026-07-20T10:00:02",
        source_partition="shard-0",
        schema_version="v1",
    )
    base.update(overrides)
    return base


def test_dedupe_sales_events_keeps_latest_ingest_ts(spark):
    rows = [
        _sales_row(event_id="evt-1", ingest_ts="2026-07-20T10:00:02", quantity=1),
        _sales_row(event_id="evt-1", ingest_ts="2026-07-20T10:00:05", quantity=2),  # dup, later
    ]
    df = spark.createDataFrame(rows)
    result = dedupe_sales_events(df).collect()
    assert len(result) == 1
    assert result[0]["quantity"] == 2  # the later-ingested duplicate wins


def test_quarantine_malformed_sales_events_splits_correctly(spark):
    rows = [
        _sales_row(event_id="ok-1"),
        _sales_row(event_id="bad-1", product_id=""),           # empty product_id
        _sales_row(event_id="bad-2", quantity=0),               # zero quantity
        _sales_row(event_id="bad-3", unit_price=-5.0),           # negative price
        _sales_row(event_id="bad-4", event_type="unknown"),      # invalid event_type
    ]
    df = spark.createDataFrame(rows)
    valid_df, quarantine_df = quarantine_malformed_sales_events(df)

    assert valid_df.count() == 1
    assert valid_df.collect()[0]["event_id"] == "ok-1"
    assert quarantine_df.count() == 4


def test_enrich_sales_events_computes_line_amount_and_late_flag(spark):
    rows = [
        _sales_row(
            event_id="evt-1",
            quantity=3,
            unit_price=50.0,
            discount_amount=10.0,
            event_ts="2026-07-20T10:00:00",
            ingest_ts="2026-07-20T10:00:05",  # 5 seconds later -- not late
        ),
        _sales_row(
            event_id="evt-2",
            quantity=1,
            unit_price=20.0,
            discount_amount=None,
            event_ts="2026-07-20T08:00:00",
            ingest_ts="2026-07-20T10:00:00",  # 2 hours later -- late arrival
        ),
    ]
    # NOTE: deliberately NOT passing an explicit `schema=[col names]` here -- doing so
    # with a list of dicts is a real PySpark footgun: Spark binds the given names
    # *positionally* against the dict's (alphabetically-sorted) keys rather than by
    # key name, silently scrambling every column. Passing no schema (or a full
    # StructType) makes Spark infer field names from the dict keys correctly instead.
    df = spark.createDataFrame(rows)
    result = {r["event_id"]: r for r in enrich_sales_events(df).collect()}

    assert result["evt-1"]["line_amount"] == 140.0  # 3*50 - 10
    assert result["evt-1"]["is_late_arrival"] is False

    assert result["evt-2"]["discount_amount"] == 0.0  # null coalesced to 0
    assert result["evt-2"]["line_amount"] == 20.0
    assert result["evt-2"]["is_late_arrival"] is True


def test_clean_sales_events_end_to_end(spark):
    rows = [
        _sales_row(event_id="evt-1"),
        _sales_row(event_id="evt-1", ingest_ts="2026-07-20T10:00:09"),  # duplicate
        _sales_row(event_id="evt-2", product_id=""),  # malformed -> quarantined
    ]
    df = spark.createDataFrame(rows)
    clean_df, quarantine_df = clean_sales_events(df)

    assert clean_df.count() == 1
    assert quarantine_df.count() == 1
    clean_cols = set(clean_df.columns)
    assert {"line_amount", "is_late_arrival", "silver_loaded_at"}.issubset(clean_cols)


def _profile_row(**overrides):
    base = dict(
        customer_id="CUST-001",
        full_name="  Jane Doe  ",
        email="JANE.DOE@Example.com",
        loyalty_tier="gold",
        home_store_id="STORE-01",
        snapshot_ts="2026-07-20T00:00:00",
        ingest_ts="2026-07-20T00:05:00",
        schema_version="v1",
    )
    base.update(overrides)
    return base


def test_clean_customer_profiles_dedupes_and_normalizes(spark):
    rows = [
        _profile_row(snapshot_ts="2026-07-20T00:00:00", ingest_ts="2026-07-20T00:05:00"),
        _profile_row(snapshot_ts="2026-07-20T00:00:00", ingest_ts="2026-07-20T00:09:00"),  # dup same day
        _profile_row(customer_id="CUST-002", loyalty_tier="not_a_real_tier"),  # invalid, dropped
    ]
    df = spark.createDataFrame(rows)
    result = clean_customer_profiles(df).collect()

    assert len(result) == 1  # CUST-001 deduped to 1, CUST-002 filtered out (invalid tier)
    row = result[0]
    assert row["email"] == "jane.doe@example.com"
    assert row["full_name"] == "Jane Doe"
