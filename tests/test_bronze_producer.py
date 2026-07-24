"""Unit tests for bronze/producer.py -- no Spark needed, no file I/O."""
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bronze"))

from producer import (  # noqa: E402
    _stable_seed,
    generate_customer_snapshots,
    generate_sales_events,
)
from schemas import SalesEvent  # noqa: E402


def test_stable_seed_is_deterministic_across_calls():
    a = _stable_seed("CUST-000001", "2026-07-20")
    b = _stable_seed("CUST-000001", "2026-07-20")
    assert a == b


def test_stable_seed_differs_by_input():
    assert _stable_seed("CUST-000001", "2026-07-20") != _stable_seed("CUST-000002", "2026-07-20")
    assert _stable_seed("CUST-000001", "2026-07-20") != _stable_seed("CUST-000001", "2026-07-21")


def test_customer_snapshots_are_deterministic_for_same_day():
    """Same customer + same ingest_date must produce the same tier/store across
    independent calls -- this is what makes multiple producer runs on the same
    simulated day dedupe cleanly in Silver instead of looking like spurious changes."""
    snapshot_ts = datetime(2026, 7, 20, tzinfo=timezone.utc)
    store_ids = ["STORE-01", "STORE-02", "STORE-03"]

    run1 = list(
        generate_customer_snapshots(
            ["CUST-000001", "CUST-000002"], snapshot_ts, "2026-07-20", store_ids=store_ids
        )
    )
    run2 = list(
        generate_customer_snapshots(
            ["CUST-000001", "CUST-000002"], snapshot_ts, "2026-07-20", store_ids=store_ids
        )
    )

    for r1, r2 in zip(run1, run2):
        assert r1.loyalty_tier == r2.loyalty_tier
        assert r1.home_store_id == r2.home_store_id
        assert r1.full_name == r2.full_name


def test_generate_sales_events_yields_valid_dataclasses():
    product_ids = ["PROD-001", "PROD-002"]
    store_ids = ["STORE-01"]
    customer_ids = ["CUST-000001"]
    start_ts = datetime(2026, 7, 20, tzinfo=timezone.utc)

    events = list(
        generate_sales_events(50, product_ids, store_ids, customer_ids, start_ts)
    )

    assert len(events) >= 50  # may be more due to injected duplicates
    assert all(isinstance(e, SalesEvent) for e in events)
    assert all(e.store_id == "STORE-01" for e in events)
    assert all(e.event_type in ("sale", "refund", "void") for e in events)


def test_generate_sales_events_injects_some_late_arrivals_and_malformed_rows():
    """With a large enough sample, the realism knobs (LATE_ARRIVAL_RATE,
    MALFORMED_RATE) should produce at least a few of each -- this is a statistical
    smoke test, not an exact-count assertion, since the rates are probabilistic."""
    product_ids = [f"PROD-{i:03d}" for i in range(20)]
    store_ids = [f"STORE-{i:02d}" for i in range(5)]
    customer_ids = [f"CUST-{i:06d}" for i in range(100)]
    start_ts = datetime(2026, 7, 20, tzinfo=timezone.utc)

    events = list(
        generate_sales_events(3000, product_ids, store_ids, customer_ids, start_ts)
    )

    malformed = [e for e in events if e.product_id == "" or e.unit_price < 0]
    late = [e for e in events if e.event_ts < e.ingest_ts and
            (datetime.fromisoformat(e.ingest_ts) - datetime.fromisoformat(e.event_ts)).total_seconds() > 3600]

    assert len(malformed) > 0
    assert len(late) > 0
