"""
Mock streaming producer — stands in for a Kafka/Pub-Sub producer + consumer pair.

See docs/adr/0002-mock-streaming-vs-real-kafka.md for why this is a local Python
generator instead of a real broker, and exactly what would change to point this at one.

Design:
    `generate_sales_events()` and `generate_customer_snapshots()` are plain Python
    generators — the same interface a real Kafka consumer loop would present to
    downstream code (`for event in consumer: ...`). `run_producer()` pulls batches off
    those generators and writes them to Parquet in the Bronze landing zone, simulating
    the "producer writes, Bronze lands" hop atomically per micro-batch.

Realism knobs (so Silver has real cleaning work to do):
    - ~1.5% of sales events are exact duplicates (same event_id re-emitted), simulating
      at-least-once delivery semantics from a real broker.
    - ~2% of events are "late arrivals" — event_ts is backdated by 1-6 hours relative to
      ingest_ts, simulating network/buffering delay.
    - ~0.5% of events have a malformed field (negative price, empty product_id) to
      exercise Silver's validation/quarantine logic.
"""
from __future__ import annotations

import argparse
import csv
import random
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from schemas import SalesEvent, CustomerProfileSnapshot, SCHEMA_VERSION

random.seed(7)

HERE = Path(__file__).resolve().parent
SEEDS_DIR = HERE.parent / "gold" / "seeds"
DEFAULT_LAKE_ROOT = HERE.parent / "data_lake"

DUPLICATE_RATE = 0.015
LATE_ARRIVAL_RATE = 0.02
MALFORMED_RATE = 0.005
N_PARTITIONS = 4


def _load_ids(csv_path: Path, column: str) -> list[str]:
    with open(csv_path, newline="") as f:
        return [row[column] for row in csv.DictReader(f)]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_sales_events(
    n_events: int,
    product_ids: list[str],
    store_ids: list[str],
    customer_ids: list[str],
    start_ts: datetime,
) -> Iterator[SalesEvent]:
    """Yields synthetic sales events, injecting duplicates/late-arrivals/malformed rows."""
    emitted_ids: list[str] = []
    t = start_ts

    for i in range(n_events):
        t = t + timedelta(seconds=random.expovariate(1 / 8))  # ~8s between events
        event_id = str(uuid.uuid4())
        emitted_ids.append(event_id)

        event_type = random.choices(
            ["sale", "refund", "void"], weights=[0.94, 0.05, 0.01]
        )[0]
        quantity = random.randint(1, 5) if event_type == "sale" else -random.randint(1, 2)
        unit_price = round(random.uniform(15, 3500), 2)
        discount = round(random.choice([0, 0, 0, 0, unit_price * random.uniform(0.05, 0.3)]), 2)

        customer_id = random.choice(customer_ids) if random.random() > 0.08 else None
        event_ts = t
        ingest_ts = t + timedelta(seconds=random.uniform(1, 4))

        is_late = random.random() < LATE_ARRIVAL_RATE
        if is_late:
            event_ts = t - timedelta(hours=random.uniform(1, 6))

        is_malformed = random.random() < MALFORMED_RATE

        event = SalesEvent(
            event_id=event_id,
            event_type=event_type,
            customer_id=customer_id,
            product_id=random.choice(product_ids) if not is_malformed else "",
            store_id=random.choice(store_ids),
            quantity=quantity if not is_malformed else 0,
            unit_price=unit_price if not is_malformed else round(-unit_price, 2),
            discount_amount=discount,
            event_ts=event_ts.isoformat(),
            ingest_ts=ingest_ts.isoformat(),
            source_partition=f"shard-{random.randint(0, N_PARTITIONS - 1)}",
            schema_version=SCHEMA_VERSION,
        )
        yield event

        if random.random() < DUPLICATE_RATE and emitted_ids:
            dup_delay = ingest_ts + timedelta(seconds=random.uniform(0.5, 5))
            yield SalesEvent(**{**asdict(event), "ingest_ts": dup_delay.isoformat()})


def _stable_seed(*parts: str) -> int:
    """
    Deterministic seed derived from arbitrary string parts via md5, unlike Python's
    built-in `hash()` which is randomized per-process (PYTHONHASHSEED) and would make
    "the same customer on the same day" produce different data on every run.
    """
    import hashlib

    digest = hashlib.md5("|".join(parts).encode()).hexdigest()
    return int(digest[:8], 16)


def generate_customer_snapshots(
    customer_ids: list[str],
    snapshot_ts: datetime,
    ingest_date: str,
    changed_only: list[str] | None = None,
    store_ids: list[str] | None = None,
) -> Iterator[CustomerProfileSnapshot]:
    """
    Yields one profile snapshot per customer for the given `ingest_date`, or, if
    `changed_only` is given, only for that subset of customers (simulating an
    incremental CDC-style feed).

    Each customer's *baseline* tier/home store is stable across runs (seeded on
    customer_id alone), but ~4% of customers "drift" (tier or home store changes) on
    any given ingest_date (seeded on customer_id + ingest_date), so that running the
    producer across several ingest dates produces genuine day-over-day profile changes
    for dim_customer_scd2 (see docs/adr/0004-scd2-merge-strategy.md) to detect.
    """
    import faker

    fake = faker.Faker()
    ids = changed_only if changed_only is not None else customer_ids
    tiers = ["bronze", "silver", "gold", "platinum"]
    tier_weights = [0.5, 0.3, 0.15, 0.05]
    stores = store_ids or [None]

    for cid in ids:
        base_rng = random.Random(_stable_seed(cid))
        base_tier = base_rng.choices(tiers, weights=tier_weights)[0]
        base_store = base_rng.choice(stores) if store_ids else None
        full_name = fake_name_for(fake, base_rng)
        email = f"{full_name.lower().replace(' ', '.')}@example.com"

        day_rng = random.Random(_stable_seed(cid, ingest_date))
        tier = base_tier
        home_store = base_store
        if day_rng.random() < 0.04:
            # drift event: bump tier by one level (capped at platinum)
            idx = min(tiers.index(base_tier) + 1, len(tiers) - 1)
            tier = tiers[idx]
        if store_ids and day_rng.random() < 0.02:
            home_store = day_rng.choice(store_ids)

        yield CustomerProfileSnapshot(
            customer_id=cid,
            full_name=full_name,
            email=email,
            loyalty_tier=tier,
            home_store_id=home_store,
            snapshot_ts=snapshot_ts.isoformat(),
            ingest_ts=_now_iso(),
            schema_version=SCHEMA_VERSION,
        )


def fake_name_for(fake, rng: "random.Random") -> str:
    """Deterministic name generation: seeds Faker's own per-instance RNG (not the
    global `random` module, which Faker does not use internally) so the same
    customer_id always yields the same name across separate producer runs."""
    fake.seed_instance(rng.randint(0, 2**31 - 1))
    return fake.name()


def write_batch_to_parquet(records: list[dict], out_dir: Path, batch_name: str) -> Path:
    """
    Bronze sink. This is the single function that would change if swapping this producer
    for a real Kafka consumer writing to the same Bronze contract (see ADR 0002).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    out_path = out_dir / f"{batch_name}.parquet"
    pq.write_table(table, out_path)
    return out_path


def run_producer(
    n_events: int = 5000,
    lake_root: Path = DEFAULT_LAKE_ROOT,
    ingest_date: str | None = None,
) -> None:
    product_ids = _load_ids(SEEDS_DIR / "ref_products.csv", "product_id")
    store_ids = _load_ids(SEEDS_DIR / "ref_stores.csv", "store_id")
    customer_ids = _load_ids(SEEDS_DIR / "ref_customer_ids.csv", "customer_id")

    ingest_date = ingest_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start_ts = datetime.now(timezone.utc) - timedelta(hours=8)

    bronze_root = lake_root / "bronze"

    # --- sales_events ---
    sales_batch = [
        e.to_dict()
        for e in generate_sales_events(n_events, product_ids, store_ids, customer_ids, start_ts)
    ]
    sales_dir = bronze_root / "sales_events" / f"ingest_date={ingest_date}"
    sales_path = write_batch_to_parquet(sales_batch, sales_dir, "batch_0001")
    print(f"[bronze] wrote {len(sales_batch)} sales events -> {sales_path}")

    # --- customer_profile_snapshots (full refresh each run) ---
    # snapshot_ts is pinned to the simulated ingest_date (not wall-clock "now") so that
    # multiple producer runs on the same real day still land distinct, dedupable daily
    # snapshots -- this is what gives dim_customer_scd2 real day-over-day history to
    # detect changes across, instead of every run collapsing into "today".
    snapshot_ts = datetime.strptime(ingest_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    profile_batch = [
        p.to_dict()
        for p in generate_customer_snapshots(
            customer_ids, snapshot_ts, ingest_date=ingest_date, store_ids=store_ids
        )
    ]
    profile_dir = bronze_root / "customer_profile_snapshots" / f"ingest_date={ingest_date}"
    profile_path = write_batch_to_parquet(profile_batch, profile_dir, "batch_0001")
    print(f"[bronze] wrote {len(profile_batch)} customer snapshots -> {profile_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock Bronze streaming producer")
    parser.add_argument("--n-events", type=int, default=5000)
    parser.add_argument("--lake-root", type=str, default=str(DEFAULT_LAKE_ROOT))
    parser.add_argument("--ingest-date", type=str, default=None)
    args = parser.parse_args()

    run_producer(
        n_events=args.n_events,
        lake_root=Path(args.lake_root),
        ingest_date=args.ingest_date,
    )
