# Bronze layer

Raw, append-only landing zone. See `docs/design.md` §2.1/§3.1-3.2 for the full contract
and `docs/adr/0002-mock-streaming-vs-real-kafka.md` for why this is a local Python
generator rather than a real Kafka/Pub-Sub producer.

## Files

- `schemas.py` — dataclass definitions for `SalesEvent` and `CustomerProfileSnapshot`,
  the Bronze contract shared with Silver.
- `reference_data.py` — generates static product/store/customer reference data (via
  Faker) and writes it to `gold/seeds/` as dbt seeds. Run this once before `producer.py`.
- `producer.py` — the mock streaming producer. Generates synthetic sales events and
  customer profile snapshots and lands them as Parquet in `data_lake/bronze/`
  (or a MinIO bucket when run through `docker-compose`), injecting realistic messiness
  (duplicates, late arrivals, malformed rows) for Silver to clean up.

## Run it

```bash
cd bronze
pip install -r ../requirements.txt
python reference_data.py          # writes gold/seeds/*.csv (run once)
python producer.py --n-events 5000 --ingest-date 2026-07-24
```

This lands two Parquet datasets under `data_lake/bronze/`:

```
data_lake/bronze/
├── sales_events/ingest_date=2026-07-24/batch_0001.parquet
└── customer_profile_snapshots/ingest_date=2026-07-24/batch_0001.parquet
```

Re-run with a different `--ingest-date` to simulate another day's traffic and exercise
the incremental Silver/Gold logic across multiple batches.

## Simulating profile changes (for SCD2 downstream)

To generate a second, incremental customer snapshot batch (some customers changed tier),
edit `producer.py`'s `run_producer()` call to pass `changed_only=[...]` to
`generate_customer_snapshots()`, or simply re-run `producer.py` with a later
`--ingest-date` — every run currently does a full refresh, which is also a valid (if
less efficient) input to the Silver/Gold dedup and SCD2 logic, since both are keyed on
`(customer_id, snapshot_ts)`, not on batch identity.
