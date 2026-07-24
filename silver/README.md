# Silver layer

PySpark batch jobs that turn raw Bronze Parquet into cleaned, deduplicated,
schema-validated event data. See `docs/design.md` §3.3/3.4 for the target schemas.

## Files

- `spark_session.py` — shared local `SparkSession` factory.
- `transforms.py` — **pure transformation functions** (DataFrame in, DataFrame out),
  deliberately separated from the job entrypoints so they're unit-testable without file
  I/O. See `tests/test_silver_transforms.py`.
- `clean_transactions.py` — job entrypoint: Bronze `sales_events` → Silver
  `sales_events_clean` (+ a `_quarantine` sink for rows that fail validation).
- `clean_customers.py` — job entrypoint: Bronze `customer_profile_snapshots` → Silver
  `customer_profiles_clean`.

## What the cleaning actually does

- **Dedup**: `sales_events` deduped on `event_id` (keep latest `ingest_ts`), simulating
  at-least-once broker delivery. `customer_profile_snapshots` deduped on
  `(customer_id, snapshot_date)`.
- **Validation / quarantine**: sales events with an empty `product_id`/`store_id`, zero
  quantity, negative price, or invalid `event_type` are routed to
  `data_lake/silver/_quarantine/sales_events/` instead of being silently dropped.
- **Enrichment**: derives `line_amount`, flags `is_late_arrival` (event_ts more than 1
  hour before ingest_ts), coalesces nullable `discount_amount` to 0.

## Run it

```bash
pip install -r ../requirements.txt
cd silver
python clean_transactions.py --lake-root ../data_lake
python clean_customers.py --lake-root ../data_lake
```

Requires Bronze data to already exist (`python ../bronze/producer.py` first) and a local
Java runtime (PySpark dependency) — see the repo root `README.md` for setup notes.

Both jobs also accept `--bronze-root` / `--silver-root` to point Bronze reads and Silver
writes at different locations independently (useful if Bronze lives in one bucket/tier
and you want Silver output somewhere else) -- they default to `<lake-root>/bronze` and
`<lake-root>/silver`.

Both outputs are **partitioned** (`sales_events_clean` by `event_date`,
`customer_profiles_clean` by `snapshot_date`) and written with Spark's dynamic partition
overwrite mode (`silver/spark_session.py`), so re-running a job only replaces the
partitions present in that run's input instead of clearing the entire output dataset --
safe to re-run per ingest date without reprocessing everything.
