# ADR 0004: SCD Type 2 merge strategy for `dim_customer`

## Status
Accepted

## Context
`customer_profiles_clean` (Silver) is a series of point-in-time snapshots per customer
(one row per `customer_id` per day it was ingested, deduplicated to at most one row per
`customer_id` per `snapshot_ts`). Gold needs a single dimension table that preserves full
attribute history (`loyalty_tier`, `home_store_id`) so that `fct_sales` can join to the
attribute values that were true *at the time of the sale*, not the customer's current
attributes.

## Decision
Silver's `customer_profiles_clean` already retains one row per `(customer_id, snapshot
date)` across every producer run (Bronze is append-only and Silver dedupes per day, not
across days -- see `silver/transforms.py::dedupe_customer_profiles`), so the full change
history is available to Gold on every run, not just the latest state. Given that,
`dim_customer_scd2.sql` is implemented as a `table`-materialized dbt model that
**recomputes SCD2 history from the full snapshot history** using window functions,
rather than as a stateful incremental merge:

1. `LAG(loyalty_tier), LAG(home_store_id)` per `customer_id` ordered by `snapshot_ts`
   identify which consecutive snapshots actually represent a change versus a repeated,
   unchanged daily snapshot.
2. Only "change points" (first snapshot, or any snapshot where a tracked column differs
   from the prior one) are kept as dimension rows.
3. `effective_start_date = snapshot_ts` of the change point; `effective_end_date` is
   derived via `LEAD(snapshot_ts)` over the same window (`NULL` for the latest row per
   customer, i.e. `is_current = true`).
4. Because this recomputes from the full history each run rather than mutating state,
   it is trivially idempotent and correct under re-runs and backfills, at the cost of
   reprocessing full history every time -- an acceptable trade-off at this data volume
   (thousands of customers), called out explicitly rather than assumed to scale
   unboundedly.

`fct_sales.sql` resolves `customer_sk` by joining `sales_events_clean.event_ts` against
`dim_customer` on `event_ts BETWEEN effective_start_date AND COALESCE(effective_end_date, CURRENT_DATE)`,
so historical facts always resolve to the dimension row that was actually current when
the sale happened, even if the customer's tier has since changed.

## Alternatives considered
- **SCD Type 1 (overwrite in place)**: rejected — it would silently rewrite history,
  e.g. a customer's past sales would appear to have been made by their *current*
  loyalty tier, breaking tier-based commission/attribution reporting, which is the
  exact business requirement driving this design.
- **Full dbt `snapshot` feature (`{% snapshot %}` blocks)**: dbt's built-in snapshot
  feature is the idiomatic production choice for SCD2 when the source is a mutable table
  you can only see the *current* state of. It was not used here because the source
  (Silver) already retains full history as distinct rows, and a window-function
  recompute makes the SCD2 mechanics (compare, detect change points, derive effective
  dates) visible and explained in this portfolio repo rather than hidden behind a
  one-line macro -- the goal is demonstrating understanding of *how* SCD2 works. In a
  production repo with a mutable upstream source, `{% snapshot %}` would be the
  preferred, less error-prone choice, and this trade-off is called out explicitly.
- **True incremental `MERGE` model** (only process new snapshots, mutate existing
  dimension rows in place): considered, but DuckDB/dbt-duckdb's incremental+merge
  support is version-sensitive and adds operational complexity (tracking what "new"
  means across re-runs) that isn't justified at this data volume. Documented here as the
  natural next step if this pipeline moved to a warehouse with mature incremental-merge
  support (e.g. the BigQuery target in `docs/adr/0003-duckdb-vs-bigquery-for-dbt.md`)
  and higher data volumes made full-history recompute too slow.

## Consequences
- `fct_sales` joins are slightly more complex (date-range join instead of simple
  equality) but produce historically-accurate attribution.
- Re-running the pipeline is idempotent: recomputing from full history always produces
  the same dimension rows for unchanged customers.
- `dim_customer` grows over time (one row per change per customer) — acceptable for a
  low-cardinality, slow-changing dimension like customer loyalty tier.
- Full-history recompute means `dim_customer_scd2` is a `table`, not `incremental`,
  model — the trade-off is simplicity/correctness now for reprocessing cost later,
  documented so it isn't mistaken for an oversight.
