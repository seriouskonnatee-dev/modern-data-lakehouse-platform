# ADR 0001: Adopt a medallion (Bronze/Silver/Gold) architecture

## Status
Accepted

## Context
Raw POS sales events arrive continuously, are not deduplicated at the source, can arrive
late, and occasionally violate the nominal schema (missing fields, out-of-range values).
Downstream consumers need three different things from the same underlying data:
data scientists want maximally raw/complete history for feature engineering; BI/analytics
engineers want clean, business-modeled, query-fast tables; auditors/debuggers want to be
able to trace a Gold-layer number back to the exact raw event that produced it.

## Decision
Split storage and processing into three layers with a strict one-directional dependency
(Bronze → Silver → Gold), each with a distinct contract:

- **Bronze**: raw, append-only, schema-on-write only in the loosest sense (Parquet with
  the producer's declared schema, no cleaning). Never mutated, never deleted. This is
  the system of record — if Silver or Gold logic has a bug, they are rebuilt from Bronze.
- **Silver**: cleaned, deduplicated, schema-and-range-validated, but still event-grained
  and free of business semantics (no star schema yet). This is what data scientists and
  ad hoc analysts should query.
- **Gold**: dimensionally modeled, business-semantic, BI-tool-ready. This is what
  dashboards and reports query, and the only layer expected to have SLAs on freshness.

## Alternatives considered
- **Single-hop ETL (raw → one clean table)**: rejected because it conflates "did we
  receive this data" with "is this data business-correct," making it impossible to
  replay/debug when transformation logic changes without re-extracting from the source
  system (which, for a real streaming source, may not even retain history).
- **ELT straight into the warehouse with no lake layer**: rejected because Bronze/Silver
  benefit from cheap, schema-flexible object storage (Parquet on MinIO/filesystem) and
  Spark-native processing, whereas Gold benefits from a real query engine (DuckDB/BigQuery)
  with dbt-managed transformations — forcing everything into the warehouse from the start
  loses the cost and flexibility benefits of the lake tier for the highest-volume, least
  business-modeled layer.

## Consequences
- More moving parts (three storage locations, two processing engines, one orchestrator)
  than a single script — justified here because the goal is to demonstrate the
  architecture pattern used at scale by real data platforms, not to minimize code volume.
- Bronze is replayable: any Silver/Gold bug fix can be applied by re-running the pipeline
  from Bronze without re-ingesting from the (simulated) source.
- Requires explicit contracts (schemas, dedup keys) at each hop, documented in
  `docs/design.md` §3, so that layers can be developed/tested independently.
