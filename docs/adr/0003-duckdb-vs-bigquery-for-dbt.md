# ADR 0003: Use DuckDB (not a live BigQuery project) as the dbt target for Gold

## Status
Accepted

## Context
`dbt` needs a warehouse to materialize Gold models into. The design targets BigQuery in
production (and `infra/` provisions BigQuery datasets via Terraform for exactly that
reason), but this repo needs to be runnable by anyone who clones it, without requiring
a GCP account, billing setup, or credentials.

## Decision
The committed dbt profile (`gold/profiles.yml.example`) targets **DuckDB** by default,
reading Silver Parquet output directly (`read_parquet('../data_lake/silver/**/*.parquet')`)
and materializing Gold models as local DuckDB tables. A second, commented-out profile
target (`bigquery`) is included and documented, matching the datasets provisioned in
`infra/bigquery.tf`, so the migration path from "runs on a laptop" to "runs on the
provisioned BigQuery datasets" is a one-line `--target` change plus `gcloud auth`, not a
rewrite of any model SQL. dbt-core is warehouse-agnostic by design (models are ANSI-ish
SQL with Jinja); this project intentionally avoids BigQuery-only SQL extensions in the
Gold models so both targets stay valid.

## Alternatives considered
- **Stand up a real, free-tier BigQuery project and commit a service account key
  reference**: rejected — committing any live cloud dependency into a portfolio repo
  is either a security risk (real credentials) or non-functional for reviewers (a
  service account that only the author can use), and the task instructions explicitly
  say not to run `terraform apply` with real credentials for this project.
- **Use SQLite instead of DuckDB**: rejected — DuckDB has native Parquet support, better
  analytical SQL (window functions, `QUALIFY`, etc.) matching what BigQuery supports,
  and is the de facto standard "warehouse-in-a-laptop" for portfolio/demo dbt projects.

## Consequences
- `dbt build` runs end-to-end with zero cloud dependencies — a strong "clone and run"
  story for recruiters.
- The BigQuery target profile is documented but untested in CI (no live GCP project to
  test against); this is called out in the README rather than silently assumed to work.
