"""
Lightweight pipeline-health summary -- the "observability" half of requirement (f)
(dbt tests are the "data quality" half). Run after `dbt build` completes.

Reads:
  - the Gold DuckDB database, for row counts and freshness of key marts
  - the Silver quarantine sink, for how many raw sales events failed Silver validation
  - the most recent `dbt build` run_results.json, for test pass/fail counts

Writes a human-readable Markdown summary to docs/pipeline_health_report.md (a sample
run's output is committed to the repo so a reviewer can see it without running anything).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
GOLD_DB = REPO_ROOT / "gold" / "lakehouse_gold.duckdb"
RUN_RESULTS = REPO_ROOT / "gold" / "target" / "run_results.json"
QUARANTINE_DIR = REPO_ROOT / "data_lake" / "silver" / "_quarantine" / "sales_events"
OUT_PATH = REPO_ROOT / "docs" / "pipeline_health_report.md"


def get_table_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    tables = [
        "marts.dim_customer_scd2",
        "marts.dim_product",
        "marts.dim_store",
        "marts.dim_date",
        "marts.fct_sales",
        "marts.mart_daily_store_sales",
        "marts.mart_customer_ltv",
    ]
    counts = {}
    for t in tables:
        try:
            counts[t] = con.execute(f"select count(*) from {t}").fetchone()[0]
        except Exception as e:  # noqa: BLE001 - report, don't crash the health check
            counts[t] = f"error: {e}"
    return counts


def get_quarantine_count() -> int:
    if not QUARANTINE_DIR.exists():
        return 0
    try:
        con = duckdb.connect()
        n = con.execute(
            f"select count(*) from read_parquet('{QUARANTINE_DIR}/**/*.parquet')"
        ).fetchone()[0]
        con.close()
        return n
    except Exception:
        return 0


def get_test_summary() -> dict:
    if not RUN_RESULTS.exists():
        return {"status": "no run_results.json found -- run `dbt build` first"}
    with open(RUN_RESULTS) as f:
        results = json.load(f)
    outcomes = {}
    for r in results.get("results", []):
        status = r.get("status", "unknown")
        outcomes[status] = outcomes.get(status, 0) + 1
    return outcomes


def main():
    lines = [
        "# Pipeline Health Report",
        "",
        f"_Generated {datetime.now(timezone.utc).isoformat()}_",
        "",
        "This is a sample output of `scripts/pipeline_health_check.py`, committed so a "
        "reviewer can see the pipeline's observability output without running the full "
        "stack. Regenerate it locally with `python scripts/pipeline_health_check.py` "
        "after `dbt build`.",
        "",
        "## Gold layer row counts",
        "",
        "| Table | Row count |",
        "|---|---|",
    ]

    if GOLD_DB.exists():
        con = duckdb.connect(str(GOLD_DB), read_only=True)
        counts = get_table_counts(con)
        con.close()
        for table, count in counts.items():
            lines.append(f"| `{table}` | {count} |")
    else:
        lines.append("| _(gold database not found -- run `dbt build` first)_ | - |")

    lines += [
        "",
        "## Silver validation",
        "",
        f"- Quarantined (failed-validation) raw sales events: **{get_quarantine_count()}**",
        "",
        "## dbt test summary",
        "",
    ]
    test_summary = get_test_summary()
    if "status" in test_summary:
        lines.append(f"_{test_summary['status']}_")
    else:
        for status, count in test_summary.items():
            lines.append(f"- {status}: {count}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text("\n".join(lines) + "\n")
    print(f"Wrote health report -> {OUT_PATH}")


if __name__ == "__main__":
    main()
