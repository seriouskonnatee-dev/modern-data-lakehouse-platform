"""
Airflow DAG orchestrating the Bronze -> Silver -> Gold medallion pipeline.

Design notes (see docs/design.md for the full architecture):
  - Bronze ingestion (`produce_bronze_events`) simulates the streaming producer landing
    a batch for the day. In production this task would be replaced by a sensor waiting
    on a real Kafka/Pub-Sub topic's watermark rather than actively producing data --
    see docs/adr/0002-mock-streaming-vs-real-kafka.md.
  - A `FileSensor`-style check (`bronze_landing_sensor`) gates Silver so Silver never
    starts against a partially-written Bronze batch.
  - Silver's two jobs (sales events, customer profiles) run in parallel -- they're
    independent PySpark jobs with no cross-dependency.
  - Gold (`dbt build`) only starts once BOTH Silver jobs succeed, since
    `dim_customer_scd2` and `fct_sales` both read Silver output.
  - `pipeline_health_check` runs last, always (even on upstream failure via
    trigger_rule), so the health report reflects the actual run outcome.
  - Retries + SLAs are set per task based on how expensive/flaky each step realistically
    is: Spark jobs get more retries (transient resource contention is common), dbt build
    gets a tighter SLA because Gold is what BI dashboards depend on for freshness.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

REPO_ROOT = "/opt/airflow/repo"  # mount point when running via docker-compose

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_medallion_pipeline",
    description="Bronze -> Silver -> Gold medallion pipeline for retail sales events",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # daily at 03:00, after the simulated trading day closes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "portfolio"],
) as dag:

    produce_bronze_events = BashOperator(
        task_id="produce_bronze_events",
        bash_command=(
            f"cd {REPO_ROOT}/bronze && "
            "python producer.py --n-events 5000 --ingest-date {{ ds }} "
            f"--lake-root {REPO_ROOT}/data_lake"
        ),
        retries=1,  # producer is idempotent-ish per ingest_date but not free of side effects
        sla=timedelta(minutes=15),
    )

    bronze_landing_sensor = FileSensor(
        task_id="bronze_landing_sensor",
        filepath=(
            f"{REPO_ROOT}/data_lake/bronze/sales_events/"
            "ingest_date={{ ds }}/batch_0001.parquet"
        ),
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",  # free up the worker slot while waiting
    )

    clean_transactions = BashOperator(
        task_id="silver_clean_transactions",
        bash_command=(
            f"cd {REPO_ROOT}/silver && "
            f"python clean_transactions.py --lake-root {REPO_ROOT}/data_lake"
        ),
        retries=3,  # Spark jobs are the most likely to hit transient resource issues
        sla=timedelta(minutes=30),
    )

    clean_customers = BashOperator(
        task_id="silver_clean_customers",
        bash_command=(
            f"cd {REPO_ROOT}/silver && "
            f"python clean_customers.py --lake-root {REPO_ROOT}/data_lake"
        ),
        retries=3,
        sla=timedelta(minutes=30),
    )

    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=(
            f"cd {REPO_ROOT}/gold && "
            "DBT_PROFILES_DIR={{ params.profiles_dir }} dbt build"
        ),
        params={"profiles_dir": f"{REPO_ROOT}/gold"},
        retries=1,  # dbt failures are usually deterministic (bad SQL/data), retrying rarely helps
        sla=timedelta(minutes=20),  # tightest SLA -- Gold freshness is what BI depends on
    )

    pipeline_health_check = PythonOperator(
        task_id="pipeline_health_check",
        python_callable=lambda: __import__("subprocess").run(
            ["python", f"{REPO_ROOT}/scripts/pipeline_health_check.py"], check=False
        ),
        trigger_rule="all_done",  # always run, even if an upstream task failed --
                                  # a health report on a failed run is the most useful one
    )

    (
        produce_bronze_events
        >> bronze_landing_sensor
        >> [clean_transactions, clean_customers]
        >> dbt_build_gold
        >> pipeline_health_check
    )
