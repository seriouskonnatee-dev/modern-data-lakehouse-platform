"""Shared SparkSession factory for the Silver jobs."""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")  # small local dataset, keep it light
        # Dynamic partition overwrite: re-running a Silver job only replaces the
        # event_date partitions present in the new batch, instead of nuking the entire
        # output directory first -- the correct, idempotent behavior for incremental
        # reprocessing of a partitioned dataset.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
