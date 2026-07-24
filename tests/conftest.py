"""Shared pytest fixtures: a single local SparkSession reused across the test session
(creating a new one per test is slow and unnecessary for these small unit tests)."""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "silver"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bronze"))


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("pytest-silver-transforms")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # Pin the driver host to loopback -- avoids hostname-resolution failures in
        # sandboxed/CI environments where the container hostname isn't resolvable
        # (the same fix applied via SPARK_LOCAL_IP when running the Silver jobs
        # directly -- see silver/README.md).
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
