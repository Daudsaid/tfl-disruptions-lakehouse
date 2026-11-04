import os
import time

import pytest

# Spark returns naive datetimes in the machine's local time zone; pin it so tests pass anywhere (e.g. a UK Mac in BST).
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    """One small local Spark session for all tests. Skipped where PySpark isn't installed (e.g. on the Mac)."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
