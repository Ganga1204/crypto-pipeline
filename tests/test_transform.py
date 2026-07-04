# tests/test_transform.py
#
# Unit tests for src/transform.py's Bronze -> Silver -> Gold logic.
# These use small hand-built Spark DataFrames, so they run in seconds
# with no S3, no real API calls, and no network access.

import pytest
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transform import bronze_to_silver, silver_to_gold


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("crypto-pipeline-tests")
        .master("local[2]")
        .getOrCreate()
    )


@pytest.fixture
def sample_bronze_df(spark, tmp_path):
    """Build a tiny bronze-layer parquet file on disk, matching real ingest.py columns."""
    data = [
        {
            "id": "bitcoin", "symbol": "btc", "name": "Bitcoin",
            "current_price": 65000.5, "market_cap": 1280000000000,
            "total_volume": 32000000000, "price_change_percentage_24h": 2.3,
            "last_updated": "2026-07-04T10:00:00.000Z",
        },
        {
            # bad row: negative price, should get filtered out in bronze_to_silver
            "id": "scamcoin", "symbol": "scam", "name": "ScamCoin",
            "current_price": -5.0, "market_cap": 1000, "total_volume": 10,
            "price_change_percentage_24h": 0.0, "last_updated": "2026-07-04T10:00:00.000Z",
        },
        {
            # bad row: null current_price, should get dropped
            "id": "nullcoin", "symbol": "null", "name": "NullCoin",
            "current_price": None, "market_cap": 500, "total_volume": 5,
            "price_change_percentage_24h": None, "last_updated": "2026-07-04T10:00:00.000Z",
        },
    ]
    df = spark.createDataFrame(data)
    path = str(tmp_path / "bronze.parquet")
    df.write.mode("overwrite").parquet(path)
    return path


def test_bronze_to_silver_drops_null_price_rows(spark, sample_bronze_df):
    silver_df = bronze_to_silver(spark, sample_bronze_df)
    ids = [r["id"] for r in silver_df.select("id").collect()]
    assert "nullcoin" not in ids


def test_bronze_to_silver_drops_non_positive_price_rows(spark, sample_bronze_df):
    silver_df = bronze_to_silver(spark, sample_bronze_df)
    ids = [r["id"] for r in silver_df.select("id").collect()]
    assert "scamcoin" not in ids


def test_bronze_to_silver_uppercases_symbol(spark, sample_bronze_df):
    silver_df = bronze_to_silver(spark, sample_bronze_df)
    symbols = [r["symbol"] for r in silver_df.select("symbol").collect()]
    assert "BTC" in symbols
    assert "btc" not in symbols


def test_bronze_to_silver_adds_processed_timestamp(spark, sample_bronze_df):
    silver_df = bronze_to_silver(spark, sample_bronze_df)
    assert "silver_processed_at" in silver_df.columns


def test_silver_to_gold_summary_has_expected_columns(spark, sample_bronze_df):
    silver_df = bronze_to_silver(spark, sample_bronze_df)
    gold_summary, gold_stats = silver_to_gold(silver_df)
    expected_cols = {
        "name", "symbol", "current_price", "market_cap",
        "total_volume", "price_change_percentage_24h",
    }
    assert expected_cols.issubset(set(gold_summary.columns))


def test_silver_to_gold_stats_has_one_row(spark, sample_bronze_df):
    silver_df = bronze_to_silver(spark, sample_bronze_df)
    gold_summary, gold_stats = silver_to_gold(silver_df)
    assert gold_stats.count() == 1


def test_silver_to_gold_summary_never_exceeds_20_rows(spark, sample_bronze_df):
    silver_df = bronze_to_silver(spark, sample_bronze_df)
    gold_summary, gold_stats = silver_to_gold(silver_df)
    assert gold_summary.count() <= 20
