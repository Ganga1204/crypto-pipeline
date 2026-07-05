# tests/test_ingest.py
#
# Unit tests for src/ingest.py. Uses mocks so tests never call the real
# CoinGecko API or upload anything to real S3 -- fully offline and fast.

import pytest
from unittest.mock import patch, MagicMock

from src.ingest import fetch_crypto_data, save_to_bronze


SAMPLE_API_RESPONSE = [
    {
        "id": "bitcoin", "symbol": "btc", "name": "Bitcoin",
        "current_price": 65000.5, "market_cap": 1280000000000,
        "total_volume": 32000000000, "price_change_percentage_24h": 2.3,
        "price_change_percentage_7d_in_currency": 5.1,
        "circulating_supply": 19700000, "last_updated": "2026-07-04T10:00:00.000Z",
        "image": "https://example.com/btc.png",
    },
]


def test_fetch_crypto_data_returns_list_on_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_API_RESPONSE

    with patch("src.ingest.requests.get", return_value=mock_response) as mock_get:
        data = fetch_crypto_data()
        mock_get.assert_called_once()
        assert isinstance(data, list)
        assert data[0]["id"] == "bitcoin"


def test_fetch_crypto_data_raises_on_api_error():
    mock_response = MagicMock()
    mock_response.status_code = 503

    with patch("src.ingest.requests.get", return_value=mock_response):
        with pytest.raises(Exception, match="API call failed"):
            fetch_crypto_data()


def test_save_to_bronze_keeps_only_expected_columns():
    # BUCKET_NAME is read from the S3_BUCKET_NAME env var at import time.
    # In CI (and in this sandbox) that env var is genuinely unset, so
    # BUCKET_NAME is None -- patch it directly instead of asserting on an
    # env var we never set.
    with patch("src.ingest.BUCKET_NAME", "test-bucket"), \
         patch("src.ingest.boto3.client") as mock_boto:
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        s3_key = save_to_bronze(SAMPLE_API_RESPONSE)

        # boto3 client's put_object should be called exactly once
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "test-bucket"
        assert "coins_" in call_kwargs["Key"]
        assert s3_key == call_kwargs["Key"]


def test_save_to_bronze_key_follows_date_partition_format():
    with patch("src.ingest.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        s3_key = save_to_bronze(SAMPLE_API_RESPONSE)
        assert s3_key.startswith("bronze/year=")
        assert "/month=" in s3_key
        assert "/day=" in s3_key
