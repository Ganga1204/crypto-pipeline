# src/ingest.py
# This file fetches crypto data and saves it to S3 as the BRONZE layer

import requests        # for calling the API
import pandas as pd    # for turning data into a table
import boto3           # for talking to S3
import json            # for reading JSON responses
import os              # for reading environment variables
from datetime import datetime  # for timestamps
from dotenv import load_dotenv # for loading .env file

# Load the .env file so Python can read your AWS keys
load_dotenv()

# --- CONFIGURATION ---
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')  # reads from .env
AWS_REGION  = os.getenv('AWS_REGION', 'us-east-1')

# CoinGecko API URL — fetches top 100 coins by market cap in USD
API_URL = (
    'https://api.coingecko.com/api/v3/coins/markets'
    '?vs_currency=usd'
    '&order=market_cap_desc'
    '&per_page=100'
    '&page=1'
    '&sparkline=false'
)

def fetch_crypto_data():
    """Call the CoinGecko API and return data as a list of dictionaries"""
    print('Fetching data from CoinGecko API...')

    # Send a GET request to the API (like opening a URL in your browser)
    response = requests.get(API_URL, timeout=30)

    # Check if the call worked (200 = success, anything else = error)
    if response.status_code != 200:
        raise Exception(f'API call failed! Status: {response.status_code}')

    data = response.json()  # convert the response text to a Python list
    print(f'Successfully fetched {len(data)} coins')
    return data

def save_to_bronze(data):
    """Save raw data to S3 bronze layer — no changes, exactly as received"""

    # Create a pandas DataFrame (think of it as an Excel table in Python)
    df = pd.DataFrame(data)

    # Select only the columns we care about
    columns = [
        'id', 'symbol', 'name', 'current_price', 'market_cap',
        'total_volume', 'price_change_percentage_24h',
        'price_change_percentage_7d_in_currency',
        'circulating_supply', 'last_updated', 'image'
    ]
    # Only keep columns that exist in the response (API may vary)
    existing = [c for c in columns if c in df.columns]
    df = df[existing]

    # Add a timestamp so we know WHEN this data was fetched
    df['ingestion_timestamp'] = datetime.utcnow().isoformat()

    # Build the S3 file path — organise by date so files don't overwrite
    # Example path: bronze/year=2024/month=01/day=15/coins_143022.parquet
    now = datetime.utcnow()
    s3_key = (
        f'bronze/year={now.year}/month={now.month:02d}/'
        f'day={now.day:02d}/coins_{now.strftime("%H%M%S")}.parquet'
    )

    # Convert the table to Parquet format
    # Parquet is like a compressed Excel file — much smaller and faster than CSV
    parquet_buffer = df.to_parquet(index=False)

    # Connect to S3 using your credentials from .env
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=AWS_REGION
    )

    # Upload the file to S3
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=parquet_buffer
    )

    print(f'Bronze layer saved to: s3://{BUCKET_NAME}/{s3_key}')
    return s3_key  # return the path so transform.py can find the file

# This block runs when you execute: python src/ingest.py
if __name__ == '__main__':
    data = fetch_crypto_data()
    s3_key = save_to_bronze(data)
    print('Ingestion complete!')

