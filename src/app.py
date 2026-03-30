# src/app.py
# Streamlit dashboard — reads gold layer from S3 and displays it

import streamlit as st  # the dashboard framework
import pandas as pd
import boto3
import os
import io
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

# ---- PAGE CONFIGURATION ----
# This must be the first Streamlit command
st.set_page_config(
    page_title='Crypto Price Dashboard',
    page_icon='',
    layout='wide'  # use full browser width
)

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

def find_latest_gold_prefix(s3, subfolder):
    """Find the most recent gold data folder in S3"""
    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=f'gold/',
        Delimiter='/'
    )
    # Navigate to most recent year/month/day/subfolder
    # For simplicity, list all objects and find latest
    all_objects = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=f'gold/'
    )
    if 'Contents' not in all_objects:
        return None
    files = [o['Key'] for o in all_objects['Contents']
             if subfolder in o['Key'] and o['Key'].endswith('.parquet')]
    if not files:
        return None
    return sorted(files, reverse=True)[0]

@st.cache_data(ttl=300)  # cache data for 5 minutes to avoid repeated S3 calls
def load_gold_data():
    """Load top 20 coins data from gold layer in S3"""
    s3 = get_s3_client()
    key = find_latest_gold_prefix(s3, 'top20')
    if key is None:
        st.error('No gold data found. Run ingest.py and transform.py first!')
        return None
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    return df

# ---- MAIN DASHBOARD ----

# Title and description
st.title('Crypto Market Dashboard')
st.markdown('**Live data pipeline:** CoinGecko API → PySpark → S3 Medallion → This dashboard')
st.divider()

# Load the data
df = load_gold_data()

if df is not None:

    # ---- ROW 1: Key metrics across the top ----
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric('Bitcoin Price', f'${df[df.symbol=="BTC"]["current_price"].values[0]:,.2f}')
    with col2:
        st.metric('Ethereum Price', f'${df[df.symbol=="ETH"]["current_price"].values[0]:,.2f}')
    with col3:
        gainers = (df['price_change_percentage_24h'] > 0).sum()
        st.metric('Gainers (24h)', f'{gainers} / {len(df)}')
    with col4:
        avg_change = df['price_change_percentage_24h'].mean()
        st.metric('Avg 24h Change', f'{avg_change:.2f}%',
                  delta=f'{avg_change:.2f}%')

    st.divider()

    # ---- ROW 2: Two side-by-side charts ----
    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader('Top 10 by Market Cap')
        top10 = df.head(10)[['name', 'market_cap']].set_index('name')
        top10['market_cap_billions'] = top10['market_cap'] / 1e9
        st.bar_chart(top10['market_cap_billions'])
        st.caption('Market cap in billions USD')

    with right_col:
        st.subheader('24h Price Change %')
        change_data = df[['symbol', 'price_change_percentage_24h']].set_index('symbol')
        st.bar_chart(change_data)

    st.divider()

    # ---- ROW 3: Full data table ----
    st.subheader('All Top 20 Coins')
    display_df = df[['name', 'symbol', 'current_price',
                      'market_cap', 'price_change_percentage_24h']].copy()
    display_df.columns = ['Name', 'Symbol', 'Price (USD)',
                          'Market Cap', '24h Change %']
    display_df['Price (USD)'] = display_df['Price (USD)'].apply(lambda x: f'${x:,.4f}')
    display_df['Market Cap'] = display_df['Market Cap'].apply(lambda x: f'${x:,.0f}')
    display_df['24h Change %'] = display_df['24h Change %'].apply(lambda x: f'{x:.2f}%')
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Refresh button
    if st.button('Refresh Data'):
        st.cache_data.clear()
        st.rerun()

else:
    st.warning('Run the pipeline first: python src/ingest.py && python src/transform.py')
