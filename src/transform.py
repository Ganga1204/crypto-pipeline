# src/transform.py
# Reads bronze layer, cleans it (silver), then aggregates it (gold)
#
# ARCHITECTURE NOTE:
# We intentionally do NOT use spark.read.parquet("s3a://...")
# because hadoop-aws 3.3.x on Windows has a bug where internal config
# defaults like "60s" and "24h" crash S3AFileSystem initialization.
# Instead: boto3 downloads the file to memory -> Spark reads from local
# temp file -> boto3 uploads the result back to S3.
# This is the permanent fix. S3A is never initialized.

import os
import io
import tempfile
import shutil
from datetime import datetime, timezone
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType
import boto3

load_dotenv()

BUCKET_NAME    = os.getenv('S3_BUCKET_NAME')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION     = os.getenv('AWS_REGION', 'us-east-1')


def get_s3():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )


def create_spark_session():
    """Start a local PySpark session — no S3A, no hadoop-aws jars needed."""
    print('Starting Spark session...')
    spark = (
        SparkSession.builder
        .appName('CryptoPipeline')
        .master('local[*]')
        # Small data — keep shuffle partitions low
        .config('spark.sql.shuffle.partitions', '4')
        # Suppress noisy logs
        .config('spark.ui.showConsoleProgress', 'false')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    print('Spark session started!')
    return spark


def get_latest_bronze_key():
    """Find the S3 key of the most recently uploaded bronze file."""
    s3 = get_s3()
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='bronze/')
    if 'Contents' not in response:
        raise Exception('No bronze files found! Run ingest.py first.')
    files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
    key = files[0]['Key']
    print(f'Latest bronze file: {key}')
    return key


def download_parquet_from_s3(s3_key):
    """Download a parquet file from S3 into a local temp file. Returns temp path."""
    s3 = get_s3()
    tmp = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    print(f'Downloading s3://{BUCKET_NAME}/{s3_key} ...')
    s3.download_fileobj(BUCKET_NAME, s3_key, tmp)
    tmp.close()
    print(f'Downloaded to temp file: {tmp.name}')
    return tmp.name


def upload_parquet_folder_to_s3(local_folder, s3_prefix):
    """Upload all parquet files from a local folder to S3."""
    s3 = get_s3()
    uploaded = 0
    for fname in os.listdir(local_folder):
        if not fname.endswith('.parquet'):
            continue
        local_path = os.path.join(local_folder, fname)
        s3_key = f'{s3_prefix}/{fname}'
        print(f'Uploading {fname} -> s3://{BUCKET_NAME}/{s3_key}')
        s3.upload_file(local_path, BUCKET_NAME, s3_key)
        uploaded += 1
    print(f'Uploaded {uploaded} parquet file(s) to s3://{BUCKET_NAME}/{s3_prefix}/')
    return uploaded


def bronze_to_silver(spark, bronze_local_path):
    """
    Read bronze parquet from local temp file, clean it, return silver DataFrame.
    """
    print('Reading bronze layer...')
    df = spark.read.parquet(bronze_local_path)

    print(f'Bronze row count: {df.count()}')
    df.show(5, truncate=True)

    # 1. Drop nulls in critical columns
    df = df.dropna(subset=['current_price', 'market_cap'])

    # 2. Cast to correct types
    df = df.withColumn('current_price',
                       F.col('current_price').cast(DoubleType()))
    df = df.withColumn('market_cap',
                       F.col('market_cap').cast(LongType()))
    df = df.withColumn('total_volume',
                       F.col('total_volume').cast(LongType()))
    df = df.withColumn('price_change_percentage_24h',
                       F.col('price_change_percentage_24h').cast(DoubleType()))

    # 3. Remove bad prices
    df = df.filter(F.col('current_price') > 0)

    # 4. Standardize symbol to uppercase
    df = df.withColumn('symbol', F.upper(F.col('symbol')))

    # 5. Parse timestamp
    df = df.withColumn('last_updated', F.to_timestamp(F.col('last_updated')))

    # 6. Add processing timestamp
    df = df.withColumn('silver_processed_at',
                       F.lit(datetime.now(timezone.utc).isoformat()))

    print(f'Silver row count after cleaning: {df.count()}')
    return df


def silver_to_gold(silver_df):
    """Build the two gold tables from the silver DataFrame."""
    print('Creating gold layer...')

    gold_summary = (
        silver_df
        .select('name', 'symbol', 'current_price', 'market_cap',
                'total_volume', 'price_change_percentage_24h')
        .orderBy(F.col('market_cap').desc())
        .limit(20)
        .withColumn('current_price',
                    F.round('current_price', 2))
        .withColumn('price_change_percentage_24h',
                    F.round('price_change_percentage_24h', 2))
    )

    gold_stats = silver_df.agg(
        F.count('id').alias('total_coins'),
        F.sum('market_cap').alias('total_market_cap'),
        F.avg('price_change_percentage_24h').alias('avg_24h_change'),
        F.max('current_price').alias('highest_price'),
        F.min('current_price').alias('lowest_price'),
    )

    print('Top 20 coins preview:')
    gold_summary.show(5)

    return gold_summary, gold_stats


if __name__ == '__main__':
    # Track all temp dirs so we can clean up at the end
    temp_dirs = []

    try:
        spark = create_spark_session()

        # ── STEP 1: Download bronze from S3 to local temp file ────────────
        bronze_key = get_latest_bronze_key()
        bronze_local = download_parquet_from_s3(bronze_key)

        # ── STEP 2: Bronze → Silver (pure local Spark, no S3A) ───────────
        silver_df = bronze_to_silver(spark, bronze_local)

        # Write silver to a local temp folder
        silver_tmp = tempfile.mkdtemp(prefix='silver_')
        temp_dirs.append(silver_tmp)
        silver_df.write.mode('overwrite').parquet(silver_tmp)
        print(f'Silver written locally to: {silver_tmp}')

        # ── STEP 3: Upload silver to S3 ───────────────────────────────────
        now = datetime.now(timezone.utc)
        silver_s3 = f'silver/year={now.year}/month={now.month:02d}/day={now.day:02d}'
        upload_parquet_folder_to_s3(silver_tmp, silver_s3)
        print(f'Silver layer saved to: s3://{BUCKET_NAME}/{silver_s3}/')

        # ── STEP 4: Silver → Gold ─────────────────────────────────────────
        gold_summary, gold_stats = silver_to_gold(silver_df)

        # Write gold tables to local temp folders
        gold_summary_tmp = tempfile.mkdtemp(prefix='gold_top20_')
        gold_stats_tmp   = tempfile.mkdtemp(prefix='gold_stats_')
        temp_dirs.extend([gold_summary_tmp, gold_stats_tmp])

        gold_summary.write.mode('overwrite').parquet(gold_summary_tmp)
        gold_stats.write.mode('overwrite').parquet(gold_stats_tmp)

        # ── STEP 5: Upload gold to S3 ─────────────────────────────────────
        base = f'gold/year={now.year}/month={now.month:02d}/day={now.day:02d}'
        upload_parquet_folder_to_s3(gold_summary_tmp, f'{base}/top20')
        upload_parquet_folder_to_s3(gold_stats_tmp,   f'{base}/market_stats')
        print(f'Gold layer saved to: s3://{BUCKET_NAME}/{base}/')

        spark.stop()
        print('\nTransformation complete!')

    finally:
        # Clean up all local temp files and folders
        try:
            os.unlink(bronze_local)
        except Exception:
            pass
        for d in temp_dirs:
            try:
                shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass
        print('Temp files cleaned up.')