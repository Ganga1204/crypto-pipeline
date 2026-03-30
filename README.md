# Crypto Price Data Pipeline

An end-to-end data pipeline that ingests live cryptocurrency prices
from the CoinGecko API, processes them using PySpark with a medallion
architecture (bronze/silver/gold), stores all layers in AWS S3,
and serves the gold layer to an interactive Streamlit dashboard.

## Architecture

```
CoinGecko API
     |
     v
  [ingest.py]
     |
     v
S3 Bronze Layer (raw JSON/parquet)
     |
     v
  [transform.py - PySpark]
  - Clean nulls, fix types, validate
     |
     v
S3 Silver Layer (cleaned parquet)
     |
  - Aggregate top 20, market stats
     v
S3 Gold Layer (analytics-ready parquet)
     |
     v
  [app.py - Streamlit]
     |
     v
Browser Dashboard (localhost:8501)
```

## Tech Stack

| Layer | Technology |
|-------|------------|
| Ingestion | Python, requests |
| Processing | PySpark 3.x |
| Storage | AWS S3 (parquet) |
| Dashboard | Streamlit |

## How to Run

1. Clone the repo and create a .env file with your AWS credentials
2. Install dependencies: pip install -r requirements.txt
3. Run the full pipeline: python run_pipeline.py
4. Start the dashboard: streamlit run src/app.py
5. Open http://localhost:8501

## Output

<img width="1884" height="1050" alt="image" src="https://github.com/user-attachments/assets/7c4cf335-c646-4bc9-8d2b-590061ec442b" />
<img width="1867" height="1045" alt="image" src="https://github.com/user-attachments/assets/2b109a3a-3cb5-42d6-89e2-4927d354c934" />

