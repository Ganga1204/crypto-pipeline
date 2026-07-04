# Crypto Price Data Pipeline

A production-grade, end-to-end data pipeline that ingests live cryptocurrency
prices from the CoinGecko API, processes them using PySpark with a medallion
architecture (Bronze → Silver → Gold), stores all layers in AWS S3, and
serves the Gold layer to an interactive Streamlit dashboard.

## Architecture

```
CoinGecko API
     |
     v  ingest.py (Python + boto3)
S3 Bronze Layer (raw parquet)
     |
     v  transform.py (PySpark - medallion architecture)
     |  - drop nulls, cast types, filter bad prices, uppercase symbol
S3 Silver Layer (cleaned parquet)
     |
     v  aggregate top 20 coins + market-wide stats
S3 Gold Layer (analytics-ready parquet)
     |
     v  app.py (Streamlit dashboard)
Browser Dashboard (localhost:8501)
```

## Tech Stack

| Component      | Technology              | Why                                    |
|----------------|--------------------------|-----------------------------------------|
| Ingestion      | Python, requests         | Simple, reliable API client             |
| Processing     | PySpark                  | Industry standard for scalable ETL      |
| Storage        | AWS S3, Parquet          | Columnar format, cheap, durable         |
| Dashboard      | Streamlit                | Fast Python-native dashboards           |
| Container      | Docker                   | Reproducible environments               |
| Orchestration  | Kubernetes (minikube)    | Auto-healing, scalable                  |
| Deployment     | Ansible                  | Idempotent, auditable automation        |
| CI             | Jenkins + SonarQube      | Code quality gate before every deploy   |
| CD             | GitHub Actions           | Auto-deploy on merge to main            |
| Testing        | PyTest + pytest-cov      | Automated unit tests on every push/PR   |
| Monitoring     | Prometheus + Grafana     | Industry-standard observability         |

## Testing

Automated tests live in `tests/` and run in CI on every push and pull
request via `.github/workflows/test.yml`.

- **`tests/test_ingest.py`** — tests `fetch_crypto_data()` and
  `save_to_bronze()` from `src/ingest.py` using mocked API responses and a
  mocked S3 client, so no real network calls happen during CI.
- **`tests/test_transform.py`** — tests `bronze_to_silver()` and
  `silver_to_gold()` from `src/transform.py` using small in-memory Spark
  DataFrames, checking:
  - null and invalid-price rows are dropped between Bronze → Silver
  - symbols are normalized to uppercase
  - the Gold summary table never exceeds its 20-row limit
  - the Gold stats table always produces exactly one row

Run tests locally:

```bash
pip install -r requirements.txt
pip install pytest pytest-cov
pytest tests/ -v --cov=src --cov-report=term-missing
```

## How to Run Locally

1. Clone the repo and create a `.env` file with your AWS credentials
2. `pip install -r requirements.txt`
3. `python src/run_pipeline.py`
4. `streamlit run src/app.py`
5. Open http://localhost:8501

## How to Deploy

1. `minikube start --driver=docker`
2. `ansible-playbook deploy.yml`
3. `minikube service crypto-pipeline-service`

## CI/CD Flow

Push to main → GitHub Actions runs PyTest suite → Jenkins builds & runs
SonarQube quality gate → Docker image pushed to DockerHub → GitHub Actions
deploys to Kubernetes.

## Output

<img width="1884" height="1050" alt="Streamlit dashboard - top coins" src="https://github.com/user-attachments/assets/7c4cf335-c646-4bc9-8d2b-590061ec442b" />
<img width="1867" height="1045" alt="Streamlit dashboard - market stats" src="https://github.com/user-attachments/assets/2b109a3a-3cb5-42d6-89e2-4927d354c934" />
