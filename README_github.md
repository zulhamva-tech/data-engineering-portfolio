# 🔁 Data Engineering Portfolio Projects

> **Ahmad Zulham Hamdan** — Data Engineer | Big Data & Cloud  
> Stack: Kafka · PySpark · Airflow · PostgreSQL · BigQuery · Redshift

---

## 📂 Repository Structure

```
data-engineering-portfolio/
│
├── project1_iot_streaming/          ← Real-time pipeline
│   ├── notebooks/
│   │   └── project1_iot_streaming_pipeline.ipynb
│   ├── sql/
│   │   └── ddl_and_queries.sql
│   ├── dags/
│   │   └── iot_streaming_dag.py
│   └── README.md
│
├── project2_batch_ingestion/        ← Batch pipeline
│   ├── notebooks/
│   │   └── project2_batch_ingestion_pipeline.ipynb
│   ├── sql/
│   │   └── ddl_and_queries.sql
│   ├── dags/
│   │   └── batch_ingestion_dag.py
│   └── README.md
│
├── architecture/
│   ├── streaming_architecture.png
│   └── batch_architecture.png
│
└── README.md                        ← This file
```

---

## 🚀 Project 1 — Real-Time IoT Weather Sensor Streaming Pipeline

### Overview
A production-grade real-time streaming pipeline that ingests live weather data from 5 virtual IoT sensor nodes across major Indonesian cities. Data flows from the Open-Meteo API through Kafka, is processed by PySpark Structured Streaming, persisted in PostgreSQL, and synced to BigQuery for analytics and dashboarding.

### Architecture
```
Open-Meteo API          Kafka               PySpark Structured     PostgreSQL       BigQuery
(IoT Sensor Nodes)  →  Topic:          →   Streaming          →  (Operational)  →  (Warehouse)
                        iot-weather-        • Parse JSON           • Dedup           • Analytics
5 cities polled         sensors             • Window agg           • upsert          • Dashboard
every 60 seconds        5 partitions        • Anomaly flag         • Sync flag       • ML-ready
```

### Data Source
| Source | Type | Access | Refresh Rate |
|---|---|---|---|
| [Open-Meteo API](https://open-meteo.com) | IoT-simulated weather sensors | Free, no API key | Every 15 min |

**Sensor Nodes:**
- 🏙️ `SNS-JKT-001` — Jakarta (-6.2088, 106.8456)
- 🏙️ `SNS-SBY-002` — Surabaya (-7.2575, 112.7521)
- 🏙️ `SNS-MDN-003` — Medan (3.5952, 98.6722)
- 🏙️ `SNS-BDG-004` — Bandung (-6.9175, 107.6191)
- 🏙️ `SNS-MKS-005` — Makassar (-5.1477, 119.4327)

**Sensor Fields:** temperature (°C), humidity (%), wind speed/direction, precipitation (mm), apparent temperature, weather code

### Tech Stack
| Layer | Technology | Purpose |
|---|---|---|
| Source | Open-Meteo REST API | Free live weather sensor data |
| Messaging | Apache Kafka | Fault-tolerant event streaming |
| Processing | PySpark Structured Streaming | Real-time ETL & window aggregation |
| Storage | PostgreSQL 15 | Operational sink with dedup |
| Warehouse | Google BigQuery | Analytics, anomaly detection |
| Orchestration | Apache Airflow | DAG scheduling, retry, alerting |
| Notebook | Jupyter Lab | Development & prototyping |

### Key Features
- ⚡ **< 60s latency** end-to-end from API → BigQuery
- 🔁 **Exactly-once semantics** via Kafka + Spark checkpointing
- 🚨 **Anomaly detection** using z-score on 7-day temperature baseline
- 📊 **5-minute tumbling window** aggregates per sensor node
- 📈 **Data quality report** — completeness, uptime, null rates

### Metrics
- ~5 events/minute | ~7,200 events/day
- 5 partitions in Kafka, replication factor = 2
- PySpark trigger: every 30 seconds
- BigQuery sync: hourly via Airflow DAG

---

## 📦 Project 2 — Batch Data Ingestion Pipeline (API + Web Scraping)

### Overview
A robust daily batch ingestion pipeline pulling data from two public APIs — GitHub (trending repositories) and Open-Meteo Historical (90-day weather archive). Data is published to Kafka, transformed by PySpark in batch mode, staged in PostgreSQL, and loaded to Amazon Redshift as the data warehouse via S3 COPY.

### Architecture
```
Source 1: GitHub API         Kafka               PySpark          PostgreSQL       Redshift
(Trending Repos)         →   batch-github-   →   Batch ETL    →   Staging      →   DW (Analytics)
150 repos × 5 languages      repos               • Dedup           • Validate       • Partitioned
                                                  • Enrich          • Audit log      • Compressed

Source 2: Open-Meteo         batch-weather-      • activity_score
Historical API           →   history             • stars_per_day
90 days × 5 cities           (5 partitions)      • is_rainy flag
```

### Data Sources
| Source | Type | Access | Records/Day |
|---|---|---|---|
| [GitHub REST API](https://api.github.com) | Public API | Free (5000 req/hr with token) | ~750 repos |
| [Open-Meteo Historical](https://archive-api.open-meteo.com) | Historical weather API | Free, no API key | 450 daily records |

**GitHub Scope:** Top 30 trending repos × 5 languages (Python, Go, Rust, TypeScript, Java) — created in the last 30 days, sorted by stars.

**Weather Scope:** 5 Indonesian cities, 90-day lookback, 11 daily weather variables.

### Tech Stack
| Layer | Technology | Purpose |
|---|---|---|
| Source | GitHub REST API + Open-Meteo Historical | Public APIs, no cost |
| Messaging | Apache Kafka | Reliable batch event transport |
| Processing | PySpark (batch mode) | Large-scale ETL + feature engineering |
| Staging | PostgreSQL 15 | Intermediate storage, data validation |
| Warehouse | Amazon Redshift | Columnar DW, COPY via S3 |
| Cloud Storage | Amazon S3 | Parquet staging for Redshift COPY |
| Orchestration | Apache Airflow | Daily DAG, SLA monitoring |
| Notebook | Jupyter Lab | Development & prototyping |

### Key Features
- 🗂️ **Dual-source ingestion** — API + historical archive in one pipeline
- 🧹 **Data quality checks** — null handling, dedup, type validation in PySpark
- 📊 **Feature engineering** — `activity_score`, `stars_per_day`, `temp_range_c`, `is_rainy`
- 🏗️ **Redshift-optimized** — DISTKEY, SORTKEY, AZ64/ZSTD compression
- 📅 **Incremental loading** — idempotent batch with `batch_date` partitioning
- ⏱️ **SLA:** Full pipeline completes in < 15 minutes

### Pipeline Schedule (Airflow)
| DAG | Schedule | Description |
|---|---|---|
| `batch_github_ingest` | Daily 02:00 WIB | Fetch GitHub trending repos |
| `batch_weather_ingest` | Daily 01:00 WIB | Fetch 1-day weather update |
| `redshift_sync` | Daily 03:00 WIB | Postgres → S3 → Redshift COPY |

---

## 🛠️ Setup & Running

### Prerequisites
```bash
Python 3.10+
Apache Kafka 3.6+
Apache Spark 3.5+
Apache Airflow 2.8+
PostgreSQL 15+
```

### Installation
```bash
git clone https://github.com/your-username/data-engineering-portfolio.git
cd data-engineering-portfolio
pip install -r requirements.txt
```

### Required Python Packages
```
kafka-python==2.0.2
pyspark==3.5.0
psycopg2-binary==2.9.9
google-cloud-bigquery==3.15.0
boto3==1.34.0
apache-airflow==2.8.0
requests==2.31.0
pandas==2.1.4
```

### Environment Variables
```bash
# PostgreSQL
export POSTGRES_HOST=localhost
export POSTGRES_DB=your_db
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=your_password

# BigQuery (Project 1)
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export GCP_PROJECT_ID=your-project-id

# AWS Redshift + S3 (Project 2)
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export REDSHIFT_HOST=your-cluster.redshift.amazonaws.com

# GitHub API (Project 2, optional)
export GITHUB_TOKEN=ghp_your_token_here
```

---

## 📊 Analytics Highlights

### Project 1 — Streaming Insights
- Hourly temperature trend across 5 Indonesian cities
- Real-time z-score anomaly detection (alert if delta > 2σ)
- Sensor uptime & data completeness dashboard

### Project 2 — Batch Insights
- Most active programming languages by engagement score
- 90-day weather pattern analysis per city
- Rainy day frequency & seasonal temperature trends
- Pipeline health monitoring (rows ingested per batch)

---

## 🗺️ Architecture Diagrams

See `/architecture/` folder for full pipeline diagrams.

---

## 👤 Author

**Ahmad Zulham Hamdan**  
Data Engineer | Big Data & Cloud  
📧 zulham.va@gmail.com  
🔗 [LinkedIn](https://linkedin.com/in/ahmad-zulham-hamdan-665170279)  
📍 Binjai, North Sumatra, Indonesia

---

## 📄 License

MIT License — free to use and modify with attribution.
