================================================================
PROJECT 3 — LINKEDIN DESCRIPTION
Title: Real-Time Reddit Post Sentiment Streaming Pipeline | Kafka · PySpark · MongoDB · BigQuery
================================================================

Overview:
Built a real-time streaming data pipeline that ingests live posts from 5 technology subreddits
(r/technology, r/datascience, r/MachineLearning, r/programming, r/Python), applies NLP sentiment
analysis using a VADER + TextBlob ensemble model, stores raw unstructured JSON documents in
MongoDB as a NoSQL data lake, and syncs aggregated results to BigQuery for trend analysis and
dashboarding — all orchestrated by Apache Airflow.

Data Source:
Reddit API via PRAW library (free, public — reddit.com/dev/api). No cost, no API key for
read-only access to public subreddits. Yields ~200–500 posts/hour across 5 subreddits.

Approach:
1. Reddit PRAW producer → polls live post submissions via multi-subreddit stream
2. Clean & serialize → strip URLs, HTML, removed/deleted content
3. Publish to Kafka → topic: `reddit-posts-stream`, 5 partitions, GZIP compression
4. PySpark Structured Streaming → parse schema, apply VADER + TextBlob sentiment UDFs
   → foreachBatch sink writes enriched documents to MongoDB (upsert by post_id)
5. MongoDB as NoSQL data lake → flexible schema stores raw fields + computed sentiment
   → MongoDB aggregation pipeline for real-time analytics queries
6. Airflow DAG (hourly) → syncs unsynced MongoDB docs → BigQuery for dashboarding
7. BigQuery → partitioned by date, subreddit daily aggregate table refreshed hourly

Key Outcomes:
- End-to-end latency: < 60 seconds from Reddit post creation to MongoDB storage
- Sentiment accuracy: VADER + TextBlob ensemble improves accuracy vs single model
- Volume handled: ~200–500 posts/hour, 4,800–12,000 posts/day
- MongoDB stores flexible unstructured JSON — no fixed schema requirement
- Hourly Airflow sync keeps BigQuery dashboard < 1 hour behind real-time

Tech Stack:
Reddit PRAW · Apache Kafka · PySpark Structured Streaming · MongoDB (NoSQL)
Google BigQuery · Apache Airflow · TextBlob · VADER Sentiment · Python · Docker

What I Learned:
- Designing NoSQL document schemas for unstructured social media data with flexible fields
- Implementing NLP ensemble models as PySpark UDFs for real-time text classification
- Using MongoDB aggregation pipelines as a lightweight real-time analytics layer
- Building Airflow DAGs with BranchPythonOperator for conditional sync logic


================================================================
PROJECT 4 — LINKEDIN DESCRIPTION
Title: World Bank Economic Indicators ETL Pipeline | Kafka · PySpark · dbt · Snowflake
================================================================

Overview:
Daily batch ETL pipeline that ingests 8 macroeconomic indicators (GDP, inflation,
unemployment, internet penetration, CO2 emissions, and more) for 30 countries spanning
30+ years from the World Bank Open Data API — completely free and requiring no API key.
Uses a 3-layer dbt transformation architecture (staging → intermediate → marts) to
deliver production-ready analytics tables in Snowflake.

Data Source:
World Bank Open Data API (api.worldbank.org/v2 — free, no registration required).
Indicators: NY.GDP.MKTP.CD, FP.CPI.TOTL.ZG, SL.UEM.TOTL.ZS, IT.NET.USER.ZS, and 4 more.
Countries: G20 nations + ASEAN-10 (30 total). Yields ~36,000 records/run.

Approach:
1. World Bank API extractor → iterates 8 indicators × 30 countries → publishes to Kafka
   (0.3s delay per request to respect rate limits)
2. Kafka topic: `batch-worldbank-indicators`, GZIP, key = country_code_indicator_year
3. PySpark batch transform → reads from Kafka → pivots indicators into wide format
   → computes YoY GDP growth via window functions → classifies income groups
   → writes to PostgreSQL staging table
4. PostgreSQL → intermediate staging before dbt transformation
5. dbt models:
   - staging/stg_worldbank_indicators.sql  → data type casting, cleaning (view)
   - intermediate/int_indicators_pivoted.sql → normalization, null handling
   - marts/mart_country_economy.sql → composite economic health score
   - marts/mart_indicator_trends.sql → 30-year rolling analytics
6. Snowflake → clustered by country_code + year, XS warehouse auto-suspend
7. Airflow DAG → daily 03:00 WIB, includes dbt run + dbt test quality checks

Key Outcomes:
- ~36,000 records/run across 8 indicators, 30 countries, 30+ years
- dbt tests catch schema drift and NULL anomalies before Snowflake load
- Economic health composite score: weighted formula (GDP growth 40%, inflation 30%, unemployment 30%)
- Snowflake compute cost: < $0.10/run with XS warehouse and auto-suspend
- Full dbt lineage documented via `dbt docs generate`

Tech Stack:
World Bank Open API · Apache Kafka · PySpark · PostgreSQL · dbt Core
Snowflake · Apache Airflow · Python · Docker Compose

What I Learned:
- Building multi-layer dbt transformation models with proper ref() dependencies
- Designing Snowflake table clustering keys for efficient analytical queries
- Implementing idempotent batch pipelines (DELETE + INSERT) for safe daily reruns
- Using PySpark window functions for time-series YoY calculations across partitions


================================================================
PROJECT 5 — LINKEDIN DESCRIPTION
Title: Crypto Market Data Lake — Lambda Architecture | Kafka · PySpark · Cassandra · Snowflake
================================================================

Overview:
Advanced Lambda Architecture pipeline for cryptocurrency market data that simultaneously
satisfies two competing requirements: millisecond-latency live price lookups and complex
analytical queries over 90-day historical OHLCV data. The speed layer streams real-time
prices every 60 seconds from CoinGecko into Cassandra (< 60s latency), while the batch
layer processes daily OHLCV candles with technical indicators into Snowflake. A Cassandra
serving layer unifies both views for the query interface.

Data Source:
CoinGecko API (api.coingecko.com — free tier, no API key needed). Top 50 coins by market cap.
Speed layer: /coins/markets endpoint polled every 60s. Batch layer: /coins/{id}/ohlc for 90-day OHLCV.
Volume: ~4.3M speed events/day + 1,800 OHLCV candles/batch run.

Approach:
SPEED LAYER (every 5 minutes via Airflow):
1. CoinGecko price producer → fetches top 50 coins in 2 API chunks
2. Enriches with 1h/24h/7d price change, ATH, circulating supply
3. Publishes to Kafka: `crypto-prices-realtime`, GZIP
4. PySpark Streaming (60s trigger) → volatility classification UDF
   (low/medium/high/extreme based on |24h change%|)
5. Writes to Cassandra `price_time_series` table (TTL: 30 days auto-expire)
   + updates `latest_price_view` for O(1) current-price lookups

BATCH LAYER (daily 04:00 WIB via Airflow):
1. CoinGecko OHLCV extractor → top 20 coins × 90-day candles
2. Publishes to Kafka: `crypto-ohlcv-batch`
3. PySpark batch → computes: daily return %, 7-day MA, 30-day MA,
   golden cross signal (7d MA crosses above 30d MA), high-low ratio
4. Loads enriched OHLCV to Snowflake clustered by coin_id + candle_date

SERVING LAYER (Cassandra):
- `price_time_series`: partitioned by coin_id, clustered by event_time DESC → latest-first O(1)
- `latest_price_view`: single-row per coin for dashboard queries at millisecond latency
- `daily_ohlcv`: batch-layer candles for medium-latency historical reads

Key Outcomes:
- Cassandra read latency: < 5ms for latest price queries
- Speed layer throughput: 50 coins × 60s = ~72,000 events/hour
- Batch layer: 20 coins × 90 candles + 7 technical indicators per row
- Lambda architecture decouples latency requirements: ms (Cassandra) vs minutes (Snowflake)
- Cassandra TTL eliminates need for manual cleanup jobs on speed data

Tech Stack:
CoinGecko API · Apache Kafka · PySpark Streaming + Batch · Apache Cassandra
Snowflake · Apache Airflow (2 DAGs) · Python · Docker Compose

What I Learned:
- Implementing Lambda Architecture: designing independent speed and batch layers that serve a unified query interface
- Cassandra data modeling: partitioning by coin_id + clustering by event_time DESC for time-series workloads
- Setting TTL on Cassandra tables to auto-expire high-volume speed data without manual cleanup
- Balancing two competing SLAs (millisecond dashboard vs daily analytics) in one cohesive system


================================================================
UPDATED GITHUB README (append to existing README.md)
================================================================

---

## Project 3 — Real-Time Reddit Sentiment Streaming Pipeline

> **Type:** Streaming | **Difficulty:** Intermediate  
> **Stack:** Reddit PRAW → Kafka → PySpark → MongoDB (NoSQL) → BigQuery → Airflow

### Data Source
Reddit API via PRAW (free, public — no API key for read-only).  
Subreddits: `r/technology`, `r/datascience`, `r/MachineLearning`, `r/programming`, `r/Python`

### Architecture
```
Reddit PRAW
    ↓ (live stream, ~200-500 posts/hr)
Kafka [reddit-posts-stream, 5 partitions]
    ↓ (30s micro-batch)
PySpark Structured Streaming
    → VADER + TextBlob ensemble NLP
    ↓ (foreachBatch upsert)
MongoDB (NoSQL Data Lake)          BigQuery (Analytics)
    raw_posts collection                ← hourly Airflow sync ←
    sentiment_results collection        partitioned by date
```

### Key Features
- VADER + TextBlob ensemble sentiment (compound average score)
- MongoDB stores raw unstructured JSON — no fixed schema
- Upsert deduplication on `post_id` in both MongoDB and BigQuery
- Airflow BranchPythonOperator: skip sync if no new records
- MongoDB aggregation pipeline for real-time subreddit ranking

### Metrics
| Metric | Value |
|---|---|
| Throughput | ~200–500 posts/hour |
| End-to-end latency | < 60 seconds |
| Sentiment model | VADER + TextBlob ensemble |
| MongoDB TTL | None (full historical archive) |

---

## Project 4 — World Bank Economic Indicators ETL Pipeline

> **Type:** Batch | **Difficulty:** Intermediate  
> **Stack:** World Bank API → Kafka → PySpark → PostgreSQL → dbt → Snowflake → Airflow

### Data Source
World Bank Open Data API (`api.worldbank.org/v2`) — free, no API key.  
8 indicators × 30 countries (G20 + ASEAN) × 30+ years = ~36,000 records/run.

### Architecture
```
World Bank Open API (free, no key)
    ↓ (8 indicators × 30 countries × 30 years)
Kafka [batch-worldbank-indicators]
    ↓ (PySpark batch)
PySpark Transform
    → pivot indicators → YoY growth → income group classification
    ↓
PostgreSQL (staging)
    ↓
dbt (3-layer transformation)
    staging/   → type casting, null handling
    intermediate/ → normalization
    marts/     → economic health composite score
    ↓
Snowflake (WORLD_BANK_DW)
    RAW → STAGING → MARTS
```

### dbt Models
| Model | Layer | Materialization |
|---|---|---|
| `stg_worldbank_indicators` | staging | view |
| `int_indicators_pivoted` | intermediate | ephemeral |
| `mart_country_economy` | marts | table, clustered |
| `mart_indicator_trends` | marts | table, partitioned |

### Metrics
| Metric | Value |
|---|---|
| Records per run | ~36,000 |
| dbt models | 4 (1 view + 2 tables + 1 ephemeral) |
| Snowflake cost | < $0.10/run (XS, auto-suspend) |
| Schedule | Daily 03:00 WIB |

---

## Project 5 — Crypto Market Lambda Architecture Pipeline ⚡ (Advanced)

> **Type:** Lambda (Streaming + Batch) | **Difficulty:** Advanced  
> **Stack:** CoinGecko API → Kafka → PySpark (Speed + Batch) → Cassandra → Snowflake → Airflow

### Data Source
CoinGecko API (`api.coingecko.com`) — free tier, no API key.  
Speed: top 50 coins polled every 60s. Batch: top 20 coins OHLCV × 90 days.

### Lambda Architecture
```
CoinGecko API (free, no key)
         │
    ┌────┴────┐
    │         │
  Speed     Batch
  Layer      Layer
  (60s)     (Daily)
    │         │
    ↓         ↓
Kafka       Kafka
[prices]   [ohlcv]
    │         │
    ↓         ↓
PySpark    PySpark
Streaming   Batch
    │    (technical indicators)
    │         │
    ↓         ↓
Cassandra  Snowflake
(serving)  (analytics)
    │
    ↓
latest_price_view  ← O(1) ms latency
price_time_series  ← 30-day TTL auto-expire
```

### Why Lambda Architecture?
| Requirement | Solution |
|---|---|
| "What is BTC price RIGHT NOW?" | Cassandra `latest_price_view` → < 5ms |
| "Show 30-day MA + golden cross signals" | Snowflake `CRYPTO_OHLCV_HISTORY` → seconds |
| "Top 10 most volatile coins today" | Cassandra `price_time_series` → ms |
| "Correlation matrix: BTC vs altcoins" | Snowflake analytical query → minutes |

### Cassandra Design
- `price_time_series`: `PRIMARY KEY (coin_id, event_time)` DESC — latest-first, O(1)
- `latest_price_view`: `PRIMARY KEY (coin_id)` — single-row update per price tick
- 30-day TTL on speed data — auto-expires without manual cleanup

### Airflow: 2 Separate DAGs
| DAG | Schedule | SLA |
|---|---|---|
| `crypto_speed_layer` | Every 5 minutes | < 60s latency |
| `crypto_batch_layer` | Daily 04:00 WIB | < 30 min runtime |

### Metrics
| Metric | Value |
|---|---|
| Speed layer throughput | ~72,000 events/hour (50 coins × 60s) |
| Cassandra read latency | < 5ms |
| Batch OHLCV records | 20 coins × 90 candles = 1,800/run |
| Technical indicators | daily return %, 7d/30d MA, golden cross |

---

## Full Tech Stack Summary

| Technology | Used In |
|---|---|
| Apache Kafka | All 5 projects |
| PySpark | All 5 projects |
| Apache Airflow | All 5 projects |
| PostgreSQL | Projects 1, 2, 4 (staging) |
| Google BigQuery | Projects 1, 3 |
| Amazon Redshift | Project 2 |
| MongoDB (NoSQL) | Project 3 (data lake) |
| Snowflake | Projects 4, 5 |
| Apache Cassandra | Project 5 (serving layer) |
| dbt Core | Project 4 |
| Amazon S3 | Project 2 |
| Docker Compose | All projects (local dev) |

---

## Setup & Running All Projects

### Prerequisites
- Python 3.10+
- Docker Desktop (for Kafka, PostgreSQL, Airflow, MongoDB, Cassandra)
- Snowflake free trial account
- Google Cloud account (BigQuery free tier)

### Quick Start
```bash
# 1. Clone repository
git clone https://github.com/zulhamva-tech/data-engineering-portfolio.git
cd data-engineering-portfolio

# 2. Start all services
docker compose up -d

# 3. Wait for services (check at http://localhost:8080 for Airflow)
# 4. Set Airflow variables (Admin > Variables)
# 5. Run desired DAG from Airflow UI

# Services after startup:
# Airflow   → http://localhost:8080 (admin/admin)
# Kafka UI  → http://localhost:9090
# Jupyter   → http://localhost:8888 (token: datatengineer)
# PgAdmin   → http://localhost:5050
```

---

*Ahmad Zulham Hamdan | Data Engineer | Kafka · PySpark · Airflow · dbt · Snowflake · Cassandra*  
*📧 zulham.va@gmail.com | 🔗 linkedin.com/in/ahmad-zulham-hamdan-665170279*
