-- ============================================================
-- Project 5: CoinGecko Crypto Lambda Architecture Pipeline
-- SQL Queries: Cassandra CQL + Snowflake SQL
-- Author: Ahmad Zulham Hamdan
-- ============================================================


-- ════════════════════════════════════════════
-- CASSANDRA CQL — Serving Layer
-- ════════════════════════════════════════════

-- Keyspace
CREATE KEYSPACE IF NOT EXISTS crypto_serving
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Speed layer: time-series price events (partitioned by coin, time DESC)
CREATE TABLE IF NOT EXISTS crypto_serving.price_time_series (
    coin_id              TEXT,
    event_time           TIMESTAMP,
    symbol               TEXT,
    name                 TEXT,
    current_price_usd    DOUBLE,
    market_cap_usd       DOUBLE,
    volume_24h_usd       DOUBLE,
    price_change_24h_pct DOUBLE,
    volatility_class     TEXT,       -- low / medium / high / extreme
    is_top10             BOOLEAN,
    ingested_at          TEXT,
    PRIMARY KEY (coin_id, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 2592000;  -- auto-expire after 30 days

-- Latest price view (single-row per coin for O(1) lookups)
CREATE TABLE IF NOT EXISTS crypto_serving.latest_price_view (
    coin_id              TEXT PRIMARY KEY,
    symbol               TEXT,
    name                 TEXT,
    current_price_usd    DOUBLE,
    market_cap_usd       DOUBLE,
    market_cap_rank      INT,
    price_change_24h_pct DOUBLE,
    volatility_class     TEXT,
    last_updated         TEXT
);

-- Batch layer: daily OHLCV stored in Cassandra for serving
CREATE TABLE IF NOT EXISTS crypto_serving.daily_ohlcv (
    coin_id       TEXT,
    candle_date   DATE,
    open_price    DOUBLE,
    high_price    DOUBLE,
    low_price     DOUBLE,
    close_price   DOUBLE,
    price_range   DOUBLE,
    is_bullish    BOOLEAN,
    batch_date    TEXT,
    loaded_at     TEXT,
    PRIMARY KEY (coin_id, candle_date)
) WITH CLUSTERING ORDER BY (candle_date DESC);

-- Cassandra: Get latest 24h prices for Bitcoin
SELECT event_time, current_price_usd, volume_24h_usd, price_change_24h_pct
FROM crypto_serving.price_time_series
WHERE coin_id = 'bitcoin'
  AND event_time >= toTimestamp(now()) - 86400s
LIMIT 100;

-- Cassandra: Get all coins with extreme volatility
SELECT coin_id, symbol, current_price_usd, price_change_24h_pct
FROM crypto_serving.latest_price_view
WHERE volatility_class = 'extreme'
ALLOW FILTERING;


-- ════════════════════════════════════════════
-- SNOWFLAKE SQL — Analytical Layer
-- ════════════════════════════════════════════

-- Snowflake DDL: OHLCV history table
CREATE DATABASE IF NOT EXISTS CRYPTO_DW;
CREATE SCHEMA  IF NOT EXISTS CRYPTO_DW.ANALYTICS;

CREATE TABLE IF NOT EXISTS CRYPTO_DW.ANALYTICS.CRYPTO_OHLCV_HISTORY (
    COIN_ID          VARCHAR(50),
    CANDLE_DATE      DATE,
    OPEN             FLOAT,
    HIGH             FLOAT,
    LOW              FLOAT,
    CLOSE            FLOAT,
    PRICE_RANGE      FLOAT,
    IS_BULLISH       BOOLEAN,
    DAILY_RETURN_PCT FLOAT,
    MA_7D            FLOAT,
    MA_30D           FLOAT,
    GOLDEN_CROSS     BOOLEAN,   -- 7d MA crosses above 30d MA
    BATCH_DATE       DATE,
    LOADED_AT        TIMESTAMP_NTZ
)
CLUSTER BY (COIN_ID, CANDLE_DATE);


-- ── QUERY 1: 30-day price performance ranking ─────────────────
WITH perf AS (
    SELECT
        coin_id,
        FIRST_VALUE(close) OVER (
            PARTITION BY coin_id ORDER BY candle_date ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                       AS price_30d_ago,
        LAST_VALUE(close) OVER (
            PARTITION BY coin_id ORDER BY candle_date ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                       AS price_latest,
        candle_date
    FROM CRYPTO_DW.ANALYTICS.CRYPTO_OHLCV_HISTORY
    WHERE candle_date >= DATEADD(day, -30, CURRENT_DATE())
)
SELECT DISTINCT
    coin_id,
    ROUND(price_30d_ago, 4)                     AS price_30d_ago,
    ROUND(price_latest, 4)                      AS price_latest,
    ROUND((price_latest - price_30d_ago) / price_30d_ago * 100, 2) AS return_30d_pct,
    RANK() OVER (ORDER BY (price_latest - price_30d_ago) / price_30d_ago DESC) AS perf_rank
FROM perf
ORDER BY return_30d_pct DESC;


-- ── QUERY 2: Volatility analysis (daily return std dev) ───────
SELECT
    coin_id,
    COUNT(*)                                    AS trading_days,
    ROUND(AVG(daily_return_pct), 3)            AS avg_daily_return,
    ROUND(STDDEV(daily_return_pct), 3)         AS daily_volatility,
    ROUND(MAX(daily_return_pct), 2)            AS max_single_day_gain,
    ROUND(MIN(daily_return_pct), 2)            AS max_single_day_loss,
    ROUND(STDDEV(daily_return_pct) * SQRT(365), 2) AS annualized_volatility,
    COUNTIF(is_bullish)                         AS bullish_days,
    ROUND(COUNTIF(is_bullish) / COUNT(*) * 100, 1) AS bull_day_pct
FROM CRYPTO_DW.ANALYTICS.CRYPTO_OHLCV_HISTORY
WHERE candle_date >= DATEADD(day, -90, CURRENT_DATE())
GROUP BY coin_id
HAVING COUNT(*) >= 60
ORDER BY annualized_volatility DESC;


-- ── QUERY 3: Golden cross signals (bullish indicator) ─────────
SELECT
    coin_id,
    candle_date                                 AS signal_date,
    ROUND(close, 4)                            AS price_at_signal,
    ROUND(ma_7d, 4)                            AS ma_7d,
    ROUND(ma_30d, 4)                           AS ma_30d,
    ROUND((ma_7d - ma_30d) / ma_30d * 100, 2) AS ma_spread_pct
FROM CRYPTO_DW.ANALYTICS.CRYPTO_OHLCV_HISTORY
WHERE golden_cross = TRUE
  AND candle_date >= DATEADD(day, -90, CURRENT_DATE())
ORDER BY candle_date DESC;


-- ── QUERY 4: Correlation matrix — BTC vs altcoins ────────────
WITH btc AS (
    SELECT candle_date, daily_return_pct AS btc_return
    FROM CRYPTO_DW.ANALYTICS.CRYPTO_OHLCV_HISTORY
    WHERE coin_id = 'bitcoin'
      AND candle_date >= DATEADD(day, -90, CURRENT_DATE())
),
others AS (
    SELECT coin_id, candle_date, daily_return_pct
    FROM CRYPTO_DW.ANALYTICS.CRYPTO_OHLCV_HISTORY
    WHERE coin_id != 'bitcoin'
      AND candle_date >= DATEADD(day, -90, CURRENT_DATE())
)
SELECT
    o.coin_id,
    ROUND(CORR(b.btc_return, o.daily_return_pct), 3) AS btc_correlation,
    COUNT(*)                                          AS data_points
FROM others o
JOIN btc b ON o.candle_date = b.candle_date
GROUP BY o.coin_id
HAVING COUNT(*) >= 50
ORDER BY btc_correlation DESC;


-- ── QUERY 5: Pipeline health — both lambda layers ─────────────
SELECT
    'Cassandra speed layer (latest 1h)' AS layer,
    CAST(NULL AS INT)                    AS record_count,
    'Query Cassandra directly'           AS note
UNION ALL
SELECT
    'Snowflake batch layer (today)',
    COUNT(*),
    MAX(CAST(loaded_at AS VARCHAR))
FROM CRYPTO_DW.ANALYTICS.CRYPTO_OHLCV_HISTORY
WHERE batch_date = CURRENT_DATE();
