-- =============================================================
-- PROJECT 1: IoT Weather Sensor Streaming Pipeline
-- PostgreSQL DDL + Queries + BigQuery Analytics
-- =============================================================

-- ─── PostgreSQL: Create Tables ────────────────────────────────

CREATE TABLE IF NOT EXISTS iot_sensor_readings (
    id                  BIGSERIAL PRIMARY KEY,
    sensor_id           VARCHAR(20)   NOT NULL,
    city                VARCHAR(50)   NOT NULL,
    latitude            DECIMAL(9,6)  NOT NULL,
    longitude           DECIMAL(9,6)  NOT NULL,
    event_time          TIMESTAMPTZ   NOT NULL,
    temperature_c       DECIMAL(5,2),
    humidity_pct        DECIMAL(5,2),
    apparent_temp_c     DECIMAL(5,2),
    precipitation_mm    DECIMAL(6,2),
    weather_code        SMALLINT,
    wind_speed_kmh      DECIMAL(6,2),
    wind_direction_deg  DECIMAL(6,2),
    ingestion_source    VARCHAR(50)   DEFAULT 'open-meteo-api',
    synced_to_bq        BOOLEAN       DEFAULT FALSE,
    created_at          TIMESTAMPTZ   DEFAULT NOW(),
    CONSTRAINT uq_sensor_event UNIQUE (sensor_id, event_time)
);

CREATE INDEX idx_iot_sensor_id    ON iot_sensor_readings (sensor_id);
CREATE INDEX idx_iot_event_time   ON iot_sensor_readings (event_time DESC);
CREATE INDEX idx_iot_city         ON iot_sensor_readings (city);
CREATE INDEX idx_iot_synced       ON iot_sensor_readings (synced_to_bq) WHERE synced_to_bq = FALSE;


-- ─── PostgreSQL: 5-Minute Window Aggregates ───────────────────

CREATE TABLE IF NOT EXISTS iot_sensor_aggregates_5min (
    id                     BIGSERIAL PRIMARY KEY,
    window_start           TIMESTAMPTZ NOT NULL,
    window_end             TIMESTAMPTZ NOT NULL,
    sensor_id              VARCHAR(20) NOT NULL,
    city                   VARCHAR(50) NOT NULL,
    avg_temperature_c      DECIMAL(5,2),
    max_temperature_c      DECIMAL(5,2),
    min_temperature_c      DECIMAL(5,2),
    avg_humidity_pct       DECIMAL(5,2),
    avg_wind_speed_kmh     DECIMAL(6,2),
    total_precipitation_mm DECIMAL(6,2),
    event_count            INTEGER,
    computed_at            TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_agg_window UNIQUE (window_start, sensor_id)
);

CREATE INDEX idx_agg_window_start ON iot_sensor_aggregates_5min (window_start DESC);
CREATE INDEX idx_agg_city         ON iot_sensor_aggregates_5min (city);


-- =============================================================
-- ANALYTICS QUERIES — BigQuery
-- =============================================================

-- Query 1: Hourly temperature trend per city (last 48 hours)
SELECT
    city,
    TIMESTAMP_TRUNC(event_time, HOUR)   AS hour_bucket,
    ROUND(AVG(temperature_c), 2)        AS avg_temp_c,
    ROUND(MIN(temperature_c), 2)        AS min_temp_c,
    ROUND(MAX(temperature_c), 2)        AS max_temp_c,
    ROUND(AVG(humidity_pct), 1)         AS avg_humidity,
    ROUND(SUM(precipitation_mm), 2)     AS total_precip_mm,
    COUNT(*)                            AS readings
FROM `{project}.iot_weather_streaming.sensor_readings`
WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
GROUP BY city, hour_bucket
ORDER BY city, hour_bucket DESC;


-- Query 2: Temperature anomaly detection (deviation from 7-day baseline)
WITH baseline AS (
    SELECT
        city,
        sensor_id,
        AVG(temperature_c) AS baseline_temp,
        STDDEV(temperature_c) AS stddev_temp
    FROM `{project}.iot_weather_streaming.sensor_readings`
    WHERE event_time BETWEEN
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AND
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    GROUP BY city, sensor_id
),
latest AS (
    SELECT
        sensor_id, city, temperature_c, event_time,
        ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY event_time DESC) AS rn
    FROM `{project}.iot_weather_streaming.sensor_readings`
)
SELECT
    l.city,
    l.sensor_id,
    ROUND(l.temperature_c, 2)                              AS current_temp_c,
    ROUND(b.baseline_temp, 2)                              AS baseline_temp_c,
    ROUND(b.stddev_temp, 2)                                AS stddev_temp,
    ROUND(l.temperature_c - b.baseline_temp, 2)           AS delta_temp,
    ROUND((l.temperature_c - b.baseline_temp) / NULLIF(b.stddev_temp, 0), 2) AS z_score,
    CASE
        WHEN ABS((l.temperature_c - b.baseline_temp) / NULLIF(b.stddev_temp, 0)) > 2
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END AS status
FROM latest l
JOIN baseline b ON l.sensor_id = b.sensor_id
WHERE l.rn = 1
ORDER BY ABS(delta_temp) DESC;


-- Query 3: Sensor uptime & data quality report
SELECT
    sensor_id,
    city,
    DATE(event_time) AS report_date,
    COUNT(*)                                            AS total_events,
    COUNTIF(temperature_c IS NULL)                     AS null_temp_count,
    COUNTIF(humidity_pct IS NULL)                      AS null_humidity_count,
    ROUND(
        COUNTIF(temperature_c IS NOT NULL) / COUNT(*) * 100, 1
    )                                                   AS temp_completeness_pct,
    MIN(event_time)                                     AS first_event,
    MAX(event_time)                                     AS last_event,
    TIMESTAMP_DIFF(MAX(event_time), MIN(event_time), MINUTE) AS active_minutes
FROM `{project}.iot_weather_streaming.sensor_readings`
WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY sensor_id, city, report_date
ORDER BY city, report_date DESC;


-- =============================================================
-- PROJECT 2: Batch Data Ingestion Pipeline
-- PostgreSQL DDL + Redshift DDL + Analytics Queries
-- =============================================================

-- ─── PostgreSQL Staging: GitHub Repositories ─────────────────

CREATE TABLE IF NOT EXISTS github_repositories (
    id              BIGSERIAL PRIMARY KEY,
    repo_id         BIGINT        NOT NULL,
    repo_name       VARCHAR(255)  NOT NULL,
    description     TEXT,
    language        VARCHAR(50),
    stars           INTEGER       DEFAULT 0,
    forks           INTEGER       DEFAULT 0,
    open_issues     INTEGER       DEFAULT 0,
    watchers        INTEGER       DEFAULT 0,
    is_fork         BOOLEAN       DEFAULT FALSE,
    license         VARCHAR(50),
    created_at      TIMESTAMPTZ,
    pushed_at       TIMESTAMPTZ,
    size_kb         INTEGER,
    topics          TEXT,
    html_url        VARCHAR(500),
    repo_age_days   INTEGER,
    stars_per_day   DECIMAL(8,2),
    activity_score  DECIMAL(10,2),
    batch_date      DATE          NOT NULL,
    ingested_at     TIMESTAMPTZ   DEFAULT NOW(),
    CONSTRAINT uq_repo_batch UNIQUE (repo_id, batch_date)
);

CREATE INDEX idx_gh_language    ON github_repositories (language);
CREATE INDEX idx_gh_stars       ON github_repositories (stars DESC);
CREATE INDEX idx_gh_batch_date  ON github_repositories (batch_date DESC);
CREATE INDEX idx_gh_activity    ON github_repositories (activity_score DESC);


-- ─── PostgreSQL Staging: Historical Weather ───────────────────

CREATE TABLE IF NOT EXISTS historical_weather (
    id                          BIGSERIAL PRIMARY KEY,
    city                        VARCHAR(50)  NOT NULL,
    latitude                    DECIMAL(9,6),
    longitude                   DECIMAL(9,6),
    date                        DATE         NOT NULL,
    temp_max_c                  DECIMAL(5,2),
    temp_min_c                  DECIMAL(5,2),
    temp_mean_c                 DECIMAL(5,2),
    temp_range_c                DECIMAL(5,2),
    precipitation_sum_mm        DECIMAL(7,2),
    rain_sum_mm                 DECIMAL(7,2),
    wind_speed_max_kmh          DECIMAL(6,2),
    wind_gusts_max_kmh          DECIMAL(6,2),
    wind_direction_dominant_deg DECIMAL(6,2),
    solar_radiation_sum         DECIMAL(8,2),
    is_rainy                    BOOLEAN,
    source                      VARCHAR(50)  DEFAULT 'open-meteo-historical',
    batch_date                  DATE         NOT NULL,
    ingested_at                 TIMESTAMPTZ  DEFAULT NOW(),
    CONSTRAINT uq_weather_city_date UNIQUE (city, date)
);

CREATE INDEX idx_hw_city       ON historical_weather (city);
CREATE INDEX idx_hw_date       ON historical_weather (date DESC);
CREATE INDEX idx_hw_is_rainy   ON historical_weather (is_rainy);


-- ─── Redshift DDL (Data Warehouse) ───────────────────────────

CREATE SCHEMA IF NOT EXISTS batch_ingestion;

CREATE TABLE IF NOT EXISTS batch_ingestion.github_repositories (
    repo_id         BIGINT        ENCODE AZ64,
    repo_name       VARCHAR(255)  ENCODE ZSTD,
    description     VARCHAR(1000) ENCODE ZSTD,
    language        VARCHAR(50)   ENCODE BYTEDICT,
    stars           INTEGER       ENCODE AZ64,
    forks           INTEGER       ENCODE AZ64,
    open_issues     INTEGER       ENCODE AZ64,
    is_fork         BOOLEAN,
    license         VARCHAR(50)   ENCODE BYTEDICT,
    created_at      TIMESTAMP,
    pushed_at       TIMESTAMP,
    size_kb         INTEGER       ENCODE AZ64,
    topics          VARCHAR(500)  ENCODE ZSTD,
    repo_age_days   INTEGER       ENCODE AZ64,
    stars_per_day   DECIMAL(8,2),
    activity_score  DECIMAL(10,2),
    batch_date      DATE          ENCODE AZ64
)
DISTSTYLE KEY DISTKEY (language)
SORTKEY (batch_date, stars);


CREATE TABLE IF NOT EXISTS batch_ingestion.historical_weather (
    city                        VARCHAR(50)  ENCODE BYTEDICT,
    date                        DATE         ENCODE AZ64,
    temp_max_c                  DECIMAL(5,2),
    temp_min_c                  DECIMAL(5,2),
    temp_mean_c                 DECIMAL(5,2),
    temp_range_c                DECIMAL(5,2),
    precipitation_sum_mm        DECIMAL(7,2),
    wind_speed_max_kmh          DECIMAL(6,2),
    wind_gusts_max_kmh          DECIMAL(6,2),
    solar_radiation_sum         DECIMAL(8,2),
    is_rainy                    BOOLEAN,
    batch_date                  DATE         ENCODE AZ64
)
DISTSTYLE KEY DISTKEY (city)
SORTKEY (date, city);


-- =============================================================
-- ANALYTICS QUERIES — Amazon Redshift
-- =============================================================

-- Query 1: Top repositories by language (current batch)
SELECT
    language,
    COUNT(*)                         AS repo_count,
    ROUND(AVG(stars), 0)             AS avg_stars,
    ROUND(AVG(stars_per_day), 2)     AS avg_stars_per_day,
    ROUND(AVG(activity_score), 2)    AS avg_activity_score,
    MAX(stars)                       AS max_stars,
    SUM(CASE WHEN license = 'UNLICENSED' THEN 1 ELSE 0 END) AS unlicensed_count
FROM batch_ingestion.github_repositories
WHERE batch_date = CURRENT_DATE
GROUP BY language
ORDER BY avg_activity_score DESC;


-- Query 2: 30-day rolling weather summary per city
SELECT
    city,
    COUNT(*)                                        AS days_recorded,
    ROUND(AVG(temp_mean_c), 2)                      AS avg_temp_c,
    ROUND(MAX(temp_max_c), 2)                       AS highest_temp_c,
    ROUND(MIN(temp_min_c), 2)                       AS lowest_temp_c,
    ROUND(SUM(precipitation_sum_mm), 1)             AS total_rain_mm,
    SUM(CASE WHEN is_rainy THEN 1 ELSE 0 END)       AS rainy_days,
    ROUND(AVG(wind_speed_max_kmh), 1)               AS avg_max_wind_kmh,
    ROUND(AVG(solar_radiation_sum), 1)              AS avg_solar_mj_m2
FROM batch_ingestion.historical_weather
WHERE date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY city
ORDER BY total_rain_mm DESC;


-- Query 3: Weekly trend — temperature & precipitation (for dashboard)
SELECT
    city,
    DATE_TRUNC('week', date)                         AS week_start,
    ROUND(AVG(temp_mean_c), 2)                       AS avg_temp_c,
    ROUND(MAX(temp_max_c), 2)                        AS max_temp_c,
    ROUND(SUM(precipitation_sum_mm), 2)              AS weekly_rain_mm,
    COUNT(CASE WHEN is_rainy THEN 1 END)             AS rainy_days_in_week
FROM batch_ingestion.historical_weather
GROUP BY city, week_start
ORDER BY city, week_start DESC;


-- Query 4: GitHub repos ingested over time (tracking pipeline health)
SELECT
    batch_date,
    language,
    COUNT(*) AS repos_ingested,
    ROUND(AVG(stars), 0) AS avg_stars
FROM batch_ingestion.github_repositories
GROUP BY batch_date, language
ORDER BY batch_date DESC, repos_ingested DESC;
