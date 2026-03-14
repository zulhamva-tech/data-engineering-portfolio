-- ============================================================
-- Project 4: World Bank Economic Indicators — dbt + Snowflake
-- SQL Queries: DDL + Snowflake Analytics
-- Author: Ahmad Zulham Hamdan
-- ============================================================


-- ── Snowflake DDL: RAW layer ──────────────────────────────────
CREATE DATABASE IF NOT EXISTS WORLD_BANK_DW;
CREATE SCHEMA  IF NOT EXISTS WORLD_BANK_DW.RAW;
CREATE SCHEMA  IF NOT EXISTS WORLD_BANK_DW.STAGING;
CREATE SCHEMA  IF NOT EXISTS WORLD_BANK_DW.MARTS;

CREATE TABLE IF NOT EXISTS WORLD_BANK_DW.RAW.RAW_WORLDBANK_INDICATORS (
    COUNTRY_CODE             VARCHAR(10),
    COUNTRY_NAME             VARCHAR(100),
    YEAR                     INT,
    GDP_CURRENT_USD          FLOAT,
    GDP_GROWTH_PCT           FLOAT,
    GDP_PER_CAPITA_USD       FLOAT,
    INFLATION_CPI_PCT        FLOAT,
    UNEMPLOYMENT_RATE_PCT    FLOAT,
    POPULATION_TOTAL         BIGINT,
    INTERNET_USERS_PCT       FLOAT,
    CO2_EMISSIONS_PER_CAPITA FLOAT,
    INCOME_GROUP             VARCHAR(50),
    IS_DIGITAL_READY         BOOLEAN,
    BATCH_DATE               DATE,
    LOADED_AT                TIMESTAMP_NTZ
)
CLUSTER BY (COUNTRY_CODE, YEAR);

-- ── PostgreSQL DDL: Staging ───────────────────────────────────
CREATE TABLE IF NOT EXISTS stg_worldbank_indicators (
    country_code             VARCHAR(10),
    country_name             VARCHAR(100),
    year                     INT,
    gdp_current_usd          DOUBLE PRECISION,
    gdp_growth_pct           DOUBLE PRECISION,
    gdp_per_capita_usd       DOUBLE PRECISION,
    inflation_cpi_pct        DOUBLE PRECISION,
    unemployment_rate_pct    DOUBLE PRECISION,
    population_total         BIGINT,
    internet_users_pct       DOUBLE PRECISION,
    co2_emissions_per_capita DOUBLE PRECISION,
    gdp_yoy_growth_calc      DOUBLE PRECISION,
    income_group             VARCHAR(50),
    is_digital_ready         BOOLEAN,
    batch_date               DATE,
    loaded_at                TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (country_code, year)
);

CREATE TABLE IF NOT EXISTS pipeline_audit (
    dag_id              VARCHAR(100),
    execution_date      DATE,
    records_extracted   INT DEFAULT 0,
    records_staged      INT DEFAULT 0,
    records_loaded      INT DEFAULT 0,
    status              VARCHAR(20),
    logged_at           TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (dag_id, execution_date)
);


-- ── QUERY 1: Top 10 fastest growing economies (latest year) ──
SELECT
    country_name,
    year,
    ROUND(gdp_growth_pct, 2)              AS gdp_growth_pct,
    ROUND(gdp_billion_usd, 1)             AS gdp_billion_usd,
    income_group
FROM WORLD_BANK_DW.MARTS.MART_COUNTRY_ECONOMY
WHERE gdp_growth_pct IS NOT NULL
ORDER BY gdp_growth_pct DESC
LIMIT 10;


-- ── QUERY 2: Economic health scorecard ───────────────────────
SELECT
    country_name,
    income_group,
    ROUND(gdp_growth_pct, 2)              AS gdp_growth_pct,
    ROUND(inflation_pct, 2)               AS inflation_pct,
    ROUND(unemployment_pct, 2)            AS unemployment_pct,
    ROUND(economic_health_score, 2)       AS health_score,
    ROUND(internet_users_pct, 1)          AS internet_pct,
    is_digital_ready,
    DENSE_RANK() OVER (ORDER BY economic_health_score DESC) AS health_rank
FROM WORLD_BANK_DW.MARTS.MART_COUNTRY_ECONOMY
ORDER BY health_score DESC;


-- ── QUERY 3: GDP 30-year trend (ASEAN countries) ─────────────
SELECT
    s.country_name,
    s.year,
    ROUND(s.GDP_CURRENT_USD / 1e9, 2)     AS gdp_billion_usd,
    ROUND(s.GDP_GROWTH_PCT, 2)            AS gdp_growth_pct,
    ROUND(s.GDP_PER_CAPITA_USD, 0)        AS gdp_per_capita,
    s.income_group
FROM WORLD_BANK_DW.STAGING.STG_WORLDBANK_INDICATORS s
WHERE s.country_code IN ('ID','MY','TH','PH','VN','SG')
  AND s.year >= 2000
ORDER BY s.country_name, s.year;


-- ── QUERY 4: Inflation vs unemployment correlation ────────────
SELECT
    country_name,
    income_group,
    ROUND(AVG(inflation_pct), 2)          AS avg_inflation,
    ROUND(AVG(unemployment_pct), 2)       AS avg_unemployment,
    ROUND(CORR(inflation_pct, unemployment_pct), 3) AS phillips_correlation,
    COUNT(*)                              AS years_of_data
FROM WORLD_BANK_DW.STAGING.STG_WORLDBANK_INDICATORS
WHERE year >= 2000
  AND inflation_pct    BETWEEN -5 AND 50
  AND unemployment_pct BETWEEN 0  AND 30
GROUP BY country_name, income_group
HAVING COUNT(*) >= 10
ORDER BY ABS(phillips_correlation) DESC;


-- ── QUERY 5: CO2 vs internet users (development index) ───────
SELECT
    country_name,
    year,
    ROUND(internet_users_pct, 1)          AS internet_pct,
    ROUND(co2_per_capita, 2)              AS co2_per_capita,
    ROUND(gdp_per_capita_usd, 0)          AS gdp_per_capita,
    income_group,
    CASE
        WHEN internet_users_pct >= 80 AND co2_per_capita <= 5 THEN 'Green Digital'
        WHEN internet_users_pct >= 80 AND co2_per_capita >  5 THEN 'High Carbon Digital'
        WHEN internet_users_pct <  50 AND co2_per_capita <= 3 THEN 'Low Impact Developing'
        ELSE 'Developing'
    END                                   AS development_profile
FROM WORLD_BANK_DW.MARTS.MART_COUNTRY_ECONOMY
ORDER BY internet_pct DESC;


-- ── QUERY 6: dbt model lineage check ─────────────────────────
SELECT
    table_schema,
    table_name,
    row_count,
    last_altered
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('STAGING', 'MARTS')
  AND table_catalog = 'WORLD_BANK_DW'
ORDER BY table_schema, table_name;
