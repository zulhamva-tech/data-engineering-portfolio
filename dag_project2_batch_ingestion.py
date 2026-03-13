"""
Airflow DAG — Project 2: Batch Data Ingestion Pipeline
Schedule  : Daily at 02:00 WIB (UTC+7 = 19:00 UTC)
Sources   : GitHub REST API + Open-Meteo Historical API
Author    : Ahmad Zulham Hamdan
"""

from datetime import datetime, timedelta, date
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ─── Default Args ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner':              'zulham-hamdan',
    'depends_on_past':    False,
    'start_date':         datetime(2024, 1, 1),
    'email':              ['zulham.va@gmail.com'],
    'email_on_failure':   True,
    'email_on_retry':     False,
    'retries':            2,
    'retry_delay':        timedelta(minutes=10),
    'retry_exponential_backoff': True,
    'execution_timeout':  timedelta(minutes=30),
}

# ─── Variables ─────────────────────────────────────────────────────────────────
GITHUB_TOKEN     = Variable.get('GITHUB_TOKEN',      default_var='')
KAFKA_BOOTSTRAP  = Variable.get('KAFKA_BOOTSTRAP',   default_var='localhost:9092')
REDSHIFT_HOST    = Variable.get('REDSHIFT_HOST',     default_var='localhost')
REDSHIFT_DB      = Variable.get('REDSHIFT_DB',       default_var='batch_dw')
REDSHIFT_USER    = Variable.get('REDSHIFT_USER',     default_var='awsuser')
REDSHIFT_PASS    = Variable.get('REDSHIFT_PASSWORD', default_var='')
S3_BUCKET        = Variable.get('S3_BUCKET',         default_var='your-bucket')
IAM_ROLE         = Variable.get('REDSHIFT_IAM_ROLE', default_var='')

KAFKA_TOPIC_GITHUB  = 'batch-github-repos'
KAFKA_TOPIC_WEATHER = 'batch-weather-history'

PROGRAMMING_LANGUAGES = ['Python', 'Go', 'Rust', 'TypeScript', 'Java']
LOCATIONS = [
    {'city': 'Jakarta',  'latitude': -6.2088,  'longitude': 106.8456},
    {'city': 'Surabaya', 'latitude': -7.2575,  'longitude': 112.7521},
    {'city': 'Medan',    'latitude': 3.5952,   'longitude': 98.6722},
    {'city': 'Bandung',  'latitude': -6.9175,  'longitude': 107.6191},
    {'city': 'Bali',     'latitude': -8.6705,  'longitude': 115.2126},
]


# ══════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def validate_api_connections(**context):
    """
    Pre-flight check: verify GitHub API and Open-Meteo are reachable.
    """
    import requests

    checks = {
        'GitHub API': 'https://api.github.com/rate_limit',
        'Open-Meteo': 'https://archive-api.open-meteo.com/v1/archive?latitude=0&longitude=0&start_date=2024-01-01&end_date=2024-01-02&daily=temperature_2m_max&timezone=UTC',
    }

    results = {}
    for name, url in checks.items():
        try:
            headers = {}
            if name == 'GitHub API' and GITHUB_TOKEN:
                headers['Authorization'] = f'token {GITHUB_TOKEN}'
            resp = requests.get(url, headers=headers, timeout=10)
            status = resp.status_code
            results[name] = status

            if name == 'GitHub API' and status == 200:
                rate = resp.json().get('rate', {})
                remaining = rate.get('remaining', 0)
                logger.info(f"   {name}: ✅ {status} | Rate limit remaining: {remaining}")
                if remaining < 50:
                    raise ValueError(f"GitHub rate limit too low: {remaining} remaining")
            else:
                logger.info(f"   {name}: ✅ {status} OK")
        except Exception as e:
            raise ConnectionError(f"❌ {name} unreachable: {e}")

    context['ti'].xcom_push(key='api_status', value=results)
    logger.info("✅ All API connections validated")


def extract_github_repos(**context):
    """
    Extract trending GitHub repos for all configured languages.
    Publish each record to Kafka topic.
    """
    import time
    import json
    import requests
    from kafka import KafkaProducer

    execution_date = context['ds']
    headers = {
        'Accept': 'application/vnd.github.v3+json',
    }
    if GITHUB_TOKEN:
        headers['Authorization'] = f'token {GITHUB_TOKEN}'

    since_date = (datetime.strptime(execution_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all',
        compression_type='gzip'
    )

    total_published = 0
    for lang in PROGRAMMING_LANGUAGES:
        url = 'https://api.github.com/search/repositories'
        params = {
            'q': f'language:{lang} created:>{since_date} stars:>50',
            'sort': 'stars', 'order': 'desc', 'per_page': 30
        }
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            repos = resp.json().get('items', [])

            for repo in repos:
                record = {
                    'repo_id':         repo['id'],
                    'repo_name':       repo['full_name'],
                    'description':     (repo.get('description') or '')[:500],
                    'language':        lang,
                    'stars':           repo['stargazers_count'],
                    'forks':           repo['forks_count'],
                    'open_issues':     repo['open_issues_count'],
                    'watchers':        repo['watchers_count'],
                    'is_fork':         repo['fork'],
                    'license':         (repo.get('license') or {}).get('spdx_id', 'NONE'),
                    'created_at':      repo['created_at'],
                    'pushed_at':       repo['pushed_at'],
                    'size_kb':         repo['size'],
                    'topics':          ','.join(repo.get('topics', [])),
                    'html_url':        repo['html_url'],
                    'batch_date':      execution_date,
                    'ingested_at':     datetime.utcnow().isoformat(),
                }
                producer.send(
                    KAFKA_TOPIC_GITHUB,
                    key=str(repo['id']).encode('utf-8'),
                    value=record
                )
                total_published += 1

            logger.info(f"   ✅ {lang}: {len(repos)} repos published to Kafka")
            time.sleep(1.5)

        except Exception as e:
            logger.warning(f"   ⚠️ {lang}: {e}")

    producer.flush()
    producer.close()

    logger.info(f"✅ GitHub extraction complete — {total_published} records published")
    context['ti'].xcom_push(key='github_records', value=total_published)
    return total_published


def extract_weather_history(**context):
    """
    Extract 1-day weather update from Open-Meteo Historical API.
    Publish records to Kafka topic.
    """
    import json
    import requests
    from kafka import KafkaProducer

    execution_date = context['ds']
    target_date    = (datetime.strptime(execution_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all'
    )

    BASE_URL = 'https://archive-api.open-meteo.com/v1/archive'
    DAILY_VARS = [
        'temperature_2m_max', 'temperature_2m_min', 'temperature_2m_mean',
        'precipitation_sum', 'rain_sum', 'wind_speed_10m_max',
        'wind_gusts_10m_max', 'wind_direction_10m_dominant',
        'shortwave_radiation_sum'
    ]

    total_published = 0
    for loc in LOCATIONS:
        try:
            params = {
                'latitude':   loc['latitude'],
                'longitude':  loc['longitude'],
                'start_date': target_date,
                'end_date':   target_date,
                'daily':      ','.join(DAILY_VARS),
                'timezone':   'Asia/Jakarta'
            }
            resp = requests.get(BASE_URL, params=params, timeout=20)
            resp.raise_for_status()
            data  = resp.json()
            daily = data.get('daily', {})
            dates = daily.get('time', [])

            for i, d in enumerate(dates):
                record = {
                    'city':                       loc['city'],
                    'latitude':                   loc['latitude'],
                    'longitude':                  loc['longitude'],
                    'date':                       d,
                    'temp_max_c':                 daily.get('temperature_2m_max', [None])[i],
                    'temp_min_c':                 daily.get('temperature_2m_min', [None])[i],
                    'temp_mean_c':                daily.get('temperature_2m_mean', [None])[i],
                    'precipitation_sum_mm':       daily.get('precipitation_sum', [None])[i],
                    'rain_sum_mm':                daily.get('rain_sum', [None])[i],
                    'wind_speed_max_kmh':         daily.get('wind_speed_10m_max', [None])[i],
                    'wind_gusts_max_kmh':         daily.get('wind_gusts_10m_max', [None])[i],
                    'wind_direction_dominant_deg':daily.get('wind_direction_10m_dominant', [None])[i],
                    'solar_radiation_sum':        daily.get('shortwave_radiation_sum', [None])[i],
                    'batch_date':                 execution_date,
                    'ingested_at':                datetime.utcnow().isoformat(),
                    'source':                     'open-meteo-historical'
                }
                producer.send(
                    KAFKA_TOPIC_WEATHER,
                    key=loc['city'].encode('utf-8'),
                    value=record
                )
                total_published += 1
            logger.info(f"   ✅ {loc['city']}: weather record published for {target_date}")

        except Exception as e:
            logger.warning(f"   ⚠️ {loc['city']}: {e}")

    producer.flush()
    producer.close()
    logger.info(f"✅ Weather extraction complete — {total_published} records published")
    context['ti'].xcom_push(key='weather_records', value=total_published)
    return total_published


def transform_and_stage_to_postgres(**context):
    """
    Read from Kafka, apply PySpark transformations, write to PostgreSQL staging.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        from_json, col, to_date, to_timestamp,
        when, lower, trim, round as spark_round,
        current_timestamp, datediff
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        DoubleType, BooleanType
    )

    execution_date = context['ds']

    spark = SparkSession.builder \
        .appName(f'BatchIngestion_{execution_date}') \
        .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
                'org.postgresql:postgresql:42.7.1') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # ── GitHub schema & read ───────────────────────────────────────
    GH_SCHEMA = StructType([
        StructField('repo_id',    IntegerType(), True),
        StructField('repo_name',  StringType(),  True),
        StructField('description',StringType(),  True),
        StructField('language',   StringType(),  True),
        StructField('stars',      IntegerType(), True),
        StructField('forks',      IntegerType(), True),
        StructField('open_issues',IntegerType(), True),
        StructField('watchers',   IntegerType(), True),
        StructField('is_fork',    BooleanType(), True),
        StructField('license',    StringType(),  True),
        StructField('created_at', StringType(),  True),
        StructField('pushed_at',  StringType(),  True),
        StructField('size_kb',    IntegerType(), True),
        StructField('topics',     StringType(),  True),
        StructField('batch_date', StringType(),  True),
    ])

    gh_raw = spark.read \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
        .option('subscribe', KAFKA_TOPIC_GITHUB) \
        .option('startingOffsets', 'earliest') \
        .option('endingOffsets', 'latest') \
        .load()

    gh_df = gh_raw \
        .select(from_json(col('value').cast('string'), GH_SCHEMA).alias('d')) \
        .select('d.*') \
        .withColumn('language',       lower(trim(col('language')))) \
        .withColumn('license',        when(col('license').isNull(), 'UNLICENSED').otherwise(col('license'))) \
        .withColumn('created_at',     to_timestamp(col('created_at'))) \
        .withColumn('pushed_at',      to_timestamp(col('pushed_at'))) \
        .withColumn('repo_age_days',  datediff(current_timestamp(), col('created_at'))) \
        .withColumn('stars_per_day',  spark_round(col('stars') / (col('repo_age_days') + 1), 2)) \
        .withColumn('activity_score', spark_round(
            col('stars') * 0.5 + col('forks') * 0.3 + col('watchers') * 0.2, 2
        )) \
        .dropDuplicates(['repo_id'])

    gh_count = gh_df.count()
    logger.info(f"   GitHub records after transform: {gh_count}")

    # ── Write GitHub to PostgreSQL ─────────────────────────────────
    jdbc_url = 'jdbc:postgresql://localhost:5432/batch_pipeline_db'
    jdbc_props = {'user': 'postgres', 'password': 'your_password', 'driver': 'org.postgresql.Driver'}
    gh_df.write.jdbc(jdbc_url, 'github_repositories', mode='append', properties=jdbc_props)

    spark.stop()
    context['ti'].xcom_push(key='staged_github', value=gh_count)
    logger.info(f"✅ Transform & stage complete — {gh_count} GitHub rows in PostgreSQL")


def load_to_redshift(**context):
    """
    Export staged data from PostgreSQL → S3 (Parquet) → Redshift COPY.
    """
    import pandas as pd
    import boto3
    from sqlalchemy import create_engine

    execution_date = context['ds']
    batch_tag      = execution_date.replace('-', '')

    # Read from PostgreSQL
    pg_url = (
        f"postgresql+psycopg2://postgres:your_password"
        f"@localhost:5432/batch_pipeline_db"
    )
    engine = create_engine(pg_url)

    for table, s3_key in [
        ('github_repositories', f'batch_pipeline/github/dt={execution_date}/repos_{batch_tag}.parquet'),
        ('historical_weather',  f'batch_pipeline/weather/dt={execution_date}/weather_{batch_tag}.parquet'),
    ]:
        df = pd.read_sql(
            f"SELECT * FROM {table} WHERE batch_date = '{execution_date}'",
            engine
        )
        if df.empty:
            logger.info(f"   ⚠️ No data in {table} for {execution_date}")
            continue

        # Upload to S3
        parquet_bytes = df.to_parquet(index=False)
        s3 = boto3.client('s3')
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=parquet_bytes)
        s3_path = f's3://{S3_BUCKET}/{s3_key}'
        logger.info(f"   ✅ Uploaded {len(df)} rows to {s3_path}")

        # Redshift target table
        rs_table = f"batch_ingestion.{table}"
        rs_url = (
            f"postgresql+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PASS}"
            f"@{REDSHIFT_HOST}:5439/{REDSHIFT_DB}"
        )
        rs_engine = create_engine(rs_url)
        with rs_engine.connect() as conn:
            conn.execute(f"""
                COPY {rs_table}
                FROM '{s3_path}'
                IAM_ROLE '{IAM_ROLE}'
                FORMAT AS PARQUET;
            """)
        logger.info(f"   ✅ COPY to Redshift: {rs_table}")

    logger.info("✅ Redshift load complete")


def pipeline_audit_log(**context):
    """
    Write pipeline run metadata to audit table in PostgreSQL.
    """
    ti              = context['ti']
    execution_date  = context['ds']
    github_records  = ti.xcom_pull(task_ids='extract_github_repos',   key='github_records')  or 0
    weather_records = ti.xcom_pull(task_ids='extract_weather_history', key='weather_records') or 0
    staged_github   = ti.xcom_pull(task_ids='transform_and_stage',     key='staged_github')   or 0

    hook = PostgresHook(postgres_conn_id='postgres_batch_db')
    hook.run("""
        INSERT INTO pipeline_audit_log (
            dag_id, execution_date, github_extracted, weather_extracted,
            github_staged, run_status, logged_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (dag_id, execution_date) DO UPDATE
        SET github_staged = EXCLUDED.github_staged,
            run_status    = EXCLUDED.run_status,
            logged_at     = NOW();
    """, parameters=(
        'batch_data_ingestion', execution_date,
        github_records, weather_records, staged_github, 'SUCCESS'
    ))
    logger.info(f"✅ Audit log written for {execution_date}")


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id='batch_data_ingestion',
    description='Daily batch: GitHub API + Open-Meteo Historical → Kafka → PySpark → PostgreSQL → Redshift',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 19 * * *',   # 02:00 WIB = 19:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=['batch', 'github', 'weather', 'redshift', 'project2'],
) as dag:

    start = EmptyOperator(task_id='start')

    # ── 1. Pre-flight API validation ─────────────────────────────
    validate_apis = PythonOperator(
        task_id='validate_api_connections',
        python_callable=validate_api_connections,
    )

    # ── 2. Parallel extraction ───────────────────────────────────
    extract_github = PythonOperator(
        task_id='extract_github_repos',
        python_callable=extract_github_repos,
    )

    extract_weather = PythonOperator(
        task_id='extract_weather_history',
        python_callable=extract_weather_history,
    )

    # ── 3. PySpark transform + stage to Postgres ─────────────────
    transform_stage = PythonOperator(
        task_id='transform_and_stage',
        python_callable=transform_and_stage_to_postgres,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── 4. Load Postgres → S3 → Redshift ────────────────────────
    load_redshift = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
    )

    # ── 5. Audit log ─────────────────────────────────────────────
    audit = PythonOperator(
        task_id='pipeline_audit_log',
        python_callable=pipeline_audit_log,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id='end')

    # ── DAG Flow ──────────────────────────────────────────────────
    (
        start
        >> validate_apis
        >> [extract_github, extract_weather]
        >> transform_stage
        >> load_redshift
        >> audit
        >> end
    )
