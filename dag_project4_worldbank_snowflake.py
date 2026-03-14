"""
Airflow DAG — Project 4: World Bank Economic Indicators Batch Pipeline
Schedule  : Daily at 03:00 WIB (20:00 UTC)
Stack     : World Bank API → Kafka → PySpark → PostgreSQL → dbt → Snowflake
Author    : Ahmad Zulham Hamdan
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner':             'zulham-hamdan',
    'depends_on_past':   False,
    'start_date':        datetime(2024, 1, 1),
    'email':             ['zulham.va@gmail.com'],
    'email_on_failure':  True,
    'retries':           2,
    'retry_delay':       timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=45),
}

KAFKA_BOOTSTRAP  = Variable.get('KAFKA_BOOTSTRAP',   default_var='localhost:9092')
SNOWFLAKE_ACCOUNT = Variable.get('SNOWFLAKE_ACCOUNT', default_var='your_account')
SNOWFLAKE_USER    = Variable.get('SNOWFLAKE_USER',    default_var='your_user')
SNOWFLAKE_PASS    = Variable.get('SNOWFLAKE_PASSWORD', default_var='')
DBT_PROJECT_DIR   = Variable.get('DBT_PROJECT_DIR',   default_var='/opt/airflow/dbt/worldbank_dbt')

INDICATORS = {
    'NY.GDP.MKTP.CD':    'gdp_current_usd',
    'NY.GDP.MKTP.KD.ZG': 'gdp_growth_pct',
    'NY.GDP.PCAP.CD':    'gdp_per_capita_usd',
    'FP.CPI.TOTL.ZG':    'inflation_cpi_pct',
    'SL.UEM.TOTL.ZS':    'unemployment_rate_pct',
    'SP.POP.TOTL':        'population_total',
    'IT.NET.USER.ZS':     'internet_users_pct',
    'EN.ATM.CO2E.PC':     'co2_emissions_per_capita',
}
COUNTRIES = [
    'US','CN','JP','DE','GB','FR','IN','IT','BR','CA',
    'KR','AU','MX','RU','ZA','AR','SA','TR','ID','MY',
    'TH','PH','VN','SG','MM','KH','LA','BN','TL','NL'
]


def validate_world_bank_api(**context):
    """Check World Bank API is reachable and returns data."""
    import requests
    url = 'https://api.worldbank.org/v2/country/US/indicator/NY.GDP.MKTP.CD?format=json&per_page=5'
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data or len(data) < 2:
            raise ValueError('World Bank API returned unexpected format')
        sample_count = len(data[1]) if data[1] else 0
        logger.info(f'✅ World Bank API OK — sample records: {sample_count}')
        context['ti'].xcom_push(key='api_status', value='ok')
    except Exception as e:
        raise ConnectionError(f'❌ World Bank API check failed: {e}')


def extract_worldbank_to_kafka(**context):
    """
    Extract all configured indicators for all countries from World Bank API
    and publish each record to Kafka.
    """
    import requests
    import json
    import time
    from kafka import KafkaProducer
    from datetime import date

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all',
        compression_type='gzip'
    )

    execution_date = context['ds']
    total = 0

    for code, name in INDICATORS.items():
        logger.info(f'📡 Extracting {name}...')
        for country in COUNTRIES:
            url = (
                f'https://api.worldbank.org/v2/country/{country}/indicator/{code}'
                f'?format=json&per_page=1000&date=1990:{datetime.now().year - 1}'
            )
            try:
                resp = requests.get(url, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                if not data or len(data) < 2 or not data[1]:
                    continue
                for item in data[1]:
                    if item.get('value') is None:
                        continue
                    record = {
                        'country_code':   item['country']['id'],
                        'country_name':   item['country']['value'],
                        'indicator_code': code,
                        'indicator_name': name,
                        'year':           int(item['date']),
                        'value':          float(item['value']),
                        'batch_date':     execution_date,
                        'ingested_at':    datetime.utcnow().isoformat(),
                        'source':         'world-bank-api'
                    }
                    producer.send(
                        'batch-worldbank-indicators',
                        key=f"{country}_{name}_{item['date']}".encode(),
                        value=record
                    )
                    total += 1
                time.sleep(0.3)
            except Exception as e:
                logger.warning(f'  ⚠️  {country}/{code}: {e}')

    producer.flush()
    producer.close()
    logger.info(f'✅ Extracted {total} records to Kafka')
    context['ti'].xcom_push(key='extracted_records', value=total)
    return total


def spark_transform_to_postgres(**context):
    """
    Read from Kafka, pivot indicators, compute derived metrics,
    write to PostgreSQL staging table.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        from_json, col, when, lit, lag,
        round as spark_round, current_timestamp
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType
    )
    from pyspark.sql.window import Window

    execution_date = context['ds']
    spark = SparkSession.builder \
        .appName(f'WorldBank_Transform_{execution_date}') \
        .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
                'org.postgresql:postgresql:42.7.1') \
        .getOrCreate()

    schema = StructType([
        StructField('country_code',   StringType(),  True),
        StructField('country_name',   StringType(),  True),
        StructField('indicator_code', StringType(),  True),
        StructField('indicator_name', StringType(),  True),
        StructField('year',           IntegerType(), True),
        StructField('value',          DoubleType(),  True),
        StructField('batch_date',     StringType(),  True),
        StructField('ingested_at',    StringType(),  True),
    ])

    raw = spark.read.format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
        .option('subscribe', 'batch-worldbank-indicators') \
        .option('startingOffsets', 'earliest') \
        .option('endingOffsets', 'latest') \
        .load()

    df = raw.select(from_json(col('value').cast('string'), schema).alias('d')).select('d.*') \
        .dropDuplicates(['country_code', 'indicator_name', 'year']) \
        .filter(col('value').isNotNull())

    pivot_df = df.groupBy('country_code', 'country_name', 'year') \
        .pivot('indicator_name', list(INDICATORS.values())) \
        .agg({'value': 'first'})

    for ind in INDICATORS.values():
        old = f'first({ind})'
        if old in pivot_df.columns:
            pivot_df = pivot_df.withColumnRenamed(old, ind)

    w = Window.partitionBy('country_code').orderBy('year')
    pivot_df = pivot_df \
        .withColumn('gdp_prev', lag('gdp_current_usd', 1).over(w)) \
        .withColumn('gdp_yoy_growth_calc',
            spark_round(
                (col('gdp_current_usd') - col('gdp_prev')) / col('gdp_prev') * 100, 2
            )
        ) \
        .withColumn('income_group',
            when(col('gdp_per_capita_usd') >= 12696, 'High income')
            .when(col('gdp_per_capita_usd') >= 4096,  'Upper-middle income')
            .when(col('gdp_per_capita_usd') >= 1046,  'Lower-middle income')
            .otherwise('Low income')
        ) \
        .withColumn('is_digital_ready',
            when(col('internet_users_pct') >= 60, True).otherwise(False)
        ) \
        .withColumn('batch_date', lit(execution_date)) \
        .withColumn('loaded_at', current_timestamp()) \
        .drop('gdp_prev')

    count = pivot_df.count()
    pivot_df.write.jdbc(
        'jdbc:postgresql://localhost:5432/worldbank_staging',
        'stg_worldbank_indicators',
        mode='overwrite',
        properties={
            'user': 'postgres', 'password': 'portfolio123',
            'driver': 'org.postgresql.Driver'
        }
    )
    spark.stop()
    logger.info(f'✅ {count} rows in PostgreSQL staging')
    context['ti'].xcom_push(key='staged_rows', value=count)
    return count


def load_postgres_to_snowflake(**context):
    """Copy staging data from PostgreSQL to Snowflake RAW schema."""
    import pandas as pd
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    from sqlalchemy import create_engine

    execution_date = context['ds']
    pg_engine = create_engine('postgresql+psycopg2://postgres:portfolio123@localhost:5432/worldbank_staging')
    df = pd.read_sql(
        f"SELECT * FROM stg_worldbank_indicators WHERE batch_date = '{execution_date}'",
        pg_engine
    )
    if df.empty:
        logger.info('ℹ️  No staging data to load')
        return 0

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASS,
        warehouse='COMPUTE_WH',
        database='WORLD_BANK_DW',
        schema='RAW'
    )
    conn.cursor().execute(
        'DELETE FROM RAW_WORLDBANK_INDICATORS WHERE BATCH_DATE = %s', (execution_date,)
    )
    df.columns = [c.upper() for c in df.columns]
    _, _, nrows, _ = write_pandas(conn, df, 'RAW_WORLDBANK_INDICATORS',
                                   database='WORLD_BANK_DW', schema='RAW')
    conn.close()
    logger.info(f'✅ Loaded {nrows} rows to Snowflake')
    context['ti'].xcom_push(key='snowflake_rows', value=nrows)
    return nrows


def write_audit_log(**context):
    ti              = context['ti']
    execution_date  = context['ds']
    extracted       = ti.xcom_pull(task_ids='extract_worldbank', key='extracted_records') or 0
    staged          = ti.xcom_pull(task_ids='spark_transform',   key='staged_rows')       or 0
    loaded          = ti.xcom_pull(task_ids='load_to_snowflake', key='snowflake_rows')    or 0
    hook = PostgresHook(postgres_conn_id='postgres_worldbank')
    hook.run("""
        INSERT INTO pipeline_audit (dag_id, execution_date, records_extracted,
            records_staged, records_loaded, status, logged_at)
        VALUES (%s,%s,%s,%s,%s,'SUCCESS',NOW())
        ON CONFLICT (dag_id, execution_date) DO UPDATE
        SET records_loaded=EXCLUDED.records_loaded, status='SUCCESS', logged_at=NOW();
    """, parameters=('worldbank_batch', execution_date, extracted, staged, loaded))
    logger.info(f'✅ Audit: extracted={extracted}, staged={staged}, loaded_to_snowflake={loaded}')


with DAG(
    dag_id='worldbank_batch_pipeline',
    description='Daily World Bank indicators ETL: API → Kafka → PySpark → PostgreSQL → dbt → Snowflake',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 20 * * *',   # 03:00 WIB = 20:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=['batch', 'worldbank', 'snowflake', 'dbt', 'project4'],
) as dag:

    start = EmptyOperator(task_id='start')

    validate_api = PythonOperator(
        task_id='validate_world_bank_api',
        python_callable=validate_world_bank_api,
    )

    extract = PythonOperator(
        task_id='extract_worldbank',
        python_callable=extract_worldbank_to_kafka,
    )

    transform = PythonOperator(
        task_id='spark_transform',
        python_callable=spark_transform_to_postgres,
    )

    load_sf = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_postgres_to_snowflake,
    )

    # dbt runs inside Snowflake — transform RAW → STAGING → MARTS
    dbt_run = BashOperator(
        task_id='dbt_run_all_models',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --no-version-check',
    )

    dbt_test = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir . --no-version-check',
    )

    audit = PythonOperator(
        task_id='write_audit_log',
        python_callable=write_audit_log,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id='end')

    (
        start
        >> validate_api
        >> extract
        >> transform
        >> load_sf
        >> dbt_run
        >> dbt_test
        >> audit
        >> end
    )
