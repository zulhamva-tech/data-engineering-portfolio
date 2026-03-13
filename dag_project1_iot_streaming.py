"""
Airflow DAG — Project 1: Real-Time IoT Weather Sensor Streaming Pipeline
Schedule  : Every 1 hour (sync PostgreSQL → BigQuery)
Author    : Ahmad Zulham Hamdan
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
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
    'retries':            3,
    'retry_delay':        timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay':    timedelta(minutes=30),
    'execution_timeout':  timedelta(minutes=20),
}

# ─── Airflow Variables (set in Airflow UI > Admin > Variables) ─────────────────
GCP_PROJECT_ID   = Variable.get('GCP_PROJECT_ID',   default_var='your-gcp-project')
BQ_DATASET       = Variable.get('BQ_DATASET',        default_var='iot_weather_streaming')
BQ_TABLE         = Variable.get('BQ_TABLE',          default_var='sensor_readings')
SLACK_WEBHOOK    = Variable.get('SLACK_WEBHOOK_URL', default_var='')
KAFKA_BOOTSTRAP  = Variable.get('KAFKA_BOOTSTRAP',   default_var='localhost:9092')
KAFKA_TOPIC      = 'iot-weather-sensors'


# ══════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def check_kafka_health(**context):
    """
    Verify Kafka broker is reachable and topic exists.
    Raises exception to fail the DAG if Kafka is down.
    """
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id='airflow-health-check',
            request_timeout_ms=5000
        )
        topics = admin.list_topics()
        admin.close()

        if KAFKA_TOPIC not in topics:
            raise ValueError(f"Kafka topic '{KAFKA_TOPIC}' not found. Available: {topics}")

        logger.info("✅ Kafka health OK — broker reachable, topic exists")
        context['ti'].xcom_push(key='kafka_status', value='healthy')
        return 'healthy'

    except NoBrokersAvailable:
        raise ConnectionError(f"❌ Kafka broker unreachable at {KAFKA_BOOTSTRAP}")


def check_new_records(**context):
    """
    Count unsynced records in PostgreSQL.
    Branch to 'sync_to_bigquery' if records exist, else skip.
    """
    hook = PostgresHook(postgres_conn_id='postgres_iot_db')
    result = hook.get_first("""
        SELECT COUNT(*)
        FROM iot_sensor_readings
        WHERE synced_to_bq = FALSE
          AND event_time >= NOW() - INTERVAL '2 hours';
    """)

    count = result[0] if result else 0
    logger.info(f"📊 Unsynced records found: {count}")
    context['ti'].xcom_push(key='unsynced_count', value=count)

    if count > 0:
        return 'sync_to_bigquery'
    else:
        logger.info("ℹ️  No new records — skipping sync")
        return 'no_new_records'


def sync_postgres_to_bigquery(**context):
    """
    Pull unsynced rows from PostgreSQL and load to BigQuery.
    Marks rows as synced after successful load.
    """
    import pandas as pd
    from google.cloud import bigquery

    # Pull unsynced data
    hook  = PostgresHook(postgres_conn_id='postgres_iot_db')
    conn  = hook.get_conn()
    query = """
        SELECT sensor_id, city, latitude, longitude, event_time,
               temperature_c, humidity_pct, apparent_temp_c,
               precipitation_mm, weather_code, wind_speed_kmh,
               wind_direction_deg, ingestion_source
        FROM iot_sensor_readings
        WHERE synced_to_bq = FALSE
          AND event_time >= NOW() - INTERVAL '2 hours'
        ORDER BY event_time ASC
        LIMIT 50000;
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        logger.info("ℹ️  No rows to sync")
        return 0

    # Load to BigQuery
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id  = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField('sensor_id',          'STRING'),
            bigquery.SchemaField('city',               'STRING'),
            bigquery.SchemaField('latitude',           'FLOAT64'),
            bigquery.SchemaField('longitude',          'FLOAT64'),
            bigquery.SchemaField('event_time',         'TIMESTAMP'),
            bigquery.SchemaField('temperature_c',      'FLOAT64'),
            bigquery.SchemaField('humidity_pct',       'FLOAT64'),
            bigquery.SchemaField('apparent_temp_c',    'FLOAT64'),
            bigquery.SchemaField('precipitation_mm',   'FLOAT64'),
            bigquery.SchemaField('weather_code',       'INTEGER'),
            bigquery.SchemaField('wind_speed_kmh',     'FLOAT64'),
            bigquery.SchemaField('wind_direction_deg', 'FLOAT64'),
            bigquery.SchemaField('ingestion_source',   'STRING'),
        ]
    )
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # wait

    # Mark as synced
    ids_synced = df['sensor_id'].tolist()
    times      = df['event_time'].tolist()

    hook2 = PostgresHook(postgres_conn_id='postgres_iot_db')
    hook2.run("""
        UPDATE iot_sensor_readings
        SET synced_to_bq = TRUE
        WHERE synced_to_bq = FALSE
          AND event_time >= NOW() - INTERVAL '2 hours';
    """)

    rows_synced = len(df)
    logger.info(f"✅ Synced {rows_synced} rows to BigQuery → {table_id}")
    context['ti'].xcom_push(key='rows_synced', value=rows_synced)
    return rows_synced


def run_data_quality_check(**context):
    """
    Validate data completeness and freshness in BigQuery.
    Fails task if quality thresholds are not met.
    """
    from google.cloud import bigquery

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id  = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    quality_sql = f"""
        SELECT
            COUNT(*)                                       AS total_rows,
            COUNTIF(temperature_c IS NULL)                 AS null_temp,
            COUNTIF(humidity_pct IS NULL)                  AS null_humidity,
            ROUND(
                COUNTIF(temperature_c IS NOT NULL) / COUNT(*) * 100, 2
            )                                              AS completeness_pct,
            MAX(event_time)                                AS latest_event,
            TIMESTAMP_DIFF(
                CURRENT_TIMESTAMP(), MAX(event_time), MINUTE
            )                                              AS minutes_since_last_event
        FROM `{table_id}`
        WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR);
    """

    result = bq_client.query(quality_sql).result()
    row = list(result)[0]

    logger.info(f"📋 Quality Report:")
    logger.info(f"   Total rows (last 3h): {row.total_rows}")
    logger.info(f"   Completeness: {row.completeness_pct}%")
    logger.info(f"   Minutes since last event: {row.minutes_since_last_event}")

    # Thresholds
    if row.completeness_pct < 95.0:
        raise ValueError(
            f"❌ Data completeness {row.completeness_pct}% < 95% threshold"
        )
    if row.minutes_since_last_event and row.minutes_since_last_event > 90:
        raise ValueError(
            f"❌ Last event was {row.minutes_since_last_event} min ago — sensor may be down"
        )

    logger.info("✅ Data quality checks passed")
    context['ti'].xcom_push(key='completeness_pct', value=float(row.completeness_pct))


def compute_hourly_aggregates(**context):
    """
    Compute hourly sensor aggregates in BigQuery for dashboard use.
    Writes to a separate `sensor_hourly_agg` table.
    """
    from google.cloud import bigquery

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    agg_sql = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.sensor_hourly_agg`
        PARTITION BY DATE(hour_bucket)
        AS
        SELECT
            TIMESTAMP_TRUNC(event_time, HOUR)  AS hour_bucket,
            city,
            sensor_id,
            ROUND(AVG(temperature_c), 2)       AS avg_temp_c,
            ROUND(MAX(temperature_c), 2)       AS max_temp_c,
            ROUND(MIN(temperature_c), 2)       AS min_temp_c,
            ROUND(AVG(humidity_pct), 2)        AS avg_humidity_pct,
            ROUND(AVG(wind_speed_kmh), 2)      AS avg_wind_speed_kmh,
            ROUND(SUM(precipitation_mm), 2)    AS total_precipitation_mm,
            COUNT(*)                           AS event_count
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        GROUP BY hour_bucket, city, sensor_id;
    """

    job = bq_client.query(agg_sql)
    job.result()
    logger.info("✅ Hourly aggregates refreshed in BigQuery")


def send_slack_success(**context):
    """Push summary notification to Slack on successful run."""
    ti             = context['ti']
    rows_synced    = ti.xcom_pull(task_ids='sync_to_bigquery', key='rows_synced') or 0
    completeness   = ti.xcom_pull(task_ids='data_quality_check', key='completeness_pct') or '-'
    execution_date = context['ds']

    message = (
        f":white_check_mark: *IoT Streaming DAG — SUCCESS*\n"
        f"> Date: `{execution_date}`\n"
        f"> Rows synced to BigQuery: `{rows_synced}`\n"
        f"> Data completeness: `{completeness}%`\n"
        f"> Pipeline: PostgreSQL → BigQuery ✅"
    )
    logger.info(f"Slack notification: {message}")
    # In production: use SlackWebhookOperator or requests.post to SLACK_WEBHOOK


def send_slack_failure(context):
    """Callback — fires on any task failure."""
    task_id    = context['task_instance'].task_id
    exec_date  = context['ds']
    exception  = context.get('exception', 'Unknown error')

    message = (
        f":red_circle: *IoT Streaming DAG — FAILED*\n"
        f"> Task: `{task_id}`\n"
        f"> Date: `{exec_date}`\n"
        f"> Error: `{str(exception)[:200]}`"
    )
    logger.error(f"DAG failure alert: {message}")
    # In production: post to SLACK_WEBHOOK


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id='iot_streaming_bq_sync',
    description='Sync IoT sensor readings from PostgreSQL to BigQuery — hourly',
    default_args={**DEFAULT_ARGS, 'on_failure_callback': send_slack_failure},
    schedule_interval='0 * * * *',   # every hour at :00
    catchup=False,
    max_active_runs=1,
    tags=['streaming', 'iot', 'bigquery', 'project1'],
) as dag:

    # ── 1. Health Checks ─────────────────────────────────────────
    start = EmptyOperator(task_id='start')

    check_kafka = PythonOperator(
        task_id='check_kafka_health',
        python_callable=check_kafka_health,
    )

    # ── 2. Branch: any new records? ──────────────────────────────
    branch_new_records = BranchPythonOperator(
        task_id='branch_new_records',
        python_callable=check_new_records,
    )

    no_new_records = EmptyOperator(task_id='no_new_records')

    # ── 3. Sync PostgreSQL → BigQuery ────────────────────────────
    sync_bq = PythonOperator(
        task_id='sync_to_bigquery',
        python_callable=sync_postgres_to_bigquery,
    )

    # ── 4. Data Quality Gate ─────────────────────────────────────
    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=run_data_quality_check,
    )

    # ── 5. Compute Aggregates ────────────────────────────────────
    hourly_agg = PythonOperator(
        task_id='compute_hourly_aggregates',
        python_callable=compute_hourly_aggregates,
    )

    # ── 6. Success Notification ──────────────────────────────────
    notify_success = PythonOperator(
        task_id='notify_slack_success',
        python_callable=send_slack_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── DAG Flow ──────────────────────────────────────────────────
    (
        start
        >> check_kafka
        >> branch_new_records
        >> [sync_bq, no_new_records]
    )
    sync_bq >> quality_check >> hourly_agg >> notify_success >> end
    no_new_records >> end
