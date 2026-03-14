"""
Airflow DAG — Project 3: Reddit Sentiment Streaming Pipeline
Schedule  : Every hour (MongoDB → BigQuery sync + quality check)
Author    : Ahmad Zulham Hamdan
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner':              'zulham-hamdan',
    'depends_on_past':    False,
    'start_date':         datetime(2024, 1, 1),
    'email':              ['zulham.va@gmail.com'],
    'email_on_failure':   True,
    'retries':            2,
    'retry_delay':        timedelta(minutes=5),
    'execution_timeout':  timedelta(minutes=20),
}

MONGO_URI   = Variable.get('MONGO_URI',    default_var='mongodb://localhost:27017')
GCP_PROJECT = Variable.get('GCP_PROJECT',  default_var='your-gcp-project')
BQ_DATASET  = Variable.get('BQ_DATASET_REDDIT', default_var='reddit_sentiment_analytics')
BQ_TABLE    = Variable.get('BQ_TABLE_REDDIT',   default_var='post_sentiment')


def check_mongodb_health(**context):
    import pymongo
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        coll  = client['reddit_data_lake']['sentiment_results']
        count = coll.count_documents({})
        client.close()
        logger.info(f'✅ MongoDB OK — total docs: {count}')
        context['ti'].xcom_push(key='total_docs', value=count)
        return 'healthy'
    except Exception as e:
        raise ConnectionError(f'❌ MongoDB unreachable: {e}')


def count_unsynced_records(**context):
    import pymongo
    from datetime import timezone
    client = pymongo.MongoClient(MONGO_URI)
    coll   = client['reddit_data_lake']['sentiment_results']
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    unsynced = coll.count_documents({
        'ingested_at': {'$gte': cutoff},
        'synced_to_bq': {'$ne': True}
    })
    client.close()
    logger.info(f'📊 Unsynced records in last 2h: {unsynced}')
    context['ti'].xcom_push(key='unsynced_count', value=unsynced)
    return 'sync_to_bigquery' if unsynced > 0 else 'no_new_records'


def sync_mongodb_to_bigquery(**context):
    import pymongo
    import pandas as pd
    from google.cloud import bigquery
    from datetime import timezone

    client = pymongo.MongoClient(MONGO_URI)
    coll   = client['reddit_data_lake']['sentiment_results']
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    records = list(coll.find(
        {'ingested_at': {'$gte': cutoff}, 'synced_to_bq': {'$ne': True}},
        {'_id': 0}
    ).limit(50000))
    client.close()

    if not records:
        logger.info('ℹ️  Nothing to sync')
        return 0

    df = pd.DataFrame(records)
    for c in ['score', 'num_comments']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
    for c in ['sentiment_score', 'upvote_ratio']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)

    bq       = bigquery.Client(project=GCP_PROJECT)
    table_id = f'{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}'
    job      = bq.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
    )
    job.result()

    post_ids = df['post_id'].tolist()
    client2  = pymongo.MongoClient(MONGO_URI)
    client2['reddit_data_lake']['sentiment_results'].update_many(
        {'post_id': {'$in': post_ids}},
        {'$set': {'synced_to_bq': True}}
    )
    client2.close()

    synced = len(records)
    logger.info(f'✅ Synced {synced} records → BigQuery')
    context['ti'].xcom_push(key='synced_count', value=synced)
    return synced


def run_quality_checks(**context):
    from google.cloud import bigquery
    bq       = bigquery.Client(project=GCP_PROJECT)
    table_id = f'{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}'
    result   = list(bq.query(f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(sentiment_label IS NULL) AS null_labels,
            ROUND(AVG(sentiment_score), 4)   AS avg_score,
            COUNTIF(sentiment_label = 'positive') AS positive_count,
            COUNTIF(sentiment_label = 'negative') AS negative_count,
            COUNTIF(sentiment_label = 'neutral')  AS neutral_count
        FROM `{table_id}`
        WHERE DATE(ingested_at) = CURRENT_DATE()
    """).result())
    row = result[0]
    logger.info(f'📋 Quality check — total: {row.total}, avg_score: {row.avg_score}')
    if row.null_labels and row.total > 0 and row.null_labels / row.total > 0.05:
        raise ValueError(f'❌ Too many NULL sentiment labels: {row.null_labels}/{row.total}')
    logger.info('✅ Quality checks passed')


def compute_subreddit_aggregates(**context):
    """Refresh daily subreddit aggregate table in BigQuery."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=GCP_PROJECT)
    bq.query(f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.subreddit_daily_agg`
        PARTITION BY post_date AS
        SELECT
            DATE(created_utc)                          AS post_date,
            subreddit,
            COUNT(*)                                   AS total_posts,
            ROUND(AVG(sentiment_score), 4)             AS avg_sentiment,
            ROUND(AVG(score), 2)                       AS avg_reddit_score,
            COUNTIF(sentiment_label = 'positive')      AS positive_count,
            COUNTIF(sentiment_label = 'negative')      AS negative_count,
            COUNTIF(sentiment_label = 'neutral')       AS neutral_count,
            ROUND(
              COUNTIF(sentiment_label='positive') / COUNT(*) * 100, 2
            )                                          AS positive_pct
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE DATE(created_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY post_date, subreddit
    """).result()
    logger.info('✅ Subreddit daily aggregates refreshed')


def send_failure_alert(context):
    task_id   = context['task_instance'].task_id
    exec_date = context['ds']
    logger.error(f'🔴 DAG FAILED — Task: {task_id} | Date: {exec_date}')


with DAG(
    dag_id='reddit_sentiment_sync',
    description='Hourly MongoDB → BigQuery sync for Reddit sentiment pipeline',
    default_args={**DEFAULT_ARGS, 'on_failure_callback': send_failure_alert},
    schedule_interval='0 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['streaming', 'reddit', 'sentiment', 'mongodb', 'bigquery', 'project3'],
) as dag:

    start = EmptyOperator(task_id='start')

    check_mongo = PythonOperator(
        task_id='check_mongodb_health',
        python_callable=check_mongodb_health,
    )

    branch = BranchPythonOperator(
        task_id='count_unsynced_records',
        python_callable=count_unsynced_records,
    )

    no_new = EmptyOperator(task_id='no_new_records')

    sync_bq = PythonOperator(
        task_id='sync_to_bigquery',
        python_callable=sync_mongodb_to_bigquery,
    )

    quality = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_quality_checks,
    )

    aggregates = PythonOperator(
        task_id='compute_subreddit_aggregates',
        python_callable=compute_subreddit_aggregates,
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    (
        start
        >> check_mongo
        >> branch
        >> [sync_bq, no_new]
    )
    sync_bq >> quality >> aggregates >> end
    no_new >> end
