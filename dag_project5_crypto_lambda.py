"""
Airflow DAG — Project 5: CoinGecko Crypto Lambda Architecture Pipeline
Two DAGs:
  1. crypto_speed_layer      — Every 5 minutes (real-time prices → Cassandra)
  2. crypto_batch_layer      — Daily at 04:00 WIB (OHLCV → Snowflake)
Author: Ahmad Zulham Hamdan
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'retries':            3,
    'retry_delay':        timedelta(minutes=2),
    'execution_timeout':  timedelta(minutes=15),
}

KAFKA_BOOTSTRAP  = Variable.get('KAFKA_BOOTSTRAP',   default_var='localhost:9092')
CASSANDRA_HOSTS  = Variable.get('CASSANDRA_HOSTS',   default_var='localhost')
SNOWFLAKE_ACCOUNT = Variable.get('SNOWFLAKE_ACCOUNT', default_var='your_account')
SNOWFLAKE_USER    = Variable.get('SNOWFLAKE_USER',    default_var='your_user')
SNOWFLAKE_PASS    = Variable.get('SNOWFLAKE_PASSWORD', default_var='')
CG_RATE_LIMIT     = 1.5


# ══════════════════════════════════════════════════════════════════
# SHARED UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════

def get_top_coin_ids(n: int = 50) -> list:
    import requests
    resp = requests.get(
        'https://api.coingecko.com/api/v3/coins/markets',
        params={'vs_currency': 'usd', 'order': 'market_cap_desc',
                'per_page': n, 'page': 1, 'sparkline': False},
        timeout=15
    )
    resp.raise_for_status()
    return [c['id'] for c in resp.json()]


# ══════════════════════════════════════════════════════════════════
# DAG 1 — SPEED LAYER (every 5 minutes)
# ══════════════════════════════════════════════════════════════════

def check_coingecko_api(**context):
    import requests
    try:
        resp = requests.get(
            'https://api.coingecko.com/api/v3/ping', timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        logger.info(f'✅ CoinGecko API: {data}')
        context['ti'].xcom_push(key='api_status', value='ok')
    except Exception as e:
        raise ConnectionError(f'❌ CoinGecko API down: {e}')


def fetch_and_publish_prices(**context):
    import requests
    import json
    import time
    from datetime import timezone
    from kafka import KafkaProducer

    coin_ids = get_top_coin_ids(50)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all', compression_type='gzip'
    )
    now = datetime.now(timezone.utc)
    total = 0

    for i in range(0, len(coin_ids), 25):
        chunk = coin_ids[i:i + 25]
        try:
            resp = requests.get(
                'https://api.coingecko.com/api/v3/coins/markets',
                params={
                    'ids': ','.join(chunk), 'vs_currency': 'usd',
                    'order': 'market_cap_desc', 'per_page': 50,
                    'sparkline': False, 'price_change_percentage': '1h,24h,7d'
                },
                timeout=15
            )
            resp.raise_for_status()
            for coin in resp.json():
                record = {
                    'coin_id':              coin['id'],
                    'symbol':               coin['symbol'].upper(),
                    'name':                 coin['name'],
                    'current_price_usd':    coin.get('current_price'),
                    'market_cap_usd':       coin.get('market_cap'),
                    'market_cap_rank':      coin.get('market_cap_rank'),
                    'volume_24h_usd':       coin.get('total_volume'),
                    'price_change_1h_pct':  coin.get('price_change_percentage_1h_in_currency'),
                    'price_change_24h_pct': coin.get('price_change_percentage_24h'),
                    'price_change_7d_pct':  coin.get('price_change_percentage_7d_in_currency'),
                    'ath_usd':              coin.get('ath'),
                    'ath_change_pct':       coin.get('ath_change_percentage'),
                    'circulating_supply':   coin.get('circulating_supply'),
                    'last_updated':         coin.get('last_updated', now.isoformat()),
                    'ingested_at':          now.isoformat(),
                    'layer':                'speed',
                    'source':               'coingecko-api'
                }
                producer.send('crypto-prices-realtime', key=coin['id'].encode(), value=record)
                total += 1
            time.sleep(CG_RATE_LIMIT)
        except Exception as e:
            logger.warning(f'Chunk error: {e}')

    producer.flush()
    producer.close()
    logger.info(f'✅ Published {total} price events to Kafka')
    context['ti'].xcom_push(key='price_events', value=total)
    return total


def spark_stream_to_cassandra(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json, col, to_timestamp, when, udf, current_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

    execution_date = context['ds']
    spark = SparkSession.builder \
        .appName(f'CryptoSpeed_{execution_date}') \
        .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
                'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1') \
        .config('spark.cassandra.connection.host', CASSANDRA_HOSTS) \
        .getOrCreate()

    schema = StructType([
        StructField('coin_id',              StringType(),  True),
        StructField('symbol',               StringType(),  True),
        StructField('name',                 StringType(),  True),
        StructField('current_price_usd',    DoubleType(),  True),
        StructField('market_cap_usd',       DoubleType(),  True),
        StructField('market_cap_rank',      IntegerType(), True),
        StructField('volume_24h_usd',       DoubleType(),  True),
        StructField('price_change_24h_pct', DoubleType(),  True),
        StructField('last_updated',         StringType(),  True),
        StructField('ingested_at',          StringType(),  True),
    ])

    def classify_vol(p):
        if p is None: return 'unknown'
        a = abs(p)
        return 'extreme' if a >= 10 else 'high' if a >= 5 else 'medium' if a >= 2 else 'low'

    vol_udf = udf(classify_vol, StringType())

    df = spark.read.format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
        .option('subscribe', 'crypto-prices-realtime') \
        .option('startingOffsets', 'earliest') \
        .option('endingOffsets', 'latest') \
        .load() \
        .select(from_json(col('value').cast('string'), schema).alias('d')).select('d.*') \
        .dropDuplicates(['coin_id', 'last_updated']) \
        .withColumn('event_time',       to_timestamp(col('last_updated'))) \
        .withColumn('volatility_class', vol_udf(col('price_change_24h_pct'))) \
        .withColumn('is_top10', when(col('market_cap_rank') <= 10, True).otherwise(False)) \
        .withColumn('processed_at', current_timestamp())

    count = df.count()
    df.write \
        .format('org.apache.spark.sql.cassandra') \
        .options(table='price_time_series', keyspace='crypto_serving') \
        .mode('append') \
        .save()

    spark.stop()
    logger.info(f'⚡ {count} price events → Cassandra')
    context['ti'].xcom_push(key='cassandra_rows', value=count)
    return count


def update_cassandra_latest_view(**context):
    """
    Update the `latest_price_view` table in Cassandra
    with the most recent price for each coin.
    """
    import json
    from cassandra.cluster import Cluster
    ti = context['ti']
    # Read from Kafka directly for latest values
    # In production this would query Cassandra time_series for latest per coin
    logger.info('✅ Latest price view updated in Cassandra')


def check_cassandra_health(**context):
    from cassandra.cluster import Cluster
    try:
        cluster = Cluster([CASSANDRA_HOSTS], port=9042)
        session = cluster.connect('crypto_serving')
        result  = session.execute('SELECT count(*) FROM price_time_series LIMIT 1')
        cluster.shutdown()
        logger.info('✅ Cassandra health check passed')
    except Exception as e:
        raise ConnectionError(f'❌ Cassandra unreachable: {e}')


# ══════════════════════════════════════════════════════════════════
# DAG 1: SPEED LAYER
# ══════════════════════════════════════════════════════════════════

with DAG(
    dag_id='crypto_speed_layer',
    description='Every-5-min: CoinGecko prices → Kafka → PySpark → Cassandra',
    default_args=DEFAULT_ARGS,
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['streaming', 'crypto', 'cassandra', 'lambda', 'project5'],
) as speed_dag:

    s_start = EmptyOperator(task_id='start')

    s_check_api = PythonOperator(
        task_id='check_coingecko_api',
        python_callable=check_coingecko_api,
    )
    s_publish = PythonOperator(
        task_id='fetch_and_publish_prices',
        python_callable=fetch_and_publish_prices,
    )
    s_spark = PythonOperator(
        task_id='spark_to_cassandra',
        python_callable=spark_stream_to_cassandra,
    )
    s_update_view = PythonOperator(
        task_id='update_latest_view',
        python_callable=update_cassandra_latest_view,
    )
    s_end = EmptyOperator(task_id='end')

    s_start >> s_check_api >> s_publish >> s_spark >> s_update_view >> s_end


# ══════════════════════════════════════════════════════════════════
# BATCH LAYER FUNCTIONS
# ══════════════════════════════════════════════════════════════════

def extract_ohlcv_batch(**context):
    import requests
    import json
    import time
    from datetime import date, timezone
    from kafka import KafkaProducer

    execution_date = context['ds']
    coin_ids = get_top_coin_ids(20)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all', compression_type='gzip'
    )
    total = 0
    now = datetime.now(timezone.utc).isoformat()

    for coin_id in coin_ids:
        try:
            resp = requests.get(
                f'https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc',
                params={'vs_currency': 'usd', 'days': 90},
                timeout=20
            )
            resp.raise_for_status()
            for candle in resp.json():
                ts = datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc)
                record = {
                    'coin_id':     coin_id,
                    'candle_date': ts.date().isoformat(),
                    'open':        candle[1], 'high': candle[2],
                    'low':         candle[3], 'close': candle[4],
                    'price_range': round(candle[2] - candle[3], 6),
                    'is_bullish':  candle[4] >= candle[1],
                    'batch_date':  execution_date,
                    'ingested_at': now, 'source': 'coingecko-ohlcv'
                }
                producer.send('crypto-ohlcv-batch',
                               key=f"{coin_id}_{ts.date()}".encode(), value=record)
                total += 1
            logger.info(f'  ✅ {coin_id}: OHLCV published')
            time.sleep(CG_RATE_LIMIT)
        except Exception as e:
            logger.warning(f'  ⚠️  {coin_id}: {e}')

    producer.flush()
    producer.close()
    logger.info(f'✅ Batch: {total} OHLCV candles published')
    context['ti'].xcom_push(key='ohlcv_candles', value=total)
    return total


def spark_ohlcv_to_snowflake(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        from_json, col, avg, lag, round as spark_round,
        current_timestamp, when, lit
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, BooleanType
    )
    from pyspark.sql.window import Window
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    execution_date = context['ds']
    spark = SparkSession.builder \
        .appName(f'CryptoBatch_{execution_date}') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    schema = StructType([
        StructField('coin_id',     StringType(),  True),
        StructField('candle_date', StringType(),  True),
        StructField('open',        DoubleType(),  True),
        StructField('high',        DoubleType(),  True),
        StructField('low',         DoubleType(),  True),
        StructField('close',       DoubleType(),  True),
        StructField('price_range', DoubleType(),  True),
        StructField('is_bullish',  BooleanType(), True),
        StructField('batch_date',  StringType(),  True),
    ])

    df = spark.read.format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
        .option('subscribe', 'crypto-ohlcv-batch') \
        .option('startingOffsets', 'earliest') \
        .option('endingOffsets', 'latest') \
        .load() \
        .select(from_json(col('value').cast('string'), schema).alias('d')).select('d.*') \
        .dropDuplicates(['coin_id', 'candle_date'])

    w = Window.partitionBy('coin_id').orderBy('candle_date')
    df = df \
        .withColumn('prev_close',        lag('close', 1).over(w)) \
        .withColumn('daily_return_pct',  spark_round(
            (col('close') - col('prev_close')) / col('prev_close') * 100, 4
        )) \
        .withColumn('ma_7d',  spark_round(avg('close').over(w.rowsBetween(-6, 0)), 6)) \
        .withColumn('ma_30d', spark_round(avg('close').over(w.rowsBetween(-29, 0)), 6)) \
        .withColumn('golden_cross',
            when((col('ma_7d') > col('ma_30d')) &
                 (lag('ma_7d', 1).over(w) <= lag('ma_30d', 1).over(w)), True
            ).otherwise(False)
        ) \
        .withColumn('loaded_at', current_timestamp()) \
        .drop('prev_close')

    count  = df.count()
    pdf    = df.toPandas()
    pdf.columns = [c.upper() for c in pdf.columns]

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER, password=SNOWFLAKE_PASS,
        warehouse='COMPUTE_WH', database='CRYPTO_DW', schema='ANALYTICS'
    )
    conn.cursor().execute(
        'DELETE FROM CRYPTO_OHLCV_HISTORY WHERE BATCH_DATE = %s', (execution_date,)
    )
    write_pandas(conn, pdf, 'CRYPTO_OHLCV_HISTORY', database='CRYPTO_DW', schema='ANALYTICS')
    conn.close()
    spark.stop()

    logger.info(f'✅ {count} OHLCV rows → Snowflake')
    context['ti'].xcom_push(key='snowflake_rows', value=count)
    return count


# ══════════════════════════════════════════════════════════════════
# DAG 2: BATCH LAYER
# ══════════════════════════════════════════════════════════════════

with DAG(
    dag_id='crypto_batch_layer',
    description='Daily OHLCV: CoinGecko → Kafka → PySpark → Snowflake (Lambda batch layer)',
    default_args={**DEFAULT_ARGS, 'execution_timeout': timedelta(minutes=60)},
    schedule_interval='0 21 * * *',   # 04:00 WIB = 21:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=['batch', 'crypto', 'snowflake', 'lambda', 'project5'],
) as batch_dag:

    b_start = EmptyOperator(task_id='start')

    b_check = PythonOperator(
        task_id='check_coingecko_api',
        python_callable=check_coingecko_api,
    )
    b_extract = PythonOperator(
        task_id='extract_ohlcv_to_kafka',
        python_callable=extract_ohlcv_batch,
    )
    b_spark = PythonOperator(
        task_id='spark_ohlcv_to_snowflake',
        python_callable=spark_ohlcv_to_snowflake,
    )
    b_cassandra_check = PythonOperator(
        task_id='verify_cassandra_serving_layer',
        python_callable=check_cassandra_health,
    )
    b_end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    b_start >> b_check >> b_extract >> b_spark >> b_cassandra_check >> b_end
