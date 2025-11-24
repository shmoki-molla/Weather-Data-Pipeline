from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import clickhouse_connect
import logging

logger = logging.getLogger(__name__)

CLICKHOUSE_PARAMS = {
    "host": "ch_server",
    "port": 8123,
    "username": "click",
    "password": "click",
    "database": "default"
}

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def create_clickhouse_tables():
    client = clickhouse_connect.get_client(**CLICKHOUSE_PARAMS)

    logger.info("Creating ClickHouse tables...")

    # 1. tracking table
    client.command("""
        CREATE TABLE IF NOT EXISTS weather_offset_tracker (
            table_name String,
            last_offset UInt64
        )
        ENGINE = MergeTree()
        ORDER BY table_name
    """)

    # ensure initial row exists
    res = client.query("SELECT count() FROM weather_offset_tracker WHERE table_name='weather_data'").result_rows
    if res[0][0] == 0:
        client.command("INSERT INTO weather_offset_tracker VALUES ('weather_data', 0)")

    # 2. daily mart
    client.command("""
        CREATE TABLE IF NOT EXISTS weather_city_daily (
            date Date,
            city String,
            avg_temp Float64,
            max_temp Float64,
            min_temp Float64,
            avg_wind_speed Float64,
            avg_humidity Float64,
            rows_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (date, city)
    """)

    # 3. hourly mart
    client.command("""
        CREATE TABLE IF NOT EXISTS weather_city_hourly (
            hour DateTime,
            city String,
            avg_temp Float64,
            avg_wind_speed Float64,
            avg_humidity Float64
        )
        ENGINE = MergeTree()
        ORDER BY (hour, city)
    """)

    logger.info("ClickHouse tables created!")


with DAG(
    "init_clickhouse_marts_dag",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=["clickhouse", "init"]
) as dag:

    create_tables = PythonOperator(
        task_id="create_clickhouse_tables",
        python_callable=create_clickhouse_tables
    )
