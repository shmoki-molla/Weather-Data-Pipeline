from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import logging

logger = logging.getLogger(__name__)

# Параметры 
HOST = "host.docker.internal"
PORT = 5435
USER = "dwh"
PASSWORD = "dwh"
DB_NAME = "weather_database"
TABLE_NAME = "weather_data"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def create_db_and_table():
    try:
        # Подключение к Postgres
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database="postgres"  # любая существующая БД
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Создаём БД, если нет
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
        if not cur.fetchone():
            logger.info(f"Creating database: {DB_NAME}")
            cur.execute(f"CREATE DATABASE {DB_NAME}")
        else:
            logger.info(f"Database {DB_NAME} already exists")

        cur.close()
        conn.close()

        # Подключаемся к новой БД и создаём таблицу
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DB_NAME
        )
        conn.autocommit = True
        cur = conn.cursor()

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            temperature FLOAT,
            humidity INTEGER,
            wind_speed FLOAT,
            city VARCHAR(100),
            timestamp TIMESTAMP,
            topic VARCHAR(100),
            partition INTEGER,
            "offset" BIGINT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        logger.info(f"Creating table {TABLE_NAME}")
        cur.execute(create_table_sql)

        cur.close()
        conn.close()
        logger.info("Database and table created successfully!")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise

with DAG(
    'create_weather_db_simple_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['postgres', 'test']
) as dag:
    task = PythonOperator(
        task_id='create_db_and_table',
        python_callable=create_db_and_table,
    )