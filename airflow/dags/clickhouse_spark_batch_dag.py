from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, to_date, date_trunc
import logging

logger = logging.getLogger(__name__)

# ClickHouse параметры подключения
CLICKHOUSE_PARAMS = {
    "host": "ch_server",
    "port": 8123,
    "username": "click",
    "password": "click",
    "database": "default"
}

# Postgres JDBC параметры
PG_URL = "jdbc:postgresql://host.docker.internal:5435/weather_database"
PG_USER = "dwh"
PG_PASSWORD = "dwh"
PG_TABLE = "weather_data"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 26)
}


def spark_batch_etl_to_clickhouse():

    # 1. Запуск Spark
    spark = (
        SparkSession.builder
            .appName("Weather_Batch_ETL_to_ClickHouse")
            .master("local[2]")
            .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.5.jar")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 2. Читаем последний offset из ClickHouse
    ch = clickhouse_connect.get_client(**CLICKHOUSE_PARAMS)

    last_offset = ch.query("""
        SELECT last_offset 
        FROM weather_offset_tracker 
        WHERE table_name='weather_data'
    """).result_rows[0][0]

    logger.info(f"LAST OFFSET in ClickHouse = {last_offset}")

    # 3. Загружаем новые строки из Postgres
    df = (
        spark.read
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", PG_TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .filter(col("offset") > last_offset)
        .orderBy(col("offset"))
    )

    if df.count() == 0:
        logger.info("No new rows — exiting")
        spark.stop()
        return

    new_max_offset = df.agg({"offset": "max"}).first()[0]
    logger.info(f"New MAX OFFSET = {new_max_offset}")

    df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # =========================================
    # 4. DAILY MART 
    # =========================================
    daily = (
        df.groupBy(to_date(col("timestamp")).alias("date"), col("city"))
            .agg(
                avg("temperature").alias("avg_temp"),
                max("temperature").alias("max_temp"),
                min("temperature").alias("min_temp"),
                avg("wind_speed").alias("avg_wind_speed"),
                avg("humidity").alias("avg_humidity"),
            )
    )

    # Добавляем rows_count
    daily = daily.join(
        df.groupBy(to_date(col("timestamp")).alias("date"), col("city"))
            .count()
            .withColumnRenamed("count", "rows_count"),
        on=["date", "city"],
        how="inner"
    )

    # =========================================
    # 5. HOURLY MART
    # =========================================
    hourly = (
        df.groupBy(date_trunc("hour", col("timestamp")).alias("hour"), col("city"))
            .agg(
                avg("temperature").alias("avg_temp"),
                avg("wind_speed").alias("avg_wind_speed"),
                avg("humidity").alias("avg_humidity"),
            )
    )

    # Конвертируем Dataframe в Pandas-формат
    daily_pd = daily.toPandas()
    hourly_pd = hourly.toPandas()

    # =========================================
    # 6. Загружаем в Clickhouse
    # =========================================
    ch.insert_df("weather_city_daily", daily_pd)
    ch.insert_df("weather_city_hourly", hourly_pd)

    # =========================================
    # 7. Обновляем offset-трекер
    # =========================================
    ch.command(f"""
        ALTER TABLE weather_offset_tracker
        UPDATE last_offset = {new_max_offset}
        WHERE table_name = 'weather_data'
    """)

    logger.info(f"Offset updated to {new_max_offset}")

    spark.stop()


with DAG(
    "load_weather_to_clickhouse_spark_dag",
    default_args=default_args,
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=["spark", "postgres", "clickhouse", "etl"],
) as dag:

    spark_batch_task = PythonOperator(
        task_id="spark_batch_etl_task",
        python_callable=spark_batch_etl_to_clickhouse
    )
