from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, ArrayType
import logging
import sys
from io import StringIO
import uuid
import shutil
import os

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 26)
}

# === PostgreSQL настройки (те же, что в init_database_test_dag.py) ===
PG_URL = "jdbc:postgresql://host.docker.internal:5435/weather_database"
PG_USER = "dwh"
PG_PASSWORD = "dwh"
PG_TABLE = "weather_data"


def spark_streaming_weather():
    try:
        logger.info("Starting Spark session for weather streaming")

        # Очистка checkpoint (только для тестов)
        checkpoint_dir = "/tmp/spark-checkpoint-weather"
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)

        # === SparkSession ===
        spark = (
            SparkSession.builder
                .appName("Airflow_Spark_Streaming_Weather_To_DB")
                .master("local[2]")
                .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
                .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.5.jar")
                .config("spark.driver.memory", "1g")
                .config("spark.executor.memory", "1g")
                #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
                .config(
                    "spark.jars",
                    "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
                    "/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,"
                    "/opt/airflow/jars/kafka-clients-3.5.1.jar,"
                    "/opt/airflow/jars/commons-pool2-2.11.1.jar,"
                    "/opt/airflow/jars/postgresql-42.7.5.jar"
            )
                .getOrCreate()
        )

        logger.info(f"Spark version: {spark.version}")

        # === схема JSON ===
        schema = StructType([
            StructField("coord", StructType([
                StructField("lon", FloatType()),
                StructField("lat", FloatType())
            ])),
            StructField("weather", ArrayType(StructType([
                StructField("id", IntegerType()),
                StructField("main", StringType()),
                StructField("description", StringType()),
                StructField("icon", StringType())
            ]))),
            StructField("main", StructType([
                StructField("temp", FloatType()),
                StructField("feels_like", FloatType()),
                StructField("temp_min", FloatType()),
                StructField("temp_max", FloatType()),
                StructField("pressure", IntegerType()),
                StructField("humidity", IntegerType())
            ])),
            StructField("wind", StructType([
                StructField("speed", FloatType()),
                StructField("deg", IntegerType()),
                StructField("gust", FloatType())
            ])),
            StructField("clouds", StructType([
                StructField("all", IntegerType())
            ])),
            StructField("dt", TimestampType()),
            StructField("sys", StructType([
                StructField("country", StringType()),
                StructField("sunrise", TimestampType()),
                StructField("sunset", TimestampType())
            ])),
            StructField("timezone", IntegerType()),
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("cod", IntegerType())
        ])

        # === Kafka consumer group ===
        group_id = f"spark-weather-{uuid.uuid4()}"

        df = (
            spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "weather_topic")
                .option("startingOffsets", "earliest")
                .option("kafka.group.id", group_id)
                .option("failOnDataLoss", "false")
                .load()
        )

        kafka_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("topic"), col("partition"), col("offset")
        )

        processed_df = kafka_df.select(
            col("data.main.temp").alias("temperature"),
            col("data.main.humidity").alias("humidity"),
            col("data.wind.speed").alias("wind_speed"),
            col("data.name").alias("city"),
            col("data.dt").alias("timestamp"),
            col("topic"),
            col("partition"),
            col("offset")
        )

        # === Функция записи в PostgreSQL ===
        def write_to_db(batch_df, batch_id):
            logger.info(f"Writing batch {batch_id} to PostgreSQL")

            batch_df.write \
                .format("jdbc") \
                .option("url", PG_URL) \
                .option("dbtable", PG_TABLE) \
                .option("user", PG_USER) \
                .option("password", PG_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            logger.info(f"Batch {batch_id} written successfully")

        # === Запуск стриминга ===
        query = (
            processed_df.writeStream
                .outputMode("append")
                .foreachBatch(write_to_db)
                .trigger(processingTime="10 seconds")
                .start()
        )

        query.awaitTermination(60)
        query.stop()
        spark.stop()

        logger.info("Streaming stopped, Spark closed")

    except Exception as e:
        logger.error(f"Error in spark_streaming_weather: {e}", exc_info=True)
        raise


with DAG(
        'consumer_to_db',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['spark', 'kafka', 'postgres', 'test']
) as dag:

    streaming_task = PythonOperator(
        task_id='spark_stream_weather',
        python_callable=spark_streaming_weather
    )
