from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import requests
import json
import logging

# Настраиваем логирование
logger = logging.getLogger(__name__)

# Список городов для запроса погоды
CITIES = ["Moscow", "Saint Petersburg", "Novosibirsk"]

# API-ключ OpenWeatherMap
API_KEY = "e18b6a035faed3364ce0faab29613288"
API_URL = "https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

# Настройки для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def fetch_weather_to_kafka():
    try:
        logger.info("Starting weather API fetch and Kafka publish")

        # Создаём продюсера Kafka
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',  
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  
        )

        # Запрашиваем погоду для каждого города
        for city in CITIES:
            url = API_URL.format(city=city, api_key=API_KEY)
            logger.info(f"Fetching weather for {city}")
            response = requests.get(url)
            response.raise_for_status()  

            weather_data = response.json()
            logger.info(f"Weather data for {city}: {weather_data}")

            # Отправляем данные в Kafka
            producer.send('weather_topic', value=weather_data)
            logger.info(f"Sent weather data for {city} to Kafka topic 'weather_topic'")

        # Гарантируем отправку всех сообщений
        producer.flush()
        producer.close()
        logger.info("All weather data sent to Kafka")

    except Exception as e:
        logger.error(f"Error in fetch_weather_to_kafka: {str(e)}", exc_info=True)
        raise



with DAG(
        'producer_api_test_dag',
        default_args=default_args,
        schedule_interval=timedelta(hours=1),  
        catchup=False,
        tags=['kafka', 'api', 'test']
) as dag:
    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_to_kafka_task',
        python_callable=fetch_weather_to_kafka,
        dag=dag
    )