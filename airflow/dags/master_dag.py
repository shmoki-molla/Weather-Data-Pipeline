# master_weather_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'master_weather_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False,
    tags=['master', 'weather', 'pipeline']
) as dag:
    
    start = DummyOperator(task_id='start_pipeline')
    
    # 1. Инициализация БД
    init_postgres = TriggerDagRunOperator(
        task_id='init_postgres_db',
        trigger_dag_id='create_weather_db_simple_dag',
        wait_for_completion=True,
        execution_date='{{ ds }}'
    )
    
    # 2. Инициализация ClickHouse
    init_clickhouse = TriggerDagRunOperator(
        task_id='init_clickhouse_tables',
        trigger_dag_id='init_clickhouse_marts_dag',
        wait_for_completion=True,
        execution_date='{{ ds }}'
    )
    
    # 3. Запуск продюсера для сбора данных
    run_producer = TriggerDagRunOperator(
        task_id='run_weather_producer',
        trigger_dag_id='producer_api_test_dag',
        wait_for_completion=True,
        execution_date='{{ ds }}'
    )
    
    # 4. Запуск консьюмера для обработки потоковых данных
    run_consumer = TriggerDagRunOperator(
        task_id='run_spark_consumer',
        trigger_dag_id='consumer_to_db',
        wait_for_completion=True,
        execution_date='{{ ds }}'
    )
    
    # 5. Запуск ETL в ClickHouse
    run_clickhouse_etl = TriggerDagRunOperator(
        task_id='run_clickhouse_etl',
        trigger_dag_id='load_weather_to_clickhouse_spark_dag',
        wait_for_completion=True,
        execution_date='{{ ds }}'
    )
    
    end = DummyOperator(task_id='pipeline_complete')
    
    # Определяем порядок выполнения
    start >> [init_postgres, init_clickhouse] >> run_producer >> run_consumer >> run_clickhouse_etl >> end