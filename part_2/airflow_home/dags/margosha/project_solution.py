# project_solution.py
import os
from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": 'margosha',
    "start_date": datetime(2025, 7, 2, 0, 0),
}

dags_dir = os.path.dirname(os.path.abspath(__file__))
# Правильний шлях всередині Docker контейнера
shared_dir = "/opt/shared"

with DAG(
    dag_id='margosha_fp_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["margosha_fp"],
    ) as dag:

    # Завдання 1: Завантаження файлів з FTP та перетворення в Bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application=os.path.join(dags_dir, 'landing_to_bronze.py'),
        conn_id='spark-default',
        application_args=[shared_dir],
        verbose=1,
        # Конфігурація Spark для local режиму
        conf={
            'spark.master': 'local[*]',
            'spark.executor.memory': '1g',
            'spark.driver.memory': '1g',
            'spark.sql.adaptive.enabled': 'false'
        }
    )

    # Завдання 2: Очищення даних та дедуплікація Bronze -> Silver
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=os.path.join(dags_dir, 'bronze_to_silver.py'),
        conn_id='spark-default',
        application_args=[shared_dir],
        verbose=1,
        # Конфігурація Spark для local режиму
        conf={
            'spark.master': 'local[*]',
            'spark.executor.memory': '1g',
            'spark.driver.memory': '1g',
            'spark.sql.adaptive.enabled': 'false'
        }
    )

    # Завдання 3: Агрегація та розрахунок середніх значень Silver -> Gold
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=os.path.join(dags_dir, 'silver_to_gold.py'),
        conn_id='spark-default',
        application_args=[shared_dir],
        verbose=1,
        # Конфігурація Spark для local режиму
        conf={
            'spark.master': 'local[*]',
            'spark.executor.memory': '1g',
            'spark.driver.memory': '1g',
            'spark.sql.adaptive.enabled': 'false'
        }
    )

    # Послідовність виконання завдань
    landing_to_bronze >> bronze_to_silver >> silver_to_gold