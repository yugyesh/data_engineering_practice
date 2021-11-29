# print hellow word daily using dag

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    "lesson1.exercise2",
    start_date=datetime.now() - timedelta(days=2),
    schedule_interval="@daily",
)


def hellow_world():
    logging.info("Hellow world")


print_hellow_task = PythonOperator(
    task_id="print_hellow_world", python_callable=hellow_world, dag=dag
)
