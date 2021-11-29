import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


def hellow_world():
    logging.info("Hellow World")


# Definition of the dag
dag = DAG(
    "lesson1.exercise1",
    start_date=datetime.datetime.now() - datetime.timedelta(days=-1),
)

greet_task = PythonOperator(
    task_id="hellow_world_task", python_callable=hellow_world, dag=dag
)
