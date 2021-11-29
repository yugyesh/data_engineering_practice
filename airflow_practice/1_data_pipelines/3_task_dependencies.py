# Create a pipeline that shows welcome message
# and perform addition and substraction before
# division

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG("lesson1.exercise3", start_date=datetime.now() - timedelta(days=1))


def print_hello_world():
    logging.info("Hellow")


def addition():
    logging.info(f"Adding two numbers {2+2}")


def substraction():
    logging.info(f"substracting two number{3-3}")


def division():
    logging.info(f"dividing two numbers{4/4}")


hello_world_task = PythonOperator(
    task_id="hello_world_task", python_callable=print_hello_world, dag=dag
)

addition_task = PythonOperator(
    task_id="addition_task", python_callable=addition, dag=dag
)
substraction_task = PythonOperator(
    task_id="substraction_task", python_callable=substraction, dag=dag
)
division_task = PythonOperator(
    task_id="division_task", python_callable=division, dag=dag
)

# Creating a task dependency
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task

hello_world_task >> addition_task
hello_world_task >> substraction_task

addition_task >> division_task
substraction_task >> division_task
