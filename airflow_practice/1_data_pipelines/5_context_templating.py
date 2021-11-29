import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def log_details(*args, **kwargs):
    #
    # TODO: Extract ds, run_id, prev_ds, and next_ds from the kwargs, and log them
    # NOTE: Look here for context variables passed in on kwargs:
    #       https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
    #
    ds = kwargs.get("ds")  # kwargs[]
    run_id = kwargs.get("run_id")  # kwargs[]
    previous_ds = kwargs.get("prev_ds")  # kwargs.get('')
    next_ds = kwargs.get("next_ds")  # kwargs.get('')

    logging.info(f"Execution date is {ds}")
    logging.info(f"My run id is {run_id}")
    if previous_ds:
        logging.info(f"My previous run was on {previous_ds}")
    if next_ds:
        logging.info(f"My next run will be {next_ds}")


dag = DAG(
    "lesson1.exercise5",
    schedule_interval="@daily",
    start_date=datetime.datetime.now() - datetime.timedelta(days=2),
)

list_task = PythonOperator(
    task_id="log_details", python_callable=log_details, provide_context=True, dag=dag
)
