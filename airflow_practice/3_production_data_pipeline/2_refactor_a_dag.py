# Instructions
# In this exercise, we’ll refactor a DAG with a single overloaded task into a DAG with several tasks with well-defined boundaries
# 1 - Read through the DAG and identify points in the DAG that could be split apart
# 2 - Split the DAG into multiple PythonOperators
# 3 - Run the DAG

# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


dag = DAG("lesson3.exercise2", start_date=datetime.datetime.utcnow())

# region find younger rider
create_younger_rider_table = PostgresOperator(
    task_id="create_younger_table",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """,
    postgres_conn_id="redshift",
)


def find_younger_rider():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(
        """
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    """
    )
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")


find_younger_rider_task = PythonOperator(
    task_id="find_younger_rider", dag=dag, python_callable=find_younger_rider
)

# endregion

# region find older rider
create_older_rider_table = PostgresOperator(
    task_id="create_older_rider",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
        );
        COMMIT;
    """,
    postgres_conn_id="redshift",
)


def find_older_rider():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(
        """
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """
    )
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")


find_older_rider_task = PythonOperator(
    task_id="find_older_rider_task", dag=dag, python_callable=find_older_rider
)
# endregion


def load_and_analyze(*args, **kwargs):
    redshift_hook = PostgresHook("redshift")

    # Find out how often each bike is ridden
    redshift_hook.run(
        """
        BEGIN;
        DROP TABLE IF EXISTS lifetime_rides;
        CREATE TABLE lifetime_rides AS (
            SELECT bikeid, COUNT(bikeid)
            FROM trips
            GROUP BY bikeid
        );
        COMMIT;
    """
    )

    # Count the number of stations by city
    redshift_hook.run(
        """
        BEGIN;
        DROP TABLE IF EXISTS city_station_counts;
        CREATE TABLE city_station_counts AS(
            SELECT city, COUNT(city)
            FROM stations
            GROUP BY city
        );
        COMMIT;
    """
    )


load_and_analyze = PythonOperator(
    task_id="load_and_analyze",
    dag=dag,
    python_callable=load_and_analyze,
    provide_context=True,
)


create_younger_rider_table >> find_younger_rider_task
create_older_rider_table >> find_older_rider_task
