# Instructions
# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.
# 1 - Run the DAG as it is first, and observe the Airflow UI
# 2 - Next, open up the DAG and add the copy and load tasks as directed in the TODOs
# 3 - Reload the Airflow UI and run the DAG once more, observing the Airflow UI

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sql_statements


def load_trip_data_to_redshift(*args, **kwargs):
    aws_hook = AwsBaseHook("aws_credentials", client_type="redshift")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.COPY_ALL_TRIPS_SQL.format(
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)


def load_station_data_to_redshift(*args, **kwargs):
    aws_hook = AwsBaseHook("aws_credentials", client_type="redshift")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)


dag = DAG("lesson2.exercise1", start_date=datetime.datetime.now())

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
)

copy_trips_task = PythonOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    python_callable=load_trip_data_to_redshift,
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
)

copy_stations_task = PythonOperator(
    task_id="load_stations_from_s3_to_redshift",
    dag=dag,
    python_callable=load_station_data_to_redshift,
)

create_trips_table >> copy_trips_task
create_stations_table >> copy_stations_task
