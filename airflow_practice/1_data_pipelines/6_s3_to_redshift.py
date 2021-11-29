# How to move data from s3 to redshift using s3 operator

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sql_statements

# create dag
dag = DAG("lesson1.exercise6", start_date=datetime.now() - timedelta(days=1))

# task to create table on redshift
create_table_task = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="redshift",
    dag=dag,
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
)

# task to copy data to redshift
def copy_data_to_redshift():
    aws_hook = AwsBaseHook("aws_credentials", client_type="redshift")
    credentials = aws_hook.get_credentials()

    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(
        sql_statements.COPY_ALL_TRIPS_SQL.format(
            credentials.access_key, credentials.secret_key
        )
    )


copy_data_task = PythonOperator(
    task_id="copy_data_to_redshift", dag=dag, python_callable=copy_data_to_redshift
)

# task to calculate location traffic
location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL,
)


# create task dependencies
create_table_task >> copy_data_task
copy_data_task >> location_traffic_task
