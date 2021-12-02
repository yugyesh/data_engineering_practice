# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import datetime

from airflow import DAG

from airflow.operators.postgres_operator import PostgresOperator

from operators import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator,
)
import sql_statements

#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
dag = DAG("lesson3.exercise4", start_date=datetime.datetime.utcnow())

#
# TODO: Load trips data from S3 to RedShift. Use the s3_key
# create trips table
create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
)

# Load trips data
copy_trips_task = S3ToRedshiftOperator(
    task_id="copy_trips",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
    table="trips",
)

# Check data quality
check_trips = HasRowsOperator(
    task_id="check_trips", table="trips", redshift_conn_id="redshift"
)

# Create fact table
calculate_facts = FactsCalculatorOperator(
    task_id="calculate_facts",
    destination_table="facts_trips",
    origin_table="trips",
    redshift_conn_id="redshift",
    fact_column="bikeid",
    groupby_column="tripduration",
)

# Ordering the dags

create_trips_table >> copy_trips_task
copy_trips_task >> calculate_facts
