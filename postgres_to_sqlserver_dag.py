from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pyodbc

default_args = {
    'start_date': datetime(2023, 1, 1),
}

def extract_from_postgres():
    # Code to extract data from PostgreSQL
    pass

def load_to_sqlserver():
    # Code to load data into SQL Server using pyodbc
    pass

with DAG('postgres_to_sqlserver_dag', default_args=default_args, schedule_interval=None) as dag:
    extract_postgres_table = PythonOperator(
        task_id='extract_postgres_table',
        python_callable=extract_from_postgres
    )

    load_sqlserver_table = PythonOperator(
        task_id='load_sqlserver_table',
        python_callable=load_to_sqlserver
    )

    extract_postgres_table >> load_sqlserver_table


# 1 TODO
    # Make sure to replace my_table, postgres_default,
    # and sqlserver_default with the appropriate table name,
    # PostgreSQL connection ID, and SQL Server connection ID, respectively.
# 2 TODO
# Install required dependencies: 
# You'll need to install additional dependencies for PostgreSQL and SQL Server 
# connections. Install the necessary packages using pip:
# pip install apache-airflow-providers-postgres apache-airflow-providers-mssql

# 3  TODO
# Start Airflow web server and scheduler: In your terminal, navigate to your 
# Airflow home directory and run the following command to start the Airflow 
# web server and scheduler:
# airflow webserver --port 8080
# airflow scheduler

# 4  TODO
# Access Airflow UI: Open your web browser and visit http://localhost:8080 
# to access the Airflow web UI.
# 5 TODO
# Enable and trigger the DAG: In the Airflow web UI, locate your DAG 
# (download_postgres_to_sqlserver_dag), 
# enable it, and manually trigger it.
