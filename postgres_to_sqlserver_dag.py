from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.mssql.operators.mssql import MsSqlOperator 
#  for some reason MsSqlOperator finally works, so i can remove pyodbc
from airflow.operators.python_operator import PythonOperator
import pyodbc

POSTGRES_CONN_ID = 'postgres_default'
USERNAME1 = 'postgres'
PASSWORD1 = 123
HOST1 = 'localhost'
PORT1 = 5432
DATABASE1 = "MyDB"
TABLE_NAME1 = "nba_forecast"

SQLSERVER_CONN_ID = 'COMPUTERVONSASC\Олександр'
HOST2 = 'Windows 10 Pro'
USERNAME2 = 'dbo'   # or COMPUTERVONSASC\Олександр
HOST2 = 'localhost'
PORT2 = 1433
SERVER = 'COMPUTERVONSASC\SQLEXPRESS'
DATABASE2 = 'MySqldatabase'
TABLE_NAME2 = 'nba_forecast2'

 
default_args = {
    'start_date': datetime(2023, 1, 1),
} 

with DAG('postgres_to_sqlserver_dag', default_args=default_args, schedule_interval=None) as dag:
    task1 = PostgresOperator(
        task_id='extract_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='SELECT * FROM {}.{}'.format(DATABASE1, TABLE_NAME1),
        dag=dag
    )

    task2 = MsSqlOperator(
        task_id='load_sqlserver_table',
        mssql_conn_id=SQLSERVER_CONN_ID,  
        sql='INSERT INTO {}.{} SELECT * FROM {}'.format(DATABASE2, TABLE_NAME2, TABLE_NAME1),
        dag=dag
    )
    
    task1 >> task2


# 1 TODO
    # Make sure to replace my_table, postgres_default,
    # and sqlserver_default with the appropriate table name,
# PostgreSQL connection ID - 1076	"postgres"	"::1"	59339	"pgAdmin 4 - DB:MyDB" 
    # , and SQL Server connection ID, respectively.
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
