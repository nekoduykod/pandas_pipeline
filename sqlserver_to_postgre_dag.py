import time
from datetime import datetime
import pwd
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

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