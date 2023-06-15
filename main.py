from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1)
}

with DAG('postgres_to_sqlserver_dag', default_args=default_args, schedule_interval=None) as dag:
    extract_postgres_table = PostgresOperator(
        task_id='extract_postgres_table',
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM my_table',
        dag=dag
    )

    load_sqlserver_table = MsSqlOperator(
        task_id='load_sqlserver_table',
        mssql_conn_id='sqlserver_default',
        sql='INSERT INTO my_table VALUES (:{{ ti.xcom_pull(task_ids="extract_postgres_table") }})',
        dag=dag
    )

    extract_postgres_table >> load_sqlserver_table
