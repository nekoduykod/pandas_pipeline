from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from conn_db import get_db_conn 

# airflow should be run from a separate repo, or in docker

default_args = {
    'start_date': datetime(2023, 6, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sql_to_postgres',
    default_args=default_args,
    description='Dag SQL Server => PostgreSQL',
    schedule_interval='@daily',
)

def extract():
    try:
        source_conn = get_db_conn(postgresql=False)  
        engine = create_engine(source_conn)
        table = 'dbo.nba_forecast'
        query = f'SELECT * FROM {table}'
        df = pd.read_sql(query, engine)
        load(df, table)
        print("Data extracted")
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        
""" 
def transform(df):
    Transform the data if necessary
    return df_cleaned 
"""

def load(df, table):
    try:
        target_conn = get_db_conn(postgresql=True)
        with create_engine(target_conn).connect():
            df.to_sql(f"stg_{table}", target_conn, if_exists='replace', index=False)
        print("Data imported")
    except Exception as e:
        print(f"Error during loading: {str(e)}")

extract_task = PythonOperator(
    task_id='extract_MSSQL',
    python_callable=extract,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_PG',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task

""" extract_task >> transform >> load_task """