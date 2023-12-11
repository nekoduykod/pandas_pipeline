from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import pyodbc 
import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

default_args = {
    'start_date': datetime(2023, 6, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sql_to_postgres',
    default_args=default_args,
    description='A DAG to extract data from SQL Server and load into PostgreSQL',
    schedule_interval='@daily',
)

def extract():
    try:
        # SQL Server connection string
        connection_string = f"mssql+pyodbc://sasha:890@{server}/{database}?driver={driver}"  
        engine = create_engine(connection_string)
        table = 'dbo.nba_forecast'
        # Query and load data into a dataframe
        query = f'SELECT * FROM {table}'
        df = pd.read_sql(query, engine)
        # Process the data or perform any necessary operations
        load(df, table)
    except Exception as e:
        print("OH NO ERROR: " + str(e))
    finally:
        # Close the connection
        engine.dispose()
""" 
def transform(df):
    Transform the data as needed
    For example, remove nulls, columns, or shorten a table, leaving only "x" rows
    return df_cleaned 
"""

def load(df, table):
    try:
        # Establish PostgreSQL connection
        connection_string = f'postgresql://{uid}:{pwd}@localhost:5432/MyDB'
        engine = create_engine(connection_string)
        # Specify table
        rows_imported = 0
        df.to_sql(f'stg_{table}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task

""" extract_task >> transform >> load_task
if many tasks, we can combine them in pairs """
