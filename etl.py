from sqlalchemy import create_engine
import pyodbc 
import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

pwd = os.environ['PGPASS']
uid = os.environ['PGUID']
driver = "ODBC Driver 17 for SQL Server"
server = "COMPUTERVONSASC\SQLEXPRESS"
database = "MSQL"

def extract():
    try:
        connection_string = f"mssql+pyodbc://sasha:890@{server}/{database}?driver={driver}"  
        engine = create_engine(connection_string)
        table = 'dbo.nba_forecast'
        query = f'SELECT * FROM {table}'
        df = pd.read_sql(query, engine)
        load(df, table)
    except Exception as e:
        print("OH NO ERROR: " + str(e))
    finally:
        engine.dispose()
         
def load(df, table):
    try:
        connection_string = f'postgresql://{uid}:{pwd}@localhost:5432/MyDB'
        engine = create_engine(connection_string)
        rows_imported = 0
        df.to_sql(f'stg_{table}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))
 
try:
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
