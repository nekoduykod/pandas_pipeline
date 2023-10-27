from sqlalchemy import create_engine
import pyodbc 
import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

#Get pass from env var
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']
#Sql server db
driver = "ODBC Driver 17 for SQL Server"
#driver = "{SQL Server Native Client 11.0}"
server = "COMPUTERVONSASC\SQLEXPRESS"
database = "MSQL"

def extract():
    try:
        # SQL Server connection string
        connection_string = f"mssql+pyodbc://sasha:890@{server}/{database}?driver={driver}"  
        engine = create_engine(connection_string)
        table = 'dbo.nba_forecast'
        #Query and load data into a dataframe
        query = f'SELECT * FROM {table}'
        df = pd.read_sql(query, engine)
        #Process the data or perform any necessary operations
        load(df, table)
    except Exception as e:
        print("OH NO ERROR: " + str(e))
    finally:
        # Close the connection
        engine.dispose()
         
def load(df, table):
    try:
        #Establish Postgre connection
        connection_string = f'postgresql://{uid}:{pwd}@localhost:5432/MyDB'
        engine = create_engine(connection_string)
        #Specify table
        rows_imported = 0
        df.to_sql(f'stg_{table}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))
 
try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
