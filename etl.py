from sqlalchemy import create_engine
import pyodbc 
import pandas as pd
import os
from dotenv import load_dotenv

#Get pass from env var
pwd1 = os.environ['PGPASS']
uid1 = os.environ['PGUID']
pwd2 = os.environ['MSUID']
uid2 = os.environ['MSPASS']
#Sql server db
driver = "{SQL Server Native Client 11.0}"
server = "COMPUTERVONSASC"
database = "MSQL"

#Extract data from postgres
def extract():
    try:
        #Establish Postgre connection
        connection_string = f'postgresql://{uid1}:{pwd1}@localhost:5432/MyDB'
        engine = create_engine(connection_string)
        #Specify table
        table = 'nba_forecast'
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
    
#load data to SQL Server
def load(df, table):
    try:
        # SQL Server connection string
        connection_string = f"mssql+pyodbc://{uid2}:{pwd2}@{server}/{database}?driver={driver}"  
        engine = create_engine(connection_string)
      
        rows_imported = 0
        print(f'Importing rows {rows_imported} to {rows_imported + len(df)} for table {table}')
        # Save df to SQL Server
        df.to_sql(f'stg_{table}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))
