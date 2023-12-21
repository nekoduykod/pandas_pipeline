from sqlalchemy import create_engine
import pandas as pd
from conn_db import get_db_conn 

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
        
def load(df, table):
    try:
        target_conn = get_db_conn(postgresql=True)
        with create_engine(target_conn).connect():
            df.to_sql(f"stg_{table}", target_conn, if_exists='replace', index=False)
        print("Data imported")
    except Exception as e:
        print(f"Error during loading: {str(e)}")

extract()