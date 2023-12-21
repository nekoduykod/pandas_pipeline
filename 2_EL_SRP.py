from sqlalchemy import create_engine
import pandas as pd
from conn_db import get_db_conn 

class DataLoader:
    def __init__(self, source_conn, target_conn):
        self.source_engine = create_engine(source_conn)
        self.target_engine = create_engine(target_conn)
        
    def extract(self, table):
        query = f'SELECT * FROM {table}'
        df = pd.read_sql(query, self.source_engine)
        return df

    def load(self, df, table):
        df.to_sql(f'stg_{table}', self.target_engine,\
                  if_exists='replace', index=False)

source_conn = get_db_conn(postgresql=False)
target_conn = get_db_conn(postgresql=True)
data_loader = DataLoader(source_conn, target_conn)
df = data_loader.extract('dbo.nba_forecast')
data_loader.load(df, 'dbo.nba_forecast')