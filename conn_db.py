import os
from dotenv import load_dotenv

load_dotenv()

def get_db_conn(postgresql=False):
    if postgresql:
        pg_uid = os.getenv("PG_UID")
        pg_pwd = os.getenv("PG_PWD")
        pg_db = os.getenv("PG_DB")
        return f'postgresql://{pg_uid}:{pg_pwd}@localhost:5432/{pg_db}'
    else:
        ms_user = os.getenv("MS_USER")
        ms_pwd = os.getenv("MS_PWD")
        driver = os.getenv("SQL_SERVER_DRIVER")
        server = os.getenv("MS_SERVER")
        ms_db = os.getenv("MS_DB")
        return f"mssql+pyodbc://{ms_user}:{ms_pwd}@{server}/{ms_db}?driver={driver}"  