from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mssql_hook import MsSqlHook


def download_from_postgresql():
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Execute the SQL query to download the data
    query = "SELECT * FROM nba_forecast"
    cursor.execute(query)

    # Fetch the data
    data = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Return the downloaded data
    return data


def load_into_sql_server(**context):
    # Retrieve the downloaded data from the previous task
    downloaded_data = context['ti'].xcom_pull(task_ids='download_from_postgresql_task')

    # Connect to SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    conn = mssql_hook.get_conn()
    cursor = conn.cursor()

    # Execute the SQL query to load the data into SQL Server
    query = "INSERT INTO nba_forecast2 VALUES (%s, %s, ...)"  # Adjust the query as per your SQL Server table structure

    # Iterate over the downloaded data and load it into SQL Server
    for row in downloaded_data:
        cursor.execute(query, row)

    # Commit the changes and close the cursor and connection
    conn.commit()
    cursor.close()
    conn.close()


# Define the DAG
dag = DAG(
    dag_id='postgresql_to_sql_server',
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 0 * * *',  # Run daily at midnight (UTC)
)

# Define the tasks
download_task = PythonOperator(
    task_id='download_from_postgresql_task',
    python_callable=download_from_postgresql,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_sql_server_task',
    python_callable=load_into_sql_server,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
download_task >> load_task
