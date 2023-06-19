POSTGRES_CONN_ID = 'postgres_default'
USERNAME1 = 'postgres'
PASSWORD1 = 123
HOST1 = 'localhost'
PORT1 = 5432
DATABASE1 = "MyDB"
TABLE_NAME1 = "nba_forecast"

SQLSERVER_CONN_ID = 'COMPUTERVONSASC\Олександр'
USERNAME2 = 'dbo'   # or COMPUTERVONSASC\Олександр
HOST2 = 'Windows 10 Pro'
PORT2 = 1433
SERVER = 'COMPUTERVONSASC\SQLEXPRESS'
DATABASE2 = 'MySqldatabase'
TABLE_NAME2 = 'nba_forecast2'

 
default_args = {
    'start_date': datetime(2023, 1, 1),
} 
with DAG('postgres_to_sqlserver_dag', default_args=default_args, schedule_interval=None) as dag:
    task1 = PostgresOperator(
        task_id='extract_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='SELECT * FROM {}.{}'.format(DATABASE1, TABLE_NAME1),
        dag=dag
    )

    task2 = MsSqlOperator(
        task_id='load_sqlserver_table',
        mssql_conn_id=SQLSERVER_CONN_ID,  
        sql='INSERT INTO {}.{} SELECT * FROM {}'.format(DATABASE2, TABLE_NAME2, TABLE_NAME1),
        dag=dag
    )
    
    task1 >> task2

 