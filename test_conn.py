import pyodbc

server = 'COMPUTERVONSASC\SQLEXPRESS'
database = 'MSQL'
username = 'sasha'
password = '890'

try:
    conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
    print("Connected to SQL Server")
except pyodbc.Error as ex:
    print("Failed to connect to SQL Server:", ex)
finally:
    conn.close()
