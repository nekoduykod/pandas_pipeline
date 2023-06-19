import pyodbc

# SQL Server connection details
server = 'COMPUTERVONSASC\SQLEXPRESS'
database = 'MSQL'
username = 'sasha'
password = '890'

# Establish a connection to SQL Server
try:
    conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
    print("Connected to SQL Server")
except pyodbc.Error as ex:
    print("Failed to connect to SQL Server:", ex)
finally:
    # Close the connection
    conn.close()