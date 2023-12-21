## About 

A small simple task: a pipeline "MS SQL Server > PostgreSQL"

<img src="images/overview.png" alt="blueprint" width="83%"/>

## Prerequisites

- Python 3.x with required modules (pandas, SQLAlchemy)
- PostgreSQL, SQL Server databases configured
- Install Airflow with Docker, or in a separate repo to avoid conflicts with the script modules

1. Download and configure databases
2. Load a sample data to a database
3. Extract the table data using python script
4. Transform, if necessary
5. Load the data to another db
   Optionally: Schedule with Airflow

## Result
 
<img src="images/goal_achieved.png" alt="Data imported finally"/>
<p align="center">
  <img src="images/rows_match_after_repl.png" alt="Each byte replicated" width="50%">
</p>