from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

# Importing python files
from Update_database.update_database import RunFile

time_zone = pendulum.timezone("America/New_York") #<-- Changes time zone from UTC to local - UTC -4 time

default_args = {
    'owner': 'airflow',
    #'start_date': days_ago(1) 
    'start_date': datetime(2025,6,13,0,tzinfo = time_zone) 
}

Update_database = DAG(
    'Update_Database_MFR_IR',
    default_args = default_args,
    description = 'Runs Update_database once a day at midnight',
    schedule = '0 0 * * *', #<-- schedules the DAG to run everyday at Midnight
    catchup = False
)

task_1 = PythonOperator(
    task_id = "Update_database",
    python_callable=RunFile,  #<-- Runs the function "RunFile" this is a workaround for a runtime namespace issue crated
                              #    when importing the "main" program
    dag=Update_database,
)

task_1 