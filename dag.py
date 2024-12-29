from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner':'airflow',
    'start_date':datetime(2023,12,18),
    'depends_on_past' : False,
    'email':['mohdzahoor29@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

dag=DAG('fetch_cricket_Stats',
        default_Args =default_args,
        description='Runs an external Python Script',
        schedule_interval='@daily',
        catchup =False)

with dag:
    run_Script_task = BashOperator(
        task_id='run_script',
        bash_command ='python /home/airflow/gcs/dags/scripts/extract_and_push_gcs.py'
    )