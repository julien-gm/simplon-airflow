from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator   import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'start_date': datetime(2022, 3, 27),

    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('test_dag_amani', default_args=default_args, schedule_interval="@daily")

existing_file = FileSensor(
    task_id='scheduler.log',
    filepath="/home/amani/airflow/logs/scheduler/2022-04-15/test_dag_amani.py.log",
    dag=dag
)
