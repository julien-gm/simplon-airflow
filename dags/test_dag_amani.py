from datetime import timedelta, datetime
import re
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator   import DummyOperator
from airflow.operators.python import BranchPythonOperator


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
    filepath="/home/julien/airflow/logs/scheduler/{{ ds }}/scheduler.py.log",
    dag=dag
)

def check_errors(filepath):
    pattern = "ERROR"
    error_count = 0
    with open(filepath,"r") as file:
        for line in file:
            if re.search(pattern, line):
                error_count += 1
    if error_count > 50:
        return "send_mail"
    return "do_nothing"

count_errors = BranchPythonOperator(
    task_id="count_errors",
    python_callable=check_errors,
    op_kwargs={'filepath': '/home/julien/airflow/logs/scheduler/{{ ds }}/scheduler.py.log'},
    dag=dag
)

send_mail = DummyOperator(task_id="send_mail", dag=dag)
do_nothing = DummyOperator(task_id="do_nothing", dag=dag)

existing_file >> count_errors >> [send_mail, do_nothing]
