from datetime import timedelta, datetime
import re

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'start_date': datetime(2022, 3, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('log_parser', default_args=default_args, schedule_interval=timedelta(days=1))

scheduler_sensor = FileSensor(
    task_id='scheduler_sensor',
    poke_interval=10,
    timeout=60,
    mode="reschedule",
    filepath='/home/julien/airflow/logs/scheduler/{{ ds }}/',
    dag=dag
)

def check_error_threshold(filepath, **kwargs):
    pattern = "ERROR"
    error_count = 0
    with open(filepath,"r") as file:
        for line in file:
            if re.search(pattern, line):
                error_count += 1
    threshold = int(Variable.get("error_threshold", default_var=3))
    print(f'Error occurrences: {error_count}, threshold: {threshold}')
    kwargs['ti'].xcom_push(key='error_count', value=error_count)
    return 'send_mail' if error_count >= threshold else 'do_nothing'

check_threshold = BranchPythonOperator(
    task_id='check_threshold',
    python_callable=check_error_threshold,
    provide_context=True,
    op_kwargs={'filepath': '/home/julien/airflow/logs/scheduler/{{ ds }}/scheduler.py.log'},
    dag=dag
)

do_nothing = DummyOperator(task_id='do_nothing', dag=dag)

send_mail = BashOperator(
    task_id='send_mail',
    bash_command="echo {{ ti.xcom_pull(key='error_count', task_ids=['check_threshold'])[0] }} errors found!",
    dag=dag
)

scheduler_sensor >> check_threshold >> [send_mail, do_nothing]
