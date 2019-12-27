from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
            'owner': 'Airflow',
            'depends_on_past': False,
            'retries': 1,
            'start_date': datetime(2019, 12, 26)}


def push_msg(*args, **context):
    ti = context['ti']
    ti.xcom_push(key="end_push_task_execution_date", value=args[0])


dag = DAG(dag_id='xcom_test_push_dag', default_args=default_args, schedule_interval=None)

start_task = DummyOperator(task_id='start_push_task', dag=dag)

delay_task = BashOperator(task_id='delay_task', bash_command='echo "sleep 1m"', dag=dag)

end_push_task = PythonOperator(
    task_id='end_push_task',
    python_callable=push_msg,
    provide_context=True,
    op_args=['{{ ts }}'],
    dag=dag)

start_task >> delay_task >> end_push_task



