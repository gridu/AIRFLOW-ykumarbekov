from datetime import datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

tmp = Variable.get('tmp_folder')

default_args = {
            'owner': 'Airflow',
            'depends_on_past': False,
            'retries': 1,
            'start_date': datetime(2019, 12, 20)}

dag = DAG(dag_id='bash_test_dag', default_args=default_args, schedule_interval=None)


start_task = DummyOperator(task_id='start_task', dag=dag)

create_time_file = BashOperator(task_id='create_time_file',
                                bash_command='touch {{ params.tmp }}finished_{{ ts_nodash }}',
                                params={'tmp': tmp},
                                dag=dag)

start_task >> create_time_file
