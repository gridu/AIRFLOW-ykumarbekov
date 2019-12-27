from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

tmp_folder = Variable.get('tmp_folder')

default_args = {
            'owner': 'Airflow',
            'depends_on_past': False,
            'retries': 3,
            'start_date': datetime(2019, 12, 25)
}

dag = DAG(
    dag_id='schedule_test_dag',
    default_args=default_args,
    schedule_interval='07 18 * * *')

start_task = DummyOperator(task_id='start_task', dag=dag)

create_time_file = BashOperator(task_id='create_time_file',
                                bash_command='touch {{ params.tmp }}schedule_test_dag_{{ ts_nodash }}',
                                params={'tmp': tmp_folder},
                                dag=dag)

start_task >> create_time_file
