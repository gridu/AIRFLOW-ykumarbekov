from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule

curr_dir = 'prod'

config = {
    'dag_1_' + curr_dir: {'schedule_interval': None, "start_date": datetime(2019, 12, 26),
                          "table_name": "table_name_1"},
    'dag_2_' + curr_dir: {'schedule_interval': None, "start_date": datetime(2019, 12, 26),
                          "table_name": "table_name_2"},
    'dag_3_' + curr_dir: {'schedule_interval': None, "start_date": datetime(2019, 12, 26),
                          "table_name": "table_name_3"}}


def check_table_exist(**kwargs):
    """ method to check that table exists """
    if True:
        return kwargs['table_name'] + '_skip_creation'
    else:
       return kwargs['table_name'] + '_create_table'


def push_msg(*args, **context):
    task_instance = context['task_instance']
    task_instance.xcom_push(key="run_id", value=args[0])
    task_instance.xcom_push(key="execution_date", value=args[1])


def create_dag(
        dag_id,
        default_args,
        schedule,
        task_id):
    """
    A function returning a DAG object.
    """
    dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args)
    with dag:
        start_process = PythonOperator(
            task_id=task_id + '_start_process',
            python_callable=lambda: logging.info("{} start processing tables in database: {}".format(task_id,
                                                                                                     "database")),
            dag_id=dag_id)
        get_user = BashOperator(task_id=task_id + '_get_current_user', bash_command='echo "$USER"', dag_id=dag_id)
        check_table = BranchPythonOperator(
            task_id=task_id + '_check_table_exists',
            python_callable=check_table_exist,
            op_kwargs={'table_name': task_id},
            dag_id=dag_id
        )
        create_table = DummyOperator(task_id=task_id + '_create_table', dag_id=dag_id)
        skip_table = DummyOperator(task_id=task_id + '_skip_creation', dag_id=dag_id)
        ins_new_row = DummyOperator(task_id=task_id + '_ins_new_row', dag_id=dag_id, trigger_rule=TriggerRule.ALL_DONE)
        query_the_table = PythonOperator(task_id=task_id + '_query_the_table',
                                         python_callable=push_msg,
                                         provide_context=True,
                                         op_args=['{{ run_id }} ended', '{{ ts }}'],
                                         dag_id=dag_id)
        
        start_process >> get_user >> \
            check_table >> [create_table, skip_table] >> ins_new_row >> query_the_table

    return dag


for k, v in config.items():
    globals()[k] = create_dag(k,
                              {'owner': 'Airflow',
                               'depends_on_past': True,
                               'retries': 1,
                               'start_date': config[k]['start_date']},
                              config[k]['schedule_interval'],
                              config[k]['table_name'])
