from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.postgres_hook import PostgresHook

curr_dir = 'prod'
db_schema = 'public'

config = {
    'db_job_dag': {'schedule_interval': None, "start_date": datetime(2019, 12, 26), "table_name": "dag_table"}}


# def check_table_exist(**kwargs):
def check_table_exist(sql_to_get_schema, sql_to_check_table_exist, table_name):
    """ method to check that table exists """
    hook = PostgresHook()
    # get schema name
    query = hook.get_records(sql=sql_to_get_schema)
    for result in query:
        if 'airflow' in result:
            schema = result[0]
            print(schema)
            break

    # check table exist
    query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))
    print(query)
    if query:
        return True
    else:
        raise ValueError("table {} does not exist".format(table_name))


def push_msg(*args, **context):
    task_instance = context['task_instance']
    task_instance.xcom_push(key="run_id", value=args[0])
    task_instance.xcom_push(key="execution_date", value=args[1])


def create_dag(dag_id, default_args, schedule, task_id):
    dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args)
    with dag:
        start_process = PythonOperator(
            task_id=task_id + '_start_process',
            python_callable=lambda: logging.info("{} start processing tables in database: {}".format(task_id,
                                                                                                     "database")),
            dag_id=dag_id)

        get_user = BashOperator(
            task_id=task_id + '_get_current_user',
            bash_command='echo "$USER"',
            xcom_push=True,
            dag_id=dag_id)

        check_table = BranchPythonOperator(
            task_id=task_id + '_check_table_exists',
            python_callable=check_table_exist,
            op_args=["select * from pg_tables;",
                     "select * from information_schema.tables "
                     "where table_schema = '{}'"
                     "and table_name = '{}';", task_id]  # task_id <=> table_name
        )

        create_table = DummyOperator(task_id=task_id + '_create_table', dag_id=dag_id)
        skip_table = DummyOperator(task_id=task_id + '_skip_creation', dag_id=dag_id)
        ins_new_row = DummyOperator(task_id=task_id + '_ins_new_row', dag_id=dag_id, trigger_rule=TriggerRule.ALL_DONE)
        query_the_table = PythonOperator(task_id=task_id + '_query_the_table',
                                         python_callable=push_msg,
                                         provide_context=True,
                                         op_args=['{{ run_id }} ended', '{{ ts }}'],
                                         dag_id=dag_id)

        start_process >> get_user >> check_table >> [create_table, skip_table] >> ins_new_row >> query_the_table

    return dag


for k, v in config.items():
    globals()[k] = create_dag(k,
                              {'owner': 'Airflow',
                               'depends_on_past': True,
                               'retries': 1,
                               'start_date': config[k]['start_date']},
                              config[k]['schedule_interval'],
                              config[k]['table_name'])
