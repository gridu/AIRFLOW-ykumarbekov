from datetime import datetime
import logging
import uuid
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults

curr_dir = 'prod'
db = 'project_db'
table_name = 'dag_table'
dag_name = 'pg_hook_test_dag'
conn_str = 'postgres'
default_args = {'owner': 'Airflow', 'depends_on_past': False, 'retries': 1}


class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')


pg_hook_test_dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2019, 12, 26))


def check_table_exist(sql_to_get_schema, sql_to_check_table_exist, db_schema, tbl):
    """ method to check that table exists """
    hook = PostgresHook(schema=db_schema)
    # get schema name
    query = hook.get_records(sql=sql_to_get_schema)
    for result in query:
        if 'airflow' in result:
            schema = result[0]
            print(schema)
            break

    # check table exist
    query = hook.get_first(sql=sql_to_check_table_exist.format(schema, tbl))
    print(query)
    if query:
        return 'skip_creation'
    else:
        # raise ValueError("table {} does not exist".format(tbl))
        return 'create_table'


def query_table(*args, **context):  # args[0] = sql, args[1] = db_schema
    task_instance = context['task_instance']
    hook = PostgresHook(schema=args[1])
    query = hook.get_first(sql=args[0])
    if query:
        task_instance.xcom_push(key="count_tbl", value=tuple(query)[0])
    else:
        return None


start_process = PythonOperator(
        task_id='start_process',
        python_callable=lambda: print("start processing"),
        dag=pg_hook_test_dag)

check_table = BranchPythonOperator(
    task_id='check_table_exists_' + table_name,
    python_callable=check_table_exist,
    op_args=["select * from pg_tables;",
             "select * from information_schema.tables "
             "where table_schema = '{}'"
             "and table_name = '{}';", db, table_name])

create_table = PostgresOperator(
        task_id='create_table',
        database=db,
        sql='''create table {}(
            custom_id integer not null, 
            timestamp timestamp not null, 
            user_id varchar(50) not null);'''.format(table_name),)

skip_table = DummyOperator(task_id='skip_creation', dag=pg_hook_test_dag)

get_user = BashOperator(
            task_id='get_current_user',
            bash_command='echo $(whoami)',
            xcom_push=True,
            trigger_rule=TriggerRule.ALL_DONE,
            dag=pg_hook_test_dag)

insert_row = CustomPostgresOperator(
        task_id='insert_row',
        database=db,
        sql='insert into {} values(%s, %s, %s)'.format(table_name),
        parameters=(
            uuid.uuid4().int % 123456789,
            datetime.now(),
            '{{ ti.xcom_pull(task_ids="get_current_user") }}')
    )

query_the_table = PythonOperator(
        task_id='query_the_table',
        python_callable=query_table,
        provide_context=True,
        op_args=['select count(*) from {}'.format(table_name), db],
        dag=pg_hook_test_dag)

start_process >> check_table >> [create_table, skip_table] >> get_user >> insert_row >> query_the_table
