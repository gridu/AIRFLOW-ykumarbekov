from datetime import datetime
import uuid
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.postgres_custom import PostgresSQLCountRows

curr_dir = 'prod'
db = 'project_db'
'''
conn_id = postgres (default postgres connection)
'''

config = {'db_job_dag': {'schedule_interval': None, "start_date": datetime(2019, 12, 26), "table_name": "dag_table"}}


class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')


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
        return tbl + '_skip_creation'
    else:
        # raise ValueError("table {} does not exist".format(tbl))
        return 'create_table_' + tbl


def push_finish_date(*args, **context):
    ti = context['ti']
    ti.xcom_push(key="execution_date", value=args[0])


def create_dag(dag_id, default_args, schedule, table_name, db_name):
    dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args)
    with dag:

        start_process = PythonOperator(
            task_id='start_process',
            python_callable=lambda: print("start processing table {} in database: {}".format(table_name, db_name)),
            dag_id=dag_id)

        get_user = BashOperator(
            task_id='get_current_user',
            bash_command='echo $(whoami)',
            xcom_push=True,
            dag_id=dag_id)

        check_table = BranchPythonOperator(
            task_id='check_table_exists_' + table_name,
            python_callable=check_table_exist,
            op_args=["select * from pg_tables;",
                     "select * from information_schema.tables "
                     "where table_schema = '{}'"
                     "and table_name = '{}';", db_name, table_name],
            dag_id=dag_id)

        create_table = PostgresOperator(
            task_id='create_table_' + table_name,
            database=db_name,
            sql='''create table {}(
                    custom_id integer not null, 
                    timestamp timestamp not null, 
                    user_id varchar(50) not null);'''.format(table_name),
            dag_id=dag_id)

        skip_table = DummyOperator(task_id=table_name + '_skip_creation', dag_id=dag_id)

        ins_new_row = CustomPostgresOperator(
            task_id='insert_row_into_' + table_name,
            database=db_name,
            sql='insert into {} values(%s, %s, %s)'.format(table_name),
            parameters=(
                uuid.uuid4().int % 123456789,
                datetime.now(),
                '{{ ti.xcom_pull(task_ids="get_current_user") }}'),
            dag_id=dag_id,
            trigger_rule=TriggerRule.ALL_DONE)

        query_the_table = PostgresSQLCountRows(
            task_id='query_the_table_' + table_name,
            db_schema=db_name,
            table_name=table_name,
            dag_id=dag_id)

        finish_process = PythonOperator(
            task_id='finish_process',
            python_callable=push_finish_date,
            provide_context=True,
            op_args=['{{ ts }}'],
            dag_id=dag_id)

        start_process >> get_user >> check_table >> [create_table, skip_table] >> ins_new_row >> \
            query_the_table >> finish_process

    return dag


for k, v in config.items():
    globals()[k] = create_dag(k,
                              {'owner': 'Airflow',
                               'depends_on_past': True,
                               'retries': 1,
                               'start_date': config[k]['start_date']},
                              config[k]['schedule_interval'],
                              config[k]['table_name'], db)
