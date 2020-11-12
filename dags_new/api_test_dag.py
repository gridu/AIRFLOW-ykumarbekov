from airflow import DAG
import airflow
import datetime
import json
import os
import requests
import ast

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow import models, configuration

# ***** Arguments
DAG_NAME = "api_test_dag"
yesterday = airflow.utils.dates.days_ago(1)

def_file = os.path.join(configuration.get('core', 'dags_folder'), 'def', DAG_NAME + ".json")
output_file_path = os.path.join(configuration.AIRFLOW_HOME, 'data')
spark_code_file_path = os.path.join(configuration.get('core', 'dags_folder'), 'sparkcode')

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': yesterday,
    'provide_context': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=3),  # Time between retries.
}

with open(def_file, 'r') as f:
    dag_config = json.load(f)
    dag_params = dag_config[DAG_NAME]['params']
    dag_params['year'] = yesterday.strftime('%Y')
    dag_params['month'] = yesterday.strftime('%m')
    dag_params['day'] = yesterday.strftime('%d')
    dag_params['output_file_path'] = output_file_path


def api_reader(api_endpoint, api_params, out_file_path, _date, **context):
    url = api_endpoint
    dt = _date.strftime('%s')
    api_params = ast.literal_eval(api_params)
    api_params['dt'] = dt
    # *****
    output_path = os.path.join(out_file_path, 'api_task', 'in')
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    # *****
    out_file = os.path.join(output_path, "weather_" + dt + ".json")
    res = requests.request('GET', url, params=api_params)
    ti = context['task_instance']
    if res.status_code == 200:
        with open(out_file, "w") as fn:
            fn.write(res.text)
        f_list = out_file
        ti.xcom_push(key="f_list", value=f_list)


def transform_api_data(**context):
    ti = context['ti']
    f_list = ti.xcom_pull(key='f_list')
    print("received message: '%s'" % f_list)
    # print("** Printing whole context **")
    # for k, v in context.items(): print("k:{}. v:{}".format(k, v))


with models.DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    default_args=DEFAULT_DAG_ARGS,
    user_defined_macros=dag_params,
) as dag:

    api_reader_task = PythonOperator(
        task_id="api_reader_task",
        python_callable=api_reader,
        op_args=['{{ api_endpoint }}', '{{ api_params }}', '{{ output_file_path }}', yesterday],
        provide_context=True,
    )

    transform_api_task = PythonOperator(
        task_id="transform_api_task",
        python_callable=transform_api_data,
        provide_context=True
    )

    spark_code_task = BashOperator(
        task_id="spark_code_task",
        bash_command="/usr/local/spark/bin/spark-submit "
                     "--master {{ params.spark_master_url }} "
                     "{{ params.spark_params }} "
                     "{{ params.spark_code_file }} "
                     "-f {{ ti.xcom_pull(key='f_list') }}",
        params={
            'spark_code_file': os.path.join(spark_code_file_path, dag_params['spark_code_file']),
            'spark_master_url': dag_params['spark_master_url'],
            'spark_params': dag_params['spark_parameters']
        },
    )

    api_reader_task >> transform_api_task >> spark_code_task
