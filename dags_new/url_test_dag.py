from airflow import DAG
import airflow
import datetime
import json
import os
import urllib3
import ast
import xml.etree.ElementTree as ET
import hdfs

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow import models, configuration

# ***** Arguments
DAG_NAME = "url_test_dag"
yesterday = airflow.utils.dates.days_ago(1)

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': yesterday,
    'provide_context': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=3),  # Time between retries.
}

# *****    Params loads
# dag_config = models.Variable.get(DAG_NAME, deserialize_json=True)
# dag_config = json.loads('{"params":{"year":"","month":"","day":"","source_table_def_path":""}}')

# *****  DAG Def loads
def_file = os.path.join(configuration.get('core', 'dags_folder'), 'def', DAG_NAME + ".json")
output_file_path = os.path.join(configuration.AIRFLOW_HOME, 'data')

with open(def_file, 'r') as f:
    dag_config = json.load(f)
    dag_params = dag_config[DAG_NAME]['params']
    dag_params['year'] = yesterday.strftime('%Y')
    dag_params['month'] = yesterday.strftime('%m')
    dag_params['day'] = yesterday.strftime('%d')
    dag_params['output_file_path'] = output_file_path

_client = hdfs.InsecureClient(dag_params['hdfs_server'], user='root')


def url_reader(url_list, out_file_path, client, **context):
    http = urllib3.PoolManager()
    v = []
    url_list = ast.literal_eval(url_list)
    ti = context['task_instance']
    f_list = []
    for i in url_list:
        try:
            r = http.request('GET', i, retries=1)
            out_file = os.path.join(out_file_path, i.split("/")[-1] + ".json")
            if r.status == 200:
                content = r.data.decode('utf-8')
                root = ET.fromstring(content)
                for p in root:
                    v.append({child.tag: child.text for child in p})
                # with open(out_file, "w") as fn:
                #     fn.write(json.dumps(v))
                client.write(out_file, json.dumps(v))
                f_list.append(out_file)
                v.clear()
        except Exception as ex:
            print(ex)
    ti.xcom_push(key="f_list", value=f_list)


def transform_xml_data(**context):
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

    start_task = DummyOperator(task_id='start_task')

    read_xml_task = PythonOperator(
        task_id="read_xml_task",
        python_callable=url_reader,
        # op_args=['{{url_list}}', '{{output_file_path}}', _client],
        op_args=['{{url_list}}', '{{hdfs_file_path}}', _client],
        provide_context=True,
    )

    transform_xml_task = PythonOperator(
        task_id="transform_xml_task",
        python_callable=transform_xml_data,
        provide_context=True
    )

    start_task >> read_xml_task >> transform_xml_task

