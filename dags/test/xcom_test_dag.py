from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
            'owner': 'Airflow',
            'depends_on_past': False,
            'retries': 1,
            'start_date': datetime(2019, 12, 25)}

dag = DAG(dag_id='xcom_test_dag', default_args=default_args, schedule_interval=None)


def push_fn(*args, **context):
    task_instance = context['task_instance']
    task_instance.xcom_push(key="run_id", value=args[0])


def pull_fn(**context):
    ti = context['ti']
    # msg = ti.xcom_pull(task_ids='push_task', key='run_id')
    msg = ti.xcom_pull(key='run_id')
    print("received message: '%s'" % msg)
    print("** Printing whole context **")
    for k, v in context.items():
        print("k:{}. v:{}".format(k, v))


push_task = PythonOperator(task_id='push_task',
                           python_callable=push_fn,
                           provide_context=True,
                           op_args=['{{ run_id }} ended'],
                           dag=dag)

pull_task = PythonOperator(task_id='pull_task',
                           python_callable=pull_fn,
                           provide_context=True,
                           dag=dag)

push_task >> pull_task
