from datetime import datetime
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from prod.subdag import sub_dag

parent_dag_name = 'dag_trigger'
# dag_triggered_name = 'xcom_test_push_dag'
dag_triggered_name = 'dag_1_prod'
child_dag_name = 'process_results_SubDAG'

run_file = Variable.get('run_file') or '/tmp/airflow_project/run'

args = {
            'owner': 'Airflow',
            'depends_on_past': False,
            'retries': 1,
            'start_date': datetime(2019, 12, 26)}

dag_trigger = DAG(dag_id=parent_dag_name, default_args=args, schedule_interval='23 11 * * *')

wait_run_file = FileSensor(task_id='wait_run_file', poke_interval=30, filepath=run_file, dag=dag_trigger)

trigger_dag = TriggerDagRunOperator(
    task_id='trigger_DAG',
    trigger_dag_id=dag_triggered_name,
    dag=dag_trigger)

process_result_sub_dag = SubDagOperator(
    subdag=sub_dag(
        parent_dag_name,
        child_dag_name, args=args, schedule=dag_trigger.schedule_interval),
    task_id=child_dag_name,
    dag=dag_trigger)

wait_run_file >> trigger_dag >> process_result_sub_dag
