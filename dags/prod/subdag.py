from airflow.models import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance, DagBag, DagModel, DagRun
import os
from airflow.models import XCom
import datetime as dt
from datetime import timezone

run_file = Variable.get('run_file') or '/tmp/airflow_project/run'
tmp_folder = Variable.get('tmp_folder') or '/tmp/'

triggered_dag = 'db_job_dag'
external_task_id_for_triggered_dag = 'finish_process'


class CustomExternalTaskSensor(ExternalTaskSensor):
    '''
    CustomExternalTaskSensor child from ExternalTaskSensor
    '''
    @apply_defaults
    def __init__(self,
                 external_dag_id,
                 external_task_id,
                 allowed_states=None,
                 execution_delta=None,
                 execution_date_fn=None,
                 check_existence=False,
                 *args,
                 **kwargs):
        super(CustomExternalTaskSensor, self).__init__(
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            allowed_states=allowed_states,
            execution_delta=execution_delta,
            execution_date_fn=execution_date_fn,
            check_existence=check_existence,
            *args, **kwargs)

    @provide_session
    def poke(self, context, session=None):
        '''
        Custom code
            here we're getting execution_date from triggered dag
            to realize this we use Xcom class, which reads data from Airflow database
            then we populate execution_date_fn with this value
            Pay attention: to receive last value we set include_prior_dates as True
        '''
        exec_dt = dt.datetime.now()
        exec_date = XCom.get_one(
            execution_date=exec_dt.replace(tzinfo=timezone.utc),
            include_prior_dates=True,
            key='execution_date',
            task_id=self.external_task_id,
            dag_id=self.external_dag_id)
        print('Triggered task execution_date: {}'.format(exec_date))
        if exec_date:
            self.execution_date_fn = lambda d1: dt.datetime.fromisoformat(exec_date)
            print("execution_date_fn: {}".format(self.execution_date_fn))
        # ****************************************
        # super(CustomExternalTaskSensor, self).poke(context=context, session=session)
        '''
        Original code
        '''
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self.execution_date_fn(context['execution_date'])
        else:
            dttm = context['execution_date']

        dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        print("dttm_filter: {}".format(dttm_filter))
        serialized_dttm_filter = ','.join([datetime.isoformat() for datetime in dttm_filter])

        self.log.info(
            'Poking for %s.%s on %s ... ',
            self.external_dag_id, self.external_task_id, serialized_dttm_filter
        )

        DM = DagModel
        TI = TaskInstance
        DR = DagRun
        if self.check_existence:
            dag_to_wait = session.query(DM).filter(DM.dag_id == self.external_dag_id).first()

            if not dag_to_wait:
                raise AirflowException('The external DAG '
                                       '{} does not exist.'.format(self.external_dag_id))
            else:
                if not os.path.exists(dag_to_wait.fileloc):
                    raise AirflowException('The external DAG '
                                           '{} was deleted.'.format(self.external_dag_id))

            if self.external_task_id:
                refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(self.external_dag_id)
                if not refreshed_dag_info.has_task(self.external_task_id):
                    raise AirflowException('The external task'
                                           '{} in DAG {} does not exist.'.format(self.external_task_id,
                                                                                 self.external_dag_id))

        if self.external_task_id:
            count = session.query(TI).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                TI.execution_date.in_(dttm_filter),
            ).count()
        else:
            count = session.query(DR).filter(
                DR.dag_id == self.external_dag_id,
                DR.state.in_(self.allowed_states),
                DR.execution_date.in_(dttm_filter),
            ).count()

        session.commit()
        return count == len(dttm_filter)
        # ****************************************
        # ****************************************


def pull_fn(*args, **context):
    xcom_value = XCom.get_one(
        execution_date=dt.datetime.now().replace(tzinfo=timezone.utc),
        include_prior_dates=True,
        key='run_id',
        task_id=args[1],
        dag_id=args[0])
    print("received message: '%s'" % xcom_value)
    print("**  Printing full context  **")
    for k, v in context.items():
        print("key:{}. val:{}".format(k, v))


def sub_dag(parent_dag, child_dag, args, schedule):

    dag = DAG('%s.%s' % (parent_dag, child_dag), schedule_interval=schedule, start_date=args['start_date'])

    sensor_trig_dag = CustomExternalTaskSensor(external_dag_id=triggered_dag,
                                               external_task_id=external_task_id_for_triggered_dag,
                                               poke_interval=10,
                                               task_id='sensor_trig_dag',
                                               dag=dag)

    print_result = PythonOperator(
        task_id='print_result',
        python_callable=pull_fn,
        op_args=[triggered_dag, external_task_id_for_triggered_dag],
        dag=dag)

    remove_run_file = BashOperator(
        task_id='remove_run_file',
        bash_command='rm {{ params.file_path }}',
        params={'file_path': run_file},
        dag=dag)

    create_finished_tm = BashOperator(
        task_id='create_finished_tm',
        bash_command='touch {{ params.tmp }}finished_{{ ts_nodash }}',
        params={'tmp': tmp_folder},
        dag=dag)

    sensor_trig_dag >> print_result >> remove_run_file >> create_finished_tm

    return dag
