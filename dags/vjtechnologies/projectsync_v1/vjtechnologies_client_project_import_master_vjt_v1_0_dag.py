
from datetime import timedelta, datetime
from pendulum import datetime as dt
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.client_project_import_master_dagid,
        description=f'VJTechnologies_{config.entity_name}_Client_Project_Import_Master_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=dt(2022, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_job_start_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_job_start_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_job_start_time=rail.PythonOperator(
            task_id='log_job_start_time',
            python_callable= lambda:  datetime.now().strftime("%d%m%YT%H%M%S")
        )

        list_input_file_path=rail.SFTPListFilesOperator(
            task_id='list_input_file_path',
            paths=[config.input_filepath],
        )

        if_file_present=rail.IfOperator(
            task_id='if_file_present',
            test=lambda: rail.result('list_input_file_path') and len(rail.result('list_input_file_path')[config.input_filepath]) > 0,
            yes_task="trigger_processing_of_each_file",
            no_task="log_to_sumo",
        )

        trigger_processing_of_each_file = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_processing_of_each_file',
            retries=0,
            items=lambda: rail.result('list_input_file_path')[config.input_filepath],
            trigger_dag_id=config.process_each_file_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'filename': config.input_filepath + '/' + item['name'],
                'filesize': item['size'],
                'jobstarttime': rail.result('log_job_start_time'),
                'masterdagid': rail.render_template('{{dag_run_ecid()}}')
            }
        )

        wait_for_file_processing = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_file_processing',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_processing_of_each_file')}}"
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> log_job_start_time
        log_job_start_time >> list_input_file_path >> if_file_present
        if_file_present >> rail.Label('Yes')  >> trigger_processing_of_each_file >> wait_for_file_processing >> log_to_sumo
        if_file_present >> rail.Label('No') >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
