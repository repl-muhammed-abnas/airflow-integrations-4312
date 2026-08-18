from datetime import timedelta
import rail
from dxctechnology.adhoc.timeentry_submission.send_logs import get_send_logs


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_timeentry_submission_master_adhoc_{config.instance}',
        description='Time Entry Submission Master Adhoc',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}",
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'Employee Id': 'employeeid',
                'Timeentry revesion ID': 'timeentryrevisionid',
                'Timeentry Id': 'timeentryid',
                'Timesheet Status': 'timesheetstatus',
                'Timesheet period ': 'timesheetperiod',
            }

        )

        process_each_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_records',
            retries=0,
            items="{{ result('create_input_data_collection') }}",
            trigger_dag_id=f'dxctechnology_timeentry_submission_child_adhoc_{config.instance}',
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            conf=lambda item: {
                'employeeid': item['employeeid'],
                'timeentryrevisionid': item['timeentryrevisionid'],
                'timeentryid': item['timeentryid'],
                'timesheetstatus': item['timesheetstatus'],
                'timesheetperiod': item['timesheetperiod'],

            }
        )

        wait_for_process_each_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_records',
            dag_runs='{{ result("process_each_records") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        send_logs_enter = get_send_logs(config)
        new_file_sensor >> download_file >> was_new_file_found
        download_file >> load_data >> create_input_data_collection
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        create_input_data_collection >> process_each_records >> wait_for_process_each_records >> send_logs_enter

    return dag


rail.for_each_instance(create_main_dag)
