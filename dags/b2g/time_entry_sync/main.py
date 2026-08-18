from datetime import timedelta
import rail
from b2g.time_entry_sync.utils import request_payload

# pylint: disable=too-many-statements


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"b2g_time_data_import_master_{config.instance}",
        description=f"B2G Time Data Import master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.max_active_runs_master
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=15),

        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Resource Sync - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content='templates/email/bad_file_format.html',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
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
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_time_data = rail.LoadCSVFileOperator(
            task_id='load_time_data',
            document="{{ result('download_file') }}",
            encoding='utf-8-sig'
        )

        create_time_data_collection = rail.CreateCollectionOperator(
            task_id='create_time_data_collection',
            source="{{ result('load_time_data') }}",
            name="input_data"
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_time_data_collection', 'length') > 0 }}",
            yes_task='process_records',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon time Sync Blank File {{ current_time_in_specified_tz() }}',
            html_content="templates/email/blank_payload.html"
        )

        process_records = rail.EmptyOperator(
            task_id="process_records"
        )

        process_time_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_time_records",
            items="{{result('create_time_data_collection')}}",
            trigger_dag_id=f"b2g_time_data_process_each_record_{config.instance}",
            conf=request_payload.get_process_time_data_records_conf,
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_time_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_time_records",
            dag_runs="{{result('process_time_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        timesheeturi_csv = rail.WriteCSVFileOperator(
            task_id='timesheeturi_csv',
            source="{{ get_master_log() }}",
            header=['username', 'timesheeturi'],
            row=['{{ item.properties.username }}',
                 '{{ item.properties.timesheeturi }}'],
        )

        timesheet_report_data_collection = rail.CreateCollectionOperator(
            task_id='timesheet_report_data_collection',
            source="{{ result('timesheeturi_csv') }}",
            name='timesheetdetails'
        )

        query_valid_timesheet_records = rail.QueryCollectionOperator(
            task_id='query_valid_timesheet_records',
            query="SELECT DISTINCT timesheeturi  FROM timesheetdetails WHERE timesheeturi IS NOT NULL"
        )

        process_timesheet_submission = rail.TriggerDagRunForEachItemOperator(
            task_id="process_timesheet_submission",
            items="{{result('query_valid_timesheet_records')}}",
            batch_size=50,
            trigger_dag_id=f"b2g_submit_timesheet_{config.instance}",
            conf=lambda item: {
                "timesheetdetails": item
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_timesheet_submission = rail.WaitForDagRunsSensor(
            task_id="wait_process_timesheet_submission",
            dag_runs="{{result('process_timesheet_submission')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['entrydate', 'username', 'hours', 'projectname',
                    'taskcode', 'comment', 'status', 'message', 'ecid'],
            row=['{{ item.properties.entrydate }}', '{{ item.properties.username }}',
                 '{{ item.properties.hours }}', '{{ item.properties.projectname }}', '{{ item.properties.taskcode }}',
                 '{{ item.properties.comment }}', '{{ item.properties.status }}', '{{ item.message}}', '"{{ item.ecid }}"'],
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/{{ result("new_file_sensor") | file_name }}_{{ current_time("%d%m%Y%H%M%S") }}.csv',
        )

        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            severity='Error',
        )

        any_records_failed = rail.IfOperator(
            task_id='any_records_failed',
            test="{{ result('filter_master_log', 'length') > 0 }}",
            yes_task='send_completion_error_mail',
            no_task='send_completion_mail'
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon time Sync is completed successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        send_completion_error_mail = rail.EmailOperator(
            task_id='send_completion_error_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Replicon time Sync is completed with error - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/import_with_error.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> is_csv >> rail.Label(
            "No") >> send_bad_file_format_email
        is_csv >> rail.Label("Yes") >> download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        download_file >> load_time_data >> create_time_data_collection\
            >> has_any_records >> rail.Label("No") >> send_blank_payload_email

        has_any_records >> rail.Label("Yes") >> process_records >> process_time_records >> wait_process_time_records\
            >> timesheeturi_csv >> timesheet_report_data_collection >> query_valid_timesheet_records >> process_timesheet_submission\
            >> wait_process_timesheet_submission >> render_logs_csv\
            >> upload_logs_to_sftp >> filter_master_log >> any_records_failed

        any_records_failed >> rail.Label(
            "Yes") >> send_completion_error_mail >> log_to_sumo
        log_to_sumo >> can_fail_dag >> fail_dagrun

        any_records_failed >> rail.Label(
            "No") >> send_completion_mail >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
