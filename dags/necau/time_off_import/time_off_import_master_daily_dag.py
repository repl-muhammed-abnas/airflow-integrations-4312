from datetime import timedelta
from pendulum import datetime
import rail
from necau.time_off_import.utils import python_callable_method
from necau.time_off_import.utils import custom_method


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'necau_time_off_import_master_daily_{config.instance}',
        description=f'NECAU - timeoff_import_Master_Daily_v2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 10, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_daily,
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        list_import_files = rail.SFTPListFilesOperator(
            task_id="list_import_files",
            paths=[config.processing_file_directory,
                   config.timeoff_import_file_directory],
        )

        previous_import_running = rail.IfOperator(
            task_id='previous_import_running',
            test=lambda: custom_method.is_previous_import_running(
                config),
            yes_task='log_to_sumo',
            no_task='get_timeoff_file_groups'
        )

        get_timeoff_file_groups = rail.PythonOperator(
            task_id='get_timeoff_file_groups',
            python_callable=lambda: python_callable_method.get_input_group(
                config)
        )

        has_valid_import_files = rail.IfOperator(
            task_id='has_valid_import_files',
            test=lambda: custom_method.has_input_file('valid'),
            yes_task='get_report_details',
            no_task='log_to_sumo'
        )

        has_invalid_import_files = rail.IfOperator(
            task_id='has_invalid_import_files',
            test=lambda: custom_method.has_input_file('invalid'),
            yes_task='process_archive_files',
            no_task='log_to_sumo'
        )

        process_archive_files = rail.TriggerDagRunForEachItemOperator(
            task_id='process_archive_files',
            retries=0,
            items=lambda: rail.result('get_timeoff_file_groups')['invalid'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_archiving_timeoff_file_child_{config.instance}',
            conf=custom_method.get_archive_file_info
        )

        wait_for_archive_process = rail.WaitForDagRunsSensor(
            task_id='wait_for_archive_process',
            dag_runs='{{ result("process_archive_files") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_warning_file_download_link = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_warning_file_download_link',
            dag_runs="{{ result('process_archive_files') }}",
            dagrun_task_id='add_files_with_names',
            flatten=True
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timeoff_import_user_referance,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='process_moving_file_to_processing',
            no_task='log_to_sumo'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            name='userdata',
            source="{{ result('load_report_data') }}",
            columns={
                'Previous Employee Number': 'prev_employee_number',
                'useruri': 'user_uri',
                'User Status': 'user_status',
                'User Email': 'user_email',
                'User Supervisor Email address': 'supervisor_email',
                'Schedule Name (Current)': 'current_schedule',
                'Auto schedule assignment - yes/no': 'auto_schedule_option',
                'Auto schedule assignment - shift': 'auto_schedule_shift_name'}
        )

        query_userdata = rail.QueryCollectionOperator(
            task_id='query_userdata',
            query='SELECT * FROM userdata'
        )

        process_moving_file_to_processing = rail.TriggerDagRunForEachItemOperator(
            task_id='process_moving_file_to_processing',
            retries=0,
            items=lambda: rail.result('get_timeoff_file_groups')['valid'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_move_files_to_processing_child_{config.instance}',
            conf=custom_method.get_timeoff_file_info
        )

        wait_for_process_moving_file_to_processing = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_moving_file_to_processing',
            dag_runs='{{ result("process_moving_file_to_processing") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_timeoffs_1 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoffs_1',
            retries=0,
            items=lambda: [rail.result('get_timeoff_file_groups')['valid'][0]],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_process_each_leave_file_child_{config.instance}',
            conf=custom_method.get_leave_info
        )

        wait_for_process_timeoffs_1 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timeoffs_1',
            dag_runs='{{ result("process_timeoffs_1") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_timeoffs_2 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoffs_2',
            retries=0,
            items=lambda: [rail.result('get_timeoff_file_groups')['valid'][1]],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_process_each_leave_file_child_{config.instance}',
            conf=custom_method.get_leave_info
        )

        wait_for_process_timeoffs_2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timeoffs_2',
            dag_runs='{{ result("process_timeoffs_2") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_timeoffs_3 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoffs_3',
            retries=0,
            items=lambda: [rail.result('get_timeoff_file_groups')['valid'][2]],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_process_each_leave_file_child_{config.instance}',
            conf=custom_method.get_leave_info
        )

        wait_for_process_timeoffs_3 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timeoffs_3',
            dag_runs='{{ result("process_timeoffs_3") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_error_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_error_logs',
            dag_runs="{{ result('process_timeoffs_1')+result('process_timeoffs_2')+result('process_timeoffs_3') }}",
            dagrun_task_id='get_errored_logs',
            flatten=True
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=python_callable_method.get_errror_logs
        )

        gather_leave_request_logs_download_link = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_leave_request_logs_download_link',
            dag_runs="{{ result('process_timeoffs_1')}}",
            dagrun_task_id='generate_download_link',
            flatten=True
        )

        gather_leave_approved_logs_download_link = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_leave_approved_logs_download_link',
            dag_runs="{{ result('process_timeoffs_2')}}",
            dagrun_task_id='generate_download_link',
            flatten=True
        )

        gather_leave_cancelled_logs_download_link = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_leave_cancelled_logs_download_link',
            dag_runs="{{ result('process_timeoffs_3') }}",
            dagrun_task_id='generate_download_link',
            flatten=True
        )

        is_file_processed = rail.IfOperator(
            task_id="is_file_processed",
            # pylint: disable=line-too-long
            test="{{ result('gather_leave_request_logs_download_link') | length > 0 or result('gather_leave_approved_logs_download_link') | length > 0 or result('gather_leave_cancelled_logs_download_link') | length > 0 }}",
            yes_task='get_html_complete_template',
            no_task='log_to_sumo'
        )

        get_html_complete_template = rail.RenderTemplateOperator(
            task_id='get_html_complete_template',
            target='result',
            template_file='templates/email/import_complete.html',
            dataset=lambda: custom_method.get_processed_logs
        )

        send_import_email = rail.EmailOperator(
            task_id='send_import_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs') | length == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Timeoff Import - " }} \
                {%- if result("get_errored_logs") | length > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="{{result('get_html_complete_template')}}",
        )

        get_html_warning_template = rail.RenderTemplateOperator(
            task_id='get_html_warning_template',
            target='result',
            template_file='templates/email/warning_batch_file.html',
            dataset=lambda: custom_method.get_download_links(
                'gather_warning_file_download_link')
        )

        send_warning_batch_email = rail.EmailOperator(
            task_id='send_warning_batch_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() + " |  Time Off Import - Warning File Batch - " }} \
                {{ " " + current_time() }}',
            html_content="{{result('get_html_warning_template')}}",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'FileCount': '.csv'
            }
        )

        list_import_files >> previous_import_running
        previous_import_running >> rail.Label("Yes") >> \
            get_timeoff_file_groups >> [
                has_valid_import_files, has_invalid_import_files]
        has_valid_import_files >> rail.Label(
            'yes') >> get_report_details >> run_report_group_entry
        run_report_group_exit >> load_report_data >> report_has_data
        report_has_data >> rail.Label('Yes') >> process_moving_file_to_processing >> wait_for_process_moving_file_to_processing >> \
            create_user_collection >> query_userdata >> process_timeoffs_1 >> \
            wait_for_process_timeoffs_1 >> \
            process_timeoffs_2 >> wait_for_process_timeoffs_2 >> process_timeoffs_3 >> wait_for_process_timeoffs_3 >> \
            gather_error_logs >> get_errored_logs >> gather_leave_request_logs_download_link >> \
            gather_leave_approved_logs_download_link >> gather_leave_cancelled_logs_download_link >> is_file_processed
        is_file_processed >> rail.Label(
            "Yes") >> get_html_complete_template >> send_import_email >> log_to_sumo
        is_file_processed >> rail.Label(
            "No") >> log_to_sumo
        previous_import_running >> rail.Label("No") >> log_to_sumo
        has_valid_import_files >> rail.Label("No") >> log_to_sumo
        has_invalid_import_files >> rail.Label(
            "Yes") >> process_archive_files >> wait_for_archive_process >> gather_warning_file_download_link >> \
            get_html_warning_template >> send_warning_batch_email >> log_to_sumo
        has_invalid_import_files >> rail.Label("No") >> log_to_sumo
        report_has_data >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
