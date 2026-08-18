import rail
from pendulum import datetime, now
from datetime import timedelta

from transparentbpo.schedule_sync_to_bamboohr.utils import custom_methods


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Sync user schedule details from Replicon to bamboohr - Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        create_log_schedule_update_logs = rail.CreateLogOperator(
            task_id='create_log_schedule_update_logs'
        )

        log_job_start_time = rail.PythonOperator(
            task_id='log_job_start_time',
            python_callable=lambda: now(
                config.time_zone).strftime("%Y-%m-%dT%H:%M:%S%z")
        )

        process_scheduled_users_child = rail.TriggerDagRunOperator(
            task_id='process_scheduled_users_child',
            retries=0,
            trigger_dag_id=config.process_scheduled_users_child_dag_id,
            conf=lambda: {
                'schedule_update_logs': rail.result('create_log_schedule_update_logs')
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_scheduled_users_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_scheduled_users_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_scheduled_users_child") }}'
        )

        gather_result_from_process_scheduled_users_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_result_from_process_scheduled_users_child',
            dag_runs="{{result('process_scheduled_users_child')}}",
            dagrun_task_id='final_response_from_dag',
            target='result'
        )

        process_shift_users_child = rail.TriggerDagRunOperator(
            task_id='process_shift_users_child',
            retries=0,
            trigger_dag_id=config.process_shift_users_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'schedule_update_logs': rail.result('create_log_schedule_update_logs')
            },
        )

        wait_for_process_shift_users_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_shift_users_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_shift_users_child") }}'
        )

        gather_result_from_process_shift_users_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_result_from_process_shift_users_child',
            dag_runs="{{result('process_shift_users_child')}}",
            dagrun_task_id='final_response_from_dag',
            target='result'
        )

        get_all_record_count = rail.PythonOperator(
            task_id='get_all_record_count',
            python_callable=lambda: int(rail.result('gather_result_from_process_scheduled_users_child')[0]) + int(rail.result(
                'gather_result_from_process_shift_users_child')[0])
        )

        records_found_to_process = rail.IfOperator(
            task_id="records_found_to_process",
            test=lambda: rail.result('get_all_record_count') > 0,
            yes_task='create_csv_lines_for_logs',
            no_task='send_no_records_found_mail'
        )

        send_no_records_found_mail = rail.EmailOperator(
            task_id='send_no_records_found_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Replicon user schedule sync to Bamboohr completed - No records -' +
            ' {{ current_time_in_specified_tz() }}',
            html_content="templates/email/no_records_found.html"
        )

        create_csv_lines_for_logs = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_for_logs',
            source="{{ result('create_log_schedule_update_logs') }}",
            header=['username',
                    'employeeid',
                    'schedulename',
                    'status',
                    'details',
                    'jobid'], 
            row=[
                "{{ item.properties.username}}",
                "{{ item.properties.empid}}",
                "{{ item.properties.schedule }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}"
            ],
        )

        get_email_and_log_file_details = rail.PythonOperator(
            task_id="get_email_and_log_file_details",
            python_callable=lambda dag_run: custom_methods.get_email_details_callable(
                config.time_zone)
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_for_logs')}}",
            output_file_name="{{ result('get_email_and_log_file_details').log_file_name }}",
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_internal_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_internal_sftp',
            content="{{ result('create_csv_lines_for_logs') }}",
            remote_filepath=config.log_filepath +
            "/{{ result('get_email_and_log_file_details').log_file_name }}",
        )

        check_error_in_final_log = rail.FilterLogEntriesOperator(
            task_id='check_error_in_final_log',
            log="{{ result('create_log_schedule_update_logs') }}",
            severity="Error",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('check_error_in_final_log', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
            {%- else -%}\
                "+config.alert_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon user schedule sync to Bamboohr - " }} \
                {%- if result("check_error_in_final_log", "length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="templates/email/completion_email.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_log_schedule_update_logs >> log_job_start_time >> process_scheduled_users_child >> wait_for_process_scheduled_users_child >> gather_result_from_process_scheduled_users_child >> process_shift_users_child
        process_shift_users_child >> wait_for_process_shift_users_child >> gather_result_from_process_shift_users_child >> get_all_record_count
        get_all_record_count >> records_found_to_process >> rail.Label(
            "Yes") >> create_csv_lines_for_logs
        records_found_to_process >> rail.Label(
            "No") >> send_no_records_found_mail

        create_csv_lines_for_logs >> get_email_and_log_file_details >> generate_download_link >> upload_log_to_internal_sftp \
            >> check_error_in_final_log >> send_import_complete_email >> log_to_sumo

    return dag


rail.for_each_instance(create_main_airflow_dag)
