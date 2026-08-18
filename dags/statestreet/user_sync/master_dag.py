
from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'statestreet_user_sync_master_{config.instance}',
        description=f'Statestreet_user_sync_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            sftp_conn_id=config.sftp_client_conn_id,
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        download_input_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_file',
            sftp_conn_id=config.sftp_client_conn_id,
            remote_filepath="{{result('new_file_sensor')}}"
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id="parse_csv",
            document="{{result('download_input_file')}}",
            encoding='UTF-8-SIG'
        )

        if_file_is_not_in_correct_format = rail.IfOperator(
            task_id='if_file_is_not_in_correct_format',
            # pylint: disable=too-many-statements line-too-long
            test="{{not (result('new_file_sensor') | file_name | starts_with('Statestreet_user_')) | is_truthy or (not (result('new_file_sensor') | file_name | ends_with('csv'))) | is_truthy }}",
            yes_task="send_incorrect_file_format_mail",
            no_task="if_file_has_no_data",
        )

        send_incorrect_file_format_mail = rail.EmailOperator(
            task_id='send_incorrect_file_format_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} - Replicon User Import - Incorrect file naming ',
            html_content="templates/emails/incorrect_fileformat_mail.html",
        )

        if_file_has_no_data = rail.IfOperator(
            task_id='if_file_has_no_data',
            test= lambda: not bool(rail.load_all_records(rail.result('parse_csv'))),
            yes_task="send_invalid_file_mail",
            no_task="get_report_details",
        )

        send_invalid_file_mail = rail.EmailOperator(
            task_id='send_invalid_file_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} - Replicon User Import - Invalid file placed ',
            html_content="templates/emails/invalid_file_mail.html",
        )

        finish_job = rail.EmptyOperator(
            task_id='finish_job'
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_report_name,
        )

        generate_report = rail.run_report2(
            group_id="generate_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "persistedReportName": null
                    }
                ]
            }
        )

        process_list = rail.TriggerDagRunOperator(
            task_id='process_list',
            retries=0,
            trigger_dag_id=f'statestreet_repeat_list_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "report_data": rail.result('generate_report.get_report_result'),
                "wait_batch": rail.result('generate_report.report_batch.wait_for_batch'),
                "file_path": rail.result('new_file_sensor')
            }
        )

        wait_for_process_list = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_list',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_list") }}'
        )

        rename_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_to_archive',
            sftp_conn_id=config.sftp_client_conn_id,
            new_filename=config.archive_filepath +
            "{{dag_run_ecid() | replace(':', '-')}}-{{result('new_file_sensor') | file_name}}",
            existing_filename="{{ result('new_file_sensor') }}"
        )

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun
        new_file_sensor >> download_input_file>> parse_csv >> if_file_is_not_in_correct_format
        if_file_is_not_in_correct_format >> rail.Label(
            'Yes') >> send_incorrect_file_format_mail >> rename_to_archive >> finish_job
        if_file_is_not_in_correct_format >> rail.Label(
            'No') >> if_file_has_no_data
        if_file_has_no_data >> rail.Label(
            'Yes') >> send_invalid_file_mail >> rename_to_archive >> finish_job
        if_file_has_no_data >> rail.Label(
            'No') >> get_report_details >> generate_report >> process_list
        process_list >> wait_for_process_list >> finish_job

        return dag


rail.for_each_instance(create_dag)
