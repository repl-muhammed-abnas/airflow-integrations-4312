from datetime import datetime as dt
from pendulum import datetime
from philipmorrisny.overtime_report_extract.utils import custom_method
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'philipmorrisny_overtime_report_extract_{config.instance}',
        description=f'philipmorrisny overtime report extract {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2023, 1, 1, tz=config.mountain_timezone),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_export = rail.IfOperator(
            task_id = "can_run_export",
            test= lambda: dt.now().isocalendar()[1]%2 == 0,
            yes_task="list_overtime_output_dict",
            no_task= "end"
        )

        end = rail.EmptyOperator(
            task_id='end'
        )

        list_overtime_output_dict = rail.SFTPListFilesOperator(
                task_id='list_overtime_output_dict',
                paths=[config.overtime_report_path]
        )

        list_overtimelog_output_dict = rail.SFTPListFilesOperator(
                task_id='list_overtimelog_output_dict',
                paths=[config.overtime_logreport_filepath]
        )

        get_overtime_file_name= rail.PythonOperator(
            task_id= 'get_overtime_file_name',
            python_callable=lambda: rail.result("list_overtime_output_dict")[config.overtime_report_path][0]['name'] if rail.result(
                "list_overtime_output_dict") else None
        )

        has_overtime_filename_ends_with_csv = rail.IfOperator(
            task_id="has_overtime_filename_ends_with_csv",
            test= '{{ result("get_overtime_file_name").split(".")[-1] == "csv" if result("get_overtime_file_name") else False }}',
            yes_task="archive_overtime_file",
            no_task="get_overtimelog_file_name",
        )

        archive_overtime_file = rail.SFTPMoveFileOperator(
            task_id='archive_overtime_file',
            existing_filename=config.overtime_report_path +
            '/{{ result("get_overtime_file_name") }}',
            new_filename=config.overtime_reportarchivepath +
            '/{{ result("get_overtime_file_name") }}'
        )

        get_overtimelog_file_name= rail.PythonOperator(
            task_id= 'get_overtimelog_file_name',
            python_callable=lambda: rail.result("list_overtimelog_output_dict")[config.overtime_logreport_filepath][0]['name'] if rail.result(
                "list_overtimelog_output_dict") else None
        )

        has_overtimelog_filename_ends_with_csv = rail.IfOperator(
            task_id="has_overtimelog_filename_ends_with_csv",
            test='{{ result("get_overtimelog_file_name").split(".")[-1] == "csv" if result("get_overtimelog_file_name") else False}}',
            yes_task="archive_overtimelog_file",
            no_task="get_overtime_report_details",
        )

        archive_overtimelog_file = rail.SFTPMoveFileOperator(
            task_id='archive_overtimelog_file',
            existing_filename=config.overtime_logreport_filepath +
            '/{{ result("get_overtimelog_file_name") }}',
            new_filename=config.overtime_logarchivepath +
            '/{{ result("get_overtimelog_file_name") }}'
        )

        get_overtime_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_overtime_report_details',
            report_name=config.overtime_report_name,
        )

        run_overtime_report_group_entry, run_overtime_report_group_exit = rail.run_report(
            group_id='run_overtime_report',
            report_params= lambda: custom_method.get_report_params("get_overtime_report_details"),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_overtimelog_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_overtimelog_report_details',
            report_name=config.overtime_log_report_name,
        )

        run_overtimelog_report_group_entry, run_overtimelog_report_group_exit = rail.run_report(
            group_id='run_overtimelog_report',
            report_params= lambda: custom_method.get_report_params("get_overtimelog_report_details"),
            replicon_conn_id=config.replicon_conn_id,
        )

        overtime_report_has_data = rail.IfOperator(
            task_id="overtime_report_has_data",
            test="{{ result('run_overtime_report.get_report_result','has_data')}}",
            yes_task='load_overtime_report_data',
            no_task='send_blank_overtime_report_email'
        )

        send_blank_overtime_report_email = rail.EmailOperator(
            task_id='send_blank_overtime_report_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Over Time Report Extract skipped - {{ current_time("%d%m%Y%H%M%S") }}',
            html_content="templates/emails/email_overtime_blank_payload.html",
        )

        load_overtime_report_data = rail.LoadCSVFileOperator(
            task_id='load_overtime_report_data',
            document="{{ result('run_overtime_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        upload_overtimereport_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_overtimereport_to_sftp',
            content="{{ result('load_overtime_report_data') }}",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.overtime_report_path +
            '/OvertimeReport_Export_{{ current_time("%m_%d_%Y") }}.csv',
        )

        overtimelog_report_has_data = rail.IfOperator(
            task_id="overtimelog_report_has_data",
            test="{{ result('run_overtimelog_report.get_report_result','has_data')}}",
            yes_task='load_overtimelog_report_data',
            no_task='send_blank_overtimelog_email'
        )

        send_blank_overtimelog_email = rail.EmailOperator(
            task_id='send_blank_overtimelog_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Over Timelog Report Extract skipped - {{ current_time("%d%m%Y%H%M%S") }}',
            html_content="templates/emails/email_overtime_blank_payload.html",
        )

        load_overtimelog_report_data = rail.LoadCSVFileOperator(
            task_id='load_overtimelog_report_data',
            document="{{ result('run_overtimelog_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        upload_overtimelog_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_overtimelog_to_sftp',
            content="{{ result('load_overtimelog_report_data') }}",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.overtime_logreport_filepath +
            '/Overtime_Log_Report_Export_{{ current_time("%m_%d_%Y") }}.csv',
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Overtime Report Extract Completed - {{ current_time("%d%m%Y%H%M%S") }}',
            html_content="templates/emails/send_email.html",
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
            no_task= "finish"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        finish= rail.EmptyOperator(
            task_id= 'finish'
        )

        can_run_export >> rail.Label("Yes") >> list_overtime_output_dict

        can_run_export >> rail.Label("No") >> end

        list_overtime_output_dict >> list_overtimelog_output_dict >> get_overtime_file_name >> \
            has_overtime_filename_ends_with_csv

        has_overtime_filename_ends_with_csv >> rail.Label(
            "Yes") >> archive_overtime_file >> get_overtimelog_file_name

        has_overtime_filename_ends_with_csv >> rail.Label(
            "No") >> get_overtimelog_file_name >> has_overtimelog_filename_ends_with_csv

        has_overtimelog_filename_ends_with_csv>> rail.Label(
            "Yes") >> archive_overtimelog_file >> get_overtime_report_details

        has_overtimelog_filename_ends_with_csv >> rail.Label(
            "No") >> get_overtime_report_details

        get_overtime_report_details >> run_overtime_report_group_entry, run_overtime_report_group_exit >> \
                get_overtimelog_report_details >> run_overtimelog_report_group_entry, run_overtimelog_report_group_exit >> overtime_report_has_data
        overtime_report_has_data >> rail.Label(
            'No') >> send_blank_overtime_report_email >> overtimelog_report_has_data
        overtime_report_has_data >> rail.Label(
            'Yes') >> load_overtime_report_data >> upload_overtimereport_to_sftp >> overtimelog_report_has_data

        overtimelog_report_has_data >> rail.Label(
            "Yes") >> load_overtimelog_report_data >> upload_overtimelog_to_sftp >> send_success_email >> log_to_sumo

        overtimelog_report_has_data >> rail.Label(
            "No") >> send_blank_overtimelog_email >> log_to_sumo >> can_fail_dag

        can_fail_dag >> rail.Label(
            "Yes") >> fail_dagrun >> finish

        can_fail_dag >> rail.Label(
            "No") >> finish

    return dag


rail.for_each_instance(create_main_dag)
