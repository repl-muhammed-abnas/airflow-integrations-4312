from datetime import timedelta
from pendulum import datetime
import rail
from pwcglobal.timesheet_approve_delete_disbaled_users.utils.custom_methods import logging_details


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwc_timesheet_approve_delete_disable_user_master_{config.instance}',
        description='PWC Timesheet Approve Delete Disable User Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=logging_details,
            op_args=[config.time_zone]
        )

        log_file_name = rail.PythonOperator(
            task_id='log_file_name',
            python_callable=lambda: "logs_pwc_timesheet_delete_approve_disabled_users_"+rail.result('get_logging_details')['logfilename_date']+".csv"
        )

        get_timesheet_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_report_details',
            report_name=config.extract_timesheet_recalc_report,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='get_report_details',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_timesheet_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("get_report_details.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('get_report_details.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('get_report_details.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='no_data',
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('get_report_details.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_time_data_collection = rail.CreateCollectionOperator(
            task_id='create_time_data_collection',
            source="{{ result('load_report_data') }}",
            name="timesheet_data"
        )

        query_valid_input_records = rail.QueryCollectionOperator(
            task_id='query_valid_input_records',
            query="""SELECT * FROM timesheet_data WHERE
              date(substr(Timesheet_End_Date, 7, 4) || '-' ||
                substr(Timesheet_End_Date , 4, 2) || '-' ||
                  substr(Timesheet_End_Date, 1, 2), 'start of day') = '{{ result('get_logging_details')['current_date'] }}'"""
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('query_valid_input_records', 'length') > 0 }}",
            yes_task='process_time_records',
            no_task='query_delete_timesheet_records'
        )

        process_time_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_time_records",
            items="{{result('query_valid_input_records')}}",
            batch_size=50,
            trigger_dag_id=f"pwc_timesheet_approve_data_process_each_record_child_{config.instance}",
            conf=lambda item: {
                "timesheetdetails": item
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_time_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_time_records",
            dag_runs="{{result('process_time_records')}}",
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        query_delete_timesheet_records = rail.QueryCollectionOperator(
            task_id='query_delete_timesheet_records',
            query="""SELECT * FROM  timesheet_data WHERE
             date(substr(Timesheet_Start_Date, 7, 4) || '-' || substr(Timesheet_Start_Date, 4, 2) || '-'
             || substr(Timesheet_Start_Date, 1, 2), 'start of day') > date(substr(User_End_Date, 7, 4) || '-'
             || substr(User_End_Date, 4, 2) || '-' || substr(User_End_Date, 1, 2), 'start of day')"""
        )

        has_delete_timesheet_records = rail.IfOperator(
            task_id='has_delete_timesheet_records',
            test="{{ result('query_delete_timesheet_records', 'length') > 0 }}",
            yes_task='process_timesheet_delete_records',
            no_task='log_to_sumo'
        )

        process_timesheet_delete_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_timesheet_delete_records",
            items="{{result('query_delete_timesheet_records')}}",
            batch_size=50,
            trigger_dag_id=f"pwc_timesheet_delete_data_process_each_record_child_{config.instance}",
            conf=lambda item: {
                "timesheetdetails": item
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_timesheet_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_timesheet_records",
            dag_runs="{{result('process_timesheet_delete_records')}}",
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['username', 'timesheetenddate', 'timesheetstartdate', 'timesheeturi',
                    'status', 'message', 'ecid'],
            row=['{{ item.properties.username }}', '{{ item.properties.timesheetenddate }}',
                 '{{ item.properties.timesheetstartdate }}', '{{ item.properties.timesheeturi }}',
                 '{{ item.properties.status }}', '{{ item.message}}', '"{{ item.ecid }}"'],
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath + '/' +
            "{{ result('log_file_name') }}",
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
            subject='{{ get_company_key() }} | Timesheet Approval for disabled users is completed successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        send_completion_error_mail = rail.EmailOperator(
            task_id='send_completion_error_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Timesheet Approval for disabled users is completed with error - {{ current_time_in_specified_tz() }}',
            html_content="templates/import_with_error.html",
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

        get_logging_details >> log_file_name >> get_timesheet_report_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label(
            "Yes") >> load_report_data >> create_time_data_collection >> query_valid_input_records\
            >> has_any_records >> rail.Label(
            "Yes") >> process_time_records >> wait_process_time_records >> rail.Label("Always") >> query_delete_timesheet_records\
            >> has_delete_timesheet_records >> rail.Label("Yes") >> process_timesheet_delete_records >> wait_process_timesheet_records\
            >> render_logs_csv >> upload_logs_to_sftp >> filter_master_log >> any_records_failed >> rail.Label("Yes") >> send_completion_error_mail\
            >> log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun

        report_has_data >> rail.Label("No") >> no_data

        has_any_records >> rail.Label("No") >> query_delete_timesheet_records

        has_delete_timesheet_records >> rail.Label("No") >> log_to_sumo

        any_records_failed >> rail.Label(
            "No") >> send_completion_mail >> log_to_sumo

    return dag


rail.for_each_instance(create_main_airflow_dag)
