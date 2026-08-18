from datetime import timedelta
from pendulum import datetime, now
import rail
from sectranorthamericainc.time_export.utils import custom_methods, request_payload
from sectranorthamericainc.time_export.tasks.time_export_task import time_data_export

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_master_dag_id,
        description="Sectra Time Export Master",
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        schedule_interval=config.daily_run_schedule_interval,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id="get_logging_details",
            python_callable=custom_methods.get_time_export_name,
            op_args=[config]
        )

        time_export_download_script = rail.RepliconServiceOperator(
            task_id='time_export_download_script',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: custom_methods.get_timeexport_fileformat(
                config, response)
        )

        create_export_start, create_export_end = time_data_export(
            group_id="time_export",
            generate_request=request_payload.get_time_export_batch_creation_payload,
            get_export_name="{{result('get_logging_details').time_export_name}}",
            file_script_uri='time_export_download_script',
            retries=1)

        create_time_export_collection = rail.CreateCollectionOperator(
            task_id="create_time_export_collection",
            source=lambda: rail.result(create_export_end.task_id),
            name="raw_time_data",
            columns={
                "Login Name": "login_name",
                "Location Name": "user_location_name",
                "Location Code": "user_location_code",
                "Time Entry ID": "time_entry_id",
                "Entry Date": "entry_date",
                "Project Code": "project_code",
                "Employee ID": "employee_id",
                "Comments": "comments",
                "Task Code": "task_code",
                "Hours": "hours",
                "Department Name": "user_department_name",
                "Department Code": "user_department_code",
                "Billing Rate Name": "billing_rate_name",
                "Timesheet Period": "timesheet_period",
                "Project Location": "project_location",
                "Project Location Code": "proj_location_code",
                "Billing Rate Currency": "billing_rate_currency",
                "Billing Rate Rate": "billing_rate_rate",
                "Activity Name": "activity_name",
                "Activity Code": "activity_code"
            }
        )

        has_any_timeexport_data = rail.IfOperator(
            task_id="has_any_timeexport_data",
            test="{{result('create_time_export_collection', 'length') > 0 }}",
            yes_task="query_blank_employee_id_records",
            no_task="empty_has_any_timeexport_data_no_task"
        )

        empty_has_any_timeexport_data_no_task = rail.EmptyOperator(
            task_id = "empty_has_any_timeexport_data_no_task"
        )

        empty_send_no_data_email = rail.EmptyOperator(
            task_id="empty_send_no_data_email"
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon time data export - No records to export - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_empty_export.html"
        )

        can_update_time_export_name = rail.IfOperator(
            task_id = "can_update_time_export_name",
            test="{{ result('create_time_export_collection', 'length') < 1}}",
            yes_task='update_export_name_with_nodata'
        )

        update_export_name_with_nodata = rail.RepliconServiceOperator(
            task_id="update_export_name_with_nodata",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('time_export.get_export_uri')}}"
                },
                "name": "{{ result('get_logging_details').no_data_time_export_name}}"
            },
        )

        log_to_sumo_blank_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_blank_export",
            data={
                'job_start_time': 'NA',
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'export_type': 'NA',
                'export_file_name': '{{ result("get_logging_details").no_data_time_export_name }}.json',
                'export_filepath': "NA",
                'export_backup_filepath': "NA",
                'numberofrecords': "0",
            },
            sumo_conn_id=config.sumo_conn_id
        )

        query_blank_employee_id_records = rail.QueryCollectionOperator(
            task_id="query_blank_employee_id_records",
            query="""SELECT * FROM raw_time_data rtd WHERE NULLIF(rtd.employee_id, '') IS NULL OR
                    NULLIF(rtd.user_location_code, '') IS NULL OR
                    NULLIF(rtd.time_entry_id , '') IS NULL OR
                    NULLIF(rtd.entry_date , '') IS NULL OR
                    NULLIF(rtd.project_code , '') IS NULL OR
                    NULLIF(rtd.employee_id, '') IS NULL OR
                    NULLIF(rtd.hours, '') IS NULL OR
                    NULLIF(rtd.user_department_code , '') IS NULL"""
        )

        has_any_blank_emp_id = rail.IfOperator(
            task_id="has_any_blank_emp_id",
            test="{{ result('query_blank_employee_id_records', 'length') > 0}}",
            yes_task="empty_revert_to_draft",
            no_task="filter_raw_time_data"
        )

        empty_revert_to_draft = rail.EmptyOperator(
            task_id = "empty_revert_to_draft"
        )

        revert_to_draft = rail.RepliconServiceOperator(
            task_id='revert_to_draft',
            endpoint='/services/TimeDataExportService1.svc/MarkTimeDataExportAsDraft',
            data=lambda: request_payload.get_revert_draft_cancel_time_export_payload(
                'time_export')
        )

        cancel_export = rail.RepliconServiceOperator(
            task_id='cancel_export',
            endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
            data=lambda: request_payload.get_revert_draft_cancel_time_export_payload(
                'time_export')
        )

        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export - Invalid records found - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_invalid_records_in_export.html"
        )

        filter_raw_time_data = rail.QueryCollectionOperator(
            task_id="filter_raw_time_data",
            query="""SELECT * FROM raw_time_data rtd WHERE
                    NULLIF(rtd.user_location_code, '') IS NOT NULL AND
                    NULLIF(rtd.time_entry_id , '') IS NOT NULL AND
                    NULLIF(rtd.entry_date , '') IS NOT NULL AND
                    NULLIF(rtd.project_code , '') IS NOT NULL AND
                    NULLIF(rtd.employee_id, '') IS NOT NULL AND
                    NULLIF(rtd.hours, '') IS NOT NULL AND
                    NULLIF(rtd.user_department_code , '') IS NOT NULL
                """
        )

        has_any_data_to_export = rail.IfOperator(
            task_id="has_any_data_to_export",
            test="{{result('filter_raw_time_data' ,'length') > 0 }}",
            yes_task="should_use_report",
            no_task="empty_has_any_data_to_export_no_task"
        )

        empty_has_any_data_to_export_no_task = rail.EmptyOperator(
            task_id="empty_has_any_data_to_export_no_task"
        )

        get_invalid_filter_raw_time_data = rail.QueryCollectionOperator(
            task_id="get_invalid_filter_raw_time_data",
            query="""SELECT * FROM raw_time_data rtd WHERE
                    NULLIF(rtd.user_location_code, '') IS NULL OR
                    NULLIF(rtd.time_entry_id , '') IS NULL OR
                    NULLIF(rtd.entry_date , '') IS NULL OR
                    NULLIF(rtd.project_code , '') IS NULL OR
                    NULLIF(rtd.employee_id, '') IS NULL OR
                    NULLIF(rtd.hours, '') IS NULL OR
                    NULLIF(rtd.user_department_code , '') IS NULL
                """
        )

        should_use_report = rail.IfOperator(
            task_id="should_use_report",
            test=config.SHOULD_USE_REPORT,
            yes_task="get_timesheet_day_report_details",
            no_task="get_query_to_get_final_data"
        )

        get_timesheet_day_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_timesheet_day_report_details",
            report_name=config.timesheet_day_report_name
        )

        run_report_start, run_report_end = rail.run_report(
            group_id="run_report",
            report_params=request_payload.get_report_generation_params
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_expected_columns"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='load_report_data',
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id="fail_invalid_report_colums",
            message="Base report column does not match"
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_report_data_collection = rail.CreateCollectionOperator(
            task_id="create_report_data_collection",
            source="{{result('load_report_data')}}",
            name="report_data",
            columns={
                'Login Name': 'login_name',
                'Project Name': 'project_name',
                'Project Code': 'project_code',
                'Project Location': 'project_location',
                'Project Location Code': 'project_location_code',
                'Billing Rate Name': 'billing_rate_name',
                'Billing rate currency ': 'billing_rate_currency',
                'Billing rate amount': 'billing_rate_amount',
                'Approval Status': 'approval_status'
            }
        )

        get_query_to_get_final_data = rail.PythonOperator(
            task_id="get_query_to_get_final_data",
            python_callable=custom_methods.get_query_to_get_final_data_callable,
            op_args=[config]
        )

        derive_missing_fields = rail.QueryCollectionOperator(
            task_id="derive_missing_fields",
            query="""{{result('get_query_to_get_final_data')}}""",
            name="final_raw_data"
        )

        query_records_with_missing_data = rail.QueryCollectionOperator(
            task_id="query_records_with_missing_data",
            query="""SELECT * FROM final_raw_data rtd WHERE CAST(rtd.hours as decimal) > 0 AND 
                    (NULLIF(rtd.user_location_code, '') IS NULL OR
                    NULLIF(rtd.time_entry_id , '') IS NULL OR
                    NULLIF(rtd.entry_date , '') IS NULL OR
                    NULLIF(rtd.project_code , '') IS NULL OR
                    NULLIF(rtd.employee_id, '') IS NULL OR
                    NULLIF(rtd.hours, '') IS NULL OR
                    NULLIF(rtd.user_department_code , '') IS NULL OR
                    NULLIF(rtd.project_location_code, '') IS NULL)"""
        )

        has_any_records_with_missing_data = rail.IfOperator(
            task_id="has_any_records_with_missing_data",
            test="{{result('query_records_with_missing_data','length') > 0}}",
            yes_task="empty_has_any_records_with_missing_data_yes_task",
            no_task="query_final_data_to_post"
        )

        empty_has_any_records_with_missing_data_yes_task = rail.EmptyOperator(
            task_id = "empty_has_any_records_with_missing_data_yes_task"
        )

        missing_fields_csv = rail.WriteCSVFileOperator(
            task_id='missing_fields_csv',
            source="{{ result('get_invalid_filter_raw_time_data') or result('query_records_with_missing_data') or result('query_blank_employee_id_records')}}",
            header=['User', 'EmployeeID', 'UserLocationCode',
                    'ProjectCode', 'EntryDate', 'UserDepartmentCode', 'ProjectLocationCode'],
            row=[
                "{{item | attr_or_default ('login_name', 'NA')}}",
                "{{item.employee_id}}",
                "{{item.user_location_code}}",
                "{{item.project_code}}",
                "{{item.entry_date}}",
                "{{item.user_department_code}}",
                "{{item | attr_or_default ('project_location_code', 'NA')}}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{result('missing_fields_csv')}}",
            output_file_name="Invalid_TimeExport_records_{{dag_run_ecid()}}_.csv",
            expires_in_seconds=7*24*60*60
        )

        query_final_data_to_post = rail.QueryCollectionOperator(
            task_id="query_final_data_to_post",
            query="""SELECT * FROM final_raw_data""",
            name="final_data_to_post"
        )

        has_any_data_to_post = rail.IfOperator(
            task_id="has_any_data_to_post",
            test="{{ result('query_final_data_to_post', 'length') > 0}}",
            yes_task="get_unique_user_timesheet_period",
            no_task="empty_has_any_data_to_post_no_task"
        )

        empty_has_any_data_to_post_no_task = rail.EmptyOperator(
            task_id= "empty_has_any_data_to_post_no_task"
        )

        get_unique_user_timesheet_period = rail.QueryCollectionOperator(
            task_id="get_unique_user_timesheet_period",
            query="SELECT DISTINCT frtd.login_name, frtd.employee_id , frtd.timesheet_period  FROM final_data_to_post frtd",
            name="unique_user_timesheet_period"
        )

        process_user_records_per_timesheet_period = rail.trigger_parallel_dagrun(
            task_id="process_user_records_per_timesheet_period",
            items=lambda: rail.result('get_unique_user_timesheet_period'),
            trigger_dag_id=config.time_export_process_export_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=10,
            conf=lambda item: {
                    "todays_date": now(tz=config.time_zone).strftime(custom_methods.EXPORT_DATE_FORMAT),
                    "timezone": config.time_zone,
                    "export_name": rail.result('get_logging_details')['time_export_name'],
                    "employee_id": item['employee_id'],
                    "timesheet_period": item['timesheet_period'],
                    "login_name": item['login_name'],
                    "transaction_id": custom_methods.get_transaction_id(item)
            }
        )

        time_data_processing_complete = rail.EmptyOperator(
            task_id="time_data_processing_complete"
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon time data export - Completed Successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_export_success.html",
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        revert_todraft = rail.RepliconServiceOperator(
            task_id='revert_todraft',
            endpoint='/services/TimeDataExportService1.svc/MarkTimeDataExportAsDraft',
            data=lambda: request_payload.get_revert_draft_cancel_time_export_payload(
                'time_export')
        )

        cancel_time_export = rail.RepliconServiceOperator(
            task_id='cancel_timedataexport',
            endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
            data=lambda: request_payload.get_revert_draft_cancel_time_export_payload(
                "time_export")
        )

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message='{{ get_error_message() }}',
        )

        get_logging_details >> time_export_download_script >> create_export_start

        create_export_end >> create_time_export_collection >> has_any_timeexport_data
        has_any_timeexport_data >> rail.Label(
            "No") >> empty_has_any_timeexport_data_no_task >> empty_send_no_data_email >> send_no_data_email\
                >> can_update_time_export_name >> rail.Label("Yes")>> update_export_name_with_nodata \
                    >> log_to_sumo_blank_export

        has_any_timeexport_data >> rail.Label(
            "Yes") >> query_blank_employee_id_records >> has_any_blank_emp_id >> rail.Label("Yes") >>\
            empty_revert_to_draft >> missing_fields_csv >> \
                generate_download_link >> revert_to_draft >> cancel_export >> send_invalid_records_email

        should_use_report >> rail.Label("No") >> get_query_to_get_final_data
        has_any_blank_emp_id >> rail.Label(
            "No") >> filter_raw_time_data >> has_any_data_to_export >> rail.Label("Yes") \
            >> should_use_report >> rail.Label("Yes") >> get_timesheet_day_report_details\
            >> run_report_start
        run_report_end >> is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_colums
        report_has_expected_columns >> rail.Label("Yes") >> load_report_data >> create_report_data_collection\
            >> get_query_to_get_final_data >> derive_missing_fields >> query_records_with_missing_data \
            >> has_any_records_with_missing_data >> rail.Label("Yes") >> empty_has_any_records_with_missing_data_yes_task\
                >> missing_fields_csv >> generate_download_link >> revert_to_draft
        has_any_records_with_missing_data >> rail.Label("No") >> query_final_data_to_post \
            >> has_any_data_to_post >> rail.Label("Yes") >> get_unique_user_timesheet_period\
            >> process_user_records_per_timesheet_period >> time_data_processing_complete >> send_success_email >> catch_error
        has_any_data_to_post >> empty_has_any_data_to_post_no_task >> rail.Label("No") >> empty_send_no_data_email >> send_no_data_email
        has_any_data_to_export >> empty_has_any_data_to_export_no_task >> rail.Label("No") >> get_invalid_filter_raw_time_data >> missing_fields_csv
        catch_error >> rail.Label(
            "On Failure") >> revert_todraft >> cancel_time_export >> fail_time_export

    return dag


rail.for_each_instance(create_main_dag)
