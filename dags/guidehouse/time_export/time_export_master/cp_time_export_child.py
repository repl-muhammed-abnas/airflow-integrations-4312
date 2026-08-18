from pendulum import datetime, now
import rail
from guidehouse.time_export.time_export_master.utils import (
    custom_methods,
    request_payload,
)
from guidehouse.time_export.time_export_master.tasks.time_export_task import (
    time_data_export
)
from guidehouse.time_export.time_export_master.tasks.update_time_export_status import (
    cancel_time_export
)
from guidehouse.time_export.time_export_master.utils.date_range import (
    format_date_for_export,
)
from guidehouse.time_export.time_export_master.tasks.approval_data_report import approval_data_report_task

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.dl_cp_export_dag_id,
        description="Guidehouse Time Export - Datalake CP Child DAG",
        start_date=datetime(2026, 5, 1, tz=config.timezone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        default_args={"sftp_conn_id": config.sftp_conn_id},
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        def get_date_window_callable():
            run_date = now(tz=config.timezone)

            end_date = run_date
            while end_date.format("dddd") != "Saturday":
                end_date = end_date.subtract(days=1)

            start_date = run_date.subtract(years=1)
            while start_date.format("dddd") != "Monday":
                start_date = start_date.subtract(days=1)

            timestamp = run_date.strftime("%Y%m%d_%H%M%S")
            return {
                "run_date": run_date.strftime("%Y-%m-%d"),
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "start_date_formatted": format_date_for_export(start_date),
                "end_date_formatted": format_date_for_export(end_date),
                "timestamp": timestamp,
                "time_export_name": "CPTime_" + timestamp,
                "no_data_export_name": f"CPTime_NoData_{timestamp}",
            }

        get_date_window = rail.PythonOperator(
            task_id="get_date_window",
            python_callable=get_date_window_callable,
        )

        get_all_service_centers = rail.RepliconServiceOperator(
            task_id="get_all_service_centers",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        create_timedata_row_counts_batch = rail.RepliconServiceOperator(
            task_id="create_timedata_row_counts_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataItemRowCountsBatch",
            data=lambda dag_run: request_payload.time_export_generate_request(dag_run,payload_type="row_count"),
        )

        (execute_row_counts_batch, wait_for_row_counts_batch) = rail.batch_execution(
            group_id="execute_row_counts_batch",
            creation_task_id=create_timedata_row_counts_batch.task_id,
            wait_timeout=60 * 60 * 5,
        )

        get_cp_row_counts_results = rail.RepliconServiceOperator(
            task_id="get_cp_row_counts_results",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataItemRowCountsBatchResults",
            data=lambda: {
                "timeDataItemRowCountsBatchUri": rail.result(
                    "create_timedata_row_counts_batch"
                )
            },
        )

        export_has_data = rail.IfOperator(
            task_id="export_has_data",
            test=lambda: rail.result("get_cp_row_counts_results")["rowCounts"][
                0
            ]
            > 0,
            yes_task="start_export",
            no_task="catch_error",
        )

        start_export = rail.EmptyOperator(task_id="start_export")

        time_export = time_data_export(
            group_id="time_export",
            get_export_name="{{ result('get_date_window').time_export_name }}",
        )

        cp_time_export_complete = rail.EmptyOperator(task_id="cp_time_export_complete")

        time_export_download_script_uri = rail.RepliconServiceOperator(
            task_id="time_export_download_script_uri",
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: custom_methods.get_timeexport_fileformat(
                config.dl_time_export_format, response
            ),
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id="create_download_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            data=lambda dag_run: request_payload.get_download_batch(
                rail.result("time_export_download_script_uri"),
            rail.result("time_export.get_export_uri")
            ),
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id="execute_download_batch",
            creation_task_id=create_download_batch.task_id,
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id="get_download_url",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            data={
                "timeDataDownloadBatchUri": "{{ result('"
                + create_download_batch.task_id
                + "') }}"
            },
            data_handler=lambda response: response["downloadUrl"],
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id="download_export",
            url="{{ result('get_download_url') }}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id="load_export",
            document="{{ result('download_export') }}",
        )

        create_raw_timeexport_data_collection = rail.CreateCollectionOperator(
            task_id="create_raw_timeexport_data_collection",
            source="{{result('load_export')}}",
            name="raw_timeexport_data",
            columns={
                "Employee ID": "employee_id",
                "User": "user",
                "Entry Date": "entry_date",
                "Project Code": "project_code",
                "Task Name": "task_name",
                "Task Name Full Path": "task_name_full_path",
                "Task Code": "task_code",
                "Pay Code": "pay_code",
                "Hours": "hours",
                "Company Code Code": "company_code_code",
                "Short Time Entry ID": "short_time_entry_id",
                "Work Location Code": "work_location_code",
                "Time Off Type Name": "timeoff_type",
                "Timesheet Period": "timesheet_period",
                "FMLA": "fmla",
                "Financial System Name": "financial_system_name",
                "Time Off Booking ID": "timeoff_booking_id",
                "Login Name": "login_name",
                "Location Name": "location_name",
                "Comments": "comments"
            },
        )

        query_blank_employee_id_records = rail.QueryCollectionOperator(
            task_id="query_blank_employee_id_records",
            query="""SELECT DISTINCT employee_id, short_time_entry_id, entry_date, project_code,
            task_name, hours, pay_code, timeoff_type
                     FROM raw_timeexport_data
                     WHERE NULLIF(employee_id, '') IS NULL""",
        )

        has_any_blank_emp_id = rail.IfOperator(
            task_id="has_any_blank_emp_id",
            test="{{ result('query_blank_employee_id_records', 'length') > 0 }}",
            yes_task="missing_employeeid_csv",
            no_task="query_distinct_tasks_per_user",
        )

        query_distinct_tasks_per_user = rail.QueryCollectionOperator(
            task_id="query_distinct_tasks_per_user",
            query="""SELECT DISTINCT employee_id, project_code, task_name_full_path
                     FROM raw_timeexport_data
                     WHERE NULLIF(timeoff_type, '') IS NULL
                       AND NULLIF(task_name_full_path, '') IS NOT NULL
                       AND NULLIF(project_code, '') IS NOT NULL
                       AND NULLIF(employee_id, '') IS NOT NULL""",
        )

        get_all_project_roles = rail.RepliconServicePageOperator(
            task_id="get_all_project_roles",
            endpoint="/services/ProjectRoleListService1.svc/GetData",
            data=lambda: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:project-role-list-column:description",
                    "urn:replicon:project-role-list-column:project-role",
                ],
                "sort": [],
                "filterExpression": None,
            },
            page_handler=custom_methods.project_role_page_handler,
            all_result_data_handler=custom_methods.build_project_role_code_map,
        )

        get_user_and_task_role_mapping = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_and_task_role_mapping",
            endpoint="/services/TaskService1.svc/BulkGetTaskResourceEstimateDetailsForTaskResourceUserAssignmentPairs",
            items="{{ result('query_distinct_tasks_per_user') }}",
            batch_size=10,
            data=lambda items: {
                "taskResourceUserAssignmentPairs": [
                    request_payload.build_task_resource_payload(item) for item in items
                ]
            },
            data_handler=lambda data, items:(items,data),
            flatten=True,
            all_result_data_handler=custom_methods.build_plc_mapping
        )

        apply_task_role_mapping = rail.DataAdaptorOperator(
            task_id="apply_task_role_mapping",
            source='{{ result("create_raw_timeexport_data_collection") }}',
            data=lambda item, dag_run: custom_methods.apply_plc_to_row(
                item, config.TIMEOFF_PROJECT_TASK_MAPPER,dag_run, config.level2_countries
            ),
        )

        create_cp_collection_with_plc = rail.CreateCollectionOperator(
            task_id="create_cp_collection_with_plc",
            source='{{ result("apply_task_role_mapping") }}',
            name="cp_export_data_with_plc",
            columns={'employee_id': 'employee_id', 'user': 'user', 'entry_date': 'entry_date', 
                     'project_code': 'project_code', 'task_name': 'task_name', 'task_name_full_path': 'task_name_full_path',
                       'task_code': 'task_code', 'pay_code': 'pay_code', 'hours': 'hours', 
                       'company_code_code': 'company_code_code', 'short_time_entry_id': 'short_time_entry_id', 'work_location_code': 'work_location_code', 'timeoff_type': 'timeoff_type', 
                     'timesheet_period': 'timesheet_period', 'fmla': 'fmla', 
                     'financial_system_name': 'financial_system_name', 
                     'timeoff_booking_id': 'timeoff_booking_id', 'login_name': 'login_name',
                     "timeoff_hours":"timeoff_hours","plc":"plc","plc_name":"plc_name",
                     "comments":"comments"}
        )

        paycodes_str = ", ".join(f"'{code}'" for code in config.paycodes_to_exclude)

        query_valid_cp_data = rail.QueryCollectionOperator(
            task_id="query_valid_cp_data",
            query=f"""SELECT * FROM cp_export_data_with_plc WHERE nullif(employee_id,'') IS NOT NULL AND
            (NULLIF(pay_code, '') IS NULL OR pay_code NOT IN ({paycodes_str}))""",
            name="valid_cp_data"
        )
        
        start_report_run = rail.EmptyOperator(task_id="start_report_run")

        run_report = approval_data_report_task("approval_report_tg", config.cp_report_name)

        stop_report_run = rail.EmptyOperator(task_id="stop_report_run")

        query_final_cp_data = rail.QueryCollectionOperator(
            task_id="query_final_cp_data",
            query=f"""SELECT rd.unique_id, cp.employee_id,
             cp.user, cp.entry_date, cp.project_code, cp.task_code,
             cp.pay_code, cp.hours, cp.company_code_code,
             rd.submitted_on AS timesheet_submitted_on,
             rd.approval_datetime,
             rd.approval_status AS timesheet_approval_status,
             cp.short_time_entry_id, cp.work_location_code,
             cp.timeoff_type, cp.timeoff_hours, cp.plc_name, cp.plc, cp.timesheet_period, cp.fmla,
             cp.financial_system_name, cp.comments
            FROM valid_cp_data cp LEFT JOIN datalake_time_extract_report rd ON
            cp.employee_id = rd.employee_id AND cp.timesheet_period = rd.timesheet_period""",
            name="final_cp_data"
        )

        write_export_cp_csv = rail.WriteCSVFileOperator(
            task_id="write_export_cp_csv",
            source='{{result("query_final_cp_data")}}',
            header=['Unique ID', 'Employee ID', 'Username', 'Entry Date', 'Project Code',
                    'Task Code', 'Pay code', 'Hours',
                    'Company Code', 'Timesheet Submitted On', 'Approval Date/Time',
                    'Timesheet Approval Status', 'Short Entry ID', 'Work Location',
                    'Time Off Type', 'Time Off Hours', 'PLC Name', 'PLC', 'Timesheet Period',
                    'Financial System', 'Comments'],
            row=[
                "{{item.unique_id}}",
                "{{item.employee_id}}",
                "{{item.user}}",
                "{{item.entry_date}}",
                "{{item.project_code}}",
                "{{item.task_code}}",
                "{{item.pay_code}}",
                "{{item.hours}}",
                "{{item.company_code_code}}",
                "{{item.timesheet_submitted_on}}",
                "{{item.approval_datetime}}",
                "{{item.timesheet_approval_status}}",
                "{{item.short_time_entry_id}}",
                "{{item.work_location_code}}",
                "{{item.timeoff_type}}",
                "{{item.timeoff_hours}}",
                "{{item.plc_name}}",
                "{{item.plc}}",
                "{{item.timesheet_period}}",
                "{{item.financial_system_name}}",
                "{{item.comments}}",
            ],
            delimiter="|",
        )

        encrypt_time_export_data_csv = rail.PGPEncryptionOperator(
            task_id="encrypt_time_export_data_csv",
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_export_cp_csv') }}"
        )

        upload_cp_time_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_cp_time_export_to_sftp",
            content='{{ result("encrypt_time_export_data_csv") }}',
            remote_filepath=config.dl_outbound_path
            + f"/DL_CP_Time{config.env_suffix.upper()}_"
            + "{{ dag_run.conf.timestamp }}.csv.pgp",
        )

        missing_employeeid_csv = rail.WriteCSVFileOperator(
            task_id="missing_employeeid_csv",
            source="{{ result('query_blank_employee_id_records') }}",
            header=[
                "Employee ID",
                "Short Time Entry ID",
                "Entry Date",
                "Project Code",
                "Task Name",
                "Hours",
                "Pay Code",
                "Time Off Type",
            ],
            row=lambda item: [
                item["employee_id"],
                item["short_time_entry_id"],
                item["entry_date"],
                item["project_code"],
                item["task_name"],
                item["hours"],
                item["pay_code"],
                item["timeoff_type"],
            ],
        )

        generate_missing_empid_file_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_missing_empid_file_link",
            artifact_name="{{ result('missing_employeeid_csv') }}",
            output_file_name="Invalid_TimeExport_records_{{ dag_run_ecid() }}.csv",
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        send_invalid_records_email = rail.EmailOperator(
            task_id="send_invalid_records_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Costpoint Time Data Export to DataLake - Invalid records found - {{ dag_run.conf.timestamp }}",
            html_content="/templates/email_invalid_records_in_export.html",
            params={"financial_system": "CostPoint"},
        )

        get_cp_export_uri_for_rollback = rail.RepliconServiceOperator(
            task_id="get_cp_export_uri_for_rollback",
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            data={
                "timeDataExportBatchUri": "{{ result('time_export.create_export') }}"
            },
            data_handler=custom_methods.retrieve_export_uri,
        )

        cancel_cp_start, cancel_cp_end = cancel_time_export(
            "cancel_cp_export",
            "get_cp_export_uri_for_rollback",
        )

        send_success_email = rail.EmailOperator(
            task_id="send_success_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} |Costpoint Replicon Data Lake time data extract is completed - {{ dag_run.conf.timestamp }}",
            html_content="/templates/email_valid_export_complete.html",
            params={
                "upload_file_path": config.dl_outbound_path,
                "cp_file_prefix": f"DL_CP_Time{config.env_suffix}_",
            },
        )

        catch_error = rail.EmptyOperator(
            task_id="catch_error",
            trigger_rule="one_failed",
        )

        get_export_uri_failed = rail.RepliconServiceOperator(
            task_id="get_export_uri_failed",
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            data={
                "timeDataExportBatchUri": "{{ result('time_export.create_export') }}"
            },
            data_handler=custom_methods.retrieve_export_uri,
        )

        mark_export_status_cancel_start, mark_export_status_cancel_end = cancel_time_export("cancel_timedata_export", 
                                                "get_export_uri_failed")

        update_export_name_cancelled = rail.RepliconServiceOperator(
            task_id="update_export_name_cancelled",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {"uri": "{{ result('get_export_uri_failed') }}"},
                "name": "Cancelled_{{ result('get_date_window').time_export_name }}",
            },
        )

        fail_time_export = rail.FailOperator(
            task_id="fail_time_export", message="{{ get_error_message() }}"
        )

        get_date_window >>\
        get_all_service_centers >>\
        create_timedata_row_counts_batch >>\
        execute_row_counts_batch >> wait_for_row_counts_batch >>\
        get_cp_row_counts_results >>\
        export_has_data >> rail.Label("No") >>\
        catch_error
        export_has_data >> rail.Label("Yes") >> start_export >>\
        time_export >> cp_time_export_complete >>\
        time_export_download_script_uri >> create_download_batch>>\
        execute_download_batch >> wait_for_download_batch >>\
        get_download_url >> download_export >>\
        load_export >> create_raw_timeexport_data_collection >>\
        query_blank_employee_id_records >>\
        has_any_blank_emp_id >> rail.Label("Yes") >>\
        missing_employeeid_csv>>\
        generate_missing_empid_file_link >> send_invalid_records_email >>\
        get_cp_export_uri_for_rollback >> cancel_cp_start >> cancel_cp_end >> catch_error
        has_any_blank_emp_id >> rail.Label("No") >>\
        query_distinct_tasks_per_user >> get_all_project_roles >> get_user_and_task_role_mapping >> \
        apply_task_role_mapping >> create_cp_collection_with_plc >>\
        query_valid_cp_data >> start_report_run >> run_report >> stop_report_run>>\
        query_final_cp_data >> write_export_cp_csv >>\
        encrypt_time_export_data_csv >>\
        upload_cp_time_export_to_sftp >> send_success_email >> catch_error
        catch_error >> get_export_uri_failed >> mark_export_status_cancel_start >> mark_export_status_cancel_end >>\
        update_export_name_cancelled >> fail_time_export

    return dag

rail.for_each_instance(create_main_dag)