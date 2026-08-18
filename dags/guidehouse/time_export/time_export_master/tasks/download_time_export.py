import rail
from guidehouse.time_export.time_export_master.utils import custom_methods,request_payload

def download_export(group_id, fileformat, fs, export_name,):
    with rail.TaskGroup(group_id=group_id) as tg:

        if_batch_uri = rail.IfOperator(
            task_id="if_batch_uri",
            test=lambda:bool(rail.result("validate_export_results")[fs]),
            yes_task=f"{group_id}.time_export_download_script_uri",
            no_task=f"{group_id}.create_empty_collection"
        )

        time_export_download_script_uri = rail.RepliconServiceOperator(
           task_id="time_export_download_script_uri",
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: custom_methods.get_timeexport_fileformat(
                fileformat, response
            ),
        )

        create_download_batch = rail.RepliconServiceOperator(
           task_id="create_download_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            data=lambda dag_run: request_payload.get_download_batch(
                rail.result(f"{group_id}.time_export_download_script_uri"),
                rail.result("validate_export_results")[fs],
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
            url=f"{{{{ result('{group_id}.get_download_url') }}}}",
        )

        load_export = rail.LoadCSVFileOperator(
           task_id="load_export",
            document=f"{{{{ result('{group_id}.download_export') }}}}",
        )

        create_raw_timeexport_data_collection = rail.CreateCollectionOperator(
           task_id="create_raw_timeexport_data_collection",
            source=f"{{{{ result('{group_id}.load_export')}}}}",
            name=export_name,
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

        create_empty_collection = rail.CreateCollectionOperator(
           task_id="create_empty_collection",
            source=[],
            name=export_name,
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
            }
        )

        if_batch_uri >> rail.Label("Yes") >>\
        time_export_download_script_uri >> create_download_batch >> execute_download_batch >>\
        wait_for_download_batch >> get_download_url >> download_export >> load_export >>\
        create_raw_timeexport_data_collection
        if_batch_uri >> rail.Label("No") >> create_empty_collection


    return tg

