from pendulum import datetime
import rail
from guidehouse.time_export.time_export_peoplesoft_india.utils import (
    custom_methods,
    request_payload,
)


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.ps_export_dag_id,
        description="Guidehouse Time Export - Child DAG for hourly and daily",
        start_date=datetime(2026, 5, 1, tz=config.timezone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        default_args={"sftp_conn_id": config.sftp_conn_id},
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        response_from_dag_var = rail.SetVariableOperator(
            task_id="response_from_dag_var",
            name="response_from_dag",
            append=False,
            value="Success",
        )

        time_export_download_script_uri = rail.RepliconServiceOperator(
            task_id="time_export_download_script_uri",
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: custom_methods.get_timeexport_fileformat(
                config.export_file_format_name, response
            ),
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id="create_download_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            data=lambda dag_run: request_payload.get_download_batch(
                rail.result("time_export_download_script_uri"),
                dag_run.conf["batch_uri"],
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
                "Short Time Entry ID": "short_time_entry_id",
                "Entry Date": "entry_date",
                "Project Code": "project_code",
                "Task Name": "task_name",
                "Hours": "hours",
                "Distributed Time Type Code": "pay_type",
                "Time Off Type Name": "timeoff_type",
                "Comments": "comments",
                "FMLA": "fmla",
                "Time Off Booking ID": "timeoff_booking_id"
            },
        )

        query_blank_employee_id_records = rail.QueryCollectionOperator(
            task_id="query_blank_employee_id_records",
            query="""SELECT DISTINCT employee_id, short_time_entry_id, entry_date, project_code, task_name, hours, pay_type, timeoff_type
                     FROM raw_timeexport_data
                     WHERE NULLIF(employee_id, '') IS NULL""",
        )

        has_any_blank_emp_id = rail.IfOperator(
            task_id="has_any_blank_emp_id",
            test="{{ result('query_blank_employee_id_records', 'length') > 0 }}",
            yes_task="empty_has_any_blank_emp_id_yes_task",
            no_task="query_timeexport_records",
        )

        empty_has_any_blank_emp_id_yes_task = rail.EmptyOperator(
            task_id="empty_has_any_blank_emp_id_yes_task",
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
                "Pay Type",
                "Time Off Type",
            ],
            row=lambda item: [
                item["employee_id"],
                item["short_time_entry_id"],
                item["entry_date"],
                item["project_code"],
                item["task_name"],
                item["hours"],
                item["pay_type"],
                item["timeoff_type"],
            ],
        )

        generate_download_link_missing_employeeid_records_csv = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link_missing_employeeid_records_csv",
            artifact_name="{{ result('missing_employeeid_csv') }}",
            output_file_name="Invalid_TimeExport_records_{{ dag_run_ecid() }}.csv",
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        send_invalid_records_email = rail.EmailOperator(
            task_id="send_invalid_records_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon {{ dag_run.conf.export_type | capitalize }} Time Data Export to {{ dag_run.conf.financial_system }} - Invalid records found - {{ dag_run.conf.timestamp }}",
            html_content="/templates/email_invalid_records_in_export.html",
        )

        set_response_from_dag_blank_employee_id_found = rail.SetVariableOperator(
            task_id="set_response_from_dag_blank_employee_id_found",
            name="response_from_dag",
            append=False,
            value="Blank employee id entry found, thus stopping the time export",
        )

        paycodes_str = ", ".join(f"'{code}'" for code in config.paycodes_to_exclude)
        query_timeexport_records = rail.QueryCollectionOperator(
            task_id="query_timeexport_records",
            query=f"""SELECT * FROM raw_timeexport_data WHERE nullif(employee_id,'') IS NOT NULL AND
            (NULLIF(pay_type, '') IS NULL OR pay_type NOT IN ({paycodes_str})) """,
        )

        has_any_timeexport_data = rail.IfOperator(
            task_id="has_any_timeexport_data",
            test="{{result('query_timeexport_records', 'length') > 0 }}",
            yes_task="update_timeexport_records",
            no_task="set_response_from_dag_no_data",
        )

        set_response_from_dag_no_data = rail.SetVariableOperator(
            task_id="set_response_from_dag_no_data",
            name="response_from_dag",
            append=False,
            value="No Data in export",
        )

        update_timeexport_records = rail.DataAdaptorOperator(
            task_id="update_timeexport_records",
            source='{{result("query_timeexport_records")}}',
            columns=[
                "employee_id",
                "short_time_entry_id",
                "entry_date",
                "project_code",
                "task_name",
                "hours",
                "pay_type",
                "comments",
            ],
            data=lambda item: custom_methods.get_peoplesoft_export_rows(
                item, config.TIMEOFF_PROJECT_TASK_MAPPER
            ),
        )

        create_timeexport_records = rail.CreateCollectionOperator(
            task_id="create_timeexport_records",
            source='{{result("update_timeexport_records")}}',
        )

        write_export_csv = rail.WriteCSVFileOperator2(
            task_id="write_export_csv",
            source='{{result("create_timeexport_records")}}',
            header=[
                "Employee ID",
                "Short Entry ID",
                "Transaction Date",
                "PeopleSoft Project ID",
                "PeopleSoft Activity ID",
                "Number of Hours",
                "Pay Types",
                "Comments",
            ],
            row=[
                "{{item.employee_id}}",
                "{{item.short_time_entry_id}}",
                "{{item.entry_date}}",
                "{{item.project_code}}",
                "{{item.task_name}}",
                "{{item.hours}}",
                "{{item.pay_type}}",
                "{{item.comments}}",
            ],
            delimiter="|",
        )

        encrypt_time_export_data_csv = rail.PGPEncryptionOperator(
            task_id="encrypt_time_export_data_csv",
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_export_csv') }}"
        )

        upload_time_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_time_export_to_sftp",
            content='{{ result("encrypt_time_export_data_csv") }}',
            remote_filepath=config.ps_outbound_path_trial
            + "/{{ dag_run.conf.time_export_name }}"
            + config.file_extension,
        )

        send_valid_export_complete_email = rail.EmailOperator(
            task_id="send_valid_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon {{ dag_run.conf.financial_system }} {{ dag_run.conf.export_type | capitalize }} time data extract is completed - {{ dag_run.conf.timestamp }}",
            html_content="/templates/email_valid_export_complete.html",
            params={"upload_file_path": config.ps_outbound_path_trial},
        )

        catch_error = rail.SetVariableOperator(
            task_id="catch_error",
            trigger_rule="one_failed",
            name="response_from_dag",
            append=False,
            value="Error in child dag - Time export to PeopleSoft",
        )

        final_response_from_dag = rail.PythonOperator(
            task_id="final_response_from_dag",
            trigger_rule="all_done",
            python_callable=lambda: rail.get_dag_run_var("response_from_dag"),
        )

        (
            response_from_dag_var
            >> time_export_download_script_uri
            >> create_download_batch
            >> execute_download_batch
            >> wait_for_download_batch
            >> get_download_url
            >> download_export
            >> load_export
            >> create_raw_timeexport_data_collection
            >> query_blank_employee_id_records
            >> has_any_blank_emp_id
        )

        (
            has_any_blank_emp_id
            >> rail.Label("Yes")
            >> empty_has_any_blank_emp_id_yes_task
            >> missing_employeeid_csv
            >> generate_download_link_missing_employeeid_records_csv
            >> send_invalid_records_email
            >> set_response_from_dag_blank_employee_id_found
            >> catch_error
        )

        (
            has_any_blank_emp_id
            >> rail.Label("No")
            >> query_timeexport_records
            >> has_any_timeexport_data
        )

        (
            has_any_timeexport_data
            >> rail.Label("No")
            >> set_response_from_dag_no_data
            >> catch_error
        )

        (
            has_any_timeexport_data
            >> rail.Label("Yes")
            >> update_timeexport_records
            >> create_timeexport_records
            >> write_export_csv
            >> encrypt_time_export_data_csv
            >> upload_time_export_to_sftp
            >> send_valid_export_complete_email
            >> catch_error
        )

        catch_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_main_dag)
