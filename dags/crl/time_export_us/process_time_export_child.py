from datetime import datetime as dt
from json import loads
from pendulum import datetime
import rail
from crl.time_export_us.utils import custom_methods, request_payload
from crl.time_export_us.tasks.time_export_task import time_data_export

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_process_export_dag_id,
        description="CRL Time Export process time export",
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_log') }}",
            message="{{ dag_run.conf.process_start_time }} - Process started",
            properties={
                "log": "{{ dag_run.conf.process_start_time }} - Process started"
            }
        )

        get_allowed_location_uris = rail.RepliconServiceOperator(
            task_id='get_allowed_location_uris',
            endpoint="/services/LocationListService1.svc/GetHierarchyData",
            data=request_payload.get_allowed_location_uris_payload(
                config.export_locations),
            data_handler=custom_methods.get_filtered_allowed_location_uris
        )

        time_export_download_script = rail.RepliconServiceOperator(
            task_id='time_export_download_script',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: custom_methods.get_timeexport_fileformat(
                config, response)
        )

        time_export_start, time_export_end = time_data_export(
            group_id="time_export",
            generate_request=request_payload.get_create_time_data_export_batch_payload,
            get_export_name="{{dag_run.conf.time_export_name}}",
            file_script_uri="time_export_download_script",
            retries=0
        )

        create_raw_timeexport_data_collection = rail.CreateCollectionOperator(
            task_id="create_raw_timeexport_data_collection",
            source="{{result('time_export.load_export')}}",
            name="raw_timeexport_data",
            columns={
                'Employee Number': 'employee_id',
                'Event Date': 'entry_date',
                'Allocated Hours': 'hours',
                'Time Type (CAN) (Code)': 'time_type_canada_code',
                'Time Off Type Description': 'timeoff_type_description',
                'Activity Type': 'activity_name',
                'Punch In Date': 'punch_in_date',
                'Punch Out Date': 'punch_out_date',
                'Project Code': 'project_code',
                'Network Code': 'network_code',
                'Network Activity Code': 'task_code',
                'spanid': 'short_id',
                'Transaction Type': 'transaction_type',
                'Punch Entry ID': 'punch_entry_id',
                'Project User': 'project_user',
                'Login Name': 'login_name',
                'Time Type (US) (Code)': 'time_type_usa_code',
                'Time Type (NA04) (Code)': 'time_type_na04_code',
                'Default Activity': 'default_activity',
                'Business Unit Name': 'business_unit_name',
                'Location Name': 'location_name',
                'Distributed Time Type Code': 'distributed_time_type_code',
                'Time Off Type Name': 'timeoff_type_name'
            }
        )

        has_any_timeexport_data = rail.IfOperator(
            task_id="has_any_timeexport_data",
            test="{{result('create_raw_timeexport_data_collection', 'length') > 0 }}",
            yes_task="query_blank_employee_id_records",
            no_task="send_no_data_email"
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon US Time Data Export - No records to export - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_empty_export.html"
        )

        update_export_name_with_nodata = rail.RepliconServiceOperator(
            task_id="update_export_name_with_nodata",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('time_export.get_export_uri')}}"
                },
                "name": "{{ dag_run.conf.no_data_time_export_name}}"
            },
        )

        log_to_sumo_blank_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_blank_export",
            data={
                'job_start_time': '{{ dag_run.conf.process_start_time }}',
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'export_file_name': '{{ dag_run.conf.no_data_time_export_name }}.json',
                'export_filepath': config.timeexport_upload_input_filepath,
                'numberofrecords': "{{ result('format_raw_export_data', 'length')}}",
            },
            sumo_conn_id=config.sumo_conn_id
        )

        query_blank_employee_id_records = rail.QueryCollectionOperator(
            task_id="query_blank_employee_id_records",
            query="""SELECT * FROM raw_timeexport_data rtd WHERE NULLIF(rtd.employee_id, '') IS NULL"""
        )

        has_any_blank_emp_id = rail.IfOperator(
            task_id="has_any_blank_emp_id",
            test="{{ result('query_blank_employee_id_records', 'length') > 0}}",
            yes_task="empty_has_any_blank_emp_id_yes_task",
            no_task="query_records_where_mandatory_field_missing"
        )

        empty_has_any_blank_emp_id_yes_task = rail.EmptyOperator(
            task_id='empty_has_any_blank_emp_id_yes_task'
        )

        query_records_where_mandatory_field_missing = rail.QueryCollectionOperator(
            task_id="query_records_where_mandatory_field_missing",
            query="""SELECT * FROM raw_timeexport_data rtd WHERE
                    NULLIF(rtd.entry_date , '') IS NULL OR NULLIF(rtd.employee_id , '') IS NULL
                """
        )

        has_missing_data = rail.IfOperator(
            task_id="has_missing_data",
            test="{{ result('query_records_where_mandatory_field_missing', 'length') > 0 }}",
            yes_task="empty_has_missing_data_yes_task",
            no_task="mandatory_fileds_query"
        )

        empty_has_missing_data_yes_task = rail.EmptyOperator(
            task_id="empty_has_missing_data_yes_task"
        )

        missing_fields_csv = rail.WriteCSVFileOperator(
            task_id='missing_fields_csv',
            source="{{ result('query_records_where_mandatory_field_missing') or result('query_blank_employee_id_records') }}",
            header=['User', 'EmployeeID', 'EntryDate', 'ProgramCode',
                    'ProjectCode', 'PunchInTime', 'PunchOutTime'],
            row=lambda item: [
                item['login_name'],
                item["employee_id"],
                item["entry_date"],
                item["project_code"],
                item["network_code"],
                item["punch_in_date"],
                item["punch_out_date"]
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{result('missing_fields_csv')}}",
            output_file_name="Invalid_TimeExport_records_{{dag_run_ecid()}}_.csv",
            expires_in_seconds=7*24*60*60
        )

        revert_to_draft = rail.RepliconServiceOperator(
            task_id='revert_to_draft',
            endpoint='/services/TimeDataExportService1.svc/MarkTimeDataExportAsDraft',
            data=lambda: request_payload.get_revert_draft_payload(
                'time_export')
        )

        cancel_export = rail.RepliconServiceOperator(
            task_id='cancel_export',
            endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
            data=lambda: request_payload.get_cancel_timeoff_export_payload(
                'time_export')
        )

        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon US Time Data Export - Invalid records found - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_invalid_records_in_export.html"
        )

        mandatory_fileds_query = rail.QueryCollectionOperator(
            task_id="mandatory_fileds_query",
            query="""SELECT * FROM raw_timeexport_data rtd WHERE
                    NULLIF(rtd.entry_date , '') IS NOT NULL AND NULLIF(rtd.employee_id , '') IS NOT NULL
                """,
            name='mandatory_data'
        )

        filter_raw_timeexport_data = rail.QueryCollectionOperator(
            task_id="filter_raw_timeexport_data",
            query="""SELECT * FROM mandatory_data WHERE ((project_user == "Yes" AND NULLIF(short_id, '') IS NOT NULL AND
                    ((NULLIF(timeoff_type_description, '') IS NULL) OR 
                    (NULLIF(timeoff_type_description, '') IS NOT NULL AND business_unit_name NOT IN ("NA04", "NA05")))) OR
                    (project_user == "No" AND NULLIF(short_id, '') IS NULL AND NULLIF(punch_entry_id,'') IS NOT NULL) OR
                    (project_user == "No" AND NULLIF(short_id, '') IS NOT NULL AND NULLIF(timeoff_type_description,'') IS NOT NULL))""",
            name='final_data'
        )

        logging_no_of_records_exported = rail.WriteLogOperator(
            task_id="logging_no_of_records_exported",
            log="{{ result('create_log') }}",
            message="{{ current_time() }} - INFO admin No of records exported = {{result('filter_raw_timeexport_data','length')}}",
            properties={
                "log": "{{ current_time() }} - INFO admin No of records exported = {{result('filter_raw_timeexport_data','length')}}",
            }
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ current_time() }} - INFO admin Export File : " +
            "log_{{dag_run.conf.time_export_name}}" + ".txt",
            properties={
                "log": " {{ current_time() }} - INFO admin Export File : " +
                    "log_{{dag_run.conf.time_export_name}}" + ".txt"
            }
        )

        format_raw_export_data = rail.DataAdaptorOperator(
            task_id="format_raw_export_data",
            source="{{ result('filter_raw_timeexport_data') }}",
            columns=['employee_id', 'entry_date', 'hours', 'time_type_canada_code',
                     'timeoff_type_description', 'activity_name', 'punch_in_date',
                     'punch_out_date', 'project_code', 'task_code', 'short_id',
                     'transaction_type'],
            data=lambda item: custom_methods.format_raw_export_data_callable(
                item, config.pai_locations, config.pay_code_mapper)
        )

        final_export_data = rail.DataAdaptorOperator(
            task_id="final_export_data",
            source="{{result('format_raw_export_data')}}",
            columns=[
                'Employee_Number',
                'Event_Date',
                'Allocated_Hours',
                'Pay_Type',
                'Activity_Type',
                'Actual_Start_Date',
                'Actual_End_Date',
                'Project_ID',
                'Network_ID',
                'Network_Activity',
                'spanid',
                'Transaction_Type'
            ],
            data=custom_methods.final_export_data_callable
        )

        create_json_payload = rail.PythonOperator(
            task_id="create_json_payload",
            python_callable=custom_methods.create_json_payload_callable,
            op_args=[final_export_data.task_id]
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            content="{{result('create_json_payload')}}",
            remote_filepath=config.timeexport_upload_input_filepath +
            '/{{dag_run.conf.time_export_name}}' + '.json'
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=lambda:  dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        logging_job_end_time = rail.WriteLogOperator(
            task_id="logging_job_end_time",
            log="{{ result('create_log') }}",
            message="{{result('process_end_time')}} - Process ended",
            properties={
                "log": "{{result('process_end_time')}} - Process ended"
            }
        )

        log_file_data_to_csv = rail.WriteCSVFileOperator(
            task_id="log_file_data_to_csv",
            source="{{ result('create_log') }}",
            header=None,
            row=[
                '{{ item.properties | attr_or_default("log", "") }}'
            ]
        )

        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.logs_filepath +
            '/log_{{dag_run.conf.time_export_name}}' + '.txt'
        )

        is_run_failed = rail.IfOperator(
            task_id="is_run_failed",
            trigger_rule = 'all_done',
            test="{{ get_error_message() | is_truthy}}",
            yes_task="fail_dagrun",
            no_task="is_run_skipped"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message="{{get_error_message()}}"
        )

        def is_run_skipped_test():
            if rail.result('create_raw_timeexport_data_collection', 'length') < 1:
                return True
            if rail.result('query_blank_employee_id_records', 'length') > 0:
                return True
            if rail.result('query_records_where_mandatory_field_missing', 'length') > 0:
                return True
            return False

        is_run_skipped = rail.IfOperator(
            task_id="is_run_skipped",
            test=is_run_skipped_test,
            no_task="send_success_email"
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon US Time Data Export - Completed Successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_export_success.html",
            params={
                'sftp_upload_path': config.timeexport_upload_input_filepath
            }
        )

        log_to_sumo_valid_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_valid_export",
            data={
                'job_start_time': '{{ dag_run.conf.process_start_time }}',
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'export_file_name': '{{ dag_run.conf.time_export_name }}.json',
                'export_filepath': config.timeexport_upload_input_filepath,
                'numberofrecords': "{{ result('format_raw_export_data', 'length')}}",
                'location': 'USA'
            },
            sumo_conn_id=config.sumo_conn_id
        )

        create_log >> logging_job_start_time >> get_allowed_location_uris >> time_export_download_script

        time_export_download_script >> time_export_start
        time_export_end >> create_raw_timeexport_data_collection >> has_any_timeexport_data

        has_any_timeexport_data >> rail.Label(
            "No") >> send_no_data_email >> update_export_name_with_nodata >> log_to_sumo_blank_export

        has_any_timeexport_data >> rail.Label(
            "Yes") >> query_blank_employee_id_records >> has_any_blank_emp_id

        has_any_blank_emp_id >> rail.Label(
            "Yes") >> empty_has_any_blank_emp_id_yes_task >> missing_fields_csv
        revert_to_draft >> cancel_export >> send_invalid_records_email

        has_any_blank_emp_id >> rail.Label(
            "No") >> query_records_where_mandatory_field_missing >> has_missing_data

        has_missing_data >> rail.Label(
            "Yes") >> empty_has_missing_data_yes_task >> missing_fields_csv >> generate_download_link >> revert_to_draft
        has_missing_data >> rail.Label(
            "No") >> mandatory_fileds_query >> filter_raw_timeexport_data

        filter_raw_timeexport_data >> logging_no_of_records_exported >> logging_file_creation >>\
            format_raw_export_data >> final_export_data

        final_export_data >> create_json_payload >> upload_export_data_to_sftp >> \
            process_end_time >> logging_job_end_time >> log_file_data_to_csv >> \
            upload_log_data_to_sftp >> is_run_failed

        is_run_failed >> rail.Label(
            "No") >> is_run_skipped >> rail.Label(
            "No") >> send_success_email\
            >> log_to_sumo_valid_export

        is_run_failed >> rail.Label(
            "Yes") >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
