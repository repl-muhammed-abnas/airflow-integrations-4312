from datetime import timedelta
from pendulum import datetime, now
import rail
from airflow.models import Variable
from bearingpoint.timedata_export.utils import custom_methods, request_payload
from bearingpoint.timedata_export.tasks.time_export_task import time_data_export
from bearingpoint.timedata_export.tasks.update_time_export_status import cancel_time_export


OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

#pylint: disable=too-many-statements

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_process_export_dag_id,
        description="Bearingpoint Time Export process time export",
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        schedule_interval=config.daily_run_schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        def get_logging_details_callable():
            curr_time = now(tz=config.time_zone)
            return {
                **{
                    "time_export_run_type": "daily",
                    "todays_date": curr_time.strftime(custom_methods.EXPORT_DATE_FORMAT),
                    "timezone": config.time_zone,
                    "process_start_time": curr_time.strftime('%Y-%m-%dT%H:%M:%S'),
                    "export_file_time_stamp": curr_time.strftime(custom_methods.EXPORT_FILE_TIMESTAMP)
                },
                **custom_methods.get_time_export_name(config)
            }

        get_logging_details = rail.PythonOperator(
            task_id = "get_logging_details",
            python_callable=get_logging_details_callable
        )

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="create_raw_timeexport_data_collection"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="create_raw_timeexport_data_collection",
            end_task="finish"
        )

        time_export_download_script = rail.RepliconServiceOperator(
            task_id='time_export_download_script',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: custom_methods.get_timeexport_fileformat(
                config, response)
        )

        time_export_start, time_export_end, time_export_uri_task_id = time_data_export(
            group_id="time_export",
            generate_request=request_payload.get_create_time_data_export_batch_payload,
            get_export_name="{{result('get_logging_details').time_export_name}}",
            file_script_uri="time_export_download_script",
            retries=0
        )

        create_raw_timeexport_data_collection = rail.CreateCollectionOperator(
            task_id="create_raw_timeexport_data_collection",
            source="{{result('time_export.load_export')}}",
            name="raw_timeexport_data",
            columns={
                'TimeEntryID': 'timeentry_id',
                'ControllingArea': 'controlling_area',
                'UserCostCenter': 'cost_center',
                'BillingRate': 'billing_rate',
                'EmployeeID': 'employee_id',
                'WorkforceID': 'workforce_id',
                'TimeEntryDate': 'timeentry_date',
                'TaskName': 'taskname',
                'Hours': 'hours',
                'Comments': 'comments',
                'UserServiceCenter': 'service_center',
                'WorkLocation': 'work_location',
                'Onsite_Remote': 'onsite_remote',
                'ProjectCode': 'project_code',
                'BillingControlCategory': 'billing_control_category',
                'ProjectCategory': 'project_category',
                'TimeOffTypeName': 'time_off_type_name',
                'TimeOffTypeDescription': 'time_off_type_description',
                'LoginName': 'LoginName'
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
            subject='{{ get_company_key() }} | Replicon Time Data Export - No records to export - {{ result("get_logging_details").process_start_time }}',
            html_content="templates/emails/email_empty_export.html"
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
            task_id="empty_has_any_blank_emp_id_yes_task"
        )

        query_records_where_mandatory_field_missing = rail.QueryCollectionOperator(
            task_id="query_records_where_mandatory_field_missing",
            query="""SELECT * FROM raw_timeexport_data rtd WHERE
                    ((NULLIF(rtd.timeentry_date , '') IS NULL OR
                    NULLIF(rtd.employee_id , '') IS NULL OR
                    NULLIF(rtd.timeentry_id, '') IS NULL))
                """
        )

        has_missing_data = rail.IfOperator(
            task_id="has_missing_data",
            test="{{ result('query_records_where_mandatory_field_missing', 'length') > 0 }}",
            yes_task="empty_has_missing_data_yes_task",
            no_task="filter_raw_timeexport_data"
        )

        empty_has_missing_data_yes_task = rail.EmptyOperator(
            task_id = "empty_has_missing_data_yes_task"
        )

        missing_fields_csv = rail.WriteCSVFileOperator(
            task_id='missing_fields_csv',
            source="{{ result('query_records_where_mandatory_field_missing') or result('query_blank_employee_id_records') }}",
            header=['LoginName', 'EmployeeID', 'EntryDate', 'TimeEntryID'],
            row=lambda item:[
                item['LoginName'],
                item['employee_id'],
                item['timeentry_date'],
                item['timeentry_id'],
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{result('missing_fields_csv')}}",
            output_file_name="Invalid_TimeExport_records_{{dag_run_ecid()}}_.csv",
            expires_in_seconds=7*24*60*60
        )

        revert_to_draft, cancel_export = cancel_time_export("cancel_time_data_export_invalid_data", time_export_uri_task_id)

        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export - Invalid records found - {{ result("get_logging_details").process_start_time }}',
            html_content="templates/emails/email_invalid_records_in_export.html"
        )

        filter_raw_timeexport_data = rail.QueryCollectionOperator(
            task_id="filter_raw_timeexport_data",
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) as record_id, * FROM raw_timeexport_data
                """,
            name="timeexport_data",
        )

        has_any_records_to_export = rail.IfOperator(
            task_id = "has_any_records_to_export",
            test="{{result('filter_raw_timeexport_data', 'length') > 0 }}",
            yes_task = "trigger_post_to_api",
            no_task="send_no_valid_data_email"
        )

        send_no_valid_data_email =rail.EmailOperator(
            task_id='send_no_valid_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export - No valid records to export - {{ result("get_logging_details").process_start_time }}',
            html_content="templates/emails/email_no_valid_data_to_export.html"
        )

        update_export_name_with_no_valid_data = rail.RepliconServiceOperator(
            task_id="update_export_name_with_no_valid_data",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('time_export.get_export_uri')}}"
                },
                "name": "{{ result('get_logging_details').no_valid_data_time_export_name}}"
            },
        )

        trigger_post_to_api = rail.TriggerDagRunOperator(
            task_id = "trigger_post_to_api",
            trigger_dag_id=config.time_export_post_export_dag_id,
            conf = lambda: {
                **rail.result('get_logging_details'),
                'twb_numberofrecords': rail.result('create_raw_timeexport_data_collection', 'length')},
            retries=0,
            execution_timeout = timedelta(days=config.execution_timeout_days_for_posting)
        )

        wait_for_trigger_post_to_api = rail.WaitForDagRunsSensor(
            task_id = "wait_for_trigger_post_to_api",
            dag_runs="{{result('trigger_post_to_api')}}",
            retries=0,
            execution_timeout = timedelta(days=config.execution_timeout_days_for_posting)
        )

        gather_submit_to_sap_child_runs = rail.GatherResultsFromDagRunsOperator(
            task_id = "gather_submit_to_sap_child_runs",
            dag_runs="{{result('trigger_post_to_api')}}",
            dagrun_task_id="gather_child_run_ids",
            flatten=True
        )

        gather_errors_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id = "gather_errors_from_child",
            dag_runs="{{result('gather_submit_to_sap_child_runs')}}",
            dagrun_task_id="catch_errors",
            flatten=True
        )

        has_any_errors = rail.IfOperator(
            task_id = "has_any_errors",
            test = lambda : bool(rail.result("gather_errors_from_child")),
            yes_task="fail_dag_run",
            no_task="finish"
        )

        fail_dag_run = rail.FailOperator(
            task_id = "fail_dag_run",
            message="failure happened while updating time entries where replicon ID is missing for more details ref: `gather_errors_from_child` task "
        )

        finish = rail.EmptyOperator(
            task_id = "finish"
        )

        def can_log_exception_export_test():
            raw_data_count = rail.result('create_raw_timeexport_data_collection', 'length')
            filtered_raw_data_count = rail.result('filter_raw_timeexport_data', 'length')
            if not filtered_raw_data_count:
                return raw_data_count < 1
            return (raw_data_count < 1) or (filtered_raw_data_count < 1)


        can_log_exception_export = rail.IfOperator(
            task_id = "can_log_exception_export",
            test=can_log_exception_export_test,
            yes_task="log_export_to_sumo"
        )

        log_export_to_sumo = rail.SendToSumoOperator(
            task_id="log_export_to_sumo",
            data={
                'job_start_time': "{{ result('get_logging_details').process_start_time }}",
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'export_type': "{{ result('get_logging_details').time_export_run_type }}",
                'export_backup_filepath': config.timeexport_upload_backup_filepath,
                'twb_numberofrecords': "{{ result('create_raw_timeexport_data_collection', 'length')}}"
            },
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label("No") >> create_raw_timeexport_data_collection

        get_logging_details >> time_export_download_script >> time_export_start
        time_export_end >> can_run_batch_task 
        create_raw_timeexport_data_collection >> has_any_timeexport_data

        has_any_timeexport_data >> rail.Label("No") >> send_no_data_email >>\
            update_export_name_with_nodata >> finish

        has_any_timeexport_data >> rail.Label(
            "Yes") >> query_blank_employee_id_records
        query_blank_employee_id_records >> has_any_blank_emp_id >> rail.Label("Yes") >>\
        empty_has_any_blank_emp_id_yes_task >> missing_fields_csv
        revert_to_draft
        cancel_export >> send_invalid_records_email >> finish

        has_any_blank_emp_id >> rail.Label("No") >> query_records_where_mandatory_field_missing \
            >> has_missing_data >> rail.Label("Yes") >> empty_has_missing_data_yes_task >> missing_fields_csv >> generate_download_link >> revert_to_draft
        has_missing_data >> rail.Label("No") >> filter_raw_timeexport_data >> has_any_records_to_export\
            >> rail.Label("Yes") >> trigger_post_to_api >> wait_for_trigger_post_to_api >> gather_submit_to_sap_child_runs >> gather_errors_from_child
        gather_errors_from_child >> has_any_errors >> rail.Label("No") >>  finish
        has_any_errors >> rail.Label("Yes") >> fail_dag_run >> finish
        has_any_records_to_export >> rail.Label("No") >> send_no_valid_data_email \
            >> update_export_name_with_no_valid_data >> finish
        
        finish >> can_log_exception_export >> rail.Label("Yes") >> log_export_to_sumo
        
    return dag


rail.for_each_instance(create_main_dag)
