import rail
from datetime import timedelta
from airflow.models import Variable
from alvarezandmarsalholdings.time_export.timeoff_export_workday.utils import custom_methods, request_payload

null = None
OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.timeoff_export_to_workday_dag_id,
        description='Alvarez and Marsal Holdings Process Time Off Export to Workday Child dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        response_from_dag_var = rail.SetVariableOperator(
            task_id="response_from_dag_var",
            name='response_from_dag',
            append=False,
            value="Success"
        )

        time_export_download_script_uri = rail.RepliconServiceOperator(
            task_id='time_export_download_script_uri',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: custom_methods.get_timeexport_fileformat(
                config, response)
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda dag_run: request_payload.time_data_download_parameters(
                rail.result('time_export_download_script_uri'), dag_run),
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id='create_download_batch',
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('create_download_batch') }}"
            },
            data_handler=lambda response: response['downloadUrl']
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('get_download_url') }}"
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('download_export') }}",
        )

        create_time_export_data_collection = rail.CreateCollectionOperator(
            task_id="create_time_export_data_collection",
            source="{{result('load_export')}}",
            name="raw_time_export_data",
            columns={
                'Employee ID': 'employee_id',
                'Short Time Entry ID': 'short_time_entry_id',
                'Entry Date': 'entry_date',
                'Hours': 'hours',
                'Time Off Type Name': 'time_off_type_name',
                'Login Name': 'login_name'
            }
        )

        has_any_timeexport_data = rail.IfOperator(
            task_id="has_any_timeexport_data",
            test="{{result('create_time_export_data_collection', 'length') > 0 }}",
            yes_task="query_blank_employee_id_records",
            no_task="set_response_from_dag_no_data"
        )

        set_response_from_dag_no_data = rail.SetVariableOperator(
            task_id="set_response_from_dag_no_data",
            name='response_from_dag',
            append=False,
            value="No Data in export"
        )

        query_blank_employee_id_records = rail.QueryCollectionOperator(
            task_id="query_blank_employee_id_records",
            query="""SELECT DISTINCT login_name , employee_id FROM raw_time_export_data rted WHERE NULLIF(rted.employee_id, '') IS NULL"""
        )

        has_any_blank_emp_id = rail.IfOperator(
            task_id="has_any_blank_emp_id",
            test="{{ result('query_blank_employee_id_records', 'length') > 0}}",
            yes_task="empty_has_any_blank_emp_id_yes_task",
            no_task="query_raw_time_data_to_get_relevant_timeoff_entries"
        )

        empty_has_any_blank_emp_id_yes_task = rail.EmptyOperator(
            task_id="empty_has_any_blank_emp_id_yes_task"
        )

        missing_employeeid_csv = rail.WriteCSVFileOperator(
            task_id='missing_employeeid_csv',
            source="{{ result('query_blank_employee_id_records') }}",
            header=['LoginName', 'EmployeeID'],
            row=lambda item: [
                item['login_name'],
                item["employee_id"]
            ]
        )

        generate_download_link_missing_employeeid_records_csv = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link_missing_employeeid_records_csv',
            artifact_name="{{result('missing_employeeid_csv')}}",
            output_file_name=config.timeoff_export_workday_file_name_format + "Invalid_records_" +
            '{{dag_run.conf.process_start_time | replace("-", "") | replace(":", "")  }}' + '.csv',
            expires_in_seconds=7*24*60*60
        )

        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Replicon Time Off Export to Workday - Invalid records found - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_invalid_records_in_export.html"
        )

        set_response_from_dag_blank_employee_id_found = rail.SetVariableOperator(
            task_id="set_response_from_dag_blank_employee_id_found",
            name='response_from_dag',
            append=False,
            value="Blank employee id entry found, thus stopping the timeoff export"
        )

        query_raw_time_data_to_get_relevant_timeoff_entries = rail.QueryCollectionOperator(
            task_id='query_raw_time_data_to_get_relevant_timeoff_entries',
            query=f"""SELECT * FROM raw_time_export_data
                WHERE (
                    NULLIF(time_off_type_name,'') IS NOT NULL and
                    time_off_type_name NOT IN {config.time_off_types_to_exclude_from_export}
                )""",
            name='raw_timeoff_entries'
        )

        get_replicon_timeoff_type_name_paycode_list = rail.RepliconServiceOperator(
            task_id='get_replicon_timeoff_type_name_paycode_list',
            endpoint="/services/TimeOffTypeListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:time-off-type-list-column:name",
                    "urn:replicon:time-off-type-list-column:pay-code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: custom_methods.get_timeoff_type_names_paycodes(
                response)
        )

        create_replicon_timeoff_type_name_paycode_list_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_timeoff_type_name_paycode_list_collection",
            columns=['timeoff_type_name', 'timeoff_type_paycode'],
            source="{{ result ('get_replicon_timeoff_type_name_paycode_list') | to_json }}",
            name="replicon_timeoff_type_names_and_paycodes"
        )

        query_join_paycodes_in_raw_timeoff_data = rail.QueryCollectionOperator(
            task_id='query_join_paycodes_in_raw_timeoff_data',
            query="""SELECT
                    rtoe.*,rtonp.timeoff_type_paycode
                FROM
                    raw_timeoff_entries AS rtoe
                LEFT JOIN
                    replicon_timeoff_type_names_and_paycodes AS rtonp
                ON
                    rtoe.time_off_type_name = rtonp.timeoff_type_name """,
            name='timeoff_export_data_to_process'
        )

        final_data_for_export = rail.DataAdaptorOperator(
            task_id="final_data_for_export",
            source="{{result('query_join_paycodes_in_raw_timeoff_data')}}",
            columns=[
                'Worker',
                'Time_Off_Entry_ID',
                'Date',
                'Requested',
                'Time_Off_Type_Reference',
            ],
            data=custom_methods.final_export_data_callable
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id='write_xml_file',
            target='artifact',
            template_file='templates/export_schema/output_template.xml',
            dataset="{{ result('final_data_for_export') }}",
        )

        # Switches off posting to the Workday API endpoint at runtime; defaults to enabled.
        if_can_post_to_api_endpoint = rail.IfOperator(
            task_id='if_can_post_to_api_endpoint',
            test=lambda: Variable.get(
                config.can_post_to_api_endpoint, default_var='true').lower() == 'true',
            yes_task='upload_xml_to_sftp_backup',
            no_task='skip_posting'
        )

        skip_posting = rail.EmptyOperator(
            task_id='skip_posting'
        )

        upload_xml_to_sftp_backup = rail.SFTPUploadFileOperator(
            task_id='upload_xml_to_sftp_backup',
            content="{{ result('write_xml_file') }}",
            remote_filepath=config.timeoff_export_upload_backup_filepath + '/' + config.timeoff_export_workday_file_name_format +
            '{{dag_run.conf.process_start_time | replace("-", "") | replace(":", "")  }}' + '.xml',
        )

        is_trial_instance = rail.IfOperator(
            task_id="is_trial_instance",
            test=lambda: config.instance == "trial" or config.instance == "dev",
            yes_task="check_failure_is_post_to_endpoint_failed",
            no_task="http_submit_data_to_endpoint"
        )

        http_submit_data_to_endpoint = rail.HTTPUploadFileOperator(
            task_id='http_submit_data_to_endpoint',
            http_conn_id=config.http_conn_id_timeoff_workday,
            method='POST',
            content_type='application/xml',
            content="{{ result('write_xml_file') }}",
            retries=0,
            extra_options={
                'verify': False
            },
            execution_timeout=timedelta(hours=config.http_post_timeout_hours)
        )

        check_failure_is_post_to_endpoint_failed = rail.IfOperator(
            task_id="check_failure_is_post_to_endpoint_failed",
            trigger_rule="one_failed",
            test="{{ get_task_state('http_submit_data_to_endpoint') | lower == 'failed' }}",
            yes_task="send_posting_failed_email",
            no_task="set_response_from_dag_as_error"
        )

        send_posting_failed_email = rail.EmailOperator(
            task_id='send_posting_failed_email',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Replicon Time Off Export To Workday - Failed while posting to API endpoint - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/post_to_api_failed.html",
            params={
                'sftp_upload_path': config.timeoff_export_upload_backup_filepath
            }
        )

        set_response_from_dag_api_post_failure = rail.SetVariableOperator(
            task_id="set_response_from_dag_api_post_failure",
            name='response_from_dag',
            append=False,
            value="Time off export completed successfully, but integration failed to upload data to customer's API endpoint"
        )

        set_response_from_dag_as_error = rail.SetVariableOperator(
            task_id="set_response_from_dag_as_error",
            name='response_from_dag',
            append=False,
            value="Error in child dag - Time off export to Workday"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule="all_done",
            python_callable=lambda: rail.get_dag_run_var('response_from_dag')
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule="all_done",
            test="{{ get_error_message() | is_truthy }}",
            yes_task="fail_dag_due_to_error",
            no_task='log_export_to_sumo'
        )

        fail_dag_due_to_error = rail.FailOperator(
            task_id="fail_dag_due_to_error",
            message='Failure in processing time off export to workday - {{ get_error_message() }}'
        )

        log_export_to_sumo = rail.SendToSumoOperator(
            task_id="log_export_to_sumo",
            data={
                'job_start_time': "{{ dag_run.conf.process_start_time }}",
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'time_export_name': "{{ dag_run.conf.time_export_name if result('create_time_export_data_collection', 'length') > 0 else dag_run.conf.no_data_export_name}}",
                'export_filepath': config.timeoff_export_upload_backup_filepath,
                'twb_numberofrecords': "{{ result('create_time_export_data_collection', 'length')}}",
            },
            sumo_conn_id='sumologic-exportlogger'
        )

        response_from_dag_var >> time_export_download_script_uri
        time_export_download_script_uri >> create_download_batch >> execute_download_batch >> wait_for_download_batch \
            >> get_download_url >> download_export >> load_export >> create_time_export_data_collection >> has_any_timeexport_data

        has_any_timeexport_data >> rail.Label(
            "No") >> set_response_from_dag_no_data >> check_failure_is_post_to_endpoint_failed
        has_any_timeexport_data >> rail.Label(
            "Yes") >> query_blank_employee_id_records >> has_any_blank_emp_id

        has_any_blank_emp_id >> rail.Label(
            "No") >> query_raw_time_data_to_get_relevant_timeoff_entries
        has_any_blank_emp_id >> rail.Label(
            "Yes") >> empty_has_any_blank_emp_id_yes_task >> missing_employeeid_csv \
            >> generate_download_link_missing_employeeid_records_csv >> send_invalid_records_email \
            >> set_response_from_dag_blank_employee_id_found >> check_failure_is_post_to_endpoint_failed

        query_raw_time_data_to_get_relevant_timeoff_entries >> get_replicon_timeoff_type_name_paycode_list \
            >> create_replicon_timeoff_type_name_paycode_list_collection >> query_join_paycodes_in_raw_timeoff_data \
            >> final_data_for_export >> write_xml_file >> if_can_post_to_api_endpoint

        if_can_post_to_api_endpoint >> rail.Label(
            "Yes") >> upload_xml_to_sftp_backup >> is_trial_instance
        if_can_post_to_api_endpoint >> rail.Label(
            "No") >> skip_posting >> check_failure_is_post_to_endpoint_failed

        is_trial_instance >> rail.Label(
            "No") >> http_submit_data_to_endpoint >> check_failure_is_post_to_endpoint_failed
        is_trial_instance >> rail.Label(
            "Yes") >> check_failure_is_post_to_endpoint_failed

        check_failure_is_post_to_endpoint_failed >> rail.Label(
            "No") >> set_response_from_dag_as_error >> final_response_from_dag
        check_failure_is_post_to_endpoint_failed >> rail.Label(
            "Yes") >> send_posting_failed_email >> set_response_from_dag_api_post_failure >> final_response_from_dag

        final_response_from_dag >> can_fail_dag

        can_fail_dag >> rail.Label(
            "Yes") >> fail_dag_due_to_error
        can_fail_dag >> rail.Label(
            "No") >> log_export_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
