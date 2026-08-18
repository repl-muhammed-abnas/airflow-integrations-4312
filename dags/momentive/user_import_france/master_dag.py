# pylint: disable=too-many-statements line-too-long
import itertools
import json
from datetime import datetime, timedelta
from pendulum import datetime as dt
from momentive.user_import_france.utils import python_callable, request_payload
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.momentive_france_user_sync_master_dag_id,
        description=f'Momentive user import France - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2026, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        if_instance_trial = rail.IfOperator(
            task_id='if_instance_trial',
            test=lambda: bool('trial' in config.instance),
            yes_task='new_file_sensor_to_process',
            no_task='get_workdayreport_http_payload'
        )

        get_workdayreport_http_payload = rail.SimpleHttpOperator(
            task_id='get_workdayreport_http_payload',
            method='GET',
            http_conn_id=config.workday_report_http_conn_id,
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        workdayreport_json_load = rail.PythonOperator(
            task_id='workdayreport_json_load',
            python_callable=lambda: json.loads(
                rail.result('get_workdayreport_http_payload'))
        )

        if_report_entries_blank = rail.IfOperator(
            task_id='if_report_entries_blank',
            test='''{{ result('workdayreport_json_load') | is_falsy or result('workdayreport_json_load')['Report_Entry'] | length == 0}}''',
            yes_task="send_mail_no_change_records",
            no_task="get_write_csv_task_source",
        )

        new_file_sensor_to_process = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor_to_process',
            path=config.input_filepath_for_trial,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor_to_process") == "success" }}',
            yes_task="download_sftp_file",
            no_task="delete_dagrun"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        download_sftp_file = rail.SFTPDownloadFileOperator(
            task_id='download_sftp_file',
            remote_filepath="{{ result('new_file_sensor_to_process') }}"
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            existing_filename='{{ result("new_file_sensor_to_process") }}',
            new_filename=config.archive_filepath +
            "/Processed{{ result('new_file_sensor_to_process') | file_name }}_{{dag_run_ecid()}}"
        )

        parse_user_sync_csv = rail.LoadCSVFileOperator(
            task_id="parse_user_sync_csv",
            document='{{result("download_sftp_file")}}',
            delimiter=","
        )

        get_write_csv_task_source = rail.PythonOperator(
            task_id='get_write_csv_task_source',
            trigger_rule='one_success',
            python_callable=lambda: json.dumps(rail.result('workdayreport_json_load')['Report_Entry']) if rail.result(
                'workdayreport_json_load') else rail.result('parse_user_sync_csv')
        )

        log_todaysdate_2 = rail.PythonOperator(
            task_id='log_todaysdate_2',
            python_callable=lambda: datetime.now().strftime("%Y_%m_%d%H_%M_%S")
        )

        create_csv_lines_12 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_12',
            source="{{ result('get_write_csv_task_source') }}",
            header=['userid', 'workerreferenceemployeeid', 'emailaddress', 'firstname', 'lastname', 'workertype',
                    'effective_date_of_worker_type', 'exemptionstatus', 'cf_lrv_job_exempt_eff_date', 'gender', 'hiredate',
                    'terminationdate', 'active', 'function', 'function_change_effective_date', 'businesstitle',
                    'cf_lrv_business_title_change', 'fieldhr', 'managerid', 'effective_date_of_manager_change', 'work_shift',
                    'work_shift_change_effective_date', 'location', 'location_change_eff_date', 'country', 'date_of_birth',
                    'cf_lrv_manager_email', 'cf_lrv_manager_first_name', 'cf_lrv_manager_last_name', 'legalentity', 'worker_subType',
                    'cost_center', 'worker_cc_change_date', 'year_of_service', 'paygroup', 'japan_special_schedule_flag',
                    'continous_service_date', 'timeoff_service_date'],
            row=request_payload.user_import_data
        )

        if_record_count_less_than_1_15 = rail.IfOperator(
            task_id='if_record_count_less_than_1_15',
            test=lambda: bool(
                int(len(rail.load_all_records(rail.result('create_csv_lines_12')))) < 1),
            yes_task="send_mail_no_change_records",
            no_task="create_collection_create_list_from_csv",
        )

        send_mail_no_change_records = rail.EmailOperator(
            task_id='send_mail_no_change_records',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key() }} - France | User import completed- No change records found - {{ current_time() }} ''',
            html_content='''templates/no_delta_records.html''',
            params=None,
        )

        create_collection_create_list_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv',
            source="{{ result('create_csv_lines_12') }}",
            name="workdayuserdata"
        )

        query_blank_loginname_records = rail.QueryCollectionOperator(
            task_id="query_blank_loginname_records",
            query="""SELECT * FROM workdayuserdata WHERE (NULLIF(userid, '') IS NULL )""",
            name="blank_records"
        )

        logger_list = rail.CreateLogOperator(
            task_id="logger_list"
        )

        supervisor_logger_list = rail.CreateLogOperator(
            task_id="supervisor_logger_list"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('logger_list') }}",
            items='{{result("query_blank_loginname_records")}}',
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: request_payload.get_invalid_record(item)
        )

        # France processes records for the France legal entity only (no exemption filter).
        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            query=f"""SELECT * FROM workdayuserdata WHERE (NULLIF(userid, '') IS NOT NULL AND \
                legalentity == '{config.eligible_legal_entity}' )""",
            name="valid_records"
        )

        is_validated_records_present = rail.IfOperator(
            task_id="is_validated_records_present",
            test="{{ result('query_valid_records', 'length') > 0 }}",
            yes_task="get_all_enabled_divisions",
            no_task="send_mail_no_change_records"
        )

        get_all_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        get_enabled_service_centers = rail.RepliconServiceOperator(
            task_id='get_enabled_service_centers',
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        get_enabled_cost_centers = rail.RepliconServiceOperator(
            task_id='get_enabled_cost_centers',
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters",
        )

        get_department_list = rail.RepliconServiceOperator(
            task_id="get_department_list",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_enabled_dept,
            data_handler=python_callable.get_department_group_list
        )

        # --- Existing Replicon user lookup via the 'userreferencereport' report ---
        # France resolves existing users from a Replicon report (Workato step 16/41,
        # 'userreferencereport' -> userrinputdetails) instead of a per-user UserList search.
        # The report CSV is parsed and normalized into a list that process_each_user_payload
        # searches by login id to resolve useruri/status/startdate/enddate per user.
        get_userlist_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_userlist_report_details',
            report_name=config.report_name,
        )

        generate_userlist_report = rail.run_report2(
            group_id='generate_userlist_report',
            target='artifact',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_userlist_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_userlist_csv = rail.LoadCSVFileOperator(
            task_id='parse_userlist_csv',
            document="{{(result('generate_userlist_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}",
            delimiter=','
        )

        load_replicon_userlist = rail.PythonOperator(
            task_id='load_replicon_userlist',
            python_callable=lambda: python_callable.normalize_replicon_userlist(
                rail.load_all_records(rail.result('parse_userlist_csv')))
        )

        # Per-user routing now lives in process_each_user_dag and is fanned out in parallel
        # lanes. Each lane triggers one run per user and waits for it. The master pre-resolves
        # all uris and existing-user state into the conf so the per-user DAG has no API calls.
        process_each_user_parallel_dagrun = rail.trigger_parallel_dagrun(
            task_id='process_each_user_parallel_dagrun',
            items="{{ result('query_valid_records') }}",
            trigger_dag_id=config.momentive_france_user_sync_process_each_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.process_each_user_trigger_parallel_count_master,
            conf=request_payload.process_each_user_payload
        )

        # all_done: a per-user run that ends failed fails its lane; the log CSV and email must
        # still be produced. `or []` covers lanes that triggered nothing (delta < lane count).
        get_process_each_user_dag_ids = rail.PythonOperator(
            task_id='get_process_each_user_dag_ids',
            trigger_rule='all_done',
            python_callable=lambda: list(itertools.chain(
                *[rail.result(f'process_each_user_parallel_dagrun_{x+1}') or []
                  for x in range(config.process_each_user_trigger_parallel_count_master)])),
            show_return_value_in_logs=False
        )

        # Each per-user run owns its log artifact; collect them all before rendering the CSV.
        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_each_user_dag_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        # Supervisor fan-out: children deferred entries into the per-run supervisor log.
        trigger_supervisor_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_supervisor_assignment',
            retries=0,
            items=lambda: rail.load_all_records(rail.result('supervisor_logger_list')),
            trigger_dag_id=config.momentive_othercountries_user_sync_supervisor_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=request_payload.process_supervisor_mapper_data
        )

        wait_for_supervisor_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_supervisor_assignment") }}'
        )

        # Merge gathered per-user logs with master's own entries (validation skips) and
        # publish error_record_count / exception_record_count keys for the email.
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.do_format_logs,
            show_return_value_in_logs=False
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id='compose_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=['jobid', 'userid', 'username', 'action', 'status', 'details'],
            row=lambda item: [
                item.get('jobid', ''),
                item.get('userid', ''),
                item.get('username', ''),
                (item.get('action', '').split('|'))[0] if '|' in item.get('action', '') else item.get('action', ''),
                item.get('status', ''),
                item.get('details', '')
            ],
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content='''{{ result('compose_logs_csv') }}''',
            remote_filepath=config.log_filepath +
            '''/france_userimport_log_{{ result('log_todaysdate_2') }}.csv''',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs_csv')}}",
            output_file_name='''france_userimport_log_{{ result('log_todaysdate_2') }}.csv''',
            expires_in_seconds=7*24*60*60,
        )

        if_log_upload_successful = rail.IfOperator(
            task_id='if_log_upload_successful',
            trigger_rule='all_done',
            test='{{ get_task_state("upload_logs_to_sftp") == "success" }}',
            yes_task='send_import_complete_email',
            no_task='send_alert_mail_log_upload_unsuccessful'
        )

        send_alert_mail_log_upload_unsuccessful = rail.EmailOperator(
            task_id='send_alert_mail_log_upload_unsuccessful',
            to='{{ var.value.dagrun_failure_alert_email }}',
            subject='''{{get_company_key() }} - France |  Failed while uploading User import Logs to SFTP  - {{ current_time() }} ''',
            html_content='''templates/log_upload_failure.html''',
            params=None,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.bcc_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " - France | User import" }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    {{" "}}completed with errors \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        {{" "}}completed with Exceptions \
                    {%- else -%} \
                        {{" "}}completed successfully \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S") }}',
            html_content="templates/import_complete_mail.html",
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        if_instance_trial >> rail.Label(
            'No') >> get_workdayreport_http_payload >> workdayreport_json_load >> if_report_entries_blank

        if_report_entries_blank >> rail.Label('No') >> get_write_csv_task_source
        if_report_entries_blank >> rail.Label('Yes') >> send_mail_no_change_records >> finish

        if_instance_trial >> rail.Label('Yes') >> new_file_sensor_to_process

        new_file_sensor_to_process >> was_new_file_found

        was_new_file_found >> rail.Label('No') >> delete_dagrun
        was_new_file_found >> rail.Label(
            'Yes') >> download_sftp_file >> parse_user_sync_csv >> get_write_csv_task_source
        download_sftp_file >> archive_input_file

        get_write_csv_task_source >> log_todaysdate_2 >> create_csv_lines_12

        create_csv_lines_12 >> if_record_count_less_than_1_15

        if_record_count_less_than_1_15 >> rail.Label('Yes') >> send_mail_no_change_records >> finish
        if_record_count_less_than_1_15 >> rail.Label('No') >> create_collection_create_list_from_csv \
            >> query_blank_loginname_records >> logger_list >> supervisor_logger_list \
            >> log_invalid_records \
            >> query_valid_records >> is_validated_records_present

        is_validated_records_present >> rail.Label('No') >> send_mail_no_change_records >> finish

        is_validated_records_present >> rail.Label('Yes') >> get_all_enabled_divisions >> get_enabled_service_centers >> \
            get_enabled_cost_centers >> get_department_list >> get_userlist_report_details >> generate_userlist_report >> \
            parse_userlist_csv >> load_replicon_userlist >> \
            process_each_user_parallel_dagrun >> get_process_each_user_dag_ids >> gather_user_logs >> \
            trigger_supervisor_assignment >> wait_for_supervisor_assignment >> format_logs >> compose_logs_csv

        compose_logs_csv >> upload_logs_to_sftp >> generate_download_link >> if_log_upload_successful

        if_log_upload_successful >> rail.Label('Yes') >> send_import_complete_email >> finish
        if_log_upload_successful >> rail.Label(
            'No') >> send_alert_mail_log_upload_unsuccessful >> finish

    return dag


rail.for_each_instance(create_dag)
