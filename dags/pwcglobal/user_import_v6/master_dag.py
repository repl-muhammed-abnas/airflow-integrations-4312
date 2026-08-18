from datetime import datetime, timedelta, timezone
import itertools
import json
import os
import rail
from rail.task_groups.batch_execution import batch_execution

from pwcglobal.user_import_v6.utils import custom_method, request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import_v6/config.py

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'PwCGlobal - User Import_Master_Process XML input file',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
        )

        is_xml = rail.IfOperator(
            task_id='is_xml',
            test='{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task='send_process_start_email',
        )

        send_process_start_email = rail.EmailOperator(
            task_id='send_process_start_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | New file placed for User import - {{ current_time() }}',
            html_content='File {{ result("new_file_sensor") | file_name }} received to be processed by the automation.',
        )

        list_ftp_files = rail.SFTPListFilesOperator(
            task_id='list_ftp_files',
            paths=[config.input_filepath]
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        catch_and_download_archive_file = rail.IfOperator(
            task_id='catch_and_download_archive_file',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            test='{{ "No such file" in get_error_message() }}',
            yes_task='download_archive_file',
            no_task='download_fail'
        )

        download_fail = rail.FailOperator(
            task_id='download_fail',
            message="{{ result('download_file') }}"
        )

        download_archive_file = rail.SFTPDownloadFileOperator(
            task_id='download_archive_file',
            remote_filepath=config.archive_filepath +
            "/{{ result('new_file_sensor') | file_name }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            trigger_rule='one_success',
            document="{{ result('download_file') or result('download_archive_file') }}"
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Person") | length > 0 }}',
            yes_task='create_user_collection',
            no_task='send_blank_payload_email',
        )

        # check Truncate all entries from PwC User File Processing Check lookup table
        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User import completed file processing is skipped - {{ current_time() }}',
            html_content="/templates/email_blank_payload.html",
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            source="{{ result('parse_xml') | xpath('Person') }}",
        )

        query_invalid_users = rail.QueryCollectionOperator(
            task_id='query_invalid_users',
            query="SELECT * FROM create_user_collection WHERE LoginName IS NULL"
        )

        has_invalid_users = rail.IfOperator(
            task_id='has_invalid_users',
            test='{{ result("query_invalid_users", "length") > 0 }}',
            yes_task='log_invalid_users',
            no_task='dummy_invalid_users'
        )

        dummy_invalid_users = rail.EmptyOperator(
            task_id='dummy_invalid_users'
        )

        log_invalid_users = rail.WriteLogOperator(
            task_id='log_invalid_users',
            message='User not processed due to following reason/s: GUID is not present.',
            items='{{ result("query_invalid_users") | load_all_records | to_json }}',
            severity='Exception',
            properties={
                'userpartyid': '{{item.EmployeeID}}',
                'username': '{{item.FirstName}} {{item.LastName}}',
                'status': 'Exception',
                'message': 'User not processed due to following reason/s: GUID is not present.',
                'legalentityid': '{{item.LegalEntity}}',
            }
        )

        query_valid_users = rail.QueryCollectionOperator(
            task_id='query_valid_users',
            query="SELECT * FROM create_user_collection WHERE LoginName IS NOT NULL"
        )

        has_valid_users = rail.IfOperator(
            task_id='has_valid_users',
            test='{{ result("query_valid_users", "length") > 0 }}',
            yes_task='query_distinct_schedule',
        )

        query_distinct_schedule = rail.QueryCollectionOperator(
            task_id='query_distinct_schedule',
            query="SELECT DISTINCT ScheduleType FROM query_valid_users WHERE ScheduleType IS NOT NULL"
        )

        query_distinct_country = rail.QueryCollectionOperator(
            task_id='query_distinct_country',
            query="SELECT DISTINCT Country FROM query_valid_users WHERE Country IS NOT NULL"
        )

        get_all_office_schedule = rail.RepliconServiceOperator(
            task_id='get_all_office_schedule',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules',
        )

        get_all_location = rail.RepliconServiceOperator(
            task_id='get_all_location',
            endpoint='/services/LocationService1.svc/GetAllLocations',
        )

        get_all_division = rail.RepliconServiceOperator(
            task_id='get_all_division',
            endpoint='/services/DivisionListService1.svc/GetData',
            data=request_payload.get_division_payload(),
            response_filter=custom_method.map_list_data
        )

        get_enabled_emp_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_emp_groups',
            endpoint='/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups',
        )

        get_dept_group = rail.RepliconServiceOperator(
            task_id='get_dept_group',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_dept_group_payload(),
            response_filter=custom_method.map_list_data
        )

        get_replicon_cost_centers = rail.RepliconServiceOperator(
            task_id='get_replicon_cost_centers',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.get_cost_center_group_payload(),
            response_filter=custom_method.map_list_data
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
        )

        get_all_approval_paths = rail.RepliconServiceOperator(
            task_id='get_all_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        get_all_timeoff_approval_paths = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_approval_paths',
            endpoint='/services/TimeOffApprovalService1.svc/GetAllApprovalPaths',
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id='get_all_timezones',
            endpoint='/services/InternationalizationService1.svc/GetAllTimeZones',
        )

        get_all_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_all_timeofftypes',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
        )

        get_all_permissionset = rail.RepliconServiceOperator(
            task_id='get_all_permissionset',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id='get_all_payrule_scripts',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        get_time_entry_approval_paths = rail.RepliconServiceOperator(
            task_id="get_time_entry_approval_paths",
            endpoint="/services/TimeEntryApprovalPathListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "1000",
                    "columnUris": [
                        "urn:replicon:time-entry-approval-path-list-column:time-entry-approval-path"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(lambda i: {
                "displayText": i["cells"][0]["textValue"],
                "uri": i["cells"][0]["uri"],
            }, response["rows"])) if response else null
        )

        get_all_customfields = rail.RepliconServiceOperator(
            task_id='get_all_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={'objectUri': 'urn:replicon:object-type:user'}
        )

        has_grade_customfield = rail.IfOperator(
            task_id='has_grade_customfield',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_customfields'), 'displayText', 'Grade', 'uri')),
            yes_task='get_grade_options',
            no_task='has_profilestatus_customfield'
        )

        get_grade_options = rail.RepliconServiceOperator(
            task_id='get_grade_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_customfields'), 'displayText', 'Grade', 'uri')
            }
        )

        has_profilestatus_customfield = rail.IfOperator(
            task_id='has_profilestatus_customfield',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_customfields'), 'displayText', 'Profile Status', 'uri')),
            yes_task='get_profilestatus_options',
            no_task='has_toil_customfield'
        )

        get_profilestatus_options = rail.RepliconServiceOperator(
            task_id='get_profilestatus_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_customfields'), 'displayText', 'Profile Status', 'uri')
            }
        )

        has_toil_customfield = rail.IfOperator(
            task_id='has_toil_customfield',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_customfields'), 'displayText', 'TOIL', 'uri')),
            yes_task='get_toil_options',
            no_task='process_office_and_location'
        )

        get_toil_options = rail.RepliconServiceOperator(
            task_id='get_toil_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_customfields'), 'displayText', 'TOIL', 'uri')
            }
        )

        process_office_and_location = rail.EmptyOperator(
            task_id='process_office_and_location'
        )

        create_office_schedule_collection = rail.CreateCollectionOperator(
            task_id="create_office_schedule_collection",
            name="replicon_office_schedule",
            source="{{ result('get_all_office_schedule') | to_json }}"
        )

        create_location_collection = rail.CreateCollectionOperator(
            task_id="create_location_collection",
            name="replicon_location",
            source="{{ result('get_all_location') | to_json }}"
        )

        query_new_scheduels = rail.QueryCollectionOperator(
            task_id='query_new_scheduels',
            query='''SELECT * FROM query_distinct_schedule
                    WHERE ScheduleType IS NOT NULL AND ScheduleType NOT IN
                    (SELECT DISTINCT Displaytext FROM replicon_office_schedule)'''
        )

        query_new_locations = rail.QueryCollectionOperator(
            task_id='query_new_locations',
            query='''SELECT * FROM query_distinct_country
                    WHERE Country IS NOT NULL AND Country NOT IN
                    (SELECT DISTINCT Displaytext FROM replicon_location)'''
        )

        process_new_scheduels = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_scheduels',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('query_new_scheduels'),
            trigger_dag_id=config.schedule_dag_id,
            conf={
                'scheduletype': '{{ item.ScheduleType }}'
            }
        )

        wait_for_process_new_scheduels = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_scheduels',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_scheduels") }}',
        )

        process_new_locations = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_locations',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('query_new_locations'),
            trigger_dag_id=config.location_dag_id,
            conf={
                'country': '{{ item.Country }}'
            }
        )

        wait_for_process_new_locations = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_locations',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_locations") }}',
        )

        get_updated_locations = rail.RepliconServiceOperator(
            task_id='get_updated_locations',
            endpoint='/services/LocationService1.svc/GetEnabledLocations',
        )

        get_updated_schedules = rail.RepliconServiceOperator(
            task_id='get_updated_schedules',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules',
        )

        can_use_report_batch = rail.IfOperator(
            task_id='can_use_report_batch',
            test=lambda: rail.result(
                "query_valid_users", "length") > config.report_process_size,
            yes_task='get_user_report_details',
            no_task='get_conf_uris'
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.user_report_name
        )

        create_user_report_generation_batch = rail.RepliconServiceOperator(
            task_id="create_user_report_generation_batch",
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_user_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]
            }
        )

        batchuri = "{{ result('create_user_report_generation_batch') }}"

        process_report_batch = batch_execution(
            group_id='execute_report_generation_batch',
            replicon_conn_id=config.replicon_conn_id,
            creation_task_id=create_user_report_generation_batch.task_id
        )

        payload = {
            "reportGenerationBatchUri": batchuri
        }

        get_report_batch_result = rail.RepliconServiceOperator(
            task_id="get_report_batch_result",
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data=payload
        )

        load_user_csv_data = rail.LoadCSVFileOperator(
            task_id="load_user_csv_data",
            document="{{ result('get_report_batch_result').reportGenerationResults[0].payload }}"
        )

        create_reportuser_data_collection = rail.CreateCollectionOperator(
            task_id="create_reportuser_data_collection",
            name="reportuserdata",
            source="{{ result('load_user_csv_data') }}"
        )

        load_all_report_users = rail.PythonOperator(
            task_id='load_all_report_users',
            python_callable=lambda: rail.load_all_records(
                rail.result('create_reportuser_data_collection'))
        )

        get_conf_uris = rail.PythonOperator(
            task_id='get_conf_uris',
            python_callable=request_payload.get_conf_uris
        )

        trigger_parallel_dagrun = rail.trigger_parallel_dagrun(
            task_id='trigger_parallel_dagrun',
            parallel_count=config.process_each_user_trigger_parallel_count,
            items=lambda: rail.result('query_valid_users'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_user_dag_id,
            conf=lambda item: request_payload.get_process_user_conf(item,config)
        )

        if_trigger_count= rail.IfOperator(
            task_id="if_trigger_count",
            trigger_rule ="all_done",
            test=lambda: bool(list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'trigger_parallel_dagrun_{x+1}') if rail.result(
                    f'trigger_parallel_dagrun_{x+1}') else [], range(config.process_each_user_trigger_parallel_count)))))),
            yes_task="process_users"
        )

        process_users = rail.PythonOperator(
            task_id='process_users',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'trigger_parallel_dagrun_{x+1}') if rail.result(
                    f'trigger_parallel_dagrun_{x+1}') else [], range(config.process_each_user_trigger_parallel_count)))))
        )

        gather_supervisor = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_supervisor',
            trigger_rule='none_failed_min_one_success',
            dag_runs="{{ result('process_users') }}",
            dagrun_task_id='get_supervisor_assignment',
            flatten=True,
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            trigger_rule='none_failed_min_one_success',
            dag_runs="{{ result('process_users') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        def get_child_failure_properties(**context):
            return {
                'failed_tasks': ', '.join(context.get('failed_task_ids', [])),
                'error_message': context.get('error_message', ''),
                'processing_status': 'Partial Success - Some child DAGs failed',
            }

        handle_child_failures = rail.WriteLogOperator(
            task_id='handle_child_failures',
            trigger_rule='one_failed',
            message='Some user processing child DAGs failed, but processing continues with successful records',
            severity='Warning',
            properties=get_child_failure_properties
        )

        process_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('gather_supervisor'),
            trigger_dag_id=config.supervisor_dag_id,
            conf={
                "useruri": "{{ item.useruri }}",
                "employeeid": "{{ item.employeeid }}",
                "firstname": "{{ item.firstname }}",
                "lastname": "{{ item.lastname }}",
                "legalentity": "{{ item.legalentity }}",
                "supervisor": "{{ item.supervisor }}",
                "supervisorlegalentityuri": "{{ item.supervisorlegalentityuri }}",
                'managerpermissionuri': "{{ result('get_all_permissionset') | find_first_by_attr_and_get_attr('displayText', 'Matrix/Team Manager', 'uri') }}",
                'log': '{{ get_master_log() }}'
            }
        )

        wait_for_process_supervisor = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_supervisor',
            trigger_rule='none_failed_min_one_success',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_supervisor") }}',
        )

        def get_failure_properties(**context):
            return {
                'failed_tasks': ', '.join(context.get('failed_task_ids', [])),
                'error_message': context.get('error_message', ''),
                'processing_status': 'Partial Success - Some supervisor DAGs failed',
            }
        
        handle_supervisor_failures = rail.WriteLogOperator(
            task_id='handle_supervisor_failures',
            trigger_rule='one_failed',
            message='Some supervisor processing child DAGs failed, but processing continues with successful records',
            severity='Warning',
            properties=get_failure_properties
        )

        was_new_file_processed = rail.IfOperator(
            task_id='was_new_file_processed',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='get_errored_master_logs'
        )

        get_errored_master_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_master_logs',
            properties={'status': 'Error'}
        )

        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ get_master_log() | load_all_records | to_json }}"
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_method.do_format_logs
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['Status'] == "ERROR", json.loads(rail.result('format_logs'))))), 'length')
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['Status'] == "EXCEPTION", json.loads(rail.result('format_logs'))))), 'length')
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id='write_xml_file',
            target='artifact',
            template_file='/templates/output_template.xml',
            dataset="{{ result('format_logs') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        upload_xml_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_xml_to_sftp',
            content="{{ result('write_xml_file') }}",
            remote_filepath=config.log_filepath +
            '/{{ result("new_file_sensor") | file_base }}_logs.xml'
        )

        can_upload_xml_to_secondary_filepath = rail.IfOperator(
            task_id='can_upload_xml_to_secondary_filepath',
            test=config.is_secondary_upload_required,
            yes_task='upload_xml_to_secondary_filepath',
            no_task='generate_download_link'
        )

        upload_xml_to_secondary_filepath = rail.SFTPUploadFileOperator(
            task_id='upload_xml_to_secondary_filepath',
            content="{{ result('write_xml_file') }}",
            remote_filepath=config.secondary_log_filepath +
            '/{{ result("new_file_sensor") | file_base }}_logs.xml'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_xml_file')}}",
            output_file_name='{{ result("new_file_sensor") | file_base }}_logs.xml',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User import - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="/templates/email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        def get_extra_info():
            # to handle all the failure scenarios
            if not rail.result('new_file_sensor'):
                return None
            if not rail.result('list_ftp_files').get(
                    config.input_filepath):
                return None
            file_name = os.path.basename(rail.result('new_file_sensor'))
            file_info = rail.find_first_by_attr_and_get_attr(rail.result('list_ftp_files').get(
                config.input_filepath), 'name', file_name)
            file_modified_datetime = datetime.strptime(
                file_info.get('modify'), '%Y%m%d%H%M%S')
            return {
                "file_name": file_name,
                "sftp_file_path": config.input_filepath,
                "file_size": file_info.get('size'),
                "record_count": rail.result('create_user_collection', 'length'),
                "file_modified_datetime": datetime(
                    file_modified_datetime.year,
                    file_modified_datetime.month,
                    file_modified_datetime.day,
                    file_modified_datetime.hour,
                    file_modified_datetime.minute,
                    file_modified_datetime.second,
                    tzinfo=timezone.utc).isoformat(),
            }

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=get_extra_info,
        )

        new_file_sensor >> is_xml >> rail.Label(
            'Yes') >> send_process_start_email >> list_ftp_files >> download_file

        download_file >> catch_and_download_archive_file
        catch_and_download_archive_file >> rail.Label(
            'No file found error') >> download_archive_file >> parse_xml
        catch_and_download_archive_file >> rail.Label(
            'Other Error') >> download_fail

        download_file >> parse_xml >> has_data >> rail.Label(
            'Yes') >> create_user_collection

        create_user_collection >> query_invalid_users >> has_invalid_users

        has_invalid_users >> rail.Label(
            'Yes') >> log_invalid_users >> dummy_invalid_users >> was_new_file_processed >> rail.Label("Yes") >> get_errored_master_logs

        has_invalid_users >> rail.Label(
            'No') >> dummy_invalid_users >> was_new_file_processed

        create_user_collection >> query_valid_users >> has_valid_users >> rail.Label('Yes') >> query_distinct_schedule >>\
            [query_distinct_country, get_all_office_schedule, get_all_location, get_all_division,
             get_enabled_emp_groups, get_dept_group, get_replicon_cost_centers, get_all_policy_sets, get_all_approval_paths, get_all_timeoff_approval_paths, get_all_holiday_calendars,
             get_all_timezones, get_all_timeofftypes, get_all_permissionset, get_all_customfields, get_all_payrule_scripts,
             get_time_entry_approval_paths] >> has_grade_customfield

        has_grade_customfield >> rail.Label(
            "Yes") >> get_grade_options >> has_profilestatus_customfield
        has_grade_customfield >> rail.Label(
            "No") >> has_profilestatus_customfield

        has_profilestatus_customfield >> rail.Label(
            'Yes') >> get_profilestatus_options >> has_toil_customfield
        has_profilestatus_customfield >> rail.Label(
            'No') >>\
            has_toil_customfield >> rail.Label(
                "Yes") >> get_toil_options >> process_office_and_location
        has_toil_customfield >> rail.Label("No") >>\
            process_office_and_location >> [
            create_office_schedule_collection, create_location_collection]

        create_office_schedule_collection >> query_new_scheduels >> process_new_scheduels >> wait_for_process_new_scheduels >> \
            get_updated_schedules >> can_use_report_batch
        create_location_collection >> query_new_locations >> process_new_locations >> wait_for_process_new_locations >> \
            get_updated_locations >> can_use_report_batch

        can_use_report_batch >> rail.Label('Yes') >> get_user_report_details >> create_user_report_generation_batch >> process_report_batch >> \
            get_report_batch_result >> load_user_csv_data >> create_reportuser_data_collection >> load_all_report_users >> get_conf_uris
        can_use_report_batch >> rail.Label('No') >> get_conf_uris

        get_conf_uris >> trigger_parallel_dagrun >> handle_child_failures >> process_users
        
        trigger_parallel_dagrun >> if_trigger_count >> rail.Label("Yes") >> process_users
        
        process_users >> gather_supervisor >> gather_logs >>\
        process_supervisor >> wait_for_process_supervisor >> handle_supervisor_failures >>\
        get_errored_master_logs >> load_master_log

        load_master_log >> format_logs >> get_errored_logs >> get_exception_logs >> \
            write_xml_file >> upload_xml_to_sftp >> can_upload_xml_to_secondary_filepath >> rail.Label(
                'Yes') >> upload_xml_to_secondary_filepath

        can_upload_xml_to_secondary_filepath >> rail.Label(
            'No') >> generate_download_link
        upload_xml_to_secondary_filepath >> generate_download_link >> send_import_complete_email >> log_to_sumo

        has_data >> rail.Label("No") >> send_blank_payload_email >> log_to_sumo
        # was_new_file_found has trigger_rule = 'all_done', so it will execute whenever download_file is done, regardless of whether it
        # succeeded, failed, or was skipped
        download_file >> rail.Label(
            "Always") >> was_new_file_found
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_dag)
