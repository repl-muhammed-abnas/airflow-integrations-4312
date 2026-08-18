from datetime import timedelta
from os import path
import rail
from rail.filters import split
from technicolorg3.user_import.utils import request_payload
from technicolorg3.user_import.utils.response_filter import get_required_usercustom_udfs, map_replicon_groups, page_handler
from technicolorg3.user_import.utils.python_callable_method import get_archive_file_name, write_skipped_user, get_all_userlogs


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


# pylint:disable = too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_master_{config.instance}',
        description=f'Technicolor_User Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject="{{ get_company_key() }} | User import - File processing is skipped - {{ current_time('%H%M%S') }}",
            html_content='templates/email/bad_file_format.html'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        load_user_data = rail.LoadCSVFileOperator(
            task_id='load_user_data',
            document="{{ result('download_file') }}"
        )

        create_raw_data_collection = rail.CreateCollectionOperator(
            task_id='create_raw_data_collection',
            source="{{ result('load_user_data') }}",
            name='rawdata',
            columns=request_payload.get_columns()
        )

        has_user_data = rail.IfOperator(
            task_id='has_user_data',
            test="{{ result('create_raw_data_collection', 'length') > 0 }}",
            yes_task='strip_csv_data',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject="{{ get_company_key() }} | User import - no records in file - {{ current_time('%d%m%Y%H%M%S') }}",
            html_content='templates/email/blank_payload.html'
        )

        def get_headers():
            columns = request_payload.get_columns()
            return list(columns.values()) + ['departmentgroup', 'code1', 'code2', 'code3', 'code4', 'servicecenter', 'location']

        strip_csv_data = rail.WriteCSVFileOperator(
            task_id='strip_csv_data',
            source="{{ result('create_raw_data_collection') }}",
            header=get_headers,
            row=request_payload.get_row_data
        )

        create_input_file_collection = rail.CreateCollectionOperator(
            task_id='create_input_file_collection',
            source="{{ result('strip_csv_data') }}",
            name='inputfile'
        )

        process_groups = rail.TriggerDagRunOperator(
            task_id='process_groups',
            retries=0,
            trigger_dag_id=f'technicolorg3_user_import_child_groups_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_process_groups = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_groups',
            dag_runs="{{ result('process_groups') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_udfs = rail.TriggerDagRunOperator(
            task_id='process_udfs',
            retries=0,
            trigger_dag_id=f'technicolorg3_user_import_child_udfs_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_process_udfs = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_udfs',
            dag_runs="{{ result('process_udfs') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_skipped_users = rail.QueryCollectionOperator(
            task_id='get_skipped_users',
            query="""SELECT * FROM inputfile WHERE
                    (NULLIF(globalid, '') IS NULL OR
                    NULLIF(employeestatus, '') IS NULL OR
                    LOWER(employeestatus) != 'active')"""
        )

        is_skipped_users = rail.IfOperator(
            task_id='is_skipped_users',
            test="{{ result('get_skipped_users', 'length') > 0 }}",
            yes_task='create_skippeduser_log',
            no_task='get_valid_users'
        )

        create_skippeduser_log = rail.CreateLogOperator(
            task_id='create_skippeduser_log'
        )

        write_log_skipped_users = rail.WriteLogOperator(
            task_id='write_log_skipped_users',
            log="{{ result('create_skippeduser_log') }}",
            severity='Skipped',
            message='Skipped Users',
            items="{{ result('get_skipped_users') }}",
            properties=write_skipped_user
        )

        get_valid_users = rail.QueryCollectionOperator(
            task_id='get_valid_users',
            query="""SELECT * FROM inputfile WHERE
                    (NULLIF(globalid, '') IS NOT NULL AND
                    LOWER(employeestatus) = 'active')""",
            name='validatedinputlist'
        )

        get_disableduser_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_disableduser_report_details',
            report_name=config.disable_user_reportname
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='disableduser_report_generation',
            report_params={
                'reportParameters': [
                    {
                        'reportUri': "{{ result('get_disableduser_report_details').uri }}",
                        'filterValues': [],
                        'outputFormatUri': 'urn:replicon:report-output-format-option:csv'
                    }
                ]
            },
            wait_timeout=3600
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test=lambda: bool(rail.result('disableduser_report_generation.get_report_result')[
                'reportGenerationResults'][0]['error']) or not rail.result('disableduser_report_generation.get_report_result', 'has_data'),
            yes_task='fail_disableduser_report_generation',
            no_task='get_requireduser_customfields'
        )

        fail_disableduser_report_generation = rail.FailOperator(
            task_id='fail_disableduser_report_generation',
            message='Userlist for disabling User not available in Replicon'
        )

        get_requireduser_customfields = rail.RepliconServiceOperator(
            task_id='get_requireduser_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={'objectUri': 'urn:replicon:object-type:user'},
            data_handler=get_required_usercustom_udfs
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('disableduser_report_generation.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_repliconusers_list = rail.CreateCollectionOperator(
            task_id='create_repliconusers_list',
            source="{{ result('load_report_data') }}",
            name='userlistfromreplicon',
            columns={
                'User Name': 'username',
                'Employee ID': 'employeeid',
                'User Status': 'enabled',
                'useruri': 'useruri',
                'User Start Date': 'startdate',
                'Admin Modified': 'adminmodified',
                'Login Name': 'loginname'
            }
        )

        query_validatedrepliconusers_list = rail.QueryCollectionOperator(
            task_id='query_validatedrepliconusers_list',
            query="""SELECT * FROM userlistfromreplicon WHERE NULLIF(employeeid,'') IS NOT NULL""",
            name='validatedrepliconusers'
        )

        query_enabledusers_list = rail.QueryCollectionOperator(
            task_id='query_enabledusers_list',
            query="""SELECT * FROM userlistfromreplicon WHERE enabled = 'Enabled' AND adminmodified = 'No'""",
            name='enabledusers'
        )

        query_userstodisable_list = rail.QueryCollectionOperator(
            task_id='query_userstodisable_list',
            query="""SELECT * FROM enabledusers WHERE (LOWER(employeeid) NOT IN
                    (SELECT LOWER(globalid) FROM validatedinputlist)) AND enabled = 'Enabled' AND adminmodified = 'No'"""
        )

        is_userstodisabled_within_threshold = rail.IfOperator(
            task_id='is_userstodisabled_within_threshold',
            test=lambda: rail.result(
                'query_userstodisable_list', 'length') <= config.disable_user_threshold,
            yes_task='trigger_disableuser_child',
            no_task='send_disableuser_exception_email'
        )

        send_disableuser_exception_email = rail.EmailOperator(
            task_id='send_disableuser_exception_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | User import has been skipped - {{ current_time('%H%M%S') }}",
            html_content='templates/email/disableuser_exception.html',
            params={
                'disable_threshold': config.disable_user_threshold
            }
        )

        trigger_disableuser_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_disableuser_child',
            retries=0,
            items="{{ result('query_userstodisable_list') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_child_disableuser_{config.instance}',
            conf=lambda item: {
                **{k: v for k, v in item.items() if k in ('useruri', 'startdate', 'employeeid')},
                **{
                    'userloginname': item['loginname'],
                    'useruri': item['useruri'],
                    'username': f"{item['username'].split(',')[-1].strip()} {item['username'].split(',')[0].strip()}"
                }
            }
        )

        wait_for_disableuser_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_disableuser_child',
            dag_runs="{{ result('trigger_disableuser_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_disableuser_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_disableuser_child_logs',
            dag_runs="{{ result('trigger_disableuser_child') }}",
            dagrun_task_id='create_user_log',
            flatten=True
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='get_archive_filename',
            no_task='delete_this_dagrun'
        )

        get_archive_filename = rail.PythonOperator(
            task_id='get_archive_filename',
            python_callable=get_archive_file_name,
            op_args=["{{ get_task_state('send_bad_file_format_email') }}",
                     "{{ get_task_state('send_disableuser_exception_email') }}"]
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/{{ result('get_archive_filename') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_referencejobcode_dropdown = rail.RepliconServiceOperator(
            task_id='get_referencejobcode_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_requireduser_customfields').referencejobcode_uri }}"}
        )

        get_referencejobtitle_dropdown = rail.RepliconServiceOperator(
            task_id='get_referencejobtitle_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_requireduser_customfields').referencejobtitle_uri }}"}
        )

        get_department_dropdown = rail.RepliconServiceOperator(
            task_id='get_department_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_requireduser_customfields').departmentudf_uri }}"}
        )

        get_jobcategory_dropdown = rail.RepliconServiceOperator(
            task_id='get_jobcategory_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_requireduser_customfields').jobcategoryudf_uri }}"}
        )

        get_costcenter_details = rail.RepliconServicePageOperator(
            task_id='get_costcenter_details',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=lambda: request_payload.get_replicon_groups_list(
                'costcenter'),
            page_handler=page_handler,
            all_result_data_handler=lambda response: map_replicon_groups(
                response, 'costcenter', '/')
        )

        get_departmentgroup_details = rail.RepliconServicePageOperator(
            task_id='get_departmentgroup_details',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=lambda: request_payload.get_replicon_groups_list(
                'department'),
            page_handler=page_handler,
            all_result_data_handler=lambda response: map_replicon_groups(
                response, 'department', '/')
        )

        get_servicecenter_details = rail.RepliconServicePageOperator(
            task_id='get_servicecenter_details',
            endpoint='/services/ServiceCenterListService1.svc/GetData',
            data=lambda: request_payload.get_replicon_groups_list(
                'servicecenter'),
            page_handler=page_handler,
            all_result_data_handler=lambda response: map_replicon_groups(
                response, 'servicecenter', '/')
        )

        get_location_details = rail.RepliconServicePageOperator(
            task_id='get_location_details',
            endpoint='/services/LocationListService1.svc/GetData',
            data=lambda: request_payload.get_replicon_groups_list('location'),
            page_handler=page_handler,
            all_result_data_handler=lambda response: map_replicon_groups(
                response, 'location', '/')
        )

        get_division_details = rail.RepliconServicePageOperator(
            task_id='get_division_details',
            endpoint='/services/DivisionListService1.svc/GetData',
            data=lambda: request_payload.get_replicon_groups_list('division'),
            page_handler=page_handler,
            all_result_data_handler=lambda response: map_replicon_groups(
                response, 'division', '/')
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        get_newusers_to_process = rail.QueryCollectionOperator(
            task_id='get_newusers_to_process',
            query="""SELECT * FROM validatedinputlist WHERE LOWER(globalid) NOT IN
                    (SELECT DISTINCT LOWER(employeeid) FROM validatedrepliconusers)"""
        )

        create_supervisorlog = rail.CreateLogOperator(
            task_id='create_supervisorlog'
        )

        get_updateusers_to_process = rail.QueryCollectionOperator(
            task_id='get_updateusers_to_process',
            query="""SELECT * FROM validatedinputlist WHERE LOWER(globalid) IN
                    (SELECT DISTINCT LOWER(employeeid) FROM validatedrepliconusers)"""
        )

        is_addusers_present = rail.IfOperator(
            task_id='is_addusers_present',
            test="{{ result('get_newusers_to_process', 'length') > 0 }}",
            yes_task='trigger_adduser_child',
            no_task='list_reference_files'
        )

        trigger_adduser_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_adduser_child',
            retries=0,
            items="{{ result('get_newusers_to_process') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_child_adduser_{config.instance}',
            conf=request_payload.get_processuser_conf
        )

        wait_for_adduser_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_adduser_child',
            dag_runs="{{ result('trigger_adduser_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_adduser_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_adduser_child_logs',
            dag_runs="{{ result('trigger_adduser_child') }}",
            dagrun_task_id='create_user_log',
            flatten=True
        )

        list_reference_files = rail.SFTPListFilesOperator(
            task_id='list_reference_files',
            paths=[config.reference_filepath]
        )

        is_updateusers_present = rail.IfOperator(
            task_id='is_updateusers_present',
            test="{{ result('get_updateusers_to_process', 'length') > 0 }}",
            yes_task='process_referencefile',
            no_task='get_supervisorcheck_queued_logs'
        )

        process_referencefile = rail.EmptyOperator(
            task_id='process_referencefile'
        )

        should_use_referencefile = rail.IfOperator(
            task_id='should_use_referencefile',
            test=lambda: config.use_reference_file and bool(rail.result('list_reference_files').get(
                config.reference_filepath)),
            yes_task='trigger_referencefile_downloadchild',
            no_task='trigger_updateuser_child'
        )

        trigger_referencefile_downloadchild = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_referencefile_downloadchild',
            retries=0,
            items=lambda: rail.result('list_reference_files')[
                config.reference_filepath],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_child_referencefile_{config.instance}',
            conf=lambda item: {
                'reference_file': f"{config.reference_filepath}/{item['name']}",
                'action': 'download'
            }
        )

        wait_for_referencefile_downloadchild = rail.WaitForDagRunsSensor(
            task_id='wait_for_referencefile_downloadchild',
            dag_runs="{{ result('trigger_referencefile_downloadchild') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_userreference_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_userreference_data',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_referencefile_downloadchild') }}",
            dagrun_task_id='create_userreference_data',
            flatten=True
        )

        create_userreference_data_collection = rail.CreateCollectionOperator(
            task_id='create_userreference_data_collection',
            name='userreferencedata',
            source=lambda: rail.result('gather_userreference_data')
        )

        get_unchanged_records_collection = rail.QueryCollectionOperator(
            task_id='get_unchanged_records_collection',
            query="""SELECT * FROM get_updateusers_to_process
                    WHERE encoded IN (SELECT DISTINCT encoded FROM userreferencedata)"""
        )

        is_unchanged_records_present = rail.IfOperator(
            task_id='is_unchanged_records_present',
            test="{{ result('get_unchanged_records_collection', 'length') > 0 }}",
            yes_task='create_unchangedrecords_log',
            no_task='get_changed_records_collection'
        )

        create_unchangedrecords_log = rail.CreateLogOperator(
            task_id='create_unchangedrecords_log'
        )

        write_unchanged_records_log = rail.WriteLogOperator(
            task_id='write_unchanged_records_log',
            severity='Skipped',
            log="{{ result('create_unchangedrecords_log') }}",
            items="{{ result('get_unchanged_records_collection') }}",
            message='No change in user record',
            properties={
                'globalid': '{{ item.globalid }}',
                'action': 'Update',
                'status': 'Skipped',
                'details': 'No change in user record',
                'username': '{{ item.firstname }} {{ item.lastname }}',
                'new_location': 'No',
                'location': '{{ item.country }}/{{ item.worklocation }}'
            }
        )

        get_changed_records_collection = rail.QueryCollectionOperator(
            task_id='get_changed_records_collection',
            query="""SELECT * FROM get_updateusers_to_process
                    WHERE encoded NOT IN (SELECT DISTINCT encoded FROM userreferencedata)"""
        )

        is_changed_records_present = rail.IfOperator(
            task_id='is_changed_records_present',
            test="{{ result('get_changed_records_collection', 'length') > 0 }}",
            yes_task='trigger_updateuser_child',
            no_task='get_supervisorcheck_queued_logs'
        )

        trigger_updateuser_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_updateuser_child',
            retries=0,
            items=lambda: rail.result('get_changed_records_collection') or rail.result(
                'get_updateusers_to_process'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_child_updateuser_{config.instance}',
            conf=request_payload.get_processuser_conf
        )

        wait_for_updateuser_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_updateuser_child',
            dag_runs="{{ result('trigger_updateuser_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_updateuser_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_updateuser_child_logs',
            dag_runs="{{ result('trigger_updateuser_child') }}",
            dagrun_task_id='create_user_log',
            flatten=True
        )

        get_supervisorcheck_queued_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_queued_logs',
            log="{{ result('create_supervisorlog') }}",
            severity='Queued',
            remove_filtered_entries=True
        )

        is_supervisorcheck_queued_logs = rail.IfOperator(
            task_id='is_supervisorcheck_queued_logs',
            test="{{ result('get_supervisorcheck_queued_logs', 'length') > 0 }}",
            yes_task='process_supervisor_child_dag',
            no_task='process_logs'
        )

        process_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child_dag',
            retries=0,
            items="{{ result('get_supervisorcheck_queued_logs') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_supervisor_child_{config.instance}',
            conf=lambda item: {
                **dict(item['properties'].items()),
                'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_permissionsets'), 'displayText', 'Supervisor', 'uri'),
                'supervisor_log': rail.result('create_supervisorlog')
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        process_logs = rail.TriggerDagRunOperator(
            task_id='process_logs',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_child_log_{config.instance}',
            conf=lambda: {
                'user_logs': get_all_userlogs(),
                'time': rail.result('get_requireduser_customfields')['time'],
                'filename': split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0],
                'inputfilesize': rail.result('create_raw_data_collection', 'length')
            }
        )

        has_reference_files_archive = rail.IfOperator(
            task_id='has_reference_files_archive',
            test=lambda: bool(rail.result('list_reference_files').get(
                config.reference_filepath)),
            yes_task='trigger_referencefile_archive_child',
            no_task='upload_referencefile_to_sftp'
        )

        trigger_referencefile_archive_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_referencefile_archive_child',
            retries=0,
            items=lambda: rail.result('list_reference_files')[
                config.reference_filepath],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_child_referencefile_{config.instance}',
            conf=lambda item: {
                'reference_file': f"{config.reference_filepath}/{item['name']}",
                'action': 'archive'
            }
        )

        wait_for_referencefile_archive_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_referencefile_archive_child',
            dag_runs="{{ result('trigger_referencefile_archive_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        upload_referencefile_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_referencefile_to_sftp',
            content="{{ result('strip_csv_data') }}",
            remote_filepath=config.reference_filepath +
            "/Ref_{{ result('new_file_sensor') | file_base }}_{{ current_time('%d%m%Y%H%M%S') }}.csv"
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_dag',
            no_task='process_logtosumo'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="{{ get_error_message() }}"
        )

        process_logtosumo = rail.EmptyOperator(
            task_id='process_logtosumo'
        )

        check_if_new_file_found = rail.IfOperator(
            task_id='check_if_new_file_found',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='dagrun_log_to_sumo'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'filename ': "{{ result('new_file_sensor') | file_name }}",
                'inputfilesize': "{{ result('create_raw_data_collection', 'length') if result('create_raw_data_collection') else 'nil' }}",
                'skipped_invalidation_records': "{{ result('get_skipped_users', 'length') if result('get_skipped_users') else 'nil' }}",
                'Add': "{{ result('get_newusers_to_process', 'length') if result('get_newusers_to_process') else 'nil' }}",
                'Update': "{{ result('get_updateusers_to_process', 'length') if result('get_updateusers_to_process') else 'nil' }}",
                'Disable': "{{ result('query_userstodisable_list', 'length') if result('query_userstodisable_list') else 'nil' }}",
                'Users_in_Replicon': "{{ result('query_validatedrepliconusers_list', 'length') }}"
            }
        )

        new_file_sensor >> is_csv

        is_csv >> rail.Label(
            'Yes') >> download_file

        is_csv >> rail.Label(
            'No') >> send_bad_file_format_email

        download_file >> load_user_data >> create_raw_data_collection >> has_user_data

        has_user_data >> rail.Label(
            'No') >> send_blank_payload_email

        has_user_data >> rail.Label(
            'Yes') >> strip_csv_data >> create_input_file_collection >> process_groups >> wait_for_process_groups >> \
            process_udfs >> wait_for_process_udfs >> get_skipped_users >> is_skipped_users

        is_skipped_users >> rail.Label(
            'Yes') >> create_skippeduser_log >> write_log_skipped_users >> \
            get_valid_users

        is_skipped_users >> rail.Label(
            'No') >> get_valid_users

        get_valid_users >> get_disableduser_report_details >> run_report_group_entry

        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label(
            'Yes') >> fail_disableduser_report_generation

        is_report_failed >> rail.Label(
            'No') >> get_requireduser_customfields >> load_report_data >> create_repliconusers_list >> \
            query_validatedrepliconusers_list >> query_enabledusers_list >> query_userstodisable_list >> \
            is_userstodisabled_within_threshold

        is_userstodisabled_within_threshold >> rail.Label(
            'No') >> send_disableuser_exception_email

        send_disableuser_exception_email >> rail.Label(
            'Always') >> was_new_file_found

        is_userstodisabled_within_threshold >> rail.Label(
            'Yes') >> trigger_disableuser_child

        trigger_disableuser_child >> rail.Label(
            'Always') >> was_new_file_found

        was_new_file_found >> rail.Label(
            'Yes') >> get_archive_filename >> archive_file

        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun

        trigger_disableuser_child >> wait_for_disableuser_child >> gather_disableuser_child_logs >> \
            [get_referencejobcode_dropdown, get_referencejobtitle_dropdown, get_department_dropdown, get_jobcategory_dropdown,
             get_costcenter_details, get_departmentgroup_details, get_servicecenter_details, get_location_details,
             get_division_details, get_all_permissionsets, get_newusers_to_process, get_updateusers_to_process,
             create_supervisorlog] >> is_addusers_present

        is_addusers_present >> rail.Label(
            'Yes') >> trigger_adduser_child >> wait_for_adduser_child >> gather_adduser_child_logs >> \
            list_reference_files

        is_addusers_present >> rail.Label(
            'No') >> list_reference_files

        list_reference_files >> is_updateusers_present

        is_updateusers_present >> rail.Label(
            'Yes') >> process_referencefile >> should_use_referencefile

        should_use_referencefile >> rail.Label(
            'Yes') >> trigger_referencefile_downloadchild >> wait_for_referencefile_downloadchild >> \
            gather_userreference_data >> create_userreference_data_collection >> get_unchanged_records_collection >> \
            is_unchanged_records_present

        is_unchanged_records_present >> rail.Label(
            'Yes') >> create_unchangedrecords_log >> write_unchanged_records_log >> \
            get_changed_records_collection

        is_unchanged_records_present >> rail.Label(
            'No') >> get_changed_records_collection

        get_changed_records_collection >> is_changed_records_present

        is_changed_records_present >> rail.Label(
            'Yes') >> trigger_updateuser_child

        should_use_referencefile >> rail.Label(
            'No') >> trigger_updateuser_child

        trigger_updateuser_child >> wait_for_updateuser_child >> gather_updateuser_child_logs >> \
            get_supervisorcheck_queued_logs

        is_changed_records_present >> rail.Label(
            'No') >> get_supervisorcheck_queued_logs

        is_updateusers_present >> rail.Label(
            'No') >> get_supervisorcheck_queued_logs

        get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs

        is_supervisorcheck_queued_logs >> rail.Label(
            'Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> process_logs

        is_supervisorcheck_queued_logs >> rail.Label(
            'No') >> process_logs

        process_logs >> has_reference_files_archive

        has_reference_files_archive >> rail.Label(
            'Yes') >> trigger_referencefile_archive_child >> wait_for_referencefile_archive_child >> \
            upload_referencefile_to_sftp

        has_reference_files_archive >> rail.Label(
            'No') >> upload_referencefile_to_sftp

        upload_referencefile_to_sftp >> rail.Label(
            'Always') >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        should_fail_dag >> rail.Label(
            'No') >> process_logtosumo >> check_if_new_file_found >> rail.Label(
                'Yes') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_main_dag)
