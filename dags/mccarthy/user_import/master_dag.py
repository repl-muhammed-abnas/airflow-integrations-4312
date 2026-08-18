from datetime import timedelta, datetime
from os import path
import itertools
import rail
from rail.filters import split


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_master_{config.instance}',
        description=f'LIVE | Mccarthy_User_Import_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_correct_file = rail.IfOperator(
            task_id='is_correct_file',
            test="{{ result('new_file_sensor') | file_ext | lower == 'csv' \
                and result('new_file_sensor') | file_name | starts_with('McCarthy_User_Upload_') }}",
            yes_task='download_file',
            no_task='send_incorrect_file_format_mail'
        )

        send_incorrect_file_format_mail = rail.EmailOperator(
            task_id='send_incorrect_file_format_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon User import - Incorrect File Format {{ current_time_in_specified_tz('America/Los_Angeles') }}",
            html_content='incorrect_file_format.html'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='get_time_for_file',
            no_task='delete_this_dagrun'
        )

        def get_dagrun_start_time(start_time):
            return datetime.fromisoformat(start_time).strftime('%d%m%YT%H%M%S')
        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=get_dagrun_start_time,
            op_args=['{{ dag_run.start_date }}']
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_file') }}"
        )

        create_feedfile_collection = rail.CreateCollectionOperator(
            task_id='create_feedfile_collection',
            source="{{ result('parse_csv') }}",
            name="datafromfeedfile",
            columns={
                'First Name': 'FirstName',
                'Last Name': 'LastName',
                'Email': 'Email',
                'Employee Id': 'EmployeeId',
                'Start Date': 'StartDate',
                'End Date': 'EndDate',
                'Login Name': 'LoginName',
                'Supervisor Employee Id': 'SupervisorEmployeeId',
                'New Supervisor Effective Date': 'NewSupervisorEffectiveDate',
                'Payroll Name': 'PayrollName',
                'Employee Category': 'EmployeeCategory',
                'Employee Work State': 'EmployeeWorkState',
                'Legal Entity': 'LegalEntity',
                'Job Title': 'JobTitle',
                'Organization': 'Organization',
                'Department': 'Department',
                'Employee Type': 'EmployeeType',
                'Timesheet Template': 'TimesheetTemplate',
                'Time Zone': 'TimeZone'
            }
        )

        is_large_file = rail.IfOperator(
            task_id='is_large_file',
            test="{{ result('create_feedfile_collection', 'length') > 500 }}",
            yes_task="should_fail_dag",
            no_task="is_no_records"
        )

        is_no_records = rail.IfOperator(
            task_id='is_no_records',
            test="{{ result('create_feedfile_collection', 'length') < 1 }}",
            yes_task="send_no_records_mail",
            no_task="query_invalid_records"
        )

        send_no_records_mail = rail.EmailOperator(
            task_id='send_no_records_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | User Import Skipped On {{ current_time_in_specified_tz('America/Los_Angeles') }}",
            html_content='no_records.html'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            query="""SELECT * FROM datafromfeedfile WHERE 
                    NULLIF(LoginName, '') IS NULL OR 
                    NULLIF(StartDate, '') IS NULL OR 
                    NULLIF(PayrollName, '') IS NULL OR 
                    NULLIF(EmployeeCategory, '') IS NULL OR 
                    NULLIF(LegalEntity, '') IS NULL
                """
        )

        is_invalid_records = rail.IfOperator(
            task_id='is_invalid_records',
            test="{{ result('query_invalid_records', 'length') > 0 }}",
            yes_task="create_invalid_records_log",
            no_task="query_valid_records"
        )

        create_invalid_records_log = rail.CreateLogOperator(
            task_id='create_invalid_records_log'
        )

        write_invalid_records = rail.WriteLogOperator(
            task_id='write_invalid_records',
            log="{{ result('create_invalid_records_log') }}",
            severity='Exception',
            message='Invalid Records',
            items="{{ result('query_invalid_records') }}",
            properties={
                'loginname': '{{ item.LoginName }}',
                'email': '{{ item.Email }}',
                'action': '',
                'status': 'Exception',
                'details': 'The user could not be synced because one or more mandatory fields (Login Name, Start Date, Payroll Name, Employee Category, and Legal Entity) are missing.'
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT * FROM datafromfeedfile WHERE 
                    NULLIF(LoginName, '') IS NOT NULL AND 
                    NULLIF(StartDate, '') IS NOT NULL AND 
                    NULLIF(PayrollName, '') IS NOT NULL AND 
                    NULLIF(EmployeeCategory, '') IS NOT NULL AND 
                    NULLIF(LegalEntity, '') IS NOT NULL
                """,
            name='validatedinputlist'
        )

        is_valid_records = rail.IfOperator(
            task_id='is_valid_records',
            test="{{ result('query_valid_records', 'length') > 0 }}",
            yes_task="get_required_user_customfields",
            no_task="process_logs"
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                "payrollname": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Payroll Name', 'uri', ''),
                "employeecategory": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Employee Category', 'uri', ''),
                "employeeworkstate": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Employee Work State', 'uri', ''),
                "legalentity": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Legal Entity', 'uri', ''),
                "jobtitle": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Title', 'uri', ''),
                "organization": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Organization', 'uri', '')
            }
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id='get_all_timezones',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones"
        )

        get_enabled_activity_uris = rail.RepliconServiceOperator(
            task_id='get_enabled_activity_uris',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
            data_handler=lambda response: [x['uri'] for x in response]
        )

        get_office_schedules = rail.RepliconServiceOperator(
            task_id='get_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id='get_enabled_locations',
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
            data_handler=lambda response: {
                'basicuserlocationuri': rail.find_first_by_attr_and_get_attr(response, 'displayText',
                                                                             'Basic User', 'uri', ''),
                'supervisorlocationuri': rail.find_first_by_attr_and_get_attr(response, 'displayText',
                                                                              'Supervisor', 'uri', '')
            }
        )

        get_required_permission_sets = rail.RepliconServiceOperator(
            task_id='get_required_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: {
                "supervisorpermission": rail.find_first_by_attr_and_get_attr(
                    response, 'slug', 'administrator-payroll-administrator-supervisor-timesheet-history-supervisor', default=''),
                "basicuser": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                                                                  '*Gen3 - Basic User', default='')
            }
        )

        query_employeetype = rail.QueryCollectionOperator(
            task_id='query_employeetype',
            query="""SELECT DISTINCT EmployeeType as displayText FROM validatedinputlist WHERE NULLIF(EmployeeType, '') IS NOT NULL""",
            name='groupvaluesfromfeedfile'
        )

        trigger_groups_validation_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_groups_validation_child_dag',
            retries=0,
            items=lambda: [-1],
            trigger_dag_id=f'mccarthy_user_import_groups_validation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "groupcollection": "{{ result('query_employeetype') }}",
                "grouptype": "Employee Type"
            }
        )

        wait_for_groups_validation_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_groups_validation_child_dag',
            dag_runs="{{ result('trigger_groups_validation_child_dag') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_enabled_employeetype_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_employeetype_groups',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups"
        )

        query_distinct_departmentgroups = rail.QueryCollectionOperator(
            task_id='query_distinct_departmentgroups',
            query="""SELECT DISTINCT Department FROM validatedinputlist WHERE NULLIF(Department, '') IS NOT NULL"""
        )

        is_distinct_departmentgroups = rail.IfOperator(
            task_id='is_distinct_departmentgroups',
            test="{{ result('query_distinct_departmentgroups', 'length') > 0 }}",
            yes_task="trigger_department_update_child",
            no_task="get_department_group_details"
        )

        trigger_department_update_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_department_update_child',
            retries=0,
            items=lambda: [-1],
            trigger_dag_id=f'mccarthy_user_import_department_update_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "groupcollection": "{{ result('query_distinct_departmentgroups') }}"
            }
        )

        wait_for_department_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_department_update_child',
            dag_runs="{{ result('trigger_department_update_child') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def get_department_groups(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            return list(map(lambda item: {
                'departmentname': item['cells'][0]['textValue'],
                'departmenturi': item['cells'][0]['uri'],
                'fullpath': rail.smartjoin_by_delim(
                    [x['textValue'] for x in item['cells'][1]['cellCollection']], '|') if [
                        x['textValue'] for x in item['cells'][1]['cellCollection']] else '',
                'length': len([x['textValue'] for x in item['cells'][1]['cellCollection']]) if [
                    x['textValue'] for x in item['cells'][1]['cellCollection']] else 0
            }, flatten_rows)) if flatten_rows else []
        get_department_group_details = rail.RepliconServicePageOperator(
            task_id='get_department_group_details',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 1000000,
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path"
                ]
            },
            page_handler=page_handler,
            all_result_data_handler=get_department_groups
        )

        query_distinct_payrollnames = rail.QueryCollectionOperator(
            task_id='query_distinct_payrollnames',
            query="""SELECT DISTINCT PayrollName as displayText FROM validatedinputlist WHERE
                    NULLIF(PayrollName, '') IS NOT NULL""",
            name='payrollvaluesfromfeedfile'
        )

        get_payroll_customfield_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_payroll_customfield_dropdown_options',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').payrollname }}"
            }
        )

        create_payrollrepliconvalues_list = rail.CreateCollectionOperator(
            task_id='create_payrollrepliconvalues_list',
            source=lambda: rail.result(
                'get_payroll_customfield_dropdown_options'),
            name="payrollcustomfieldvaluesinreplicon"
        )

        trigger_customfields_validation_payrollname = rail.TriggerDagRunOperator(
            task_id='trigger_customfields_validation_payrollname',
            retries=0,
            trigger_dag_id=f'mccarthy_user_import_custom_fields_validation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "feedfiletablename": "{{ result('query_distinct_payrollnames', 'table') }}",
                "repliconvaluestablename": "{{ result('create_payrollrepliconvalues_list', 'table') }}",
                "replicon_dropdowns": "{{ result('create_payrollrepliconvalues_list') }}",
                "customFieldUri": "{{ result('get_required_user_customfields').payrollname }}"
            }
        )

        query_distinct_employeetypes = rail.QueryCollectionOperator(
            task_id='query_distinct_employeetypes',
            query="""SELECT DISTINCT EmployeeCategory as displayText FROM validatedinputlist WHERE
                    NULLIF(EmployeeCategory, '') IS NOT NULL""",
            name='employeetypevaluesfromfeedfile'
        )

        get_employeetype_customfield_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_employeetype_customfield_dropdown_options',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').employeecategory }}"
            }
        )

        create_employeetyperepliconvalues_list = rail.CreateCollectionOperator(
            task_id='create_employeetyperepliconvalues_list',
            source=lambda: rail.result(
                'get_employeetype_customfield_dropdown_options'),
            name="employeetypecustomfieldvaluesinreplicon"
        )

        trigger_customfields_validation_employeecategory = rail.TriggerDagRunOperator(
            task_id='trigger_customfields_validation_employeecategory',
            retries=0,
            trigger_dag_id=f'mccarthy_user_import_custom_fields_validation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "feedfiletablename": "{{ result('query_distinct_employeetypes', 'table') }}",
                "repliconvaluestablename": "{{ result('create_employeetyperepliconvalues_list', 'table') }}",
                "replicon_dropdowns": "{{ result('create_employeetyperepliconvalues_list') }}",
                "customFieldUri": "{{ result('get_required_user_customfields').employeecategory }}"
            }
        )

        query_distinct_employeeworkstate = rail.QueryCollectionOperator(
            task_id='query_distinct_employeeworkstate',
            query="""SELECT DISTINCT EmployeeWorkState as displayText FROM validatedinputlist WHERE
                    NULLIF(EmployeeWorkState, '') IS NOT NULL""",
            name='employeeworkstatevaluesfromfeedfile'
        )

        get_employeeworkstate_customfield_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_employeeworkstate_customfield_dropdown_options',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').employeeworkstate }}"
            }
        )

        create_employeeworkstaterepliconvalues_list = rail.CreateCollectionOperator(
            task_id='create_employeeworkstaterepliconvalues_list',
            source=lambda: rail.result(
                'get_employeeworkstate_customfield_dropdown_options'),
            name="employeeworkstatecustomfieldvaluesinreplicon"
        )

        trigger_customfields_validation_employeeworkstate = rail.TriggerDagRunOperator(
            task_id='trigger_customfields_validation_employeeworkstate',
            retries=0,
            trigger_dag_id=f'mccarthy_user_import_custom_fields_validation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "feedfiletablename": "{{ result('query_distinct_employeeworkstate', 'table') }}",
                "repliconvaluestablename": "{{ result('create_employeeworkstaterepliconvalues_list', 'table') }}",
                "replicon_dropdowns": "{{ result('create_employeeworkstaterepliconvalues_list') }}",
                "customFieldUri": "{{ result('get_required_user_customfields').employeeworkstate }}"
            }
        )

        query_distinct_legalentity = rail.QueryCollectionOperator(
            task_id='query_distinct_legalentity',
            query="""SELECT DISTINCT LegalEntity as displayText FROM validatedinputlist WHERE
                    NULLIF(LegalEntity, '') IS NOT NULL""",
            name='legalentityvaluesfromfeedfile'
        )

        get_legalentity_customfield_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_legalentity_customfield_dropdown_options',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').legalentity }}"
            }
        )

        create_legalentityrepliconvalues_list = rail.CreateCollectionOperator(
            task_id='create_legalentityrepliconvalues_list',
            source=lambda: rail.result(
                'get_legalentity_customfield_dropdown_options'),
            name="legalentitycustomfieldvaluesinreplicon"
        )

        trigger_customfields_validation_legalentity = rail.TriggerDagRunOperator(
            task_id='trigger_customfields_validation_legalentity',
            retries=0,
            trigger_dag_id=f'mccarthy_user_import_custom_fields_validation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "feedfiletablename": "{{ result('query_distinct_legalentity', 'table') }}",
                "repliconvaluestablename": "{{ result('create_legalentityrepliconvalues_list', 'table') }}",
                "replicon_dropdowns": "{{ result('create_legalentityrepliconvalues_list') }}",
                "customFieldUri": "{{ result('get_required_user_customfields').legalentity }}"
            }
        )

        wait_for_customfields_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_customfields_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ [result('trigger_customfields_validation_payrollname'), \
                result('trigger_customfields_validation_payrollname'), \
                    result('trigger_customfields_validation_employeeworkstate'), \
                        result('trigger_customfields_validation_legalentity')] }}"
        )

        create_supervisorlog = rail.CreateLogOperator(
            task_id='create_supervisorlog'
        )

        def get_usersync_conf(item):
            timeoff_template_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_policy_sets'), 'displayText', '*Gen3 - Default Timeoff Template', 'uri', '')
            default_timesheet_template_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_policy_sets'), 'displayText', 'Exempt_All_without A & E', 'uri', '')
            return {
                'Activities': rail.result('get_enabled_activity_uris'),
                'Scheduletype': 'Default Schedule',
                'Timezone': item['TimeZone'],
                'Timezoneuri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_timezones'), 'displayText', item['TimeZone'], 'uri', ''),
                'Timeofftemplate': '*Gen3 - Default Timeoff Template',
                'Timeofftemplateuri': timeoff_template_uri,
                'Timesheettemplate': item['TimesheetTemplate'],
                'Timesheettemplateuri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_policy_sets'), 'displayText', item['TimesheetTemplate'], 'uri', ''),
                'Location': 'Basic User',
                'Locationuri': rail.result('get_enabled_locations')['basicuserlocationuri'],
                'Employeetype': item['EmployeeType'],
                'Employeetypeuri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_enabled_employeetype_groups'), 'displayText', item['EmployeeType'], 'uri', ''),
                'Department': item['Department'],
                'Departmenturi': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_department_group_details'), 'fullpath',
                    f"McCarthy Holdings, Inc.|{item['Department']}", 'departmenturi', ''),
                'Permissions': rail.result('get_required_permission_sets')['basicuser']['name'],
                'Permissionsuri': rail.result('get_required_permission_sets')['basicuser']['uri'],
                'Organization': item['Organization'],
                'Organizationuri': rail.result('get_required_user_customfields')['organization'],
                'Jobtitleuri': rail.result('get_required_user_customfields')['jobtitle'],
                'Legalentityuri': rail.result('get_required_user_customfields')['legalentity'],
                'Employeeworkstateuri': rail.result('get_required_user_customfields')['employeeworkstate'],
                'Employeecategoryuri': rail.result('get_required_user_customfields')['employeecategory'],
                'Payrollnameuri': rail.result('get_required_user_customfields')['payrollname'],
                'Jobtitle': item['JobTitle'],
                'Legalentity': item['LegalEntity'],
                'Employeeworkstate': item['EmployeeWorkState'],
                'Employeecategory': item['EmployeeCategory'],
                'Payrollname': item['PayrollName'],
                'Supervisoreffectivedate': item['NewSupervisorEffectiveDate'],
                'Supervisorid': item['SupervisorEmployeeId'],
                'Loginname': item['LoginName'],
                'Enddate': item['EndDate'],
                'Startdate': item['StartDate'],
                'Employeeid': item['EmployeeId'],
                'Email': item['Email'],
                'Lastname': item['LastName'],
                'Firstname': item['FirstName'],
                'Defaulttimesheettemplate': default_timesheet_template_uri,
                'locationforsupervisor': rail.result('get_enabled_locations')['supervisorlocationuri'],
                'supervisor_log': rail.result('create_supervisorlog')
            }
        trigger_user_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_sync_child_dag',
            retries=0,
            items="{{ result('query_valid_records') }}",
            trigger_dag_id=f'mccarthy_user_import_user_sync_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_usersync_conf
        )

        wait_for_user_sync_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_sync_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_user_sync_child_dag") }}'
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs="{{ result('trigger_user_sync_child_dag') }}",
            dagrun_task_id='create_log',
            flatten=True
        )

        get_supervisorcheck_queued_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_queued_logs',
            log="{{ result('create_supervisorlog') }}",
            severity='Pending',
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
            trigger_dag_id=f'mccarthy_user_import_supervisor_assignment_child_{config.instance}',
            conf=lambda item: {
                **dict(item['properties'].items()),
                'supervisorpermissionuri': rail.result('get_required_permission_sets')['supervisorpermission']['uri'],
                'supervisorpermissionname': rail.result('get_required_permission_sets')['supervisorpermission']['name'],
                'locationuri': rail.result('get_enabled_locations')['supervisorlocationuri']
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_user_logs():
            logs = []
            create_invalid_records_log = rail.result(
                'create_invalid_records_log')
            if create_invalid_records_log:
                logs.append(create_invalid_records_log)
            gather_child_logs = rail.result('gather_child_logs')
            if gather_child_logs:
                logs.extend(gather_child_logs)
            return logs
        process_logs = rail.TriggerDagRunOperator(
            task_id='process_logs',
            retries=0,
            trigger_dag_id=f'mccarthy_user_import_child_log_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'logs': get_user_logs(),
                'filename': split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0],
                'inputfilesize': rail.result('create_feedfile_collection', 'length')
            }
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
                'Filename': "{{ result('new_file_sensor') | file_base }}",
                'Records': "{{ result('create_feedfile_collection', 'length') \
                    if get_task_state('create_feedfile_collection') == 'success' else 'Nil' }}"
            }
        )

        new_file_sensor >> is_correct_file
        is_correct_file >> rail.Label(
            'Yes') >> download_file
        download_file >> rail.Label(
            'Always') >> was_new_file_found
        was_new_file_found >> rail.Label(
            'Yes') >> get_time_for_file >> archive_file
        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun
        download_file >> parse_csv >> create_feedfile_collection >> is_large_file
        is_large_file >> rail.Label(
            'Yes') >> should_fail_dag
        is_large_file >> rail.Label(
            'No') >> is_no_records
        is_no_records >> rail.Label(
            'Yes') >> send_no_records_mail >> should_fail_dag
        is_no_records >> rail.Label(
            'No') >> query_invalid_records >> is_invalid_records
        is_invalid_records >> rail.Label(
            'Yes') >> create_invalid_records_log >> write_invalid_records >> query_valid_records
        is_invalid_records >> rail.Label(
            'No') >> query_valid_records
        query_valid_records >> is_valid_records
        is_valid_records >> rail.Label(
            'Yes') >> get_required_user_customfields >> get_all_timezones >> get_enabled_activity_uris >> \
            get_office_schedules >> get_all_policy_sets >> get_enabled_locations >> \
            get_required_permission_sets >> query_employeetype >> \
            trigger_groups_validation_child_dag >> wait_for_groups_validation_child_dag >> get_enabled_employeetype_groups >> \
            query_distinct_departmentgroups >> is_distinct_departmentgroups
        is_distinct_departmentgroups >> rail.Label(
            'Yes') >> trigger_department_update_child >> wait_for_department_update_child >> get_department_group_details
        is_distinct_departmentgroups >> rail.Label(
            'No') >> get_department_group_details
        get_department_group_details >> query_distinct_payrollnames >> get_payroll_customfield_dropdown_options >> \
            create_payrollrepliconvalues_list >> trigger_customfields_validation_payrollname >> query_distinct_employeetypes >> \
            get_employeetype_customfield_dropdown_options >> create_employeetyperepliconvalues_list >> \
            trigger_customfields_validation_employeecategory >> query_distinct_employeeworkstate >> \
            get_employeeworkstate_customfield_dropdown_options >> create_employeeworkstaterepliconvalues_list >> \
            trigger_customfields_validation_employeeworkstate >> query_distinct_legalentity >> \
            get_legalentity_customfield_dropdown_options >> create_legalentityrepliconvalues_list >> \
            trigger_customfields_validation_legalentity >> wait_for_customfields_child >> create_supervisorlog >> \
            trigger_user_sync_child_dag >> wait_for_user_sync_child >> gather_child_logs >> get_supervisorcheck_queued_logs >> \
            is_supervisorcheck_queued_logs

        is_supervisorcheck_queued_logs >> rail.Label(
            'Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> process_logs
        is_supervisorcheck_queued_logs >> rail.Label(
            'No') >> process_logs

        is_valid_records >> rail.Label(
            'No') >> process_logs

        process_logs >> should_fail_dag

        is_correct_file >> rail.Label(
            'No') >> send_incorrect_file_format_mail >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        should_fail_dag >> rail.Label(
            'No') >> process_logtosumo >> check_if_new_file_found >> rail.Label(
                'Yes') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
