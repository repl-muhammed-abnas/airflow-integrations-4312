from datetime import timedelta
import rail
from gee.user_import.utils import python_callable, response_filter, request_payload


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'GEE User Import V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=15),
        )

        master_dag_triggered_at = rail.PythonOperator(
            task_id='master_dag_triggered_at',
            python_callable=python_callable.get_dag_trigger_time
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test="{{ result('new_file_sensor') | file_ext | lower == 'csv' }}",
            yes_task='download_input_file',
            no_task='send_wrong_file_format_email'
        )

        send_wrong_file_format_email = rail.EmailOperator(
            task_id='send_wrong_file_format_email',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject="{{ get_company_key() }} | Replicon user import failed | {{ result('master_dag_triggered_at').dag_trigger_time }}",
            html_content="templates/emails/send_wrong_file_format_email.html"
        )

        archive_invalid_file = rail.SFTPMoveFileOperator(
            task_id='archive_invalid_file',
            new_filename=config.archive_filepath +
            '/{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath +
            '/{{ result("new_file_sensor") | file_name }}'
        )

        download_input_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            no_task = 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath + '/archive_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath+'/{{ result("new_file_sensor") | file_name }}',
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            headers=['Login Name', 'First Name', 'Last Name', 'Employee Type', 'Department', 'Enabled', 'Employee Id',
                     'Start Date', 'End Date', 'Email Address', 'Supervisor ID', 'Permission Sets', 'Location', 'Time Zone', 'Work Week',
                     'Holiday Calendar', 'Initial Schedule Name', 'Annual Salary', 'ELT', '2nd Line Manager', 'Work week Hours',
                     'Business card Title', 'Cost Center', 'Division'],
            delimiter=',',
            document="{{ result('download_input_file') }}",
        )

        if_lenght_of_csv_is_less_than_1 = rail.IfOperator(
            task_id='if_lenght_of_csv_is_less_than_1',
            test=lambda: len(rail.load_all_records(
                rail.result('parse_csv'))) < 1,
            yes_task='send_no_records_found_email',
            no_task='compose_csv_and_create_md5_file_for_input_file'
        )

        send_no_records_found_email = rail.EmailOperator(
            task_id='send_no_records_found_email',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject="{{ get_company_key() }} | Replicon user import skipped | {{ result('master_dag_triggered_at').dag_trigger_time }}",
            html_content="templates/emails/send_no_records_found_email.html"
        )

        compose_csv_and_create_md5_file_for_input_file = rail.WriteCSVFileOperator(
            task_id='compose_csv_and_create_md5_file_for_input_file',
            source="{{ result('parse_csv') }}",
            header=[
                'Login Name',
                'First Name',
                'Last Name',
                'Employee Type',
                'Department',
                'Enabled',
                'Employee Id',
                'Start Date',
                'End Date',
                'Email Address',
                'Supervisor ID',
                'Permission Sets',
                'Location',
                'Time Zone',
                'Work Week',
                'Holiday Calendar',
                'Initial Schedule Name',
                'Annual Salary',
                'ELT',
                '2nd Line Manager',
                'Work week Hours',
                'Business card Title',
                'Cost Center',
                'Division',
                'md5'
            ],
            row=python_callable.get_csv_line_items
        )

        list_dir = rail.SFTPListFilesOperator(
            task_id='list_dir',
            paths=[config.reference_filepath],
        )

        reference_filename = rail.PythonOperator(
            task_id='reference_filename',
            python_callable=lambda: config.reference_filepath + '/' +
            rail.result('list_dir')[config.reference_filepath][0]['name']
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath="{{result('reference_filename')}}"
        )

        create_inputfilewithmd5_collection = rail.CreateCollectionOperator(
            task_id='create_inputfilewithmd5_collection',
            source="{{ result('compose_csv_and_create_md5_file_for_input_file') }}",
            name="inputfilewithmd5",
            columns={
                'Login Name':'loginname',
                'First Name':'firstname',
                'Last Name':'lastname',
                'Employee Type':'employeetype',
                'Department':'department',
                'Enabled':'enabled',
                'Employee Id':'employeeid',
                'Start Date':'startdate',
                'End Date':'enddate',
                'Email Address':'emailaddress',
                'Supervisor ID':'supervisorid',
                'Permission Sets':'permissionsets',
                'Location':'location',
                'Time Zone':'timezone',
                'Work Week':'workweek',
                'Holiday Calendar':'holidaycalendar',
                'Initial Schedule Name':'initialschedulename',
                'Annual Salary':'annualsalary',
                'ELT':'elt',
                '2nd Line Manager':'secondlinemanager',
                'Work week Hours':'workweekhours',
                'Business card Title':'businesscardtitle',
                'Cost Center':'costcenter',
                'Division':'division',
                'md5':'md5'
            }
        )

        load_referencefilewithmd5_csv = rail.LoadCSVFileOperator(
            task_id='load_referencefilewithmd5_csv',
            document="{{ result('download_reference_file') }}"
        )

        create_referencefilewithmd5_collection = rail.CreateCollectionOperator(
            task_id='create_referencefilewithmd5_collection',
            source="{{ result('load_referencefilewithmd5_csv') }}",
            name="referencefilewithmd5",
            columns={
                'Login Name':'loginname',
                'First Name':'firstname',
                'Last Name':'lastname',
                'Employee Type':'employeetype',
                'Department':'department',
                'Enabled':'enabled',
                'Employee Id':'employeeid',
                'Start Date':'startdate',
                'End Date':'enddate',
                'Email Address':'emailaddress',
                'Supervisor ID':'supervisorid',
                'Permission Sets':'permissionsets',
                'Location':'location',
                'Time Zone':'timezone',
                'Work Week':'workweek',
                'Holiday Calendar':'holidaycalendar',
                'Initial Schedule Name':'initialschedulename',
                'Annual Salary':'annualsalary',
                'ELT':'elt',
                '2nd Line Manager':'secondlinemanager',
                'Work week Hours':'workweekhours',
                'Business card Title':'businesscardtitle',
                'Cost Center':'costcenter',
                'Division':'division',
                'md5':'md5'
            }
        )

        get_records_without_employee_id_data = rail.QueryCollectionOperator(
            task_id='get_records_without_employee_id_data',
            query="""SELECT * FROM inputfilewithmd5 i LEFT JOIN referencefilewithmd5 r ON i.employeeid = r.employeeid WHERE r.employeeid IS NULL""",
            name='recordswithoutemployeeid'
        )

        if_without_employee_id_record_is_greater_than_0 = rail.IfOperator(
            task_id='if_without_employee_id_record_is_greater_than_0',
            test='{{ result("get_records_without_employee_id_data", "length") > 0 }}',
            yes_task='compose_csv_without_employee_id',
            no_task='identify_unchanged_records'
        )

        compose_csv_without_employee_id = rail.WriteCSVFileOperator(
            task_id='compose_csv_without_employee_id',
            source="{{ result('get_records_without_employee_id_data') }}",
            header=[
                'loginname',
                'employeeid',
                'action',
                'status',
                'details',
                'jobid'
            ],
            row=[
                "{{ item['loginname'] }}",
                "{{ item['employeeid'] }}",
                "Precheck",
                "Ignored",
                "Employee ID is not present",
                "{{ dag_run_ecid() }}",
            ]
        )

        upload_csv_without_employee_id_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_without_employee_id_to_sftp',
            content="{{ result('compose_csv_without_employee_id') }}",
            remote_filepath=config.usersync_filepath +
            '/{{ result("new_file_sensor") | file_name | replace(".csv", "") }}_{{ result("master_dag_triggered_at").dag_trigger_time }}.csv',
        )

        identify_unchanged_records = rail.QueryCollectionOperator(
            task_id='identify_unchanged_records',
            query="""SELECT * FROM inputfilewithmd5 WHERE md5 IN (SELECT md5 FROM referencefilewithmd5)""",
            name='identifyunchangedrecords'
        )

        identify_changed_records_with_employeeid = rail.QueryCollectionOperator(
            task_id='identify_changed_records_with_employeeid',
            query="""SELECT * FROM inputfilewithmd5 WHERE md5 NOT IN (SELECT md5 FROM referencefilewithmd5) AND employeeid IS NOT NULL""",
            name='identifychangedrecords'
        )

        if_changed_records_with_employeeid_is_less_than_1 = rail.IfOperator(
            task_id='if_changed_records_with_employeeid_is_less_than_1',
            test='{{ result("identify_changed_records_with_employeeid", "length") < 1 }}',
            yes_task='send_import_completed_with_nochange_email',
            no_task='if_changed_records_with_employeeid_is_greater_than_0'
        )

        send_import_completed_with_nochange_email = rail.EmailOperator(
            task_id='send_import_completed_with_nochange_email',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject="{{get_company_key()}} | Replicon user import completed - No changed records - {{result('master_dag_triggered_at').dag_trigger_time}}",
            html_content="templates/emails/send_import_completed_with_nochange_email.html"
        )

        if_changed_records_with_employeeid_is_greater_than_0 = rail.IfOperator(
            task_id='if_changed_records_with_employeeid_is_greater_than_0',
            test='{{ result("identify_changed_records_with_employeeid", "length") > 0 }}',
            yes_task='get_reportdetails',
            no_task='archive_reference_file'
        )

        get_reportdetails = rail.RepliconReportDetailsOperator(
            task_id='get_reportdetails',
            report_name=config.enabled_user_report_name,
        )

        generate_report_data = rail.run_report2(
            group_id="generate_report_data",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_reportdetails')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "persistedReportName": None
                    }
                ]
            }
        )

        report_has_no_data = rail.IfOperator(
            task_id = "report_has_no_data",
            test= "{{ result('generate_report_data.get_report_result').reportGenerationResults[0].payload | starts_with('No Data')}}",
            yes_task='log_to_sumo',
            no_task= 'is_report_does_not_has_expected_columns'
        )

        expected_report_columns = 'User Name,User Email,Employee ID,UserUri'
        is_report_does_not_has_expected_columns = rail.IfOperator(
            task_id='is_report_does_not_has_expected_columns',
            test="{{ result('generate_report_data.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') | is_falsy }}" % expected_report_columns,
            yes_task="log_to_sumo",
            no_task="parse_report_payload",
        )

        parse_report_payload = rail.LoadCSVFileOperator(
            task_id='parse_report_payload',
            document="{{ result('generate_report_data.get_report_result').reportGenerationResults[0].payload }}",
        )

        get_user_custom_fields = rail.RepliconServiceOperator(
            task_id="get_user_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'businesscardtitle': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Business Card Title', 'uri'),
                'annualsalary': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'ANNUAL SALARY', 'uri'),
                'firstlinemanger': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', '1st Line Mgr', 'uri'),
                'secondlinemanger': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', '2nd Line Mgr', 'uri'),
                'elt': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'ELT', 'uri'),
                'workweekhours': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Work Week Hours', 'uri'),
            },
        )

        get_supervisors_from_feed_file = rail.QueryCollectionOperator(
            task_id='get_supervisors_from_feed_file',
            query="""SELECT * FROM inputfilewithmd5 WHERE employeeid IN (SELECT DISTINCT supervisorid FROM inputfilewithmd5)""",
            name='supervisorsfromfeedfile'
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_custome_fields_dropdown_options = rail.RepliconServiceOperator(
            task_id="get_all_custome_fields_dropdown_options",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_custom_fields')['workweekhours']
                }
        )

        get_company_department = rail.RepliconServiceOperator(
            task_id='get_company_department',
            endpoint="/services/DepartmentService1.svc/GetCompanyDepartment"
        )

        get_child_department_details = rail.RepliconServiceOperator(
            task_id='get_child_department_details',
            endpoint="/services/DepartmentService1.svc/GetChildrenDepartmentDetails",
            data={
                "parentDepartmentUri": "{{ result('get_company_department').uri }}"
            }
        )

        get_all_time_zones = rail.RepliconServiceOperator(
            task_id='get_all_time_zones',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones"
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_data_employee_type_group_list_service = rail.RepliconServiceOperator(
            task_id='get_data_employee_type_group_list_service',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:code",
                    "urn:replicon:employee-type-group-list-column:employee-type-group"
                ],
                "sort": [],
                "filterExpression": None
            }
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id='get_enabled_locations',
            endpoint="/services/LocationService1.svc/GetEnabledLocations"
        )

        employee_type_group_source_list = rail.PythonOperator(
            task_id='employee_type_group_source_list',
            python_callable=response_filter.employee_type_group_source_list
        )

        gee_user_import_logs = rail.CreateLogOperator(
            task_id='gee_user_import_logs'
        )

        gee_supervisor_logs = rail.CreateLogOperator(
            task_id='gee_supervisor_logs'
        )

        variable_trigger_dag_ids = rail.SetVariableOperator(
            task_id='variable_trigger_dag_ids',
            append=False,
            name='trigger_dag_ids',
            value=[]
        )

        for_each_changed_records = rail.ForEachOperator(
            task_id='for_each_changed_records',
            items="{{ result('identify_changed_records_with_employeeid') | load_all_records() | to_json }}",
            start_task='if_employeeid_present_49',
            end_task='for_each_changed_records_end'
        )

        if_employeeid_present_49 = rail.IfOperator(
            task_id = "if_employeeid_present_49",
            test= "{{ result('for_each_changed_records').employeeid | is_truthy }}",
            yes_task='user_check',
            no_task= 'add_entry_to_lookup_table'
        )

        user_check = rail.PythonOperator(
            task_id='user_check',
            python_callable=python_callable.user_check,
            op_args=["{{ result('for_each_changed_records').employeeid }}"]
        )

        get_supervisor_details = rail.PythonOperator(
            task_id='get_supervisor_details',
            python_callable=python_callable.get_supervisor_details,
            op_args=[
                "{{ result('for_each_changed_records').supervisorid }}"
            ]
        )

        is_enabled = rail.IfOperator(
            task_id='is_enabled',
            test="{{ result('for_each_changed_records').enabled | lower == 'yes' }}",
            yes_task='if_user_not_present',
            no_task='is_enabled_equals_no'
        )

        if_user_not_present = rail.IfOperator(
            task_id='if_user_not_present',
            test="{{ result('user_check').user | is_falsy }}",
            yes_task='trigger_create_user_dag',
            no_task='trigger_update_user_dag'
        )

        trigger_create_user_dag = rail.TriggerDagRunOperator(
            task_id='trigger_create_user_dag',
            trigger_dag_id=config.create_user_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.trigger_create_user_dag(rail.result('for_each_changed_records'))
        )

        insert_to_trigger_dag_ids_create = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_create',
            append=True,
            name='{{ result("variable_trigger_dag_ids").name }}',
            value="{{result('trigger_create_user_dag')}}"
        )

        trigger_update_user_dag = rail.TriggerDagRunOperator(
            task_id='trigger_update_user_dag',
            trigger_dag_id=config.update_user_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.trigger_update_user_dag(rail.result('for_each_changed_records'))
        )

        insert_to_trigger_dag_ids_update = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_update',
            append=True,
            name='{{ result("variable_trigger_dag_ids").name }}',
            value="{{result('trigger_update_user_dag')}}"
        )

        is_enabled_equals_no = rail.IfOperator(
            task_id='is_enabled_equals_no',
            test="{{ result('for_each_changed_records').enabled | lower == 'no' }}",
            yes_task='if_user_not_present_58',
            no_task='is_enabled_does_not_equals_yes_and_no'
        )

        if_user_not_present_58 = rail.IfOperator(
            task_id='if_user_not_present_58',
            test="{{ result('user_check').user | is_falsy }}",
            yes_task='add_entry_to_lookup_table_59',
            no_task='trigger_disable_user_dag'
        )

        add_entry_to_lookup_table_59 = rail.WriteLogOperator(
            task_id='add_entry_to_lookup_table_59',
            log="{{ result('gee_user_import_logs') }}",
            message="Ignored",
            severity='Success',
            properties={
                'loginname': "{{ result('for_each_changed_records').loginname }}",
                'empid': "{{ result('for_each_changed_records').employeeid }}",
                'action': "Precheck",
                'status': "Ignored",
                'details': "User is not available or already disabled in Replicon",
                'jobid': '{{ dag_run_ecid() }}',
                'childjobid':''
            }
        )

        trigger_disable_user_dag = rail.TriggerDagRunOperator(
            task_id='trigger_disable_user_dag',
            trigger_dag_id=config.disable_user_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.trigger_disable_user_dag(rail.result('for_each_changed_records'))
        )

        insert_to_trigger_dag_ids_disable = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_disable',
            append=True,
            name='{{ result("variable_trigger_dag_ids").name }}',
            value="{{result('trigger_disable_user_dag')}}"
        )

        is_enabled_does_not_equals_yes_and_no = rail.IfOperator(
            task_id='is_enabled_does_not_equals_yes_and_no',
            test=lambda: bool(rail.result('for_each_changed_records')['enabled'].lower() != 'no' and
                              rail.result('for_each_changed_records')['enabled'].lower() != 'yes'),
            yes_task='add_entry_to_lookup_table_63',
        )

        add_entry_to_lookup_table_63 = rail.WriteLogOperator(
            task_id='add_entry_to_lookup_table_63',
            log="{{ result('gee_user_import_logs') }}",
            message="Ignored",
            severity='Success',
            properties={
                'loginname': "{{ result('for_each_changed_records').loginname }}",
                'empid': "{{ result('for_each_changed_records').employeeid }}",
                'action': "Precheck",
                'status': "Ignored",
                'details': "Unknown enabled status",
                'jobid': '{{ dag_run_ecid() }}',
                'childjobid':''
            }
        )

        add_entry_to_lookup_table = rail.WriteLogOperator(
            task_id='add_entry_to_lookup_table',
            log="{{ result('gee_user_import_logs') }}",
            message="Ignored",
            severity='Success',
            properties={
                'loginname': "{{ result('for_each_changed_records').loginname }}",
                'empid': "{{ result('for_each_changed_records').employeeid }}",
                'action': "Precheck",
                'status': "Ignored",
                'details': "Employee ID is not present",
                'jobid': '{{ dag_run_ecid() }}',
                'childjobid':''
            }
        )

        for_each_changed_records_end = rail.EmptyOperator(
            task_id='for_each_changed_records_end',
        )

        get_variable_trigger_dag_ids = rail.GetVariableOperator(
            task_id='get_variable_trigger_dag_ids',
            name='{{ result("variable_trigger_dag_ids").name }}'
        )

        wait_for_variable_trigger_dag_ids = rail.WaitForDagRunsSensor(
            task_id='wait_for_variable_trigger_dag_ids',
            dag_runs='{{ result("get_variable_trigger_dag_ids").value | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        supervisor_logs_search_entries = rail.FilterLogEntriesOperator(
            task_id='supervisor_logs_search_entries',
            log="{{result('gee_supervisor_logs')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            },
            remove_filtered_entries=True
        )

        if_log_entry_found = rail.IfOperator(
            task_id='if_log_entry_found',
            test=lambda: bool(rail.load_all_records(rail.result(
                'supervisor_logs_search_entries'))),
            yes_task='process_each_supervisor_records',
            no_task='trigger_gee_usersync_sendlog'
        )

        process_each_supervisor_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_supervisor_records',
            items=lambda: rail.load_all_records(rail.result(
                'supervisor_logs_search_entries')),
            trigger_dag_id=config.gee_supervisor_assignment_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_supervisor_conf_payload
        )

        wait_for_process_each_supervisor_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_supervisor_records',
            dag_runs='{{ result("process_each_supervisor_records") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        trigger_gee_usersync_sendlog = rail.TriggerDagRunOperator(
            task_id='trigger_gee_usersync_sendlog',
            trigger_dag_id=config.gee_usersync_sendlog,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'jobid': "{{dag_run_ecid()}}",
                'filename': "{{ result('new_file_sensor') | file_name| replace('.csv', '') }}_{{result('master_dag_triggered_at').dag_trigger_time}}.csv",
                'emailid': config.tenant_email,
                'filepath': config.usersync_filepath,
                'user_logs':"{{result('gee_user_import_logs')}}"
            }
        )

        wait_for_gee_usersync_sendlog = rail.WaitForDagRunsSensor(
            task_id='wait_for_gee_usersync_sendlog',
            dag_runs='{{ result("trigger_gee_usersync_sendlog") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        archive_reference_file_75 = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file_75',
            new_filename=config.archive_filepath +
            '/{{ result("reference_filename").split("/")[-1] }}',
            existing_filename=config.reference_filepath +
            '/{{ result("reference_filename").split("/")[-1] }}'
        )

        upload_the_new_reference_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_the_new_reference_file_to_sftp',
            content="{{ result('compose_csv_and_create_md5_file_for_input_file') }}",
            remote_filepath=config.reference_filepath +
            '/userreference_{{ result("master_dag_triggered_at").dag_trigger_time }}.csv',
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            new_filename=config.archive_filepath + '/{{ result("reference_filename").split("/")[-1] }}',
            existing_filename=config.reference_filepath +
            '/{{ result("reference_filename").split("/")[-1] }}'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        new_file_sensor >> master_dag_triggered_at >> is_csv >> rail.Label(
            "Yes") >> download_input_file >> archive_file >> parse_csv
        is_csv >> rail.Label(
            "No") >> send_wrong_file_format_email >> archive_invalid_file >> log_to_sumo
        download_input_file >> was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        parse_csv >> if_lenght_of_csv_is_less_than_1 >> rail.Label(
            "Yes") >> send_no_records_found_email >> log_to_sumo
        if_lenght_of_csv_is_less_than_1 >> rail.Label(
            "No") >> compose_csv_and_create_md5_file_for_input_file >> list_dir >> \
        reference_filename >> download_reference_file >> create_inputfilewithmd5_collection >> load_referencefilewithmd5_csv >> \
        create_referencefilewithmd5_collection >> get_records_without_employee_id_data >> \
        if_without_employee_id_record_is_greater_than_0 >> rail.Label(
            "Yes") >> compose_csv_without_employee_id >> upload_csv_without_employee_id_to_sftp >> identify_unchanged_records
        if_without_employee_id_record_is_greater_than_0 >> rail.Label(
            "No") >> identify_unchanged_records >> identify_changed_records_with_employeeid >> \
        if_changed_records_with_employeeid_is_less_than_1 >> rail.Label(
            "Yes") >> send_import_completed_with_nochange_email >> log_to_sumo
        if_changed_records_with_employeeid_is_less_than_1 >> rail.Label(
            "No") >> if_changed_records_with_employeeid_is_greater_than_0 >> \
        rail.Label("Yes") >> get_reportdetails >> generate_report_data >> report_has_no_data >> rail.Label(
            "Yes") >> log_to_sumo
        report_has_no_data >> rail.Label("No") >> is_report_does_not_has_expected_columns >> rail.Label(
            "Yes") >> log_to_sumo
        is_report_does_not_has_expected_columns >> rail.Label(
            "No") >> parse_report_payload >> get_user_custom_fields >> \
        get_supervisors_from_feed_file >> get_all_permission_sets >> get_all_custome_fields_dropdown_options >> \
        get_company_department >> get_child_department_details >> get_all_time_zones >> get_all_office_schedules >> \
        get_data_employee_type_group_list_service >> get_enabled_locations >> employee_type_group_source_list >> gee_user_import_logs >> \
        gee_supervisor_logs >> variable_trigger_dag_ids >> for_each_changed_records >> for_each_changed_records_end
        for_each_changed_records >> if_employeeid_present_49 >> rail.Label(
            "Yes") >> user_check >> \
        get_supervisor_details >> is_enabled >> rail.Label(
            "Yes") >> if_user_not_present >> rail.Label(
            "Yes") >> trigger_create_user_dag >> insert_to_trigger_dag_ids_create >> for_each_changed_records_end
        if_user_not_present >> rail.Label(
            "No") >> trigger_update_user_dag >> insert_to_trigger_dag_ids_update >> for_each_changed_records_end
        is_enabled >> rail.Label(
            "No") >> is_enabled_equals_no >> rail.Label(
            "Yes") >> if_user_not_present_58 >> rail.Label(
            "Yes") >> add_entry_to_lookup_table_59 >> for_each_changed_records_end
        if_user_not_present_58 >> rail.Label(
            "No") >> trigger_disable_user_dag >> insert_to_trigger_dag_ids_disable >> for_each_changed_records_end
        is_enabled_equals_no >> rail.Label(
            "No") >> is_enabled_does_not_equals_yes_and_no >> rail.Label(
            "Yes") >> add_entry_to_lookup_table_63 >> for_each_changed_records_end
        if_employeeid_present_49 >> rail.Label("No") >> add_entry_to_lookup_table >> for_each_changed_records_end >> \
        get_variable_trigger_dag_ids >> wait_for_variable_trigger_dag_ids >> supervisor_logs_search_entries
        supervisor_logs_search_entries >> if_log_entry_found >> rail.Label(
            "Yes") >> process_each_supervisor_records >> wait_for_process_each_supervisor_records >> \
        trigger_gee_usersync_sendlog >> wait_for_gee_usersync_sendlog >> archive_reference_file_75
        if_log_entry_found >> rail.Label(
            "No") >> trigger_gee_usersync_sendlog >> wait_for_gee_usersync_sendlog >> archive_reference_file_75
        archive_reference_file_75 >> upload_the_new_reference_file_to_sftp >> log_to_sumo

        if_changed_records_with_employeeid_is_greater_than_0 >> rail.Label("No") >> archive_reference_file >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
