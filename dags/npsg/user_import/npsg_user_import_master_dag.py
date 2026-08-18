
from datetime import timedelta, datetime
import hashlib
from airflow.models import Variable
import rail


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'npsg_user_import_master_{config.instance}',
        description=f'NPSG_User import - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "{{dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_formatted_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_formatted_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_formatted_time = rail.PythonOperator(
            task_id='log_formatted_time',
            python_callable=lambda: datetime.now().strftime("%Y%m%dT%H%M%S")
        )

        parse_csv_15 = rail.LoadCSVFileOperator(
            task_id='parse_csv_15',
            document="{{result('download_file')}}"
        )

        create_csv_lines_create_m_d5filefor_inputfile_16 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_create_m_d5filefor_inputfile_16',
            source="{{ result('parse_csv_15') }}",
            header=['First Name',
                    'Last Name',
                    'Email Address',
                    'Employee ID',
                    'Start Date',
                    'End Date',
                    'Employment Status',
                    'Division',
                    'Position',
                    'Employee State',
                    'Employee City',
                    'Login Name',
                    'Supervisor',
                    'Department',
                    'Employee Type',
                    'Is Login Enabled',
                    'License',
                    'Job Family',
                    'Management Level',
                    'Location',
                    'Punch Time Entry',
                    'Timesheet Template',
                    'Time Off Template',
                    'Time Zone',
                    'Holiday Calendar',
                    'Payrules',
                    'md5'],
            row=lambda item: [
                item['First Name'] or '',
                item['Last Name'] or '',
                item['Email Address'] or '',
                item['Employee ID'] or '',
                item['Start Date'] or '',
                item['End Date'] or '',
                item['Employment Status'] or '',
                item['Division'] or '',
                item['Position'] or '',
                item['Employee State'] or '',
                item['Employee City'] or '',
                item['Login Name'] or '',
                item['Supervisor'] or '',
                item['Department'] or '',
                item['Employee Type'] or '',
                item['Is Login Enabled'] or '',
                item['License'] or '',
                item['Job Family'] or '',
                item['Management Level'] or '',
                item['Location'] or '',
                item['Punch Time Entry'] or '',
                item['Timesheet Template'] or '',
                item['Time Off Template'] or '',
                item['Time Zone'] or '',
                item['Holiday Calendar'] or '',
                item['Payrules'] or '',
                hashlib.md5(str(str(item['First Name']) + "_" + str(item['Last Name']) + "_" + str(item['Email Address']) + "_" +
                    str(item['Employee ID']) + "_" +
                    str(item['Start Date']) + "_" + str(item['End Date']) + "_" + str(item['Employment Status']) + "_" + str(item['Division']) + "_" +
                    str(item['Position']) + "_" + str(item['Employee State']) + "_" + str(item['Employee City']) + "_" + str(item['Login Name']) + "_" +
                    str(item['Supervisor']) + "_" + str(item['Department']) + "_" + str(item['Employee Type']) + "_" + str(item['Is Login Enabled']) + "_" +
                    str(item['License']) + "_" + str(item['Job Family']) + "_" + str(item['Management Level']) + "_" + str(item['Location']) + "_" +
                    str(item['Punch Time Entry']) + "_" + str(item['Timesheet Template']) + "_" + str(item['Time Off Template']) + "_" +
                    str(item['Time Zone']) + "_" + str(item['Holiday Calendar']) + "_" + str(item['Payrules'])).encode('utf-8')).hexdigest()
            ],
        )

        download_downloadthereferencefile_17 = rail.SFTPDownloadFileOperator(
            task_id='download_downloadthereferencefile_17',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath + 'npsg_reference.csv'
        )

        create_collection_create_list_from_csv_18 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_18',
            source="{{ result('create_csv_lines_create_m_d5filefor_inputfile_16') }}",
            name="inputfilewithmd5",
            columns={
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Email Address': 'email',
                'Employee ID': 'employeeid',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Employment Status': 'employmentstatus',
                'Division': 'division',
                'Position': 'position',
                'Employee State': 'employeestate',
                'Employee City': 'employeecity',
                'Login Name': 'loginanme',
                'Supervisor': 'supervisorid',
                'Department': 'department',
                'Employee Type': 'employeetype',
                'Is Login Enabled': 'loginenabled',
                'License': 'license',
                'Job Family': 'jobfamily',
                'Management Level': 'managementlevel',
                'Location': 'location',
                'Punch Time Entry': 'punchtimenetry',
                'Timesheet Template': 'timesheettemplate',
                'Time Off Template': 'timeofftemplate',
                'Time Zone': 'timezone',
                'Holiday Calendar': 'holidaycalendar',
                'Payrules': 'payrule',
                'md5': 'md5'
            }
        )

        load_csv_create_list_from_csv_19 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_19",
            document="{{result('download_downloadthereferencefile_17')}}",
            delimiter=','
        )

        create_collection_create_list_from_csv_19 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_19',
            source="{{ result('load_csv_create_list_from_csv_19') }}",
            name="referencefilewithmd5",
            columns={
                'firstname': 'firstname',
                'lastname': 'lastname',
                'email': 'email',
                'employeeid': 'employeeid',
                'startdate': 'startdate',
                'enddate': 'enddate',
                'employmentstatus': 'employmentstatus',
                'division': 'division',
                'position': 'position',
                'employeestate': 'employeestate',
                'employeecity': 'employeecity',
                'loginanme': 'loginanme',
                'supervisorid': 'supervisorid',
                'department': 'department',
                'employeetype': 'employeetype',
                'loginenabled': 'loginenabled',
                'license': 'license',
                'jobfamily': 'jobfamily',
                'managementlevel': 'managementlevel',
                'location': 'location',
                'punchtimenetry': 'punchtimenetry',
                'timesheettemplate': 'timesheettemplate',
                'timeofftemplate': 'timeofftemplate',
                'timezone': 'timezone',
                'holidaycalendar': 'holidaycalendar',
                'payrule': 'payrule',
                'md5': 'md5'
            }
        )

        create_user_import_log_table = rail.CreateLogOperator(
            task_id='create_user_import_log_table'
        )

        create_supervisor_check_lookup_table = rail.CreateLogOperator(
            task_id='create_supervisor_check_lookup_table'
        )

        query_list_identify_unchangedrecords_21 = rail.QueryCollectionOperator(
            task_id='query_list_identify_unchangedrecords_21',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.md5 IN (SELECT  referencefilewithmd5.md5 FROM  referencefilewithmd5)""",
        )

        if_query_list_identify_unchangedrecords_21_rows_greater_than_0_22 = rail.IfOperator(
            task_id='if_query_list_identify_unchangedrecords_21_rows_greater_than_0_22',
            test="{{result('query_list_identify_unchangedrecords_21','length') > 0}}",
            yes_task="add_unchanged_records_log",
            no_task="query_list_identify_changedrecords_24",
        )

        add_unchanged_records_log = rail.WriteLogOperator(
            task_id='add_unchanged_records_log',
            log="{{result('create_user_import_log_table')}}",
            items="{{result('query_list_identify_unchangedrecords_21')}}",
            severity='NA',
            message='Skipped',
            properties=lambda item: {
                'empid': item['employeeid'],
                'username': item['firstname'] + ' ' + item['lastname'],
                'action': 'pre-check',
                'status': 'ignored',
                'details': 'No changes in user records',
                'parentjob': rail.render_template("{{dag_run_ecid()}}"),
                'childjob': ''
            }
        )

        query_list_identify_changedrecords_24 = rail.QueryCollectionOperator(
            task_id='query_list_identify_changedrecords_24',
            name='changedrecordslist',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.md5 NOT IN (SELECT  referencefilewithmd5.md5 FROM  referencefilewithmd5)""",
        )

        query_list_changedrecordswithout_mandatoryfields_26 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswithout_mandatoryfields_26',
            query="""SELECT * FROM changedrecordslist WHERE ( changedrecordslist.firstname= "" OR  changedrecordslist.lastname= "" OR
                changedrecordslist.email= "" OR  changedrecordslist.employeeid= "" OR  changedrecordslist.startdate= "" OR
                changedrecordslist.loginanme= "" OR  changedrecordslist.employeetype= "" OR  changedrecordslist.department="" OR
                changedrecordslist.license= "" OR  NULLIF(firstname,'') IS NULL OR  NULLIF(lastname,'') IS NULL OR  NULLIF(email,'') IS NULL OR
                NULLIF(employeeid,'') IS NULL OR  NULLIF(startdate,'') IS NULL OR  NULLIF(loginanme,'') IS NULL OR  NULLIF(employeetype,'') IS NULL OR
                NULLIF(department,'') IS NULL OR  NULLIF(license,'') IS NULL)""",
        )

        if_query_list_changedrecordswithout_mandatoryfields_26_rows_greater_than_0_27 = rail.IfOperator(
            task_id='if_query_list_changedrecordswithout_mandatoryfields_26_rows_greater_than_0_27',
            test='''{{ result('query_list_changedrecordswithout_mandatoryfields_26','length') > 0 }}''',
            yes_task="add_invalid_records_log",
            no_task="query_list_changedrecordswith_mandatoryfields_29",
        )

        add_invalid_records_log = rail.WriteLogOperator(
            task_id='add_invalid_records_log',
            log="{{result('create_user_import_log_table')}}",
            items="{{result('query_list_changedrecordswithout_mandatoryfields_26')}}",
            severity='NA',
            message='Skipped',
            properties=lambda item: {
                'empid': item['employeeid'],
                'username': item['firstname'] + ' ' + item['lastname'],
                'action': 'pre-check',
                'status': 'ignored',
                'details': 'One or more mandatory fields are missing',
                'parentjob': rail.render_template("{{dag_run_ecid()}}"),
                'childjob': ''
            }
        )

        query_list_changedrecordswith_mandatoryfields_29 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswith_mandatoryfields_29',
            query="""SELECT * FROM  changedrecordslist WHERE ( changedrecordslist.firstname!= "" AND  changedrecordslist.lastname!= "" AND
                changedrecordslist.email!= "" AND  changedrecordslist.employeeid!= "" AND  changedrecordslist.startdate!= "" AND
                changedrecordslist.loginanme!= "" AND  changedrecordslist.employeetype!= "" AND  changedrecordslist.license!= "" AND
                changedrecordslist.department!= "")""",
        )

        if_query_list_changedrecordswith_mandatoryfields_29_rows_greater_than_0_30 = rail.IfOperator(
            task_id='if_query_list_changedrecordswith_mandatoryfields_29_rows_greater_than_0_30',
            test='''{{ result('query_list_changedrecordswith_mandatoryfields_29','length') > 0 }}''',
            yes_task="get_enabled_users_report_details",
            no_task="search_log_entries",
        )

        get_enabled_users_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_enabled_users_report_details',
            report_name=config.enabled_users_report
        )

        run_enabled_users_report_details = rail.run_report2(
            group_id='run_enabled_users_report_details',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_enabled_users_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        if_generate_report_5_payload_starts_with_nodata_6 = rail.IfOperator(
            task_id='if_generate_report_5_payload_starts_with_nodata_6',
            #pylint: disable = line-too-long
            test="{{(result('run_enabled_users_report_details.get_report_result')| load_json_artifact).reportGenerationResults[0].payload | starts_with('No Data')}}",
            yes_task="stop_7",
            no_task="if_generate_report_5_payload_not_starts_with_usernameloginnameemployeeiduseruriuserenddatedaydiff_8",
        )

        stop_7 = rail.FailOperator(
            task_id='stop_7',
            message='''No Data in the base report'''
        )

        if_generate_report_5_payload_not_starts_with_usernameloginnameemployeeiduseruriuserenddatedaydiff_8 = rail.IfOperator(
            task_id='if_generate_report_5_payload_not_starts_with_usernameloginnameemployeeiduseruriuserenddatedaydiff_8',
            #pylint: disable = line-too-long
            test="{{not (result('run_enabled_users_report_details.get_report_result')| load_json_artifact).reportGenerationResults[0].payload | starts_with('User Name,Login Name,Employee ID,UserUri,User End Date,daydiff')}}",
            yes_task="stop_9",
            no_task="parse_csv_10",
        )

        stop_9 = rail.FailOperator(
            task_id='stop_9',
            message='''Base report column order doesn't match'''
        )

        parse_csv_10 = rail.LoadCSVFileOperator(
            task_id='parse_csv_10',
            document="{{(result('run_enabled_users_report_details.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}"
        )

        load_enabled_users = rail.PythonOperator(
            task_id='load_enabled_users',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_10'))
        )

        get_all_custom_fields_42 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_42',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'division': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Division', 'uri', ' '),
                'position': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Position', 'uri', ' '),
                'employeestate': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Employee state', 'uri', ' '),
                'employeecity': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Employee City', 'uri', ' '),
                'employmentstatus': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Employment Status', 'uri', '- '),
            }
        )

        query_users_in_file_tobe_assigned_as_supervisor = rail.QueryCollectionOperator(
            task_id='query_users_in_file_tobe_assigned_as_supervisor',
            query="SELECT * FROM inputfilewithmd5 WHERE inputfilewithmd5.employeeid IN (SELECT DISTINCT inputfilewithmd5.supervisorid FROM inputfilewithmd5)",
        )

        load_users_tobe_assigned_as_supervisor = rail.PythonOperator(
            task_id='load_users_tobe_assigned_as_supervisor',
            python_callable=lambda: rail.load_all_records(
                rail.result('query_users_in_file_tobe_assigned_as_supervisor'))
        )

        query_list_getalldivisions_45 = rail.QueryCollectionOperator(
            task_id='query_list_getalldivisions_45',
            name='divisionfromfeed',
            query='''SELECT DISTINCT  inputfilewithmd5.division as divisionoption FROM  inputfilewithmd5 WHERE  inputfilewithmd5.division!= "" ''',
        )

        trigger_child_to_process_division = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_process_division',
            retries=0,
            trigger_dag_id=f'npsg_user_import_process_custom_field_division_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "customFieldUri": "{{ result('get_all_custom_fields_42').division }}"
            }
        )

        wait_for_child_to_process_division = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_process_division',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_process_division") }}'
        )

        query_list_getallpositions_47 = rail.QueryCollectionOperator(
            task_id='query_list_getallpositions_47',
            name='positionfromfeed',
            query="""SELECT DISTINCT  inputfilewithmd5.position as positionoption FROM  inputfilewithmd5 WHERE  inputfilewithmd5.position!= "" """,
        )

        trigger_child_to_process_position = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_process_position',
            retries=0,
            trigger_dag_id=f'npsg_user_import_process_custom_field_position_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "customFieldUri": "{{ result('get_all_custom_fields_42').position }}"
            }
        )

        wait_for_child_to_process_position = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_process_position',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_process_position") }}'
        )

        query_list_getallemployeestates_49 = rail.QueryCollectionOperator(
            task_id='query_list_getallemployeestates_49',
            name='employeestatefromfeed',
            query="""SELECT DISTINCT  inputfilewithmd5.employeestate FROM  inputfilewithmd5 WHERE  inputfilewithmd5.employeestate!= "" """,
        )

        trigger_child_to_process_employeestate = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_process_employeestate',
            retries=0,
            trigger_dag_id=f'npsg_user_import_process_custom_field_employee_state_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "customFieldUri": "{{ result('get_all_custom_fields_42').employeestate }}"
            }
        )

        wait_for_child_to_process_employeestate = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_process_employeestate',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_process_employeestate") }}'
        )

        get_all_custom_field_drop_down_optionsdivision_51 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsdivision_51',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_42').division }}"
            }
        )

        get_all_custom_field_drop_down_optionsposition_52 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsposition_52',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_42').position }}"
            }
        )

        get_all_custom_field_drop_down_optionsemployeestate_53 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsemployeestate_53',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_42').employeestate }}"
            }
        )

        get_all_custom_field_drop_down_optionsemployementstatus_54 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsemployementstatus_54',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_42').employmentstatus }}"
            }
        )

        get_data_location_list_service_55 = rail.RepliconServiceOperator(
            task_id='get_data_location_list_service_55',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:code",
                    "urn:replicon:location-list-column:location"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: [{
                'code': location['cells'][0].get('textValue'),
                'textvalue': (location['cells'][1].get('textValue')).lower(),
                'uri': location['cells'][1].get('uri')
            } for location in response['rows']]
        )

        get_enabled_departments_57 = rail.RepliconServiceOperator(
            task_id='get_enabled_departments_57',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
        )

        get_all_employee_type_details_58 = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details_58',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
        )

        get_all_time_zones_alltimezones_59 = rail.RepliconServiceOperator(
            task_id='get_all_time_zones_alltimezones_59',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_all_permission_allpermissions_60 = rail.RepliconServiceOperator(
            task_id='get_all_permission_allpermissions_60',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_policy_sets_get_all_policy_sets_61 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_get_all_policy_sets_61',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_all_public_licensed_products_get_all_public_licensed_products_62 = rail.RepliconServiceOperator(
            task_id='get_all_public_licensed_products_get_all_public_licensed_products_62',
            endpoint="/services/AccountManagementService1.svc/GetAllPublicLicensedProducts",
        )

        npsg_permission_mapper_search_entries_63 = rail.PythonOperator(
            task_id='npsg_permission_mapper_search_entries_63',
            python_callable=lambda:  list(
                filter(lambda x: x["allowed"] == "yes", config.permission_mapper))
        )

        create_add_update_child_triggered_list = rail.SetVariableOperator(
            task_id='create_add_update_child_triggered_list',
            name='childtriggeredlist',
            append=False,
            value=[]
        )

        foreach_query_list_changedrecordswith_mandatoryfields_29_64 = rail.ForEachOperator(
            task_id='foreach_query_list_changedrecordswith_mandatoryfields_29_64',
            items="{{ result('query_list_changedrecordswith_mandatoryfields_29') }}",
            start_task='invoke_custom_ruby_code_65',
            end_task='foreach_query_list_changedrecordswith_mandatoryfields_29_64_end'
        )

        invoke_custom_ruby_code_65 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_65',
            python_callable=lambda: {
                "user": rail.find_first_by_attr_and_get_attr(rail.result('load_enabled_users'), 'Login Name', rail.result(
                    'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['loginanme'], 'User Name', ''),
                "useruri": rail.find_first_by_attr_and_get_attr(rail.result('load_enabled_users'), 'Login Name', rail.result(
                    'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['loginanme'], 'UserUri', ''),
                "supervisordetails": rail.find_first_by_attr_and_get_attr(rail.result(
                    'load_users_tobe_assigned_as_supervisor'), 'employeeid', rail.result(
                    'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['supervisorid'])
            }
        )

        if_pluckcol3_smart_join_present_67 = rail.IfOperator(
            task_id='if_pluckcol3_smart_join_present_67',
            test=lambda: bool((rail.smartjoin_by_delim([item['permission_set'] for item in (list(
                filter(lambda entry: entry['management_level'] == rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['managementlevel'], rail.result(
                'npsg_permission_mapper_search_entries_63'))))]," | ")) if rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['managementlevel'] else (
                (rail.smartjoin_by_delim([item['permission_set'] for item in (list(
                filter(lambda entry: entry['job_faimly'] == rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['jobfamily'], rail.result(
                'npsg_permission_mapper_search_entries_63'))))]," | ")))),
            yes_task="invoke_custom_ruby_code_68",
            no_task="if_foreach_query_list_changedrecordswith_mandatoryfields_29_64_license_present_70",
        )

        def get_required_permission_uris_to_assign():
            permissionsets = rail.smartjoin_by_delim([entry['permission_set'] for entry in rail.result(
                'npsg_permission_mapper_search_entries_63') if entry['management_level'] == rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['managementlevel']], '|') if rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['managementlevel'] else rail.smartjoin_by_delim(
                [entry['permission_set'] for entry in rail.result('npsg_permission_mapper_search_entries_63') if entry['job_faimly'] == rail.result(
                    'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['jobfamily']], '|')
            permission_list = [{
                'permission': permission
            } for permission in permissionsets.split('|')]
            permission_list_with_uri = [{
                'name': permission['permission'],
                'uri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_allpermissions_60'), 'name', (permission['permission']).strip(), 'uri', ''),
                'userid': rail.result('foreach_query_list_changedrecordswith_mandatoryfields_29_64')['employeeid']
            } for permission in permission_list]
            return list(filter(lambda x: x['uri'] != '', permission_list_with_uri))

        invoke_custom_ruby_code_68 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_68',
            python_callable=get_required_permission_uris_to_assign
        )

        if_foreach_query_list_changedrecordswith_mandatoryfields_29_64_license_present_70 = rail.IfOperator(
            task_id='if_foreach_query_list_changedrecordswith_mandatoryfields_29_64_license_present_70',
            test='''{{ result('foreach_query_list_changedrecordswith_mandatoryfields_29_64').license | is_truthy }}''',
            yes_task="invoke_custom_ruby_code_71",
            no_task="if_output_user_blank_73",
        )

        def get_required_licenses_to_assign():
            licenses = rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_29_64')['license']
            license_list = [{
                'license': license
            } for license in licenses.split(' | ')]
            license_list_with_uri = [{
                'name': license['license'],
                'uri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_public_licensed_products_get_all_public_licensed_products_62'), 'displayText', (
                    license['license']).strip(), 'uri', ''),
                'employeeid': rail.result('foreach_query_list_changedrecordswith_mandatoryfields_29_64')['employeeid']
            } for license in license_list]
            return list(filter(lambda x: x['uri'] != '', license_list_with_uri))

        invoke_custom_ruby_code_71 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_71',
            python_callable=get_required_licenses_to_assign
        )

        if_output_user_blank_73 = rail.IfOperator(
            task_id='if_output_user_blank_73',
            test='''{{ result('invoke_custom_ruby_code_65').user | is_falsy }}''',
            yes_task="trigger_dag_run_npsg_user_import_npsg_create_user_v1_0async_74",
            no_task="trigger_dag_run_npsg_user_import_npsg_update_user_v1_0async_76",
        )

        def get_add_update_user_payload(action):
            user = rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_29_64')
            conf = {
                "firstname": user['firstname'],
                "lastname": user['lastname'],
                "email": user['email'],
                "employeeid": user['employeeid'],
                "startdate": user['startdate'],
                "enddate": user['enddate'],
                "employmentstatus": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_custom_field_drop_down_optionsemployementstatus_54'), 'displayText', user['employmentstatus'], 'uri', '') if user[
                    'employmentstatus'] else '',
                "division": user['division'],
                "position": user['position'],
                "employeestate": user['employeestate'],
                "employeecity": user['employeecity'],
                "loginanme": user['loginanme'],
                "supervisorid": user['supervisorid'],
                "department": user['department'],
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_enabled_departments_57'), 'displayText', user['department'], 'uri', '') if user['department'] else '',
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_employee_type_details_58'), 'displayText', user['employeetype'], 'uri', '') if user['employeetype'] else '',
                "employeetype": user['employeetype'],
                "loginenabled": user['loginenabled'],
                "license": [license['uri'] for license in rail.result(
                    'invoke_custom_ruby_code_71')] if rail.result('invoke_custom_ruby_code_71') and user['employeeid'] == rail.result(
                    'invoke_custom_ruby_code_71')[0]['employeeid'] else [],
                "jobfamily": user['jobfamily'],
                "managementlevel": user['managementlevel'],
                "location": user['location'],
                "locationuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_data_location_list_service_55'), 'code', user['location'], 'uri', '') if user['location'] else '',
                "punchtimenetry": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_policy_sets_get_all_policy_sets_61'), 'displayText', user['punchtimenetry'], 'uri', '') if user['punchtimenetry'] else '',
                "timesheettemplate": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_policy_sets_get_all_policy_sets_61'), 'displayText', user['timesheettemplate'], 'uri', '') if user['timesheettemplate'] else '',
                "timeofftemplate": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_policy_sets_get_all_policy_sets_61'), 'displayText', user['timeofftemplate'], 'uri', '') if user['timeofftemplate'] else '',
                "timezone": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_time_zones_alltimezones_59'), 'displayText', user['timezone'], 'uri', '') if user['timezone'] else '',
                "holidaycalendar": user['holidaycalendar'],
                "payrule": user['payrule'],
                "udfuri_division": rail.result('get_all_custom_fields_42')['division'],
                "udfuri_position": rail.result('get_all_custom_fields_42')['position'],
                "udfuri_employeestate": rail.result('get_all_custom_fields_42')['employeestate'],
                "udfuri_employeecity": rail.result('get_all_custom_fields_42')['employeecity'],
                "udfuri_employementstatus": rail.result('get_all_custom_fields_42')['employmentstatus'],
                "permissions": [permission['uri'] for permission in rail.result(
                    'invoke_custom_ruby_code_68')] if user['employeeid'] == (rail.result(
                    'invoke_custom_ruby_code_68')[0]['userid'] if rail.result('invoke_custom_ruby_code_68') else '') else [],
                "expensetemplate": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_policy_sets_get_all_policy_sets_61'), 'displayText', 'Expenses', 'uri', '') if 'Expense' in user['license'] else '',
                "employeestateuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_custom_field_drop_down_optionsemployeestate_53'), 'displayText', user['employeestate'], 'uri', '') if user[
                        'employeestate'] else '',
                "positionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_custom_field_drop_down_optionsposition_52'), 'displayText', user['position'], 'uri', '') if user['position'] else '',
                "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_custom_field_drop_down_optionsdivision_51'), 'displayText', user['division'], 'uri', '') if user['division'] else '',
                "callerjobid": rail.render_template("{{dag_run_ecid()}}"),
                "userimportlogtable": rail.result('create_user_import_log_table'),
                "supervisorlookup": rail.result('create_supervisor_check_lookup_table')
            }
            if action == 'update':
                conf.update({'useruri': rail.result(
                    'invoke_custom_ruby_code_65')['useruri']})
            return conf

        trigger_dag_run_npsg_user_import_npsg_create_user_v1_0async_74 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_npsg_user_import_npsg_create_user_v1_0async_74',
            retries=0,
            trigger_dag_id=f'npsg_user_import_create_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: get_add_update_user_payload('add')
        )

        add_create_user_dag_id = rail.SetVariableOperator(
            task_id='add_create_user_dag_id',
            name='childtriggeredlist',
            append=True,
            #pylint: disable = line-too-long
            value="{{result('trigger_dag_run_npsg_user_import_npsg_create_user_v1_0async_74')}}"
        )

        trigger_dag_run_npsg_user_import_npsg_update_user_v1_0async_76 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_npsg_user_import_npsg_update_user_v1_0async_76',
            retries=0,
            trigger_dag_id=f'npsg_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: get_add_update_user_payload('update')
        )

        add_update_user_dag_id = rail.SetVariableOperator(
            task_id='add_update_user_dag_id',
            name='childtriggeredlist',
            append=True,
            #pylint: disable = line-too-long
            value="{{result('trigger_dag_run_npsg_user_import_npsg_update_user_v1_0async_76')}}"
        )

        foreach_query_list_changedrecordswith_mandatoryfields_29_64_end = rail.EmptyOperator(
            task_id='foreach_query_list_changedrecordswith_mandatoryfields_29_64_end',
        )

        if_add_update_child_triggered = rail.IfOperator(
            task_id='if_add_update_child_triggered',
            test=lambda: bool(rail.get_dag_run_var('childtriggeredlist')),
            yes_task='get_task_ids_to_wait',
            no_task='npsg_supervisor_check_search_entries_79'
        )

        get_task_ids_to_wait = rail.PythonOperator(
            task_id = 'get_task_ids_to_wait',
            python_callable=lambda: rail.get_dag_run_var('childtriggeredlist')
        )

        wait_for_add_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_update_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('get_task_ids_to_wait') | to_json}}"
        )

        npsg_supervisor_check_search_entries_79 = rail.FilterLogEntriesOperator(
            task_id='npsg_supervisor_check_search_entries_79',
            log="{{result('create_supervisor_check_lookup_table')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        if_entry_col1_present_80 = rail.IfOperator(
            task_id='if_entry_col1_present_80',
            test='''{{ result('npsg_supervisor_check_search_entries_79','length') > 0 }}''',
            yes_task="trigger_dag_run_supervisor_assignment_child",
            no_task="search_user_import_logs",
        )

        trigger_dag_run_supervisor_assignment_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_supervisor_assignment_child',
            retries=0,
            items="{{result('npsg_supervisor_check_search_entries_79')}}",
            trigger_dag_id=f'npsg_user_import_supervisor_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": item['properties']['userempid'],
                "username": item['properties']['username'],
                "supervisorloginname": item['properties']['supervisorempid'],
                "parentjobid": item['properties']['jobid'],
                "childjobid": item['properties']['childjobid'],
                "useruri": item['properties']['useruri'],
                "action": item['properties']['action'],
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_allpermissions_60'), 'displayText', 'Supervisor', 'uri', ''),
                "enduserpermissionformanager": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_allpermissions_60'), 'displayText', 'Project Resource with Reports', 'uri', ''),
                "supeffectivedate": item['properties']['effectivedate'],
                "userimportlogtable": rail.result('create_user_import_log_table'),
                "supervisorlookup": rail.result('create_supervisor_check_lookup_table')
            }
        )

        wait_for_dag_run_supervisor_assignment_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_dag_run_supervisor_assignment_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_supervisor_assignment_child") }}'
        )

        search_user_import_logs = rail.FilterLogEntriesOperator(
            task_id='search_user_import_logs',
            log="{{result('create_user_import_log_table')}}",
            properties={
                'parentjob': "{{dag_run_ecid()}}"
            }
        )

        if_first_id_present_5 = rail.IfOperator(
            task_id='if_first_id_present_5',
            test='''{{ result('search_user_import_logs','length') > 0}}''',
            yes_task="invoke_custom_ruby_code_7",
            no_task="send_mail_15",
        )

        def get_logs_meta_data():
            logs = rail.load_all_records(
                rail.result('search_user_import_logs'))
            errorcheck = rail.find_first_by_attr_and_get_attr(
                logs, 'properties.status', 'error')
            errorlogsnumber = len(
                [log['properties']['status'] for log in logs if log['properties']['status'] == 'error']) if errorcheck else 0
            exceptioncheck = rail.find_first_by_attr_and_get_attr(
                logs, 'properties.status', 'exception')
            exceptionlogsnumber = len(
                [log['properties']['status'] for log in logs if log['properties']['status'] == 'exception']) if exceptioncheck else 0
            return {
                "errorcheck": errorcheck,
                "exceptioncheck": exceptioncheck,
                "subject": 'completed with errors' if errorcheck else ('completed with exceptions' if exceptioncheck else 'completed succesfully'),
                #pylint: disable = line-too-long
                "body": "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if errorcheck else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>",
                "errors": errorlogsnumber,
                "success": rail.result('query_list_changedrecordswith_mandatoryfields_29', 'length') - (errorlogsnumber + exceptionlogsnumber),
                "exception": exceptionlogsnumber
            }

        invoke_custom_ruby_code_7 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_7',
            python_callable=get_logs_meta_data
        )
        create_logs_csv = rail.WriteCSVFileOperator(
            task_id='create_logs_csv',
            source="{{ result('search_user_import_logs') }}",
            header=['employeeid',
                    'username',
                    'action',
                    'status',
                    'details',
                    'jobid'],
            row=lambda item: [
                item['properties']['empid'],
                item['properties']['username'],
                item['properties']['action'],
                item['properties']['status'],
                item['properties']['details'],
                item['properties']['parentjob'] + ' | ' + item['properties']['childjob']
            ],
        )

        upload_logs_upload_10 = rail.SFTPUploadFileOperator(
            task_id='upload_logs_upload_10',
            content='''{{ result('create_logs_csv') }}''',
            remote_filepath=config.log_filepath + '''/log_{{ result('log_formatted_time') }}_{{dag_run_ecid()}}.csv''',
        )

        send_mail_15 = rail.EmailOperator(
            task_id='send_mail_15',
            to="{%- if result('invoke_custom_ruby_code_7')['errorcheck'] -%}\
                    "+config.tenant_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            bcc="{%- if result('invoke_custom_ruby_code_7')['errorcheck'] -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() }}| Replicon user sync - {{ result('invoke_custom_ruby_code_7').subject }} - {{ result('log_formatted_time') }}''',
            html_content='''templates/success_mail.html''',
            params={
                'logfilepath': config.log_filepath
            },
        )

        rename_archivethereferencefile_99 = rail.SFTPMoveFileOperator(
            task_id='rename_archivethereferencefile_99',
            new_filename=config.archive_filepath +
            '''{{ dag_run_ecid() }}_npsg_reference.csv''',
            existing_filename=config.reference_filepath + '''npsg_reference.csv''',
        )

        upload_uploadthenewreferencefile_100 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadthenewreferencefile_100',
            content='''{{ result('create_csv_lines_create_m_d5filefor_inputfile_16') }}''',
            remote_filepath=config.reference_filepath + '''npsg_reference.csv''',
        )

        search_log_entries = rail.FilterLogEntriesOperator(
            task_id='search_log_entries',
            log="{{result('create_user_import_log_table')}}",
            properties={
                'parentjob': "{{dag_run_ecid()}}"
            }
        )

        if_output_loggers_greater_than_0_105 = rail.IfOperator(
            task_id='if_output_loggers_greater_than_0_105',
            test='''{{ result('search_log_entries','length') > 0 }}''',
            yes_task="create_csv_lines_106",
            no_task="send_mail_108",
        )

        create_csv_lines_106 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_106',
            source="{{ result('search_log_entries') }}",
            header=['employeeid',
                    'username',
                    'action',
                    'status',
                    'details',
                    'jobid'],
            row=[
                "{{ item.properties.empid }}",
                "{{ item.properties.username }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.properties.parentjob }} | {{ item.properties.childjob }}"
            ],
        )

        upload_107 = rail.SFTPUploadFileOperator(
            task_id='upload_107',
            content='''{{ result('create_csv_lines_106') }}''',
            remote_filepath=config.log_filepath +
            '''/log_{{ result('log_formatted_time') }}_{{ dag_run_ecid() }}.csv''',
        )

        send_mail_108 = rail.EmailOperator(
            task_id='send_mail_108',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Replicon user import completed successfully - {{ current_time() }} ''',
            html_content='''templates/success_with_no_changed_valid_records_mail.html''',
            params={
                'logfilepath': config.log_filepath
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        download_file >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> log_formatted_time >> parse_csv_15
        parse_csv_15 >> create_csv_lines_create_m_d5filefor_inputfile_16 >> download_downloadthereferencefile_17
        download_downloadthereferencefile_17 >> create_collection_create_list_from_csv_18 >> load_csv_create_list_from_csv_19
        load_csv_create_list_from_csv_19 >> create_collection_create_list_from_csv_19 >> create_user_import_log_table
        create_user_import_log_table >> create_supervisor_check_lookup_table >> query_list_identify_unchangedrecords_21
        query_list_identify_unchangedrecords_21 >> if_query_list_identify_unchangedrecords_21_rows_greater_than_0_22
        if_query_list_identify_unchangedrecords_21_rows_greater_than_0_22 >> rail.Label(
            'Yes') >> add_unchanged_records_log >> query_list_identify_changedrecords_24
        if_query_list_identify_unchangedrecords_21_rows_greater_than_0_22 >> rail.Label(
            'No') >> query_list_identify_changedrecords_24 >> query_list_changedrecordswithout_mandatoryfields_26
        query_list_changedrecordswithout_mandatoryfields_26 >> if_query_list_changedrecordswithout_mandatoryfields_26_rows_greater_than_0_27
        if_query_list_changedrecordswithout_mandatoryfields_26_rows_greater_than_0_27 >> rail.Label(
            'Yes') >> add_invalid_records_log >> query_list_changedrecordswith_mandatoryfields_29
        if_query_list_changedrecordswithout_mandatoryfields_26_rows_greater_than_0_27 >> rail.Label(
            'No') >> query_list_changedrecordswith_mandatoryfields_29 >> if_query_list_changedrecordswith_mandatoryfields_29_rows_greater_than_0_30
        if_query_list_changedrecordswith_mandatoryfields_29_rows_greater_than_0_30 >> rail.Label(
            'Yes') >> get_enabled_users_report_details >> run_enabled_users_report_details
        run_enabled_users_report_details >> if_generate_report_5_payload_starts_with_nodata_6
        if_generate_report_5_payload_starts_with_nodata_6 >> rail.Label(
            'Yes') >> stop_7 >> log_to_sumo
        if_generate_report_5_payload_starts_with_nodata_6 >> rail.Label(
            'No') >> if_generate_report_5_payload_not_starts_with_usernameloginnameemployeeiduseruriuserenddatedaydiff_8
        if_generate_report_5_payload_not_starts_with_usernameloginnameemployeeiduseruriuserenddatedaydiff_8 >> rail.Label(
            'Yes') >> stop_9 >> log_to_sumo
        if_generate_report_5_payload_not_starts_with_usernameloginnameemployeeiduseruriuserenddatedaydiff_8 >> rail.Label(
            'No') >> parse_csv_10 >> load_enabled_users >> get_all_custom_fields_42
        get_all_custom_fields_42 >> query_users_in_file_tobe_assigned_as_supervisor
        query_users_in_file_tobe_assigned_as_supervisor >> load_users_tobe_assigned_as_supervisor >> query_list_getalldivisions_45
        query_list_getalldivisions_45 >> trigger_child_to_process_division
        trigger_child_to_process_division >> wait_for_child_to_process_division >> query_list_getallpositions_47
        query_list_getallpositions_47 >> trigger_child_to_process_position >> wait_for_child_to_process_position >> query_list_getallemployeestates_49
        query_list_getallemployeestates_49 >> trigger_child_to_process_employeestate >> wait_for_child_to_process_employeestate
        wait_for_child_to_process_employeestate >> get_all_custom_field_drop_down_optionsdivision_51 >> get_all_custom_field_drop_down_optionsposition_52
        get_all_custom_field_drop_down_optionsposition_52 >> get_all_custom_field_drop_down_optionsemployeestate_53
        get_all_custom_field_drop_down_optionsemployeestate_53 >> get_all_custom_field_drop_down_optionsemployementstatus_54
        get_all_custom_field_drop_down_optionsemployementstatus_54 >> get_data_location_list_service_55 >> get_enabled_departments_57
        get_enabled_departments_57 >> get_all_employee_type_details_58 >> get_all_time_zones_alltimezones_59 >> get_all_permission_allpermissions_60
        get_all_permission_allpermissions_60 >> get_all_policy_sets_get_all_policy_sets_61
        get_all_policy_sets_get_all_policy_sets_61 >> get_all_public_licensed_products_get_all_public_licensed_products_62
        get_all_public_licensed_products_get_all_public_licensed_products_62 >> npsg_permission_mapper_search_entries_63
        npsg_permission_mapper_search_entries_63 >> create_add_update_child_triggered_list
        create_add_update_child_triggered_list >> foreach_query_list_changedrecordswith_mandatoryfields_29_64 >> invoke_custom_ruby_code_65
        invoke_custom_ruby_code_65 >> if_pluckcol3_smart_join_present_67
        if_pluckcol3_smart_join_present_67 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_68
        invoke_custom_ruby_code_68 >> if_foreach_query_list_changedrecordswith_mandatoryfields_29_64_license_present_70
        if_pluckcol3_smart_join_present_67 >> rail.Label(
            'No') >> if_foreach_query_list_changedrecordswith_mandatoryfields_29_64_license_present_70
        if_foreach_query_list_changedrecordswith_mandatoryfields_29_64_license_present_70 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_71 >> if_output_user_blank_73
        if_foreach_query_list_changedrecordswith_mandatoryfields_29_64_license_present_70 >> rail.Label(
            'No') >> if_output_user_blank_73
        if_output_user_blank_73 >> rail.Label(
            'Yes') >> trigger_dag_run_npsg_user_import_npsg_create_user_v1_0async_74 >> add_create_user_dag_id
        add_create_user_dag_id >> foreach_query_list_changedrecordswith_mandatoryfields_29_64_end
        if_output_user_blank_73 >> rail.Label(
            'No') >> trigger_dag_run_npsg_user_import_npsg_update_user_v1_0async_76 >> add_update_user_dag_id
        add_update_user_dag_id >> foreach_query_list_changedrecordswith_mandatoryfields_29_64_end
        foreach_query_list_changedrecordswith_mandatoryfields_29_64 >> foreach_query_list_changedrecordswith_mandatoryfields_29_64_end
        foreach_query_list_changedrecordswith_mandatoryfields_29_64_end >> if_add_update_child_triggered
        if_add_update_child_triggered >> rail.Label(
            'Yes') >> get_task_ids_to_wait >> wait_for_add_update_child >> npsg_supervisor_check_search_entries_79
        if_add_update_child_triggered >> rail.Label(
            'No') >> npsg_supervisor_check_search_entries_79
        npsg_supervisor_check_search_entries_79 >> if_entry_col1_present_80
        if_entry_col1_present_80 >> rail.Label(
            'Yes') >> trigger_dag_run_supervisor_assignment_child >> wait_for_dag_run_supervisor_assignment_child
        wait_for_dag_run_supervisor_assignment_child >> search_user_import_logs
        if_entry_col1_present_80 >> rail.Label(
            'No') >> search_user_import_logs >> if_first_id_present_5
        if_first_id_present_5 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_7 >> create_logs_csv >> upload_logs_upload_10 >> send_mail_15
        if_first_id_present_5 >> rail.Label(
            'No') >> send_mail_15 >> rename_archivethereferencefile_99 >> upload_uploadthenewreferencefile_100 >> log_to_sumo
        if_query_list_changedrecordswith_mandatoryfields_29_rows_greater_than_0_30 >> rail.Label(
            'No') >> search_log_entries >> if_output_loggers_greater_than_0_105
        if_output_loggers_greater_than_0_105 >> rail.Label(
            'Yes') >> create_csv_lines_106 >> upload_107 >> send_mail_108
        if_output_loggers_greater_than_0_105 >> rail.Label(
            'No') >> send_mail_108 >> rename_archivethereferencefile_99 >> upload_uploadthenewreferencefile_100 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
