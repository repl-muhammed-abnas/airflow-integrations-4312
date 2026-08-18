import hashlib
from datetime import timedelta, datetime
from nttdata.user_import.mappers import nttdata_user_mapper
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdata_user_import_nttdata_user_import_process_each_file_master{config.instance}',
        description=f'NttData User import - Process each file {config.instance}',
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
            new_filename=config.archive_filepath + "{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_formatted_dateandtime'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_formatted_dateandtime',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_formatted_dateandtime=rail.PythonOperator(
            task_id='log_formatted_dateandtime',
            python_callable= lambda:  datetime.now().strftime("%Y%m%dT%H%M%S")
        )

        if_file_ends_with_csv=rail.IfOperator(
            task_id='if_file_ends_with_csv',
            test='''{{ result('new_file_sensor') | ends_with('.csv')}}''',
            yes_task="parse_csv_inputfile",
            no_task="send_mail_incorrectfileformat",
        )

        send_mail_incorrectfileformat=rail.EmailOperator(
            task_id='send_mail_incorrectfileformat',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | User integration to Replicon failed - {{ result('log_formatted_dateandtime') }} ''',
            html_content= '''templates/incorrect_file_format_mail.html''',
        )

        parse_csv_inputfile=rail.LoadCSVFileOperator(
            task_id='parse_csv_inputfile',
            delimiter=',',
            document="{{result('download_file')}}"
        )

        compose_csv_with_md5=rail.WriteCSVFileOperator(
            task_id='compose_csv_with_md5',
            source="{{ result('parse_csv_inputfile') }}",
            header=['UserID',
                    'LegalFirstName',
                    'LegalLastName',
                    'Employeetype',
                    'Company',
                    'Authtype',
                    'status',
                    'EmpID',
                    'HireDate',
                    'Enddate',
                    'Email',
                    'Licenses',
                    'SupID',
                    'SupEffDate',
                    'Permissionset',
                    'Timesheettemplate',
                    'TimesheetPeriodType',
                    'TimesheetApprovalPath',
                    'ExpenseTemplate',
                    'ExpenseApprovalPath',
                    'Timezone',
                    'Workweek',
                    'HolidayCalendar',
                    'Workshechdule',
                    'InitalPayruleName',
                    'TimeoffTemplate',
                    'TimeoffType',
                    'TimeoffapprovalPath',
                    'DellBadgeID',
                    'Country',
                    'Jobcode',
                    'Jobcodestartdate',
                    'costcenterDell',
                    'Dellcostcenterstartdate',
                    'stateofprovince',
                    'worklocation',
                    'locationstartdate',
                    'grade',
                    'gradestartdate',
                    'costcenterntt',
                    'constcenterstartdate',
                    'OTEligibleCompensationInfo',
                    'OTEligibleStartDate',
                    'md5'],
            row= lambda item:[
                item['UserID'],
                item['LegalFirstName'],
                item['LegalLastName'],
                item['EmployeeType'],
                item['Company'],
                'urn:replicon:user-authentication-type:sso',
                item['Enabled'],
                item['EmployeeID'],
                item['HireDateUser'],
                item['EndDate'],
                item['EmailInformation'],
                'urn:replicon-saas:product:psm-enterprise',
                item['SupervisorPortalID'],
                item['SupervisorEffectiveStartDate'],
                item['PermissionSet'],
                item['TimesheetTemplate'] if item['TimesheetTemplate'] else 'Clarity Pilot Agile Timesheet',
                "System",
                "System Approval",
                null,
                null,
                item['TimeZone'],
                "urn:replicon:day-of-week:monday",
                item['HolidayCalendar'],
                item['WorkSchedule'],
                null,
                "Time Off",
                null,
                "Supervisor",
                item['DellBadgeID'],
                item['Country'],
                item['JobCode'],
                item['JobCodeStartDate'],
                item['CostCenterDell'],
                item['DellCostCenterStartDate'],
                item['StateOrProvince'],
                item['WorkLocation'],
                item['LocationStartDate'],
                item['Grade'],
                item['GradeStartDate'],
                item['CostCenterNTT'],
                item['NTTCostCenterStartDate'],
                null,
                null,
                hashlib.md5((str(str(item['UserID']) + '_' + str(item['LegalFirstName']) + '_' + str(item['LegalLastName']) + '_' +
                str(item['EmployeeType']) + '_' + str(item['Company']) + '_' + str(item['AuthenticationType']) + '_' +
                str(item['Enabled']) + '_' + str(item['EmployeeID']) + '_' + str(item['HireDateUser']) + '_' +
                str(item['EndDate']) + '_' + str(item['EmailInformation']) + '_' + str(item['Licenses']) + '_' +
                str(item['SupervisorPortalID']) + '_' + str(item['SupervisorEffectiveStartDate']) + '_' + str(item['PermissionSet']) + '_' +
                str(item['TimesheetTemplate']) + '_' + str(item['TimesheetPeriodType']) + '_' + str(item['TimesheetApprovalPath']) + '_' +
                str(item['ExpenseTemplate']) + '_' + str(item['ExpenseApprovalPath']) + '_' + str(item['TimeZone']) + '_' +
                str(item['WorkWeek']) + '_' + str(item['HolidayCalendar']) + '_' + str(item['WorkSchedule']) + '_' +
                str(item['WorkSchedule']) + '_' + str(item['InitialPayruleName']) + '_' + str(item['TimeOffTemplate']) + '_' +
                str(item['TimeOffType']) + '_' + str(item['TimeOffApprovalPath']) + '_' + str(item['DellBadgeID']) + '_' +
                str(item['Country']) + '_' + str(item['JobCode']) + '_' + str(item['JobCodeStartDate']) + '_' + str(item['CostCenterDell']) + '_' +
                str(item['DellCostCenterStartDate']) + '_' + str(item['StateOrProvince']) + '_' + str(item['WorkLocation']) + '_' +
                str(item['LocationStartDate']) + '_' + str(item['Grade']) + '_' + str(item['GradeStartDate']) + '_' + str(item['CostCenterNTT']) + '_' +
                str(item['NTTCostCenterStartDate']) + '_' + str(item['OTEligibleCompensationInfo']) + '_' + str(item['OTEligibleStartDate'])
                )).encode('utf-8')).hexdigest()
            ],
        )

        create_collection_inputfilewithmd5 = rail.CreateCollectionOperator(
            task_id='create_collection_inputfilewithmd5',
            source = "{{ result('compose_csv_with_md5') }}",
            name = "inputfilewithmd5",
            columns = {
                'UserID':'userid', 
                'LegalFirstName':'firstname', 
                'LegalLastName':'lastname', 
                'Employeetype':'employeetype', 
                'Company':'company', 
                'Authtype':'authtype', 
                'status':'enabled', 
                'EmpID':'empid', 
                'HireDate':'hiredate', 
                'Enddate':'enddate', 
                'Email':'email', 
                'Licenses':'license', 
                'SupID':'supid', 
                'SupEffDate':'supeffdate', 
                'Permissionset':'permissionset', 
                'Timesheettemplate':'timesheetemplate', 
                'TimesheetPeriodType':'timesheetperiod', 
                'TimesheetApprovalPath':'timesheetapprovapath', 
                'ExpenseTemplate':'expensetemplate', 
                'ExpenseApprovalPath':'expenseapprvalpath', 
                'Timezone':'timezone', 
                'Workweek':'workweek', 
                'HolidayCalendar':'holidaycalendar', 
                'Workshechdule':'workschedule', 
                'InitalPayruleName':'payrule', 
                'TimeoffTemplate':'timeofftemplate', 
                'TimeoffType':'timeofftype', 
                'TimeoffapprovalPath':'timeoffapproval', 
                'DellBadgeID':'dellbadgeid', 
                'Country':'country', 
                'Jobcode':'jobcode', 
                'Jobcodestartdate':'jobcodechangedate', 
                'costcenterDell':'costcenterdell', 
                'Dellcostcenterstartdate':'costcenterdellchangedate', 
                'stateofprovince':'stateorprovince', 
                'worklocation':'worklocation', 
                'locationstartdate':'locationstartdate', 
                'grade':'grade', 
                'gradestartdate':'gradechangedate', 
                'costcenterntt':'costcenterntt', 
                'constcenterstartdate':'costcenternttchangedate', 
                'OTEligibleCompensationInfo':'oteeligible', 
                'OTEligibleStartDate':'oteeligiblechangedate', 
                'md5':'md5'
            }
        )

        download_reference_file=rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath + "nttdataclarityref.csv"
        )

        load_csv_reference_file=rail.LoadCSVFileOperator(
            task_id="load_csv_reference_file",
            document="{{result('download_reference_file') }}",
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source = "{{ result('load_csv_reference_file') }}",
            name = "referencefilewithmd5",
            columns = {
                'UserID':'userid', 
                'LegalFirstName':'firstname', 
                'LegalLastName':'lastname', 
                'Employeetype':'employeetype', 
                'Company':'company', 
                'Authtype':'authtype', 
                'status':'enabled', 
                'EmpID':'empid', 
                'HireDate':'hiredate', 
                'Enddate':'enddate', 
                'Email':'email', 
                'Licenses':'license', 
                'SupID':'supid', 
                'SupEffDate':'supeffdate', 
                'Permissionset':'permissionset', 
                'Timesheettemplate':'timesheetemplate', 
                'TimesheetPeriodType':'timesheetperiod', 
                'TimesheetApprovalPath':'timesheetapprovapath', 
                'ExpenseTemplate':'expensetemplate', 
                'ExpenseApprovalPath':'expenseapprvalpath', 
                'Timezone':'timezone', 
                'Workweek':'workweek', 
                'HolidayCalendar':'holidaycalendar', 
                'Workshechdule':'workschedule', 
                'InitalPayruleName':'payrule', 
                'TimeoffTemplate':'timeofftemplate', 
                'TimeoffType':'timeofftype', 
                'TimeoffapprovalPath':'timeoffapproval', 
                'DellBadgeID':'dellbadgeid', 
                'Country':'country', 
                'Jobcode':'jobcode', 
                'Jobcodestartdate':'jobcodechangedate', 
                'costcenterDell':'costcenterdell', 
                'Dellcostcenterstartdate':'costcenterdellchangedate', 
                'stateofprovince':'stateorprovince', 
                'worklocation':'worklocation', 
                'locationstartdate':'locationstartdate', 
                'grade':'grade', 
                'gradestartdate':'gradechangedate', 
                'costcenterntt':'costcenterntt', 
                'constcenterstartdate':'costcenternttchangedate', 
                'OTEligibleCompensationInfo':'oteeligible', 
                'OTEligibleStartDate':'oteeligiblechangedate', 
                'md5':'md5'
            }
        )

        query_changed_records=rail.QueryCollectionOperator(
            task_id='query_changed_records',
            name = 'changed_records_list',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.md5 NOT IN (SELECT  referencefilewithmd5.md5 FROM  referencefilewithmd5)""",
        )

        query_changed_records_without_mandatory_fields=rail.QueryCollectionOperator(
            task_id='query_changed_records_without_mandatory_fields',
            query="""SELECT * FROM  changed_records_list WHERE ( changed_records_list.userid= "" OR  changed_records_list.firstname= "" OR
                    changed_records_list.lastname= "" OR  changed_records_list.employeetype= "" OR  changed_records_list.company= "" OR
                    changed_records_list.authtype= "" OR  changed_records_list.enabled= "" OR  changed_records_list.empid= "" OR
                    changed_records_list.hiredate= "" OR  changed_records_list.email= "" OR  changed_records_list.country= "" OR
                    changed_records_list.worklocation= "" OR  changed_records_list.grade= "")""",
        )

        create_nttdata_userimport_logs_lookuptable = rail.CreateLogOperator(
            task_id = 'create_nttdata_userimport_logs_lookuptable'
        )

        create_nttdata_supervisor_check_lookuptable = rail.CreateLogOperator(
            task_id = 'create_nttdata_supervisor_check_lookuptable'
        )

        log_mandatory_fields_value_missing=rail.WriteLogOperator(
            task_id='log_mandatory_fields_value_missing',
            items="{{result('query_changed_records_without_mandatory_fields')}}",
            log="{{ result('create_nttdata_userimport_logs_lookuptable') }}",
            message="na",
            severity="Ignored",
            properties={
                "userid": "{{item.userid}}",
                "username": "{{ item.firstname }}{{ item.lastname }}",
                "status": "ignored",
                "details": "One or more mandatory fields value is missing",
                "action": "pre-check",
                "childjobis": "{{dag_run_ecid()}}",
                "parentjobid": "{{ dag_run_ecid() }}",
            }
        )

        query_changed_records_with_mandatory_fields=rail.QueryCollectionOperator(
            task_id='query_changed_records_with_mandatory_fields',
            query="""SELECT * FROM  changed_records_list WHERE ( changed_records_list.userid!= "" AND  changed_records_list.firstname!= "" AND
                    changed_records_list.lastname!= "" AND  changed_records_list.employeetype!= "" AND  changed_records_list.company!= "" AND
                    changed_records_list.authtype!= "" AND  changed_records_list.enabled!= "" AND  changed_records_list.empid!= "" AND
                    changed_records_list.hiredate!= "" AND  changed_records_list.email!= "" AND  changed_records_list.country!= "" AND
                    changed_records_list.worklocation!= "" AND  changed_records_list.grade!= "")""",
        )

        if_changed_records_with_or_without_mandatory_fileds_present=rail.IfOperator(
            task_id='if_changed_records_with_or_without_mandatory_fileds_present',
            test='''{{ result('query_changed_records_with_mandatory_fields','length') > 0 or
                result('query_changed_records_without_mandatory_fields','length') > 0 }}''',
            yes_task="if_changed_records_with_mandatory_fields_present",
            no_task="send_mail_no_changed_records_found",
        )

        if_changed_records_with_mandatory_fields_present=rail.IfOperator(
            task_id='if_changed_records_with_mandatory_fields_present',
            test='''{{ result('query_changed_records_with_mandatory_fields','length') > 0 }}''',
            yes_task="get_user_reference_data_report_details",
            no_task="search_entries_in_supervisorcheck_lookuptable",
        )

        get_user_reference_data_report_details = rail.RepliconReportDetailsOperator(
            task_id = 'get_user_reference_data_report_details',
            report_name=config.user_reference_data_report
        )

        run_user_reference_report=rail.run_report2(
            group_id='run_user_reference_report',
            report_params=lambda:{
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_user_reference_data_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        if_report_has_no_data=rail.IfOperator(
            task_id='if_report_has_no_data',
            test="{{result('run_user_reference_report.get_report_result','has_data') | is_falsy}}",
            yes_task="fail_dag_wth_error",
            no_task="if_payload_doesnt_start_with_required_columns",
        )

        fail_dag_wth_error=rail.FailOperator(
            task_id='fail_dag_wth_error',
            message='''No Data in the base report'''
        )

        if_payload_doesnt_start_with_required_columns=rail.IfOperator(
            task_id='if_payload_doesnt_start_with_required_columns',
            #pylint:disable = line-too-long
            test="{{(result('run_user_reference_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | starts_with('User Name,User Email,Employee ID,useruri,Existing Replicon Users,Clarity Integration,Login Name,User Status') | is_falsy}}",
            yes_task="fail_dag_columnconfig_doesnt_match",
            no_task="parse_csv_from_user_report",
        )

        fail_dag_columnconfig_doesnt_match=rail.FailOperator(
            task_id='fail_dag_columnconfig_doesnt_match',
            message='''Base report column order doesn't match'''
        )

        parse_csv_from_user_report=rail.LoadCSVFileOperator(
            task_id='parse_csv_from_user_report',
            document = "{{(result('run_user_reference_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}"
        )

        get_entries_from_mapper=rail.PythonOperator(
            task_id='get_entries_from_mapper',
            python_callable= lambda: [entry for entry in nttdata_user_mapper.nttdata_user_mapper if entry['check'] == 'yes']
        )

        get_all_custom_fields=rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
            "objectUri": "urn:replicon:object-type:user"
            }
        )

        def get_cutomfielduris():
            all_customfields = rail.result('get_all_custom_fields')
            return {
                "country": rail.find_first_by_attr_and_get_attr(all_customfields,'displayText', "Country",'uri',''),
                "jobcode": rail.find_first_by_attr_and_get_attr(all_customfields,'displayText', "Job Code",'uri',''),
                "jobcodestartdate": rail.find_first_by_attr_and_get_attr(all_customfields,'displayText', "Job Code start date",'uri',''),
                "costcenterdell": rail.find_first_by_attr_and_get_attr(all_customfields,'displayText', "Cost Center-Dell",'uri',''),
                "dell_cost_center_start_date_dsi_data": rail.find_first_by_attr_and_get_attr(
                    all_customfields,'displayText', "Dell Cost CenterStart Date-DSI Data",'uri',''),
                "state_province": rail.find_first_by_attr_and_get_attr(all_customfields,'displayText', "State/Province",'uri',''),
                "dell_badge_id": rail.find_first_by_attr_and_get_attr(all_customfields,'displayText', "Dell Badge ID",'uri',''),
                "clarity_integration":rail.find_first_by_attr_and_get_attr(all_customfields,'displayText', "Clarity Integration",'uri',''),
                "existing_replicon_users": rail.find_first_by_attr_and_get_attr(
                    all_customfields,'displayText', "Existing Replicon Users",'uri','')
            }

        get_uris_of_required_customfields=rail.PythonOperator(
            task_id='get_uris_of_required_customfields',
            python_callable= get_cutomfielduris
        )

        get_all_custom_field_drop_down_options_country=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_country',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_uris_of_required_customfields').country }}"
            }
        )

        get_all_custom_field_drop_down_options_state_province=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_state_province',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_uris_of_required_customfields').state_province }}"
            }
        )

        get_all_custom_field_drop_down_options_clarity_integration=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_clarity_integration',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_uris_of_required_customfields').clarity_integration }}"
            }
        )

        get_all_custom_field_drop_down_options_existingrepliconuser=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_existingrepliconuser',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_uris_of_required_customfields').existing_replicon_users }}"
            }
        )

        get_data_department_list_service=rail.RepliconServiceOperator(
            task_id='get_data_department_list_service',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
        )

        get_enabled_locations=rail.RepliconServiceOperator(
            task_id='get_enabled_locations',
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
        )

        get_enabled_service_centers=rail.RepliconServiceOperator(
            task_id='get_enabled_service_centers',
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        get_enabled_cost_centers=rail.RepliconServiceOperator(
            task_id='get_enabled_cost_centers',
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters",
        )

        get_enabled_divisions=rail.RepliconServiceOperator(
            task_id='get_enabled_divisions',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        get_all_permission_sets=rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_scriptsallpayrules=rail.RepliconServiceOperator(
            task_id='get_all_scriptsallpayrules',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        get_all_approval_paths_timesheetapprovalpaths=rail.RepliconServiceOperator(
            task_id='get_all_approval_paths_timesheetapprovalpaths',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
        )

        get_all_approval_paths_timeoffapprovalpaths=rail.RepliconServiceOperator(
            task_id='get_all_approval_paths_timeoffapprovalpaths',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
        )

        get_all_policy_sets=rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_all_holiday_calendars=rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )

        get_all_time_zones=rail.RepliconServiceOperator(
            task_id='get_all_time_zones',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_all_office_schedules=rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        get_data_employee_type_group_list_service=rail.RepliconServiceOperator(
            task_id='get_data_employee_type_group_list_service',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
        )

        get_enabled_dropdownoptions_for_stateprovince=rail.RepliconServiceOperator(
            task_id='get_enabled_dropdownoptions_for_stateprovince',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda:{
                "customFieldUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user-defined-field:21caa067-ccc7-401e-8af8-99fe5a3aefc4"
            }
        )

        if_state_province_uri_is_not_present=rail.IfOperator(
            task_id='if_state_province_uri_is_not_present',
            test=lambda: not bool( rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_dropdownoptions_for_stateprovince'),'displayText',
                    rail.result('get_uris_of_required_customfields')['state_province'],'uri','')),
            yes_task="create_list_of_existing_state_province",
            no_task="foreach_changed_record_with_mandatory_fields",
        )

        create_list_of_existing_state_province = rail.PythonOperator(
            task_id = 'create_list_of_existing_state_province',
            python_callable=lambda: [{
                'name': state['displayText'],
                'isEnabled': state['isEnabled']
            } for state in rail.result('get_enabled_dropdownoptions_for_stateprovince')]
        )

        foreach_changed_record_with_mandatory_fields=rail.ForEachOperator(
            task_id='foreach_changed_record_with_mandatory_fields',
            items="{{ result('query_changed_records_with_mandatory_fields') }}",
            start_task = 'get_state_province_to_check_timezone',
            end_task = 'foreach_changed_record_with_mandatory_fields_end'
        )

        def get_state_province():
            record = rail.result('foreach_changed_record_with_mandatory_fields')
            return ( ( record['stateorprovince'] if (('USA' or 'CAN') in record['country']) else 'Any' ) if record['country'] else 'Any')

        get_state_province_to_check_timezone=rail.PythonOperator(
            task_id='get_state_province_to_check_timezone',
            python_callable= get_state_province
        )

        def get_stateprovince_for_holiday():
            record = rail.result('foreach_changed_record_with_mandatory_fields')
            return ( ( record['stateorprovince'] if (('AU' or 'AUS' or 'CAN' or 'MYS') in record['country']) else 'Any' ) if record['country'] else 'Any' )

        get_state_province_to_check_holiday=rail.PythonOperator(
            task_id='get_state_province_to_check_holiday',
            python_callable= get_stateprovince_for_holiday
        )

        def get_usercheck_and_mappers():
            users = rail.load_all_records(rail.result('parse_csv_from_user_report'))
            record = rail.result('foreach_changed_record_with_mandatory_fields')
            mapperentries = rail.result('get_entries_from_mapper')
            holidaycalendar = (list(filter(lambda entry: entry['type'] == 'Holiday Calendar' and entry['country'] == record['country'] and
                                entry['state'] == rail.result('get_state_province_to_check_holiday'),mapperentries)))
            timezone = (list(filter(lambda entry: entry['type'] == 'Time Zone' and entry['country'] == record['country'] and
                            entry['state'] == rail.result('get_state_province_to_check_timezone'),mapperentries)))
            activities = (list(filter(lambda entry: entry['type'] == 'Activity' and entry['country'] == record['country'] and
                            entry['state'] == 'Any',mapperentries)))
            return {
                "user": list(filter(lambda user: user['Login Name']==record['userid'] ,users)),
                "useruri": rail.find_first_by_attr_and_get_attr(users,'Login Name',record['userid'],'useruri',''),
                "status": rail.find_first_by_attr_and_get_attr(users,'Login Name',record['userid'],'User Status',''),
                "existingusercheck": rail.find_first_by_attr_and_get_attr(users,'Login Name',record['userid'],'Existing Replicon Users',''),
                "holidaycalendar": holidaycalendar[0]['value'] if holidaycalendar else '',
                "timezone": timezone[0]['value'] if timezone else '',
                "activities": activities[0]['value'] if activities else '',
                "claritycheck": rail.find_first_by_attr_and_get_attr(users,'Login Name',record['userid'],'Clarity Integration',''),
            }

        get_user_check_and_mappers=rail.PythonOperator(
            task_id='get_user_check_and_mappers',
            python_callable= get_usercheck_and_mappers
        )

        if_stateorprovince_present_but_not_in_dropdownoptions=rail.IfOperator(
            task_id='if_stateorprovince_present_but_not_in_dropdownoptions',
            test=lambda: bool(rail.result('foreach_changed_record_with_mandatory_fields')['stateorprovince'] and not rail.find_first_by_attr_and_get_attr(
                rail.result('get_enabled_dropdownoptions_for_stateprovince'),'displayText',rail.result(
                'foreach_changed_record_with_mandatory_fields')['stateorprovince'],'uri','')),
            yes_task="create_new_stateprovince_list",
            no_task="get_all_custom_field_dropdownoptions_stateprovince",
        )

        create_new_stateprovince_list = rail.PythonOperator(
            task_id = 'create_new_stateprovince_list',
            python_callable=lambda: [{
                'name': rail.result('foreach_changed_record_with_mandatory_fields')['stateorprovince'],
                'isEnabled': 'true'
            }]
        )

        log_final_stateprovince_list=rail.PythonOperator(
            task_id='log_final_stateprovince_list',
            python_callable= lambda: ((rail.result('create_list_of_existing_state_province')) + rail.result('create_new_stateprovince_list')) if
                                rail.result('create_list_of_existing_state_province') else rail.result('create_new_stateprovince_list')
        )

        put_drop_down_optionsforstate=rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsforstate',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda:{
                "customFieldUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user-defined-field:21caa067-ccc7-401e-8af8-99fe5a3aefc4",
                "customFieldDropDownOptionUris": rail.result('log_final_stateprovince_list')
            }
        )

        get_all_custom_field_dropdownoptions_stateprovince=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_dropdownoptions_stateprovince',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_uris_of_required_customfields').state_province }}"
            }
        )

        create_child_dag_runs_list = rail.SetVariableOperator(
            task_id='create_child_dag_runs_list',
            name='child_dag_runs_list',
            value=[]
        )

        if_useruri_not_present=rail.IfOperator(
            task_id='if_useruri_not_present',
            test='''{{ result('get_user_check_and_mappers').useruri | is_falsy }}''',
            yes_task="trigger_add_user_child",
            no_task="if_existingusercheck_no_but_claritycheck_equals_yes",
        )

        def get_add_user_child_payload():
            user = rail.result('foreach_changed_record_with_mandatory_fields')
            return {
                "loginname": user['userid'],
                "firstname": user['firstname'],
                "lastname": user['lastname'],
                "employeetype": user['employeetype'],
                "department": user['company'],
                "location": user['worklocation'],
                "authenticationtype": user['authtype'],
                "enabled": user['enabled'],
                "employeeid": user['empid'],
                "startdate": user['hiredate'],
                "enddate": user['enddate'],
                "emailaddress": user['email'],
                "initialsupervisorloginname": user['supid'],
                "permissionsets": user['permissionset'],
                "timesheettemplate": user['timesheetemplate'],
                "timesheetperiodtype": user['timesheetperiod'],
                "timesheetapprovalpath": user['timesheetapprovapath'],
                "timezone": rail.result('get_user_check_and_mappers')['timezone'],
                "workweek": user['workweek'],
                "holidaycalendar": rail.result('get_user_check_and_mappers')['holidaycalendar'],
                "initialschedulename": user['workschedule'],
                "timeofftemplate": user['timeofftemplate'],
                "timeoffapprovalpath": user['timeoffapproval'],
                "initialpayrulename": user['payrule'],
                "dellbadgeid": user['dellbadgeid'],
                "country": user['country'],
                "jobcode": user['jobcode'],
                "jobcodestartdate": user['jobcodechangedate'],
                "costcenterdell": user['costcenterdell'],
                "costcenterdellstartdate": user['costcenterdellchangedate'],
                "state": user['stateorprovince'],
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_data_department_list_service'),'displayText',
                    (user['company'].split("|"))[-1],'uri','') if rail.result('get_data_department_list_service')[0]['uri'] else null,
                "locationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_locations'),'displayText', user['worklocation'],'uri',''),
                "servicecenteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_service_centers'),'displayText',user['grade'],'uri'),
                "costcenteruri": (rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_cost_centers'),'displayText',
                    user['oteeligible'],'uri')) if user['oteeligible'] else null,
                "costcentereffectivedate": user['costcenterdellchangedate'],
                "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divisions'),'displayText', user['costcenterntt'],'uri',''),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),'displayText', "Supervisor",'uri'),
                "timesheettemplateuri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_policy_sets'),'displayText', user['timesheetemplate'],'uri',''),
                "timesheetperioduri": "urn:replicon:timesheet-period-type:system",
                "timesheetapprovalpathuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_approval_paths_timesheetapprovalpaths'),
                    'displayText', "System Approval",'uri',''),
                "license": user['license'],
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_employee_type_group_list_service'),'displayText',
                    user['employeetype'],'uri','') if rail.result('get_data_employee_type_group_list_service')[0]['uri'] else null,
                "supervisoreffectivedate": user['supeffdate'],
                "holidaycalendaruri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendars'),'displayText',
                    rail.result('get_user_check_and_mappers')['holidaycalendar'] ,'uri',''),
                "officescheduleuri": (rail.find_first_by_attr_and_get_attr(rail.result('get_all_office_schedules'),'displayText',
                    user['workschedule'],'uri','')) if user['workschedule']  else rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_office_schedules'),'displayText','8*5MF','uri',''),
                "timeofftemplateuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'),'displayText', "Time Off",'uri'),
                "timeoffapprovalpathuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_approval_paths_timeoffapprovalpaths'),'displayText',"Supervisor",'uri'),
                "payruleuri": null,
                "dellbadgeudfuri": rail.result('get_uris_of_required_customfields')['dell_badge_id'],
                "countryudfuri": rail.result('get_uris_of_required_customfields')['country'],
                "countrydropdownuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_country'),'displayText',
                    user['country'],'uri','') if rail.result('get_all_custom_field_drop_down_options_country')[0]['uri'] else null,
                "jobcodeudfuri": rail.result('get_uris_of_required_customfields')['jobcode'],
                "jobcodestartdateudfuri": rail.result('get_uris_of_required_customfields')['jobcodestartdate'],
                "costcenterdelludfuri": rail.result('get_uris_of_required_customfields')['costcenterdell'],
                "costcenterdellstartdateudfuri": rail.result('get_uris_of_required_customfields')['dell_cost_center_start_date_dsi_data'],
                "stateudfuri": rail.result('get_uris_of_required_customfields')['state_province'],
                "statedropdownuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_state_province'),'displayText',
                    user['stateorprovince'],'uri','') if rail.result('get_all_custom_field_drop_down_options_state_province')[0]['uri'] else null,
                "timeoffuri": null,
                "clarityuserudfuri": rail.result('get_uris_of_required_customfields')['clarity_integration'],
                "clarityuserdropdownuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_clarity_integration'),
                    'displayText','Yes','uri','') if rail.result('get_all_custom_field_drop_down_options_clarity_integration')[0]['uri'] else null,
                "parentjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "locationname": user['worklocation'],
                "activities": rail.result('get_user_check_and_mappers')['activities'],
                "existinguserudfuri": rail.result('get_uris_of_required_customfields')['existing_replicon_users'],
                "existinguserdropdownuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_existingrepliconuser'),
                    'displayText','No','uri','') if rail.result('get_all_custom_field_drop_down_options_existingrepliconuser')[0]['uri'] else null,
                "grade": user['grade'],
                "costcenterntt": user['costcenterntt'],
                "stateorprovince": user['stateorprovince'],
                "logslookuptable": rail.result('create_nttdata_userimport_logs_lookuptable'),
                "callerjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "supervisorchecklookup": rail.result('create_nttdata_supervisor_check_lookuptable')
            }

        trigger_add_user_child=rail.TriggerDagRunOperator(
            task_id='trigger_add_user_child',
            retries=0,
            trigger_dag_id=f'nttdata_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_add_user_child_payload
        )

        if_existingusercheck_no_but_claritycheck_equals_yes=rail.IfOperator(
            task_id='if_existingusercheck_no_but_claritycheck_equals_yes',
            test="{{ result('get_user_check_and_mappers').existingusercheck == 'No' and result('get_user_check_and_mappers').claritycheck == 'Yes' }}",
            yes_task="trigger_update_user_child",
            no_task="log_user_not_updated_as_existinguser_flag_set_yes",
        )

        def get_update_user_child_payload():
            user = rail.result('foreach_changed_record_with_mandatory_fields')
            holidaycalendar = (list(filter(lambda entry: (entry['type'] == 'Holiday Calendar' and entry['country'] == user['country'] and
                entry['state'] == rail.result('get_state_province_to_check_timezone')),rail.result('get_entries_from_mapper'))))
            return {
                "loginname": user['userid'],
                "firstname": user['firstname'],
                "lastname": user['lastname'],
                "employeetype": user['employeetype'],
                "department": user['company'],
                "location": user['worklocation'],
                "authenticationtype": user['authtype'],
                "enabled": user['enabled'],
                "employeeid": user['empid'],
                "startdate": user['hiredate'],
                "enddate": user['enddate'],
                "emailaddress": user['email'],
                "initialsupervisorloginname": user['supid'],
                "permissionsets": user['permissionset'],
                "timesheettemplate": user['timesheetemplate'],
                "timesheetperiodtype": user['timesheetperiod'],
                "timesheetapprovalpath": user['timesheetapprovapath'],
                "timezone": rail.result('get_user_check_and_mappers')['timezone'],
                "workweek": user['workweek'],
                "holidaycalendar": holidaycalendar[0]['value'] if holidaycalendar else '',
                "initialschedulename": user['workschedule'],
                "timeofftemplate": user['timeofftemplate'],
                "timeoffapprovalpath": user['timeoffapproval'],
                "initialpayrulename": user['payrule'],
                "dellbadgeid": user['dellbadgeid'],
                "country": user['country'],
                "jobcode": user['jobcode'],
                "jobcodestartdate": user['jobcodechangedate'],
                "costcenterdell": user['costcenterdell'],
                "costcenterdellstartdate": user['costcenterdellchangedate'],
                "state": user['stateorprovince'],
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_data_department_list_service'),'displayText',
                    (user['company'].split("|"))[-1],'uri','') if rail.result('get_data_department_list_service')[0]['uri'] else null,
                "locationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_locations'),'displayText', user['worklocation'],'uri',''),
                "servicecenteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_service_centers'),'displayText',user['grade'],'uri'),
                "costcenteruri": ( rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_cost_centers'),'displayText',
                    user['oteeligible'],'uri')) if user['oteeligible'] else null,
                "costcentereffectivedate": user['oteeligiblechangedate'],
                "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divisions'),'displayText', user['costcenterntt'],'uri',''),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),'displayText', "Supervisor",'uri'),
                "timesheettemplateuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_policy_sets'),'displayText', user['timesheetemplate'],'uri',''),
                "timesheetperioduri": "urn:replicon:timesheet-period-type:system",
                "timesheetapprovalpathuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_approval_paths_timesheetapprovalpaths'),'displayText',"System Approval",'uri',''),
                "license": user['license'],
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_employee_type_group_list_service'),'displayText',
                    user['employeetype'],'uri','') if rail.result('get_data_employee_type_group_list_service')[0]['uri'] else null,
                "supervisoreffectivedate": user['supeffdate'],
                "holidaycalendaruri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendars'),'displayText',rail.result(
                    'get_user_check_and_mappers')['holidaycalendar'] ,'uri',''),
                "officescheduleuri": (rail.find_first_by_attr_and_get_attr(rail.result('get_all_office_schedules'),'displayText',
                    user['workschedule'],'uri','')) if user['workschedule']  else rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_office_schedules'),'displayText','8*5MF','uri',''),
                "timeofftemplateuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'),'displayText', "Time Off",'uri'),
                "timeoffapprovalpathuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_approval_paths_timeoffapprovalpaths'),'displayText',"Supervisor",'uri'),
                "payruleuri": null,
                "dellbadgeudfuri": rail.result('get_uris_of_required_customfields')['dell_badge_id'],
                "countryudfuri": rail.result('get_uris_of_required_customfields')['country'],
                "countrydropdownuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_country'),'displayText',
                    user['country'],'uri','') if rail.result('get_all_custom_field_drop_down_options_country')[0]['uri'] else null,
                "jobcodeudfuri": rail.result('get_uris_of_required_customfields')['jobcode'],
                "jobcodestartdateudfuri": rail.result('get_uris_of_required_customfields')['jobcodestartdate'],
                "costcenterdelludfuri": rail.result('get_uris_of_required_customfields')['costcenterdell'],
                "costcenterdellstartdateudfuri": rail.result('get_uris_of_required_customfields')['dell_cost_center_start_date_dsi_data'],
                "stateudfuri": rail.result('get_uris_of_required_customfields')['state_province'],
                "statedropdownuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_state_province'),'displayText',
                    user['stateorprovince'],'uri','') if rail.result('get_all_custom_field_drop_down_options_state_province')[0]['uri'] else null,
                "timeoffuri": null,
                "clarityuserudfuri": rail.result('get_uris_of_required_customfields')['clarity_integration'],
                "clarityuserdropdownuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_clarity_integration'),
                    'displayText','Yes','uri','') if rail.result('get_all_custom_field_drop_down_options_clarity_integration')[0]['uri'] else null,
                "parentjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "locationname": user['worklocation'],
                "activities": rail.result('get_user_check_and_mappers')['activities'],
                "useruri": rail.result('get_user_check_and_mappers')['useruri'],
                "timezoneuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_zones'),'ianaName',rail.result(
                    'get_user_check_and_mappers')['timezone'],'uri',''),
                "locationchangedate": user['locationstartdate'],
                "servicecenterchangedate": user['gradechangedate'],
                "divisionchangedate": user['costcenternttchangedate'],
                "servicecenter": user['grade'],
                "costcenter": user['oteeligible'],
                "division": user['costcenterntt'],
                "supervisorchecklookup": rail.result('create_nttdata_supervisor_check_lookuptable'),
                "callerjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "logslookuptable": rail.result('create_nttdata_userimport_logs_lookuptable'),
            }

        trigger_update_user_child=rail.TriggerDagRunOperator(
            task_id='trigger_update_user_child',
            retries=0,
            trigger_dag_id=f'nttdata_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_update_user_child_payload
        )

        insert_child_dag_id_to_list = rail.SetVariableOperator(
            task_id = 'insert_child_dag_id_to_list',
            append=True,
            name="{{result('create_child_dag_runs_list')}}",
            value="{{result('trigger_add_user_child') or result('trigger_update_user_child')}}"
        )

        log_user_not_updated_as_existinguser_flag_set_yes=rail.WriteLogOperator(
            task_id='log_user_not_updated_as_existinguser_flag_set_yes',
            log="{{ result('create_nttdata_userimport_logs_lookuptable') }}",
            message="na",
            severity="Skipped",
            properties={
                #pylint: disable = line-too-long
                "userid": "{{result('foreach_changed_record_with_mandatory_fields').userid}}",
                "username": "{{ result('foreach_changed_record_with_mandatory_fields').firstname }} {{ result('foreach_changed_record_with_mandatory_fields').lastname }}",
                "action": "update",
                "status": "Skipped",
                "details": 'User not updated  since "Existing Replicon users" flag set to "Yes"',
                "childjobis": "{{dag_run_ecid()}}",
                "parentjobid": "{{ dag_run_ecid() }}"
            }
        )

        foreach_changed_record_with_mandatory_fields_end=rail.EmptyOperator(
            task_id='foreach_changed_record_with_mandatory_fields_end',
        )

        if_update_or_add_user_child_triggered = rail.IfOperator(
            task_id = 'if_update_or_add_user_child_triggered',
            test="{{result('insert_child_dag_id_to_list') | is_truthy }}",
            yes_task='wait_for_child_dags',
            no_task='search_entries_in_supervisorcheck_lookuptable'
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('insert_child_dag_id_to_list').value | to_json }}"
        )

        search_entries_in_supervisorcheck_lookuptable=rail.FilterLogEntriesOperator(
            task_id='search_entries_in_supervisorcheck_lookuptable',
            log="{{result('create_nttdata_supervisor_check_lookuptable')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        if_entry_present=rail.IfOperator(
            task_id='if_entry_present',
            test="{{ result('search_entries_in_supervisorcheck_lookuptable','length') > 0 }}",
            yes_task="trigger_child_supervisor_assignment",
            no_task="log_formatted_date_and_time",
        )

        trigger_child_supervisor_assignment=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_supervisor_assignment',
            retries=0,
            items="{{ result('search_entries_in_supervisorcheck_lookuptable') }}",
            trigger_dag_id=f'nttdata_user_import_supervisor_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "loginname": item['properties']['userloginname'],
                "username": item['properties']['username'],
                "supervisorloginname": item['properties']['supervisorloginname'],
                "parentjobid": rail.render_template("{{dag_run_ecid()}}"),
                "childjobid": item['properties']['childjobid'],
                "useruri": item['properties']['useruri'],
                "action": item['properties']['action'],
                "enduserpermissionformanager": null,
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),'displayText', "Supervisor",'uri',''),
                "supeffectivedate": item['properties']['effectivedate'] if item['properties']['effectivedate'] else datetime.now().strftime('%Y-%m-%d'),
                "logslookuptable": rail.result('create_nttdata_userimport_logs_lookuptable'),
                "supervisorchecklookup": rail.result('create_nttdata_supervisor_check_lookuptable'),
                "jobid": item['properties']['jobid']
            }
        )

        wait_for_supervisor_assignment_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_assignment_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_supervisor_assignment") }}'
        )

        log_formatted_date_and_time=rail.PythonOperator(
            task_id='log_formatted_date_and_time',
            python_callable= lambda: datetime.now().strftime("%Y%m%dT%H%M%S")
        )

        search_entries_in_user_import_logs_lookuptable = rail.FilterLogEntriesOperator(
            task_id = 'search_entries_in_user_import_logs_lookuptable',
            log="{{result('create_nttdata_userimport_logs_lookuptable')}}",
            properties={
                'parentjobid': "{{dag_run_ecid()}}"
            }
        )

        if_logs_not_present = rail.IfOperator(
            task_id = 'if_logs_not_present',
            test="{{result('search_entries_in_user_import_logs_lookuptable','length') < 1}}",
            yes_task='fail_job_with_error',
            no_task='if_logs_present'
        )

        fail_job_with_error = rail.FailOperator(
            task_id = 'fail_job_with_error',
            message='No entry present'
        )

        if_logs_present = rail.IfOperator(
            task_id = 'if_logs_present',
            test="{{result('search_entries_in_user_import_logs_lookuptable','length') > 0}}",
            yes_task='compose_logs_csv',
            no_task='archive_reference_file'
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id = 'compose_logs_csv',
            source="{{result('search_entries_in_user_import_logs_lookuptable')}}",
            header=['username',
                    'loginname',
                    'importaction',
                    'status',
                    'details',
                    'jobid'],
            row=[
                "{{item.properties.username}}",
                "{{item.properties.userid}}",
                "{{item.properties.action}}",
                "{{item.properties.status}}",
                "{{item.properties.details}}",
                "{{item.properties.parentjobid}}|{{item.properties.childjobis}}",
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs_csv')}}",
            output_file_name="{{ result('log_formatted_dateandtime') }}_logs_{{result('new_file_sensor') | file_name}}",
            expires_in_seconds=7*24*60*60,
        )

        def get_statuses():
            logentries = rail.load_all_records(rail.result('search_entries_in_user_import_logs_lookuptable'))
            error_present = rail.find_first_by_attr_and_get_attr(logentries,'status','Error','status','')
            exception_present = rail.find_first_by_attr_and_get_attr(logentries,'status','Exception','status','')
            return {
                'errorcheck': error_present,
                'exceptioncheck': exception_present,
                'subject': "completed with errors" if error_present else ( "completed with exceptions" if exception_present else 'completed succesfully' ),
                #pylint: disable = line-too-long
                'body': "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if error_present else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
            }

        get_import_statuses = rail.PythonOperator(
            task_id = 'get_import_statuses',
            python_callable=get_statuses
        )

        send_completion_mail=rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc="{%- if result('get_import_statuses').errorcheck -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject="{{ get_company_key() }}| User import {{ result('get_import_statuses').subject }} - {{ result('log_formatted_dateandtime') }}",
            html_content= '''templates/completed_mail.html''',
        )

        archive_reference_file=rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            new_filename=config.input_filepath + "/Archive/{{ result('log_formatted_dateandtime') }}_nttdataclarityref.csv",
            existing_filename=config.reference_filepath + 'nttdataclarityref.csv',
        )

        upload_new_reference_file=rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('compose_csv_with_md5') }}''',
            remote_filepath= config.reference_filepath + 'nttdataclarityref.csv',
        )

        send_mail_no_changed_records_found=rail.EmailOperator(
            task_id='send_mail_no_changed_records_found',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | UserImport completed - No change records found - {{ result('log_formatted_dateandtime') }} ''',
            html_content= '''templates/no_change_records_found_mail.html''',
        )

        archive_referencefile=rail.SFTPMoveFileOperator(
            task_id='archive_referencefile',
            new_filename=config.input_filepath + "/Archive/{{ result('log_formatted_dateandtime') }}_nttdataclarityref.csv",
            existing_filename=config.reference_filepath + 'nttdataclarityref.csv',
        )

        upload_new_referencefile=rail.SFTPUploadFileOperator(
            task_id='upload_new_referencefile',
            content='''{{ result('compose_csv_with_md5') }}''',
            remote_filepath= config.reference_filepath + 'nttdataclarityref.csv',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        download_file >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_formatted_dateandtime
        log_formatted_dateandtime >> if_file_ends_with_csv
        if_file_ends_with_csv >> rail.Label('No')  >> send_mail_incorrectfileformat >> finish
        if_file_ends_with_csv >> rail.Label('Yes') >> parse_csv_inputfile >> compose_csv_with_md5 >> create_collection_inputfilewithmd5
        create_collection_inputfilewithmd5 >> download_reference_file >> load_csv_reference_file >> create_referencefile_collection >> query_changed_records
        query_changed_records >> query_changed_records_without_mandatory_fields >> create_nttdata_userimport_logs_lookuptable
        create_nttdata_userimport_logs_lookuptable >> create_nttdata_supervisor_check_lookuptable >> log_mandatory_fields_value_missing
        log_mandatory_fields_value_missing >> query_changed_records_with_mandatory_fields
        query_changed_records_with_mandatory_fields >> if_changed_records_with_or_without_mandatory_fileds_present
        if_changed_records_with_or_without_mandatory_fileds_present >> rail.Label('Yes')  >> if_changed_records_with_mandatory_fields_present
        if_changed_records_with_mandatory_fields_present >> rail.Label(
            'Yes') >> get_user_reference_data_report_details >> run_user_reference_report >> if_report_has_no_data
        if_changed_records_with_mandatory_fields_present >> rail.Label('No') >> search_entries_in_supervisorcheck_lookuptable
        if_report_has_no_data >> rail.Label('Yes')  >> fail_dag_wth_error >> finish
        if_report_has_no_data >> rail.Label('No') >> if_payload_doesnt_start_with_required_columns
        if_payload_doesnt_start_with_required_columns >> rail.Label('Yes')  >> fail_dag_columnconfig_doesnt_match >> finish
        if_payload_doesnt_start_with_required_columns >> rail.Label('No') >> parse_csv_from_user_report >> get_entries_from_mapper >> get_all_custom_fields
        get_all_custom_fields >> get_uris_of_required_customfields >> get_all_custom_field_drop_down_options_country
        get_all_custom_field_drop_down_options_country >> get_all_custom_field_drop_down_options_state_province
        get_all_custom_field_drop_down_options_state_province >> get_all_custom_field_drop_down_options_clarity_integration
        get_all_custom_field_drop_down_options_clarity_integration >> get_all_custom_field_drop_down_options_existingrepliconuser
        get_all_custom_field_drop_down_options_existingrepliconuser >> get_data_department_list_service >> get_enabled_locations >> get_enabled_service_centers
        get_enabled_service_centers >> get_enabled_cost_centers >> get_enabled_divisions >> get_all_permission_sets >> get_all_scriptsallpayrules
        get_all_scriptsallpayrules >> get_all_approval_paths_timesheetapprovalpaths >> get_all_approval_paths_timeoffapprovalpaths >> get_all_policy_sets
        get_all_policy_sets >> get_all_holiday_calendars >> get_all_time_zones >> get_all_office_schedules >> get_data_employee_type_group_list_service
        get_data_employee_type_group_list_service >> get_enabled_dropdownoptions_for_stateprovince >> if_state_province_uri_is_not_present
        if_state_province_uri_is_not_present >> rail.Label('Yes')  >> create_list_of_existing_state_province >> foreach_changed_record_with_mandatory_fields
        if_state_province_uri_is_not_present >> rail.Label('No') >> foreach_changed_record_with_mandatory_fields >> get_state_province_to_check_timezone
        get_state_province_to_check_timezone >> get_state_province_to_check_holiday >> get_user_check_and_mappers
        get_user_check_and_mappers >> if_stateorprovince_present_but_not_in_dropdownoptions
        if_stateorprovince_present_but_not_in_dropdownoptions >> rail.Label('Yes')  >> create_new_stateprovince_list >> log_final_stateprovince_list
        log_final_stateprovince_list >> put_drop_down_optionsforstate >> get_all_custom_field_dropdownoptions_stateprovince
        if_stateorprovince_present_but_not_in_dropdownoptions >> rail.Label(
            'No') >> get_all_custom_field_dropdownoptions_stateprovince >> create_child_dag_runs_list >> if_useruri_not_present
        if_useruri_not_present >> rail.Label(
            'Yes') >> trigger_add_user_child >> insert_child_dag_id_to_list >> foreach_changed_record_with_mandatory_fields_end
        if_useruri_not_present >> rail.Label('No') >> if_existingusercheck_no_but_claritycheck_equals_yes
        if_existingusercheck_no_but_claritycheck_equals_yes >> rail.Label(
            'Yes')  >> trigger_update_user_child >> insert_child_dag_id_to_list >> foreach_changed_record_with_mandatory_fields_end
        if_existingusercheck_no_but_claritycheck_equals_yes >> rail.Label(
            'No') >> log_user_not_updated_as_existinguser_flag_set_yes >> foreach_changed_record_with_mandatory_fields_end
        foreach_changed_record_with_mandatory_fields >> foreach_changed_record_with_mandatory_fields_end >> if_update_or_add_user_child_triggered
        if_update_or_add_user_child_triggered >> rail.Label('Yes') >> wait_for_child_dags >> search_entries_in_supervisorcheck_lookuptable >> if_entry_present
        if_update_or_add_user_child_triggered >> rail.Label('No') >> search_entries_in_supervisorcheck_lookuptable >> if_entry_present
        if_entry_present >> rail.Label('Yes') >> trigger_child_supervisor_assignment >> wait_for_supervisor_assignment_child >> log_formatted_date_and_time
        if_entry_present >> rail.Label('No') >> log_formatted_date_and_time >> search_entries_in_user_import_logs_lookuptable
        search_entries_in_user_import_logs_lookuptable >> if_logs_not_present >> rail.Label('Yes') >> fail_job_with_error >> finish
        if_logs_not_present >> rail.Label('No') >> if_logs_present >> rail.Label(
            'Yes') >> compose_logs_csv >> generate_download_link >> get_import_statuses >> send_completion_mail >> archive_reference_file
        if_logs_present >> rail.Label('No') >> archive_reference_file >> upload_new_reference_file >> finish >> log_to_sumo
        if_changed_records_with_or_without_mandatory_fileds_present >> rail.Label(
            'No') >> send_mail_no_changed_records_found >> archive_referencefile >> upload_new_referencefile >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
