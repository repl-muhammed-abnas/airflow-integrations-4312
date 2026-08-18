
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'hawaiigas_user_import_hawaiigas_new_users_{config.instance}',
        description=f'Live|HawaiiGas_New users {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_departmentlist_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_departmentlist_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_department_uri(response,dag_run):
            all_departments = response['rows']
            matching_department = list(filter(lambda department: department['cells'][1]['textValue'] == dag_run.conf['department'],all_departments))
            return matching_department[0]['cells'][0]['uri'] if matching_department else ''

        get_departmentlist_3=rail.RepliconServiceOperator(
            task_id='get_departmentlist_3',
            endpoint="/services/DepartmentListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "1000",
              "columnUris": [
                "urn:replicon:department-list-column:department",
                "urn:replicon:department-list-column:code"
              ],
              "sort": [],
              "filterExpression": {
                "leftExpression": {
                  "leftExpression": null,
                  "operatorUri": null,
                  "rightExpression": null,
                  "value": null,
                  "filterDefinitionUri": "urn:replicon:department-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                  "leftExpression": null,
                  "operatorUri": null,
                  "rightExpression": null,
                  "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": "{{ dag_run.conf.department }}",
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                  },
                  "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
              }
            },
            data_handler=get_department_uri
        )

        def get_task_state(task_id):
            return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

        get_all_employee_type_details_7=rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details_7',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
        )

        log_employeetypeuri_8=rail.PythonOperator(
            task_id='log_employeetypeuri_8',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr( rail.result(
                'get_all_employee_type_details_7'),'name',dag_run.conf['classid'],'uri','')
        )

        if_log_departmenturi_6_blank_9=rail.IfOperator(
            task_id='if_log_departmenturi_6_blank_9',
            test='''{{ result('get_departmentlist_3') | is_falsy  or result('log_employeetypeuri_8') | is_falsy }}''',
            yes_task="hawaiigas_userimport_logs_prod_add_entry_10",
            no_task="get_hire_date",
        )

        hawaiigas_userimport_logs_prod_add_entry_10=rail.WriteLogOperator(
            task_id='hawaiigas_userimport_logs_prod_add_entry_10',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employee'] + "|" + dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Exception",
                "details": rail.smartjoin_by_delim(("User not added as " +
                ("" if rail.result('get_departmentlist_3') else "department not available in Replicon,") +
                ("" if rail.result('log_employeetypeuri_8') else "employee type not available in Replicon") + "|" +
                rail.render_template("{{dag_run_ecid()}}")).split(','),','),
                "jobid": dag_run.conf['callerjobid']
            }
        )

        def get_date_obj(datestring):
            dateobj = datetime.strptime(datestring,'%m/%d/%Y')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        get_hire_date=rail.PythonOperator(
            task_id='get_hire_date',
            python_callable= lambda dag_run: get_date_obj(dag_run.conf['hiredate'])
        )

        log_emailaddressderived_15=rail.PythonOperator(
            task_id='log_emailaddressderived_15',
            python_callable= lambda dag_run:  (str((dag_run.conf['firstname'][0]).lower() if dag_run.conf['firstname'] else '') +
                                str((dag_run.conf['lastname']).lower() if dag_run.conf['lastname'] else '')) +
                                "@hawaiigas.com" if (str((dag_run.conf['firstname'][0]).lower() if dag_run.conf['firstname'] else '') +
                                str((dag_run.conf['lastname']).lower() if dag_run.conf['lastname'] else '')) else ""
        )

        createuser_19=rail.RepliconServiceOperator(
            task_id='createuser_19',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=lambda dag_run: {
              "user": {
                "target": {
                  "uri": null,
                  "loginName": dag_run.conf['employee'],
                  "parameterCorrelationId": null
                },
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "emailAddress": rail.result('log_emailaddressderived_15'),
                "employeeId": dag_run.conf['employee'],
                "department": {
                  "uri": rail.result('get_departmentlist_3'),
                  "name": null,
                  "parent": null,
                  "parameterCorrelationId": null
                },
                "supervisorAssignmentSchedule": null,
                "schedulePolicySchedule": [
                  {
                    "schedulePolicy": {
                      "officeScheduleUri": null,
                      "name": "Default Schedule",
                      "officeSchedule": null,
                      "scheduleTypeUri": null
                    },
                    "effectiveDate": null
                  }
                ],
                "workWeekStartDayUri": null,
                "employmentDateRange": {
                  "startDate": {
                    "year": rail.result('get_hire_date')['year'],
                    "month": rail.result('get_hire_date')['month'],
                    "day": rail.result('get_hire_date')['day']
                  },
                  "endDate": null,
                  "relativeDateRangeUri": null,
                  "relativeDateRangeAsOfDate": null
                },
                "securityConfiguration": {
                  "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                  ],
                  "isLoginEnabled": "true",
                  "loginName": dag_run.conf['employee'],
                  "password": null
                },
                "holidayCalendar": null,
                "timeOffPolicy": null,
                "permissionSets": [
                {
                    "uri": null,
                    "name": "Gen3 User - No Report Access"
                  }
                  ],
                "policySets": [
                {
                    "uri": null,
                    "name": "Gen3 Timesheet"
                  },
                  {
                    "uri": null,
                    "name": "Default Timeoff Template"
                  }
                ],
                "employeeType": {
                  "uri": rail.result('log_employeetypeuri_8'),
                  "name": null
                },
                "timesheetPeriodTypeUri": null,
                "costRateSchedule": null,
                "payrollRateSchedule": null,
                "defaultBillingRate": null,
                "timesheetApprovalPath": null,
                "expenseApprovalPath": null,
                "timeOffApprovalPath": null,
                "customFieldValues": [],
                "assignedActivities": [],
                "timeZone": {
                  "uri": "urn:replicon:time-zone:pacific-honolulu",
                  "IANAName": null
                },
                "overtimeRuleAssignmentSchedule": null,
                "validationRuleAssignmentSchedule": null,
                "locationSchedule": [],
                "divisionSchedule": [],
                "costCenterSchedule": [],
                "serviceCenterSchedule": [],
                "policyDataAccessScopes": [],
                "policyDataAccessScopes2": [],
                "payRuleScriptSchedule": []
              }
            }
        )

        remove_time_offtype_21=rail.RepliconServiceOperator(
            task_id='remove_time_offtype_21',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
              "userUri": "{{ result('createuser_19').uri }}",
              "timeOffTypeUris": [ ]
            }
        )

        get_all_customfieldsfor_user_22=rail.RepliconServiceOperator(
            task_id='get_all_customfieldsfor_user_22',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
              "objectUri": "urn:replicon:object-type:user"
            }
        )

        if_request_employmenttype_present_23=rail.IfOperator(
            task_id='if_request_employmenttype_present_23',
            test='''{{ dag_run.conf.employmenttype | is_truthy }}''',
            yes_task="log_get_urifor_employment_type_24",
            no_task="if_request_supervisor_present_47",
        )

        log_get_urifor_employment_type_24=rail.PythonOperator(
            task_id='log_get_urifor_employment_type_24',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                              'get_all_customfieldsfor_user_22'),'displayText', "Employment Type",'uri','')
        )

        get_dropdownoptionsfor_employmenttype_25=rail.RepliconServiceOperator(
            task_id='get_dropdownoptionsfor_employmenttype_25',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
              "customFieldUri": "{{ result('log_get_urifor_employment_type_24') }}"
            }
        )

        def get_dropdown_uri_and_status(dag_run):
            all_options = rail.result('get_dropdownoptionsfor_employmenttype_25')
            required_dropdown = list(filter(lambda dropdown: dropdown['displayText'] == dag_run.conf['employmenttype'],all_options))
            return {
              'uri': required_dropdown[0]['uri'] if required_dropdown else '',
              'status': required_dropdown[0]['isEnabled'] if required_dropdown else ''
            }

        get_dropdownoption_uri_and_status=rail.PythonOperator(
            task_id='get_dropdownoption_uri_and_status',
            python_callable= get_dropdown_uri_and_status
        )

        if_log_dropdownoptionuri_26_blank_28=rail.IfOperator(
            task_id='if_log_dropdownoptionuri_26_blank_28',
            test='''{{ result('get_dropdownoption_uri_and_status').uri | is_falsy }}''',
            yes_task="get_final_list_of_dropdownoptions",
            no_task="if_log_dropdownoptionstatus_27_equals_to_false_35",
        )

        def get_list_of_dropdownoptions(dag_run):
            current_dropdownoptions = rail.result('get_dropdownoptionsfor_employmenttype_25')
            dropdownoption = [{
                'target':{
                    'uri': option['uri'],
                    'name': option['displayText']
                },
                'name': option['displayText'],
                'isEnabled': 'true'
            } for option in current_dropdownoptions]
            dropdownoption.append({
                'target':{
                    'uri': null,
                    'name': dag_run.conf['employmenttype']
                },
                'name': dag_run.conf['employmenttype'],
                'isEnabled': 'true'
            })
            return dropdownoption

        get_final_list_of_dropdownoptions=rail.PythonOperator(
            task_id='get_final_list_of_dropdownoptions',
            python_callable= get_list_of_dropdownoptions
        )

        putnewdropdownoption_34=rail.RepliconServiceOperator(
            task_id='putnewdropdownoption_34',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda:{
              "customFieldUri": rail.result('log_get_urifor_employment_type_24'),
              "customFieldDropDownOptionUris": rail.result('get_final_list_of_dropdownoptions')
            }
        )

        if_log_dropdownoptionstatus_27_equals_to_false_35=rail.IfOperator(
            task_id='if_log_dropdownoptionstatus_27_equals_to_false_35',
            test=lambda: rail.result('get_dropdownoption_uri_and_status')['status'] is False,
            yes_task="get_final_list_of_dropdown_options",
            no_task="get_dropdownoptionsfor_employmenttype_44",
        )

        def get_list_of_dropdown_options(dag_run):
            current_dropdownoptions = rail.result('get_dropdownoptionsfor_employmenttype_25')
            dropdownoption = [{
                'target':{
                    'uri': option['uri'],
                    'name': option['displayText']
                },
                'name': option['displayText'],
                'isEnabled': 'true' if option['displayText'] == dag_run.conf['employmenttype'] else option['isEnabled']
            } for option in current_dropdownoptions]
            return dropdownoption


        get_final_list_of_dropdown_options=rail.PythonOperator(
            task_id='get_final_list_of_dropdown_options',
            python_callable=get_list_of_dropdown_options
        )

        putnewdropdownoption_43=rail.RepliconServiceOperator(
            task_id='putnewdropdownoption_43',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda:{
              "customFieldUri": rail.result('log_get_urifor_employment_type_24'),
              "customFieldDropDownOptionUris": rail.result('get_final_list_of_dropdown_options')
            }
        )

        get_dropdownoptionsfor_employmenttype_44=rail.RepliconServiceOperator(
            task_id='get_dropdownoptionsfor_employmenttype_44',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
              "customFieldUri": "{{ result('log_get_urifor_employment_type_24') }}"
            }
        )

        log_dropdownoptionuritobeassignedtotheuser_45=rail.PythonOperator(
            task_id='log_dropdownoptionuritobeassignedtotheuser_45',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
              'get_dropdownoptionsfor_employmenttype_44'),'displayText', dag_run.conf['employmenttype'],'uri','')
        )

        assign_drop_downoptionfor_employment_type_46=rail.RepliconServiceOperator(
            task_id='assign_drop_downoptionfor_employment_type_46',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
              "objectUri": "{{ result('createuser_19').uri }}",
              "customFieldUri": "{{ result('log_get_urifor_employment_type_24') }}",
              "customFieldDropDownOptionUri": "{{ result('log_dropdownoptionuritobeassignedtotheuser_45') }}"
            }
        )

        if_request_supervisor_present_47=rail.IfOperator(
            task_id='if_request_supervisor_present_47',
            test='''{{ dag_run.conf.supervisor | is_truthy }}''',
            yes_task="searchsupervisor_48",
            no_task="get_enabled_time_offtypes_72",
        )

        def get_uri_and_status(response,dag_run):
            users_found = response['rows']
            supervisor = {}
            for user in users_found:
                if user['cells'][0]['textValue'] == dag_run.conf['supervisor']:
                    supervisor = user
                    break
            return {
                'uri': supervisor['cells'][0]['uri'] if supervisor else '',
                'status': supervisor['cells'][1]['textValue'] if supervisor else ''
            }

        searchsupervisor_48=rail.RepliconServiceOperator(
            task_id='searchsupervisor_48',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "1000",
              "columnUris": [
                "urn:replicon:user-list-column:login-name",
                "urn:replicon:user-list-column:enabled"
              ],
              "sort": [],
              "filterExpression": {
                "leftExpression": {
                  "leftExpression": null,
                  "operatorUri": null,
                  "rightExpression": null,
                  "value": null,
                  "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                  "leftExpression": null,
                  "operatorUri": null,
                  "rightExpression": null,
                  "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": "{{ dag_run.conf.supervisor }}",
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                  },
                  "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
              }
            },
            data_handler=get_uri_and_status
        )

        if_log_getthesupervisoruri_51_present_53=rail.IfOperator(
            task_id='if_log_getthesupervisoruri_51_present_53',
            test='''{{ result('searchsupervisor_48').uri | is_truthy  and result('searchsupervisor_48').status | matches('True') }}''',
            yes_task="getpermissionsassigned_54",
            no_task="if_log_getthesupervisoruri_51_present_68",
        )

        getpermissionsassigned_54=rail.RepliconServiceOperator(
            task_id='getpermissionsassigned_54',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
              "userUri": "{{ result('searchsupervisor_48').uri }}"
            }
        )

        log_checkifthesupervisorpermissionisassigned_55=rail.PythonOperator(
            task_id='log_checkifthesupervisorpermissionisassigned_55',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getpermissionsassigned_54'),'policyUri','urn:replicon:policy:supervision','user.uri',null)
        )

        if_log_checkifthesupervisorpermissionisassigned_55_blank_56=rail.IfOperator(
            task_id='if_log_checkifthesupervisorpermissionisassigned_55_blank_56',
            test='''{{ result('log_checkifthesupervisorpermissionisassigned_55') | is_falsy }}''',
            yes_task="get_allpermissionsets_61",
            no_task="supervisor_assignment_schedule_66",
        )

        get_allpermissionsets_61=rail.RepliconServiceOperator(
            task_id='get_allpermissionsets_61',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        def get_required_permission_sets_uri():
            assigned_permissions = rail.result('getpermissionsassigned_54')
            required_permissions = [{
                'uri': permission['permissionSet']['uri']
            } for permission in assigned_permissions if permission['policyUri'] != 'urn:replicon:policy:user']
            required_permissions.append({
                'uri':rail.find_first_by_attr_and_get_attr(rail.result('get_allpermissionsets_61'),'displayText','Gen3 Supervisor','uri','')
            })
            required_permissions.append({
                'uri':rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_allpermissionsets_61'),'displayText','Gen3 User - Substitute User Access','uri','')
            })
            return [permission['uri'] for permission in required_permissions if permission['uri'] != '']

        get_all_permission_sets_required_uri=rail.PythonOperator(
            task_id='get_all_permission_sets_required_uri',
            python_callable=get_required_permission_sets_uri
        )

        assign_permissionsetsto_supervisor_65=rail.RepliconServiceOperator(
            task_id='assign_permissionsetsto_supervisor_65',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda:{
              "userUri": rail.result('searchsupervisor_48')['uri'],
              "permissionSetUris": rail.result('get_all_permission_sets_required_uri')
            }
        )

        supervisor_assignment_schedule_66=rail.RepliconServiceOperator(
            task_id='supervisor_assignment_schedule_66',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
              "userUri": "{{ result('createuser_19').uri }}",
              "initialSupervisorUri": "{{ result('searchsupervisor_48').uri }}",
              "scheduleEntries": []
            }
        )

        if_log_getthesupervisoruri_51_present_68=rail.IfOperator(
            task_id='if_log_getthesupervisoruri_51_present_68',
            test='''{{ result('searchsupervisor_48').uri | is_truthy  and not (result('searchsupervisor_48').status |  matches('True')) }}''',
            yes_task="log_forlogging_69",
            no_task="if_log_getthesupervisoruri_51_blank_70",
        )

        log_forlogging_69=rail.PythonOperator(
            task_id='log_forlogging_69',
            python_callable= lambda:  "Supervisor not assigned since " +  "Supervisor is in disabled status."
        )

        if_log_getthesupervisoruri_51_blank_70=rail.IfOperator(
            task_id='if_log_getthesupervisoruri_51_blank_70',
            test='''{{ result('searchsupervisor_48').uri | is_falsy }}''',
            yes_task="hawaii_gas_supervisor_lookup_prod_add_entry_71",
            no_task="get_enabled_time_offtypes_72",
        )

        hawaii_gas_supervisor_lookup_prod_add_entry_71=rail.WriteLogOperator(
            task_id='hawaii_gas_supervisor_lookup_prod_add_entry_71',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "userloginname": "{{ dag_run.conf.employee }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}",
                "enduseruri": "{{ result('createuser_19').uri }}"
            }
        )

        get_enabled_time_offtypes_72=rail.RepliconServiceOperator(
            task_id='get_enabled_time_offtypes_72',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        def get_required_timeofftype_uris():
            timeofftypes = [{
                'uri': rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_offtypes_72'),'displayText','Sick','uri',''),
                'name': 'Sick'
            },{
                'uri': rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_offtypes_72'),'displayText','Vacation','uri',''),
                'name': 'Vacation'
            }]
            return [timeofftype['uri'] for timeofftype in timeofftypes]

        get_timeofftypes_to_assign_uris=rail.PythonOperator(
            task_id='get_timeofftypes_to_assign_uris',
            python_callable=get_required_timeofftype_uris
        )

        if_log_time_offuriwhenonlyonetimeofftypeispresent_76_present_77=rail.IfOperator(
            task_id='if_log_time_offuriwhenonlyonetimeofftypeispresent_76_present_77',
            test='''{{ result('get_timeofftypes_to_assign_uris') | is_truthy }}''',
            yes_task="assign_time_offtype_78",
            no_task="if_request_vacationbalance_present_79",
        )

        assign_time_offtype_78=rail.RepliconServiceOperator(
            task_id='assign_time_offtype_78',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda:{
              "userUri": rail.result('createuser_19')['uri'],
              "timeOffTypeUris": rail.result('get_timeofftypes_to_assign_uris')
            }
        )

        if_request_vacationbalance_present_79=rail.IfOperator(
            task_id='if_request_vacationbalance_present_79',
            test='''{{ dag_run.conf.vacationbalance | is_truthy  or dag_run.conf.sickbalance | is_truthy }}''',
            yes_task="get_all_scripts_validationscripts_80",
            no_task="if_request_status_equals_to_inactive_102",
        )

        get_all_scripts_validationscripts_80=rail.RepliconServiceOperator(
            task_id='get_all_scripts_validationscripts_80',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
        )

        get_all_timebalanceeventscripts_81=rail.RepliconServiceOperator(
            task_id='get_all_timebalanceeventscripts_81',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        get_required_script_uris=rail.PythonOperator(
            task_id='get_required_script_uris',
            python_callable= lambda: {
                'startingbalancescript': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_timebalanceeventscripts_81'),'displayText', "Starting Balance Set To",'uri',''),
                'preventbalanceoverdrawscript': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_scripts_validationscripts_80'),'displayText', "Prevent balance overdraw",'uri','')
            }
        )

        get_today_date_object=rail.PythonOperator(
            task_id='get_today_date_object',
            python_callable= lambda: {
                'day': datetime.now().day,
                'month': datetime.now().month,
                'year': datetime.now().year
            }
        )

        get_timesheet_for_date2_87=rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_87',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data={
              "userUri": "{{ result('createuser_19').uri }}",
              "date": {
                "year": "{{ result('get_today_date_object').year }}",
                "month": "{{ result('get_today_date_object').month }}",
                "day": "{{ result('get_today_date_object').day }}"
              },
              "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        get_timesheet_details_88=rail.RepliconServiceOperator(
            task_id='get_timesheet_details_88',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
              "timesheetUri": "{{ result('get_timesheet_for_date2_87').timesheet.uri }}"
            }
        )

        get_effective_date_object=rail.PythonOperator(
            task_id='get_effective_date_object',
            python_callable= lambda: {
                'day': int(rail.result('get_timesheet_details_88')['dateRange']['startDate']['day']),
                'month': int(rail.result('get_timesheet_details_88')['dateRange']['startDate']['month']),
                'year': int(rail.result('get_timesheet_details_88')['dateRange']['startDate']['year']),
            }
        )

        log_ifsickbalanceispresentthengetsicktimeoffuri_92=rail.PythonOperator(
            task_id='log_ifsickbalanceispresentthengetsicktimeoffuri_92',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_enabled_time_offtypes_72'),'displayText','Sick','uri','') if dag_run.conf['sickbalance'] else null
        )

        if_log_ifsickbalanceispresentthengetsicktimeoffuri_92_present_93=rail.IfOperator(
            task_id='if_log_ifsickbalanceispresentthengetsicktimeoffuri_92_present_93',
            test='''{{ result('log_ifsickbalanceispresentthengetsicktimeoffuri_92') | is_truthy }}''',
            yes_task="update_sick_timeoffbalance_94",
            no_task="log_forlogging_96",
        )

        update_sick_timeoffbalance_94=rail.RepliconServiceOperator(
            task_id='update_sick_timeoffbalance_94',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
              "timeOffAccount": {
                "userUri": "{{ result('createuser_19').uri }}",
                "timeOffTypeUri": "{{ result('log_ifsickbalanceispresentthengetsicktimeoffuri_92') }}"
              },
              "policySetScheduleEntries": [
                {
                  "effectiveDate": {
                    "year": "{{ result('get_effective_date_object').year }}",
                    "month": "{{ result('get_effective_date_object').month }}",
                    "day": "{{ result('get_effective_date_object').day }}",
                  },
                  #pylint: disable = line-too-long
                  "description": "Effective {{ result('get_effective_date_object').month }}/{{ result('get_effective_date_object').day }}/{{ result('get_effective_date_object').year }}",
                  "policySet": {
                    "timeOffBalanceEventScripts": [
                      {
                        "scriptTarget": {
                          "uri": "{{ result('get_required_script_uris').startingbalancescript }}",
                          "slug": null,
                          "name": null
                        },
                        "additionalParameters": [
                          {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                              "uri": null,
                              "slug": null,
                              "bool": null,
                              "date": null,
                              "number": "{{ dag_run.conf.sickbalance }}",
                              "text": null,
                              "time": null,
                              "calendarDayDurationValue": null,
                              "workdayDurationValue": null,
                              "dateRange": null,
                              "collection": []
                            }
                          },
                          {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                              "uri": null,
                              "slug": null,
                              "bool": null,
                              "date": null,
                              "number": "20",
                              "text": null,
                              "time": null,
                              "calendarDayDurationValue": null,
                              "workdayDurationValue": null,
                              "dateRange": null,
                              "collection": []
                            }
                          }
                        ]
                      }
                    ],
                    "timeOffValidationScripts": [
                      {
                        "scriptTarget": {
                          "uri": "{{ result('get_required_script_uris').preventbalanceoverdrawscript }}"
                        },
                        "additionalParameters": [
                          {
                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                            "value": {
                              "number": "0"
                            }
                          }
                        ]
                      }
                    ]
                  }
                }
              ]
            }
        )

        log_forlogging_96=rail.PythonOperator(
            task_id='log_forlogging_96',
            python_callable= lambda:  "Sick balance not assigned" + " as there is no value received for Sick balance."
        )

        log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97=rail.PythonOperator(
            task_id='log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_enabled_time_offtypes_72'),'displayText','Vacation','uri','') if dag_run.conf['vacationbalance'] else null
        )

        if_log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97_present_98=rail.IfOperator(
            task_id='if_log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97_present_98',
            test='''{{ result('log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97') | is_truthy }}''',
            yes_task="update_vacation_timeoffbalance_99",
            no_task="log_forlogging_101",
        )

        update_vacation_timeoffbalance_99=rail.RepliconServiceOperator(
            task_id='update_vacation_timeoffbalance_99',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
              "timeOffAccount": {
                "userUri": "{{ result('createuser_19').uri }}",
                "timeOffTypeUri": "{{ result('log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97') }}"
              },
              "policySetScheduleEntries": [
                {
                  "effectiveDate": {
                    "year": "{{ result('get_effective_date_object').year }}",
                    "month": "{{ result('get_effective_date_object').month }}",
                    "day": "{{ result('get_effective_date_object').day }}"
                  },
                  #pylint: disable = line-too-long
                  "description": "Effective {{ result('get_effective_date_object').month }}/{{ result('get_effective_date_object').day }}/{{ result('get_effective_date_object').year }}",
                  "policySet": {
                    "timeOffBalanceEventScripts": [
                      {
                        "scriptTarget": {
                          "uri": "{{ result('get_required_script_uris').startingbalancescript }}",
                          "slug": null,
                          "name": null
                        },
                        "additionalParameters": [
                          {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                              "uri": null,
                              "slug": null,
                              "bool": null,
                              "date": null,
                              "number": "{{ dag_run.conf.vacationbalance }}",
                              "text": null,
                              "time": null,
                              "calendarDayDurationValue": null,
                              "workdayDurationValue": null,
                              "dateRange": null,
                              "collection": []
                            }
                          },
                          {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                              "uri": null,
                              "slug": null,
                              "bool": null,
                              "date": null,
                              "number": "20",
                              "text": null,
                              "time": null,
                              "calendarDayDurationValue": null,
                              "workdayDurationValue": null,
                              "dateRange": null,
                              "collection": []
                            }
                          }
                        ]
                      }
                    ],
                    "timeOffValidationScripts": [
                      {
                        "scriptTarget": {
                          "uri": "{{ result('get_required_script_uris').preventbalanceoverdrawscript }}"
                        },
                        "additionalParameters": [
                          {
                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                            "value": {
                              "number": "0"
                            }
                          }
                        ]
                      }
                    ]
                  }
                }
              ]
            }
        )

        log_forlogging_101=rail.PythonOperator(
            task_id='log_forlogging_101',
            python_callable= lambda:  "Vacation balance not assigned" + " as there is no value received for Vacation balance."
        )

        if_request_status_equals_to_inactive_102=rail.IfOperator(
            task_id='if_request_status_equals_to_inactive_102',
            test='''{{ dag_run.conf.status == 'Inactive' }}''',
            yes_task="disable_login_103",
            no_task="hawaiigas_userimport_logs_prod_add_entry_111",
        )

        disable_login_103=rail.RepliconServiceOperator(
            task_id='disable_login_103',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
              "userUri": "{{ result('createuser_19').uri }}"
            }
        )

        if_request_terminationdate_present_104=rail.IfOperator(
            task_id='if_request_terminationdate_present_104',
            test='''{{ dag_run.conf.terminationdate | is_truthy }}''',
            yes_task="get_termination_dateobject",
            no_task="hawaiigas_userimport_logs_prod_add_entry_109",
        )

        get_termination_dateobject=rail.PythonOperator(
            task_id='get_termination_dateobject',
            python_callable= lambda dag_run: get_date_obj(dag_run.conf['terminationdate'])
        )

        update_end_date_108=rail.RepliconServiceOperator(
            task_id='update_end_date_108',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
              "user": {
                "uri": "{{ result('createuser_19').uri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
                "timezoneToApply": null,
                "workWeekStartToApply": null,
                "holidayCalendarToApply": null,
                "schedulePolicyToApply": null,
                "locationScheduleToApply": null,
                "divisionScheduleToApply": null,
                "costCenterScheduleToApply": null,
                "departmentGroupScheduleToApply": null,
                "employeeTypeGroupScheduleToApply": null,
                "timesheetPeriodScheduleToApply": null,
                "serviceCenterScheduleToApply": null,
                "permissionSetsToApply": null,
                "policySetsToApply": null,
                "policyDataAccessScopesToApply": null,
                "policyDataAccessScopesToApply2": null,
                "notificationPreferencesToApply": null,
                "timesheetPeriodTypeToApply": null,
                "timesheetApprovalPathToApply": null,
                "validationRuleToApply": null,
                "activitiesToApply": [],
                "activitiesToApply2": null,
                "defaultActivityToApply": null,
                "defaultActivityToApply2": null,
                "expenseApprovalPathToApply": null,
                "timeOffApprovalPathToApply": null,
                "productAssignmentsToApply": null,
                "timeBankPolicyToApply": null,
                "securitySettingsToApply": null,
                "supervisorsToApply": null,
                "supervisorsModifications": null,
                "payrollRatesToApply": null,
                "payrollRatesModifications": null,
                "overtimeRulesToApply": null,
                "overtimeRulesModifications": null,
                "customFieldValuesToApply": [],
                "departmentToApply": null,
                "employeeTypeToApply": null,
                "userDetailsToApply": {
                  "firstName": null,
                  "lastName": null,
                  "emailAddress": null,
                  "language": null,
                  "employmentDateRange": null,
                  "employmentStartDate": null,
                  "employmentEndDate": {
                    "date": {
                      "year": "{{ result('get_termination_dateobject').year }}",
                      "month": "{{ result('get_termination_dateobject').month }}",
                      "day": "{{ result('get_termination_dateobject').day }}"
                    }
                  },
                  "employeeId": null
                },
                "payRulesToApply": null,
                "payRulesScheduleModifications": null,
                "payRatesModifications": null,
                "placeAssignmentsModifications": null,
                "resourceAllocationAfterUserEndDateOptionUri": null
              }
            }
        )

        hawaiigas_userimport_logs_prod_add_entry_109=rail.WriteLogOperator(
            task_id='hawaiigas_userimport_logs_prod_add_entry_109',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run:{
                "employeeid": dag_run.conf['employee'] + "|" + dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Success",
                "details": rail.smartjoin_by_delim(("User added in disabled status, since the status received is Inactive" + "," +
                  (rail.result('log_forlogging_69') if rail.result('log_forlogging_69') else "") + "," +
                  (rail.result('log_forlogging_96') if rail.result('log_forlogging_96') else "") + "," +
                  (rail.result('log_forlogging_101') if rail.result('log_forlogging_101') else "") + "," + "|" +
                  rail.render_template('{{dag_run_ecid()}}')).split(','),','),
                "jobid": "{{dag_run.conf.callerjobid}}"
            }
        )

        hawaiigas_userimport_logs_prod_add_entry_111=rail.WriteLogOperator(
            task_id='hawaiigas_userimport_logs_prod_add_entry_111',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run:{
                "employeeid": dag_run.conf['employee'] + "|" + dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Success",
                "details": rail.smartjoin_by_delim(((rail.result('log_forlogging_69') if rail.result('log_forlogging_69') else "") + "," +
                  (rail.result('log_forlogging_96') if rail.result('log_forlogging_96') else "") + "," +
                  (rail.result('log_forlogging_101') if rail.result('log_forlogging_101') else "") + "," + "|" +
                  rail.render_template('{{dag_run_ecid()}}')).split(','),','),
                "jobid": "{{dag_run.conf.callerjobid}}"
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.logslookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties=lambda dag_run:{
                "employeeid": dag_run.conf['employee'] + "|" + dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Error",
                "details": (rail.smartjoin_by_delim(("User created, but not all fields are updated" + "," +
                  (rail.result('log_forlogging_69') if rail.result('log_forlogging_69') else "") + "," +
                  (rail.result('log_forlogging_96') if rail.result('log_forlogging_96') else "") + "," +
                  (rail.result('log_forlogging_101') if rail.result('log_forlogging_101') else "") + "," + "|" +
                  rail.render_template('{{dag_run_ecid()}}')).split(','),',')) if get_task_state('createuser_19') == 'success' else ("User not created," +
                  rail.render_template("{{get_error_message()}}|{{dag_run_ecid()}}")),
                "jobid": "{{dag_run.conf.callerjobid}}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_departmentlist_3
        get_departmentlist_3 >> get_all_employee_type_details_7 >> log_employeetypeuri_8 >> if_log_departmenturi_6_blank_9
        if_log_departmenturi_6_blank_9 >> rail.Label('Yes')  >> hawaiigas_userimport_logs_prod_add_entry_10 >> catch_and_log_error
        if_log_departmenturi_6_blank_9 >> rail.Label('No') >> get_hire_date >> log_emailaddressderived_15 >> createuser_19
        createuser_19 >> remove_time_offtype_21 >> get_all_customfieldsfor_user_22 >> if_request_employmenttype_present_23
        if_request_employmenttype_present_23 >> rail.Label(
          'Yes') >> log_get_urifor_employment_type_24 >> get_dropdownoptionsfor_employmenttype_25 >> get_dropdownoption_uri_and_status
        get_dropdownoption_uri_and_status >> if_log_dropdownoptionuri_26_blank_28
        if_log_dropdownoptionuri_26_blank_28 >> rail.Label(
            'Yes') >> get_final_list_of_dropdownoptions >> putnewdropdownoption_34 >> if_log_dropdownoptionstatus_27_equals_to_false_35
        if_log_dropdownoptionuri_26_blank_28 >> rail.Label('No') >> if_log_dropdownoptionstatus_27_equals_to_false_35
        if_log_dropdownoptionstatus_27_equals_to_false_35 >> rail.Label(
            'Yes') >> get_final_list_of_dropdown_options >> putnewdropdownoption_43 >> get_dropdownoptionsfor_employmenttype_44
        if_log_dropdownoptionstatus_27_equals_to_false_35 >> rail.Label(
            'No') >> get_dropdownoptionsfor_employmenttype_44 >> log_dropdownoptionuritobeassignedtotheuser_45 >> assign_drop_downoptionfor_employment_type_46
        assign_drop_downoptionfor_employment_type_46 >> if_request_supervisor_present_47
        if_request_employmenttype_present_23 >> rail.Label('No') >> if_request_supervisor_present_47
        if_request_supervisor_present_47 >> rail.Label('Yes')  >> searchsupervisor_48 >> if_log_getthesupervisoruri_51_present_53
        if_log_getthesupervisoruri_51_present_53 >> rail.Label(
            'Yes') >> getpermissionsassigned_54 >> log_checkifthesupervisorpermissionisassigned_55
        log_checkifthesupervisorpermissionisassigned_55 >> if_log_checkifthesupervisorpermissionisassigned_55_blank_56
        if_log_checkifthesupervisorpermissionisassigned_55_blank_56 >> rail.Label(
            'Yes') >> get_allpermissionsets_61 >> get_all_permission_sets_required_uri >> assign_permissionsetsto_supervisor_65
        assign_permissionsetsto_supervisor_65 >> supervisor_assignment_schedule_66
        log_checkifthesupervisorpermissionisassigned_55 >> if_log_checkifthesupervisorpermissionisassigned_55_blank_56
        if_log_checkifthesupervisorpermissionisassigned_55_blank_56 >> rail.Label(
            'No') >> supervisor_assignment_schedule_66 >> if_log_getthesupervisoruri_51_blank_70
        if_log_getthesupervisoruri_51_present_53 >> rail.Label('No') >> if_log_getthesupervisoruri_51_present_68
        if_log_getthesupervisoruri_51_present_68 >> rail.Label('Yes')  >> log_forlogging_69 >> if_log_getthesupervisoruri_51_blank_70
        if_log_getthesupervisoruri_51_present_68 >> rail.Label('No') >> if_log_getthesupervisoruri_51_blank_70
        if_log_getthesupervisoruri_51_blank_70 >> rail.Label('Yes')  >> hawaii_gas_supervisor_lookup_prod_add_entry_71 >> get_enabled_time_offtypes_72
        if_log_getthesupervisoruri_51_blank_70 >> rail.Label('No') >> get_enabled_time_offtypes_72
        if_request_supervisor_present_47 >> rail.Label(
            'No') >> get_enabled_time_offtypes_72 >> get_timeofftypes_to_assign_uris >> if_log_time_offuriwhenonlyonetimeofftypeispresent_76_present_77
        if_log_time_offuriwhenonlyonetimeofftypeispresent_76_present_77 >> rail.Label('Yes') >> assign_time_offtype_78 >> if_request_vacationbalance_present_79
        if_log_time_offuriwhenonlyonetimeofftypeispresent_76_present_77 >> rail.Label('No') >> if_request_vacationbalance_present_79
        if_request_vacationbalance_present_79 >> rail.Label(
            'Yes') >> get_all_scripts_validationscripts_80 >> get_all_timebalanceeventscripts_81 >> get_required_script_uris >> get_today_date_object
        get_today_date_object >> get_timesheet_for_date2_87 >> get_timesheet_details_88 >> get_effective_date_object
        get_effective_date_object >> log_ifsickbalanceispresentthengetsicktimeoffuri_92 >> if_log_ifsickbalanceispresentthengetsicktimeoffuri_92_present_93
        if_log_ifsickbalanceispresentthengetsicktimeoffuri_92_present_93 >> rail.Label(
            'Yes') >> update_sick_timeoffbalance_94 >> log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97
        if_log_ifsickbalanceispresentthengetsicktimeoffuri_92_present_93 >> rail.Label(
            'No') >> log_forlogging_96 >> log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97
        log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97 >> if_log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97_present_98
        if_log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97_present_98 >> rail.Label(
            'Yes') >> update_vacation_timeoffbalance_99 >> if_request_status_equals_to_inactive_102
        if_log_if_vacationbalanceispresentthenget_vacationtimeoffuri_97_present_98 >> rail.Label(
            'No') >> log_forlogging_101 >> if_request_status_equals_to_inactive_102
        if_request_vacationbalance_present_79 >> rail.Label('No') >> if_request_status_equals_to_inactive_102
        if_request_status_equals_to_inactive_102 >> rail.Label('Yes')  >> disable_login_103 >> if_request_terminationdate_present_104
        if_request_terminationdate_present_104 >> rail.Label(
            'Yes') >> get_termination_dateobject >> update_end_date_108 >> hawaiigas_userimport_logs_prod_add_entry_109
        if_request_terminationdate_present_104 >> rail.Label('No') >> hawaiigas_userimport_logs_prod_add_entry_109 >> catch_and_log_error
        if_request_status_equals_to_inactive_102 >> rail.Label('No') >> hawaiigas_userimport_logs_prod_add_entry_111 >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
