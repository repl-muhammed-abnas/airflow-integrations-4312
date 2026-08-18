# pylint: disable=line-too-long too-many-statements
from datetime import timedelta, datetime
from pendulum import now
from airflow.models import Variable
import rail
from momentive.user_import_thailand.utils import request_payload, python_callable
from momentive.user_import_thailand.mappers.momentive_thailand_mapper import mapper_value

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_thailand_user_sync_child_add_user_dag_id,
        description=f'Momentive_thailand_user_sync_add_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_exceptionlogger_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_exceptionlogger_list',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Recipe [163]: accumulate soft-failure reasons (supervisor multiple-match,
        # work week / schedule / holiday calendar not found) so the success log can
        # escalate status to Exception and surface them in the details.
        create_exceptionlogger_list = rail.SetVariableOperator(
            task_id='create_exceptionlogger_list',
            append=False,
            name='exceptionlogger_list',
            value=[]
        )

        # Recipe [2]: collect the names of any missing mandatory fields.
        def required_fields_missing(dag_run):
            c = dag_run.conf
            checks = [
                (c.get('User_ID'), "Login name not present"),
                (c.get('First_Name'), "First_Name not present"),
                (c.get('Last_Name'), "Last_Name not present"),
                (c.get('Hire_Date'), "Hire date not present"),
                (c.get('Email_Address'), "Email_Address not present"),
                (c.get('Exemption_Status'), "Excemption Status not present"),
                (c.get('Worker_Type'), "Worker type not present"),
                (c.get('Location'), "Department (location) not present"),
                (c.get('Active'), "Employee status not present"),
                (c.get('Manager_ID'), "Manager ID not present"),
                (c.get('Country'), "Country not present"),
            ]
            return ", ".join(msg for val, msg in checks if not val)

        validate_required_fields = rail.PythonOperator(
            task_id='validate_required_fields',
            python_callable=required_fields_missing
        )

        # Recipe [3]: any missing field -> log warning and stop.
        if_required_fields_missing = rail.IfOperator(
            task_id='if_required_fields_missing',
            test='''{{ result('validate_required_fields') | is_truthy }}''',
            yes_task='log_user_not_created_required',
            no_task='if_gender_blank_and_employee'
        )

        log_user_not_created_required = rail.WriteLogOperator(
            task_id='log_user_not_created_required',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Warning",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Add",
                "status": "Warning",
                "details": "User not created, {{ result('validate_required_fields') }}",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        # Recipe [6]/[7]: gender is mandatory for Employee worker type.
        if_gender_blank_and_employee = rail.IfOperator(
            task_id='if_gender_blank_and_employee',
            test='''{{ dag_run.conf.Gender | is_falsy and dag_run.conf.Worker_Type == 'Employee' }}''',
            yes_task='log_user_not_created_gender',
            no_task='get_all_employee_type_details'
        )

        log_user_not_created_gender = rail.WriteLogOperator(
            task_id='log_user_not_created_gender',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Warning",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Add",
                "status": "Warning",
                "details": "User not created, gender must be present for users with employee worker type",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        # Recipe [10]/[11]/[12]: reference data.
        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups"
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id='get_all_timezones',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones"
        )

        # Recipe [13]-[25]: derived key columns for the Momentive_Thailand_Mapper lookups.
        # Shared with the update flow; the add flow's cost center is in conf['costcenter'].
        compute_mapper_keys = rail.PythonOperator(
            task_id='compute_mapper_keys',
            python_callable=lambda dag_run: python_callable.mapper_keys(dag_run, costcenter_key='costcenter')
        )

        # Recipe [26]/[28]: map Worker_Type/exemption/shift -> Employee Type Group name -> URI.
        log_employee_type_group_uri = rail.PythonOperator(
            task_id='log_employee_type_group_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_employee_type_details'), 'displayText',
                mapper_value('Employee Type', workertype=dag_run.conf['Worker_Type'], location='Null',
                             exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'],
                             shift=rail.result('compute_mapper_keys')['shift'], gender='Any', cost_center='Any'),
                'uri', '')
        )

        # Recipe [29]: neither employee type group nor department resolved -> cannot create.
        if_emp_type_group_and_dept_blank = rail.IfOperator(
            task_id='if_emp_type_group_and_dept_blank',
            test='''{{ result('log_employee_type_group_uri') | is_falsy and dag_run.conf.departmentgroupuri | is_falsy }}''',
            yes_task='log_user_not_created_no_emp_type',
            no_task='if_emp_type_group_and_dept_present'
        )

        log_user_not_created_no_emp_type = rail.WriteLogOperator(
            task_id='log_user_not_created_no_emp_type',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Exception",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Add",
                "status": "Exception",
                "details": "{%- if result('log_employee_type_group_uri') | is_falsy -%}User not created, since Employee type group doesn't exist in Replicon{%- endif -%}{%- if dag_run.conf.departmentgroupuri | is_falsy -%};User not created, since Department (location) doesn't exist in Replicon{%- endif -%}",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        # Recipe [32]: both present -> proceed to create the user.
        if_emp_type_group_and_dept_present = rail.IfOperator(
            task_id='if_emp_type_group_and_dept_present',
            test='''{{ result('log_employee_type_group_uri') | is_truthy and dag_run.conf.departmentgroupuri | is_truthy }}''',
            yes_task='get_split_hiredate',
            no_task='finish'
        )

        # Recipe [33]: split the hire date for the employment date range.
        get_split_hiredate = rail.PythonOperator(
            task_id='get_split_hiredate',
            python_callable=lambda dag_run: python_callable.split_date_string(dag_run.conf['Hire_Date'], 'int') if dag_run.conf.get('Hire_Date') else null
        )

        # Recipe [35]: pay rule scripts (to resolve the mapped payrule name).
        get_all_pay_rule_scripts = rail.RepliconServiceOperator(
            task_id='get_all_pay_rule_scripts',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        # Recipe [36]-[52]: resolve the values that feed PutUser2.
        def pre_putuser_values(dag_run):
            c = dag_run.conf
            keys = rail.result('compute_mapper_keys')
            crit = dict(workertype=c['Worker_Type'], location=c['Location'],
                        exemptstatus=keys['exemptstatus'], shift=keys['shift'],
                        gender=keys['gender'], cost_center=keys['cost_center'])
            timesheet = mapper_value('Timesheet Template', **crit)
            punch = mapper_value('Punch Entry Policy', **crit)
            payrule_name = mapper_value('Payrule', **crit)
            payrule_uri = rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_pay_rule_scripts'), 'displayText', payrule_name, 'uri', '') if payrule_name else ''
            basic_perm_uri = rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permission_sets'), 'name', 'Basic User with Reports', 'uri', '')
            # Recipe [37]-[40]: System Approval for exempt users, otherwise Supervisor.
            approval = "System Approval" if c.get('Exemption_Status') == "1" else "Supervisor"
            # Recipe [47]-[51]: policy sets to assign.
            if c['Location'] == 'TH Bangkok':
                policysets = [{"uri": None, "name": "Time Off"}]
            else:
                policysets = [{"uri": None, "name": timesheet},
                              {"uri": None, "name": punch},
                              {"uri": None, "name": "Time Off"}]
            # Drop unmapped (empty) policy sets / pay rule so PutUser3 never receives an
            # empty-name target (InvalidPolicySetTargetParameterError) — matches Japan.
            policysets = [p for p in policysets if p.get("name") or p.get("uri")]
            payrule_schedule = [{"payRuleScript": {"uri": payrule_uri, "name": None}, "effectiveDate": None}] if payrule_uri else []
            return {
                'timesheet': timesheet,
                'punch': punch,
                'payrule_uri': payrule_uri,
                'basic_perm_uri': basic_perm_uri,
                'approval': approval,
                'policysets': policysets,
                'payrule_schedule': payrule_schedule,
            }

        log_pre_putuser_values = rail.PythonOperator(
            task_id='log_pre_putuser_values',
            python_callable=pre_putuser_values
        )

        # Recipe [54]: PutUser3.
        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {"uri": None, "loginName": dag_run.conf['User_ID'], "parameterCorrelationId": None},
                    "firstname": dag_run.conf['First_Name'],
                    "lastname": dag_run.conf['Last_Name'],
                    "emailAddress": dag_run.conf['Email_Address'],
                    "employeeId": dag_run.conf['Worker_Reference_Employee_ID'],
                    "department": None,
                    "supervisorAssignmentSchedule": None,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
                    "employmentDateRange": {
                        "startDate": rail.result('get_split_hiredate'),
                        "endDate": None,
                        "relativeDateRangeUri": None,
                        "relativeDateRangeAsOfDate": None
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['User_ID'],
                        "SSOName": dag_run.conf['User_ID'],
                        "password": None
                    },
                    "holidayCalendar": None,
                    "timeOffPolicy": None,
                    "permissionSets": [{"uri": rail.result('log_pre_putuser_values')['basic_perm_uri'], "name": None}],
                    "policySets": rail.result('log_pre_putuser_values')['policysets'],
                    "employeeType": None,
                    "timesheetPeriodTypeUri": None,
                    "costRateSchedule": None,
                    "payrollRateSchedule": None,
                    "defaultBillingRate": None,
                    "timesheetApprovalPath": {"uri": None, "name": rail.result('log_pre_putuser_values')['approval']},
                    "expenseApprovalPath": None,
                    "timeOffApprovalPath": None,
                    "customFieldValues": [],
                    "assignedActivities": [],
                    "timeZone": {"uri": None, "IANAName": "Asia/Bangkok"},
                    "overtimeRuleAssignmentSchedule": None,
                    "validationRuleAssignmentSchedule": None,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": [{"departmentGroup": {"uri": dag_run.conf['departmentgroupuri'], "parent": None, "name": None, "parameterCorrelationId": None}, "effectiveDate": None}],
                    "employeeTypeGroupSchedule": [{"employeeTypeGroup": {"uri": rail.result('log_employee_type_group_uri'), "parent": None, "name": None, "parameterCorrelationId": None}, "effectiveDate": None}],
                    "timesheetPeriodSchedule": [{"timesheetPeriod": {"uri": None, "name": "Thai_Monthly"}, "effectiveDate": None}],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": rail.result('log_pre_putuser_values')['payrule_schedule']
                }
            }
        )

        # Recipe [56]: clear any auto-assigned time off types (managed by the timeoff child).
        unassign_all_timeoffs = rail.RepliconServiceOperator(
            task_id='unassign_all_timeoffs',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {"userUri": rail.result('create_user')['uri'], "timeOffTypeUris": []}
        )

        # Recipe [58]: custom field definitions for the user object.
        get_custom_fields = rail.RepliconServiceOperator(
            task_id='get_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={"objectUri": "urn:replicon:object-type:user"}
        )

        # Recipe [59]-[63]: Date of Birth.
        if_dob_present = rail.IfOperator(
            task_id='if_dob_present',
            # Recipe pairs the value check with an inner "custom-field URI present" guard:
            # a UDF absent from the instance is skipped, never posted with a blank URI.
            test=lambda dag_run: bool(dag_run.conf.get('CF_Date_of_Birth_MM_DD_YYYY')) and bool(
                python_callable.custom_field_uri('Date of Birth')),
            yes_task='update_dob_udf',
            no_task='if_title_present'
        )

        update_dob_udf = rail.RepliconServiceOperator(
            task_id='update_dob_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fields'), 'displayText', 'Date of Birth', 'uri', ''),
                "value": python_callable.split_date_string(dag_run.conf['CF_Date_of_Birth_MM_DD_YYYY'], 'int')
            }
        )

        # Recipe [64]-[67]: Title (Business Title).
        if_title_present = rail.IfOperator(
            task_id='if_title_present',
            # Recipe pairs the value check with an inner "custom-field URI present" guard:
            # a UDF absent from the instance is skipped, never posted with a blank URI.
            test=lambda dag_run: bool(dag_run.conf.get('Business_Title')) and bool(
                python_callable.custom_field_uri('Title')),
            yes_task='update_title_udf',
            no_task='if_yos_present'
        )

        update_title_udf = rail.RepliconServiceOperator(
            task_id='update_title_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fields'), 'displayText', 'Title', 'uri', ''),
                "value": dag_run.conf['Business_Title']
            }
        )

        # Recipe [68]-[71]: Years of Service.
        if_yos_present = rail.IfOperator(
            task_id='if_yos_present',
            # Recipe pairs the value check with an inner "custom-field URI present" guard:
            # a UDF absent from the instance is skipped, never posted with a blank URI.
            test=lambda dag_run: bool(dag_run.conf.get('Years_of_service')) and bool(
                python_callable.custom_field_uri('Years of Service')),
            yes_task='update_yos_udf',
            no_task='if_hrm_present'
        )

        update_yos_udf = rail.RepliconServiceOperator(
            task_id='update_yos_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fields'), 'displayText', 'Years of Service', 'uri', ''),
                "value": dag_run.conf['Years_of_service']
            }
        )

        # Recipe [72]-[75]: HRM (Field HR).
        if_hrm_present = rail.IfOperator(
            task_id='if_hrm_present',
            # Recipe pairs the value check with an inner "custom-field URI present" guard:
            # a UDF absent from the instance is skipped, never posted with a blank URI.
            test=lambda dag_run: bool(dag_run.conf.get('Field_HR')) and bool(
                python_callable.custom_field_uri('HRM')),
            yes_task='update_hrm_udf',
            no_task='if_gender_present'
        )

        update_hrm_udf = rail.RepliconServiceOperator(
            task_id='update_hrm_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fields'), 'displayText', 'HRM', 'uri', ''),
                "value": dag_run.conf['Field_HR']
            }
        )

        # Recipe [76]-[79]: Gender.
        if_gender_present = rail.IfOperator(
            task_id='if_gender_present',
            # Recipe pairs the value check with an inner "custom-field URI present" guard:
            # a UDF absent from the instance is skipped, never posted with a blank URI.
            test=lambda dag_run: bool(dag_run.conf.get('Gender')) and bool(
                python_callable.custom_field_uri('Gender')),
            yes_task='update_gender_udf',
            no_task='if_manager_present'
        )

        update_gender_udf = rail.RepliconServiceOperator(
            task_id='update_gender_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fields'), 'displayText', 'Gender', 'uri', ''),
                "value": dag_run.conf['Gender']
            }
        )

        # Recipe [80]-[99]: supervisor assignment.
        if_manager_present = rail.IfOperator(
            task_id='if_manager_present',
            test='''{{ dag_run.conf.Manager_ID | is_truthy }}''',
            yes_task='search_supervisor',
            no_task='if_location_not_bangkok'
        )

        search_supervisor = rail.RepliconServiceOperator(
            task_id='search_supervisor',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: request_payload.search_user_by_empid_payload(dag_run.conf['Manager_ID']),
            data_handler=lambda response, dag_run: [r for r in response['rows'] if r['cells'][0]['textValue'] == dag_run.conf['Manager_ID']]
        )

        # Recipe [83]: ambiguous (more than one match) -> do not assign here.
        if_multiple_supervisors = rail.IfOperator(
            task_id='if_multiple_supervisors',
            test=lambda: bool(len(rail.result('search_supervisor')) > 1),
            yes_task='log_exception_supervisor_multiple',
            no_task='if_supervisor_exists'
        )

        log_exception_supervisor_multiple = rail.SetVariableOperator(
            task_id='log_exception_supervisor_multiple',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
                "log": 'Supervisor not assigned for user "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}" as multiple users have same Employee ID: {{ dag_run.conf.Manager_ID }}'
            }
        )

        # Recipe [86]: supervisor user found in Replicon.
        if_supervisor_exists = rail.IfOperator(
            task_id='if_supervisor_exists',
            test=lambda: bool(rail.result('search_supervisor') and rail.result('search_supervisor')[0]['cells'][1]['uri']),
            yes_task='log_supervisor_details',
            no_task='log_supervisor_assignment_entry_not_found'
        )

        # Recipe [87]/[88]: supervisor useruri + enabled flag.
        log_supervisor_details = rail.PythonOperator(
            task_id='log_supervisor_details',
            python_callable=lambda: {
                'useruri': rail.result('search_supervisor')[0]['cells'][1]['uri'],
                'enabled': rail.result('search_supervisor')[0]['cells'][2]['boolValue']
            }
        )

        # Recipe [89]: supervisor present and enabled -> assign directly.
        if_supervisor_enabled = rail.IfOperator(
            task_id='if_supervisor_enabled',
            test=lambda: bool(rail.result('log_supervisor_details')['useruri'] and rail.result('log_supervisor_details')['enabled']),
            yes_task='get_assigned_permission_sets_for_supervisor',
            no_task='log_supervisor_assignment_entry_disabled'
        )

        # Recipe [90]: does the supervisor already have a supervision permission?
        get_assigned_permission_sets_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_supervisor',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: {"userUri": rail.result('log_supervisor_details')['useruri']},
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'user', '')
        )

        # Recipe [92]: no supervision permission -> grant Supervisor - Edit.
        if_supervisor_has_no_permission = rail.IfOperator(
            task_id='if_supervisor_has_no_permission',
            test='''{{ result('get_assigned_permission_sets_for_supervisor') | is_falsy }}''',
            yes_task='assign_supervisor_permission',
            no_task='update_initial_supervisor'
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: {
                "userUri": rail.result('log_supervisor_details')['useruri'],
                "permissionSetUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'), 'displayText', 'Supervisor - Edit', 'uri', '')
            }
        )

        # Recipe [95]: assign the supervisor to the new user.
        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "supervisorUri": rail.result('log_supervisor_details')['useruri'],
                "dateRange": None
            }
        )

        # Recipe [97]/[99]: defer to the master's supervisor-assignment flow.
        def _supervisor_log_props(dag_run):
            return {
                "parentjobid": dag_run.conf['parentjobid'],
                "loginid": dag_run.conf['User_ID'],
                "supervisorempid": dag_run.conf['Manager_ID'],
                "useruri": rail.result('create_user')['uri'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "type": "add",
                "sup_email": dag_run.conf['CF_LRV_Manager_Email'] or '',
                "sup_firstname": dag_run.conf['CF_LRV_Manager_First_Name'] or '',
                "sup_lastname": dag_run.conf['CF_LRV_Manager_Last_Name'] or '',
                "sup_change_effective_date": dag_run.conf['Effective_Date_of_Manager_Change']
                if dag_run.conf.get('Effective_Date_of_Manager_Change') else str(now(tz=config.time_zone).date()),
            }

        log_supervisor_assignment_entry_disabled = rail.WriteLogOperator(
            task_id='log_supervisor_assignment_entry_disabled',
            log="{{ dag_run.conf.supervisor_assignment_logs }}",
            message="na",
            severity="na",
            properties=_supervisor_log_props
        )

        log_supervisor_assignment_entry_not_found = rail.WriteLogOperator(
            task_id='log_supervisor_assignment_entry_not_found',
            log="{{ dag_run.conf.supervisor_assignment_logs }}",
            message="na",
            severity="na",
            properties=_supervisor_log_props
        )

        # Recipe [100]/[101]: ensure a timesheet exists for non-Bangkok users.
        if_location_not_bangkok = rail.IfOperator(
            task_id='if_location_not_bangkok',
            test='''{{ dag_run.conf.Location != 'TH Bangkok' }}''',
            yes_task='get_timesheet_for_date',
            no_task='get_all_days_of_week'
        )

        get_timesheet_for_date = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "date": rail.result('get_split_hiredate'),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        # Recipe [102]-[106]: work week start day from the mapper.
        get_all_days_of_week = rail.RepliconServiceOperator(
            task_id='get_all_days_of_week',
            endpoint="/services/InternationalizationService1.svc/GetAllDaysOfWeek"
        )

        log_work_week_uri = rail.PythonOperator(
            task_id='log_work_week_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_days_of_week'), 'name',
                (mapper_value('Work week', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                              exemptstatus='Any', shift='Any', gender='Any', cost_center='Any') or '').split(" ")[0].strip(),
                'uri', '')
        )

        if_work_week_present = rail.IfOperator(
            task_id='if_work_week_present',
            test='''{{ result('log_work_week_uri') | is_truthy }}''',
            yes_task='update_work_week',
            no_task='log_exception_work_week_not_found'
        )

        log_exception_work_week_not_found = rail.SetVariableOperator(
            task_id='log_exception_work_week_not_found',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value=lambda dag_run: {
                "log": f'''Work week "{mapper_value('Work week', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'], exemptstatus='Any', shift='Any', gender='Any', cost_center='Any') or ''}" not found in the instance/disabled hence not assigned'''
            }
        )

        update_work_week = rail.RepliconServiceOperator(
            task_id='update_work_week',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data=lambda: {"userUri": rail.result('create_user')['uri'], "dayOfWeekUri": rail.result('log_work_week_uri')}
        )

        # Recipe [109]-[118]: schedule policy.
        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        log_schedule_name = rail.PythonOperator(
            task_id='log_schedule_name',
            python_callable=lambda dag_run: mapper_value(
                'Schedule', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'], shift=rail.result('compute_mapper_keys')['shift'],
                gender=rail.result('compute_mapper_keys')['gender'], cost_center=rail.result('compute_mapper_keys')['cost_center'])
        )

        if_schedule_is_shift = rail.IfOperator(
            task_id='if_schedule_is_shift',
            test='''{{ result('log_schedule_name') == 'Shift' }}''',
            yes_task='put_shift_schedule',
            no_task='log_office_schedule_uri'
        )

        put_shift_schedule = rail.RepliconServiceOperator(
            task_id='put_shift_schedule',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{"schedulePolicy": {"officeScheduleUri": None, "name": None, "officeSchedule": None, "scheduleTypeUri": "urn:replicon:schedule-type:shift"}, "effectiveDate": None}]
            }
        )

        log_office_schedule_uri = rail.PythonOperator(
            task_id='log_office_schedule_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_office_schedules'), 'displayText', rail.result('log_schedule_name'), 'uri', '')
        )

        if_office_schedule_present = rail.IfOperator(
            task_id='if_office_schedule_present',
            test='''{{ result('log_office_schedule_uri') | is_truthy }}''',
            yes_task='put_office_schedule',
            no_task='log_exception_schedule_not_found'
        )

        log_exception_schedule_not_found = rail.SetVariableOperator(
            task_id='log_exception_schedule_not_found',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
                "log": '''Schedule "{{ result('log_schedule_name') }}" not found in the instance/ disabled hence schedule not assigned.'''
            }
        )

        put_office_schedule = rail.RepliconServiceOperator(
            task_id='put_office_schedule',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{"schedulePolicy": {"officeScheduleUri": rail.result('log_office_schedule_uri'), "name": None, "officeSchedule": None, "scheduleTypeUri": None}, "effectiveDate": None}]
            }
        )

        # Recipe [120]-[123]: holiday calendar.
        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        log_holiday_calendar_uri = rail.PythonOperator(
            task_id='log_holiday_calendar_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_holiday_calendars'), 'name',
                mapper_value('Holiday Calendar', workertype='Any', location=dag_run.conf['Location'],
                             exemptstatus='Any', shift=rail.result('compute_mapper_keys')['shift'], gender='Any', cost_center='Any'),
                'uri', '')
        )

        if_holiday_calendar_present = rail.IfOperator(
            task_id='if_holiday_calendar_present',
            test='''{{ result('log_holiday_calendar_uri') | is_truthy }}''',
            yes_task='update_holiday_calendar',
            no_task='log_exception_holiday_not_found'
        )

        log_exception_holiday_not_found = rail.SetVariableOperator(
            task_id='log_exception_holiday_not_found',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value=lambda dag_run: {
                "log": f'''Holiday calendar  "{mapper_value('Holiday Calendar', workertype='Any', location=dag_run.conf['Location'], exemptstatus='Any', shift=rail.result('compute_mapper_keys')['shift'], gender='Any', cost_center='Any')}" not found in the instance hence holiday calendar not assigned.'''
            }
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data=lambda: {"userUri": rail.result('create_user')['uri'], "holidayCalendarUri": rail.result('log_holiday_calendar_uri')}
        )

        # Recipe [126]-[130]: activity assignment.
        log_activity_name = rail.PythonOperator(
            task_id='log_activity_name',
            python_callable=lambda dag_run: mapper_value(
                'Activity', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'], shift=rail.result('compute_mapper_keys')['shift'],
                gender=rail.result('compute_mapper_keys')['gender'], cost_center=rail.result('compute_mapper_keys')['cost_center'])
        )

        if_activity_present = rail.IfOperator(
            task_id='if_activity_present',
            test='''{{ result('log_activity_name') | is_truthy }}''',
            yes_task='get_enabled_activities',
            no_task='if_legalentity_present'
        )

        get_enabled_activities = rail.RepliconServiceOperator(
            task_id='get_enabled_activities',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities"
        )

        put_activity_assignments = rail.RepliconServiceOperator(
            task_id='put_activity_assignments',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "activityUris": [rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_activities'), 'name', rail.result('log_activity_name'), 'uri', '')]
            }
        )

        # Recipe [131]-[137]: legal entity (division) schedule.
        if_legalentity_present = rail.IfOperator(
            task_id='if_legalentity_present',
            test='''{{ dag_run.conf.legalentity | is_truthy }}''',
            yes_task='search_legal_entity',
            no_task='if_location_present_dept'
        )

        search_legal_entity = rail.RepliconServiceOperator(
            task_id='search_legal_entity',
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.enabled_list_getdata_payload('division', 'division'),
            data_handler=request_payload.enabled_list_rows_handler
        )

        if_legalentity_uri_present = rail.IfOperator(
            task_id='if_legalentity_uri_present',
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(rail.result('search_legal_entity'), 'name', dag_run.conf['legalentity'], 'uri', '')),
            yes_task='put_division_schedule',
            no_task='if_location_present_dept'
        )

        put_division_schedule = rail.RepliconServiceOperator(
            task_id='put_division_schedule',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{"division": {"uri": rail.find_first_by_attr_and_get_attr(rail.result('search_legal_entity'), 'name', dag_run.conf['legalentity'], 'uri', ''), "parent": None, "name": None, "parameterCorrelationId": None}, "effectiveDate": None}]
            }
        )

        # Recipe [138]-[145]: department (location) data access scope.
        if_location_present_dept = rail.IfOperator(
            task_id='if_location_present_dept',
            test='''{{ dag_run.conf.Location | is_truthy }}''',
            yes_task='search_department_group',
            no_task='if_paygroup_present'
        )

        search_department_group = rail.RepliconServiceOperator(
            task_id='search_department_group',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.enabled_list_getdata_payload('department-group', 'department-group'),
            data_handler=request_payload.enabled_list_rows_handler
        )

        if_department_uri_present = rail.IfOperator(
            task_id='if_department_uri_present',
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(rail.result('search_department_group'), 'name', dag_run.conf['Location'], 'uri', '')),
            yes_task='put_department_access_scope',
            no_task='if_paygroup_present'
        )

        put_department_access_scope = rail.RepliconServiceOperator(
            task_id='put_department_access_scope',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "policyDataAccessScopes": [{
                    "policyUri": "urn:replicon:policy:time-off",
                    "locations": [], "divisions": [], "costCenters": [], "serviceCenters": [],
                    "departmentGroups": [{"departmentGroup": {"uri": rail.find_first_by_attr_and_get_attr(rail.result('search_department_group'), 'name', dag_run.conf['Location'], 'uri', ''), "parentUri": None, "name": None}, "groupSpecificationModeUri": None, "groupDescendantModeUri": None}],
                    "employeeTypeGroups": []
                }]
            }
        )

        # Recipe [146]-[152]: paygroup (service center) schedule.
        if_paygroup_present = rail.IfOperator(
            task_id='if_paygroup_present',
            test='''{{ dag_run.conf.paygroup | is_truthy }}''',
            yes_task='search_service_center',
            no_task='if_costcenter_present'
        )

        search_service_center = rail.RepliconServiceOperator(
            task_id='search_service_center',
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data=request_payload.enabled_list_getdata_payload('service-center', 'service-center'),
            data_handler=request_payload.enabled_list_rows_handler
        )

        if_service_center_uri_present = rail.IfOperator(
            task_id='if_service_center_uri_present',
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(rail.result('search_service_center'), 'name', dag_run.conf['paygroup'], 'uri', '')),
            yes_task='put_service_center_schedule',
            no_task='if_costcenter_present'
        )

        put_service_center_schedule = rail.RepliconServiceOperator(
            task_id='put_service_center_schedule',
            endpoint="/services/ServiceCenterService1.svc/PutServiceCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{"serviceCenter": {"uri": rail.find_first_by_attr_and_get_attr(rail.result('search_service_center'), 'name', dag_run.conf['paygroup'], 'uri', ''), "parent": None, "name": None, "parameterCorrelationId": None}, "effectiveDate": None}]
            }
        )

        # Recipe [153]-[159]: cost center schedule.
        if_costcenter_present = rail.IfOperator(
            task_id='if_costcenter_present',
            test='''{{ dag_run.conf.costcenter | is_truthy }}''',
            yes_task='search_cost_center',
            no_task='log_timeoff_types'
        )

        search_cost_center = rail.RepliconServiceOperator(
            task_id='search_cost_center',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.enabled_list_getdata_payload('cost-center', 'cost-center'),
            data_handler=request_payload.enabled_list_rows_handler
        )

        if_cost_center_uri_present = rail.IfOperator(
            task_id='if_cost_center_uri_present',
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(rail.result('search_cost_center'), 'name', dag_run.conf['costcenter'], 'uri', '')),
            yes_task='put_cost_center_schedule',
            no_task='log_timeoff_types'
        )

        put_cost_center_schedule = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{"costCenter": {"uri": rail.find_first_by_attr_and_get_attr(rail.result('search_cost_center'), 'name', dag_run.conf['costcenter'], 'uri', ''), "parent": None, "name": None, "parameterCorrelationId": None}, "effectiveDate": None}]
            }
        )

        # Recipe [160]-[162]: assign time off types via the timeoff child.
        log_timeoff_types = rail.PythonOperator(
            task_id='log_timeoff_types',
            python_callable=lambda dag_run: mapper_value(
                'Time off types', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'], shift=rail.result('compute_mapper_keys')['shift'],
                gender=rail.result('compute_mapper_keys')['gender'], cost_center=rail.result('compute_mapper_keys')['cost_center'])
        )

        if_timeoff_types_present_and_active = rail.IfOperator(
            task_id='if_timeoff_types_present_and_active',
            test='''{{ result('log_timeoff_types') | is_truthy and dag_run.conf.Active == '1' }}''',
            yes_task='trigger_timeoff_add_new_user',
            no_task='log_user_created'
        )

        trigger_timeoff_add_new_user = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_add_new_user',
            retries=0,
            trigger_dag_id=config.momentive_thailand_user_sync_child_add_timeoff_new_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "user_import_logs": dag_run.conf['user_import_logs'],
                "lastname": dag_run.conf['Last_Name'],
                "firstname": dag_run.conf['First_Name'],
                "employeeid": dag_run.conf['Worker_Reference_Employee_ID'],
                "loginname": dag_run.conf['User_ID'],
                "supervisor": dag_run.conf['Manager_ID'],
                "emailaddress": dag_run.conf['Email_Address'],
                "startdate": dag_run.conf['Hire_Date'],
                "useruri": rail.result('create_user')['uri'],
                "workertype": dag_run.conf['Worker_Type'],
                "effectivedate_workertype": dag_run.conf['Effective_Date_of_Worker_Type'],
                "exemptionstatus": dag_run.conf['Exemption_Status'],
                "exemption_effdate": dag_run.conf['Exemption_Eff_Date'],
                "gender": dag_run.conf['Gender'],
                "terminationdate": dag_run.conf['Termination_Date'],
                "active": dag_run.conf['Active'],
                "function": dag_run.conf['Function'],
                "function_effdate": dag_run.conf['Function_Change_Effective_Date'],
                "businesstitle": dag_run.conf['Business_Title'],
                "businesstitle_effdate": dag_run.conf['CF_LRV_Business_Title_Change_Eff_Date'],
                "fieldhr": dag_run.conf['Field_HR'],
                "workshift": dag_run.conf['Work_Shift'],
                "workshift_effdate": dag_run.conf['Work_Shift_Change_Effective_Date'],
                "location": dag_run.conf['Location'],
                "location_effdate": dag_run.conf['CF_LRV_Location_Change_Effective_Date'],
                "birthdate": dag_run.conf['CF_Date_of_Birth_MM_DD_YYYY'],
                "sup_email": dag_run.conf['CF_LRV_Manager_Email'],
                "sup_firstname": dag_run.conf['CF_LRV_Manager_First_Name'],
                "sup_lastname": dag_run.conf['CF_LRV_Manager_Last_Name'],
                "timeofftypes": rail.result('log_timeoff_types'),
            }
        )

        wait_for_timeoff_add_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_add_new_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_add_new_user") }}'
        )

        # Child->parent error aggregation: gather the timeoff child's catch_error.
        # Truthy => the child failed; surface it so it lands in user_import_logs.
        gather_result_from_timeoff_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_result_from_timeoff_child',
            dag_runs='''{{ result('trigger_timeoff_add_new_user') }}''',
            dagrun_task_id='catch_error',
            target='result'
        )

        if_error_in_timeoff_child = rail.IfOperator(
            task_id='if_error_in_timeoff_child',
            test='''{{ result('gather_result_from_timeoff_child') | is_truthy }}''',
            yes_task='stop_processing_due_to_error_in_child',
            no_task='log_user_created'
        )

        stop_processing_due_to_error_in_child = rail.FailOperator(
            task_id='stop_processing_due_to_error_in_child',
            message='Error in adding timeoff types for new user'
        )

        # Recipe [163]: success log. Status escalates to Exception when any
        # soft-failure was accumulated; details appends those reasons.
        log_user_created = rail.WriteLogOperator(
            task_id='log_user_created',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity=lambda: "Exception" if rail.get_dag_run_var('exceptionlogger_list') else "Success",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['User_ID'],
                "username": rail.render_template("{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}"),
                "action": "Add",
                "status": "Exception" if rail.get_dag_run_var('exceptionlogger_list') else "Success",
                "details": ";".join(["User added successfully"] + [log['log'] for log in rail.get_dag_run_var('exceptionlogger_list')])
                if rail.get_dag_run_var('exceptionlogger_list') else "User added successfully",
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
            }
        )

        # Recipe [164]-[167]: error log. "partially updated" once the user exists, else "not created".
        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Add",
                "status": "Error",
                "details": '''{%- if get_task_state("create_user") == "success" -%}User created, but partially updated, {{ get_error_message() }}{%- else -%}User not created, {{ get_error_message() }}{%- endif -%}''',
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        finish = rail.EmptyOperator(task_id='finish')

        # ---- wiring ----
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_exceptionlogger_list

        create_exceptionlogger_list >> validate_required_fields >> if_required_fields_missing
        if_required_fields_missing >> rail.Label('Yes') >> log_user_not_created_required >> finish
        if_required_fields_missing >> rail.Label('No') >> if_gender_blank_and_employee
        if_gender_blank_and_employee >> rail.Label('Yes') >> log_user_not_created_gender >> finish
        if_gender_blank_and_employee >> rail.Label('No') >> get_all_employee_type_details

        get_all_employee_type_details >> get_all_permission_sets >> get_all_timezones >> compute_mapper_keys \
            >> log_employee_type_group_uri >> if_emp_type_group_and_dept_blank
        if_emp_type_group_and_dept_blank >> rail.Label('Yes') >> log_user_not_created_no_emp_type >> finish
        if_emp_type_group_and_dept_blank >> rail.Label('No') >> if_emp_type_group_and_dept_present
        if_emp_type_group_and_dept_present >> rail.Label('No') >> finish
        if_emp_type_group_and_dept_present >> rail.Label('Yes') >> get_split_hiredate >> get_all_pay_rule_scripts \
            >> log_pre_putuser_values >> create_user >> unassign_all_timeoffs >> get_custom_fields >> if_dob_present

        if_dob_present >> rail.Label('Yes') >> update_dob_udf >> if_title_present
        if_dob_present >> rail.Label('No') >> if_title_present
        if_title_present >> rail.Label('Yes') >> update_title_udf >> if_yos_present
        if_title_present >> rail.Label('No') >> if_yos_present
        if_yos_present >> rail.Label('Yes') >> update_yos_udf >> if_hrm_present
        if_yos_present >> rail.Label('No') >> if_hrm_present
        if_hrm_present >> rail.Label('Yes') >> update_hrm_udf >> if_gender_present
        if_hrm_present >> rail.Label('No') >> if_gender_present
        if_gender_present >> rail.Label('Yes') >> update_gender_udf >> if_manager_present
        if_gender_present >> rail.Label('No') >> if_manager_present

        if_manager_present >> rail.Label('Yes') >> search_supervisor >> if_multiple_supervisors
        if_manager_present >> rail.Label('No') >> if_location_not_bangkok
        if_multiple_supervisors >> rail.Label('Yes') >> log_exception_supervisor_multiple >> if_location_not_bangkok
        if_multiple_supervisors >> rail.Label('No') >> if_supervisor_exists
        if_supervisor_exists >> rail.Label('Yes') >> log_supervisor_details >> if_supervisor_enabled
        if_supervisor_exists >> rail.Label('No') >> log_supervisor_assignment_entry_not_found >> if_location_not_bangkok
        if_supervisor_enabled >> rail.Label('Yes') >> get_assigned_permission_sets_for_supervisor >> if_supervisor_has_no_permission
        if_supervisor_enabled >> rail.Label('No') >> log_supervisor_assignment_entry_disabled >> if_location_not_bangkok
        if_supervisor_has_no_permission >> rail.Label('Yes') >> assign_supervisor_permission >> update_initial_supervisor
        if_supervisor_has_no_permission >> rail.Label('No') >> update_initial_supervisor
        update_initial_supervisor >> if_location_not_bangkok

        if_location_not_bangkok >> rail.Label('Yes') >> get_timesheet_for_date >> get_all_days_of_week
        if_location_not_bangkok >> rail.Label('No') >> get_all_days_of_week
        get_all_days_of_week >> log_work_week_uri >> if_work_week_present
        if_work_week_present >> rail.Label('Yes') >> update_work_week >> get_all_office_schedules
        if_work_week_present >> rail.Label('No') >> log_exception_work_week_not_found >> get_all_office_schedules

        get_all_office_schedules >> log_schedule_name >> if_schedule_is_shift
        if_schedule_is_shift >> rail.Label('Yes') >> put_shift_schedule >> get_all_holiday_calendars
        if_schedule_is_shift >> rail.Label('No') >> log_office_schedule_uri >> if_office_schedule_present
        if_office_schedule_present >> rail.Label('Yes') >> put_office_schedule >> get_all_holiday_calendars
        if_office_schedule_present >> rail.Label('No') >> log_exception_schedule_not_found >> get_all_holiday_calendars

        get_all_holiday_calendars >> log_holiday_calendar_uri >> if_holiday_calendar_present
        if_holiday_calendar_present >> rail.Label('Yes') >> update_holiday_calendar >> log_activity_name
        if_holiday_calendar_present >> rail.Label('No') >> log_exception_holiday_not_found >> log_activity_name

        log_activity_name >> if_activity_present
        if_activity_present >> rail.Label('Yes') >> get_enabled_activities >> put_activity_assignments >> if_legalentity_present
        if_activity_present >> rail.Label('No') >> if_legalentity_present

        if_legalentity_present >> rail.Label('Yes') >> search_legal_entity >> if_legalentity_uri_present
        if_legalentity_present >> rail.Label('No') >> if_location_present_dept
        if_legalentity_uri_present >> rail.Label('Yes') >> put_division_schedule >> if_location_present_dept
        if_legalentity_uri_present >> rail.Label('No') >> if_location_present_dept

        if_location_present_dept >> rail.Label('Yes') >> search_department_group >> if_department_uri_present
        if_location_present_dept >> rail.Label('No') >> if_paygroup_present
        if_department_uri_present >> rail.Label('Yes') >> put_department_access_scope >> if_paygroup_present
        if_department_uri_present >> rail.Label('No') >> if_paygroup_present

        if_paygroup_present >> rail.Label('Yes') >> search_service_center >> if_service_center_uri_present
        if_paygroup_present >> rail.Label('No') >> if_costcenter_present
        if_service_center_uri_present >> rail.Label('Yes') >> put_service_center_schedule >> if_costcenter_present
        if_service_center_uri_present >> rail.Label('No') >> if_costcenter_present

        if_costcenter_present >> rail.Label('Yes') >> search_cost_center >> if_cost_center_uri_present
        if_costcenter_present >> rail.Label('No') >> log_timeoff_types
        if_cost_center_uri_present >> rail.Label('Yes') >> put_cost_center_schedule >> log_timeoff_types
        if_cost_center_uri_present >> rail.Label('No') >> log_timeoff_types

        log_timeoff_types >> if_timeoff_types_present_and_active
        if_timeoff_types_present_and_active >> rail.Label('Yes') >> trigger_timeoff_add_new_user \
            >> wait_for_timeoff_add_new_user >> gather_result_from_timeoff_child >> if_error_in_timeoff_child
        if_error_in_timeoff_child >> rail.Label('Yes') >> stop_processing_due_to_error_in_child >> log_user_created
        if_error_in_timeoff_child >> rail.Label('No') >> log_user_created
        if_timeoff_types_present_and_active >> rail.Label('No') >> log_user_created
        log_user_created >> catch_and_log_error >> finish

    return dag


rail.for_each_instance(create_dag)
