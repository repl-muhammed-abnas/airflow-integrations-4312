
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_add_user_dag_id,
        description=f'CentricBrands User Import - Add User Child',
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
            no_task='if_loginname_not_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_loginname_not_present',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_loginname_not_present = rail.IfOperator(
            task_id='if_loginname_not_present',
            test='''{{ dag_run.conf.loginname | is_falsy }}''',
            yes_task="log_username_not_present",
            no_task="if_isloginenabled_not_equals_to_yes",
        )

        log_username_not_present = rail.WriteLogOperator(
            task_id='log_username_not_present',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added - Loginname is required",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        if_isloginenabled_not_equals_to_yes = rail.IfOperator(
            task_id='if_isloginenabled_not_equals_to_yes',
            test='''{{ dag_run.conf.isloginenabled != 'Yes' }}''',
            yes_task="log_isloginenabled_not_set_yes",
            no_task="search_user_by_loginname",
        )

        log_isloginenabled_not_set_yes = rail.WriteLogOperator(
            task_id='log_isloginenabled_not_set_yes',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added - 'Isloginenabled' is not set to 'Yes'",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        def get_user_uri_by_loginname(response, dag_run):
            users_found = response['rows']
            matching_user = list(filter(
                lambda user: user['cells'][1]['textValue'] == dag_run.conf['loginname'], users_found))
            return matching_user[0]['cells'][0]['uri'] if matching_user else ''

        search_user_by_loginname = rail.RepliconServiceOperator(
            task_id='search_user_by_loginname',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{ dag_run.conf.loginname }}"
                        }
                    }
                }
            },
            data_handler=get_user_uri_by_loginname
        )

        if_useruri_present = rail.IfOperator(
            task_id='if_useruri_present',
            test='''{{ result('search_user_by_loginname') | is_truthy }}''',
            yes_task="log_user_already_exists",
            no_task="If_startdate_not_present",
        )

        log_user_already_exists = rail.WriteLogOperator(
            task_id='log_user_already_exists',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added - Loginname '{{dag_run.conf.loginname}}' already exists",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        If_startdate_not_present = rail.IfOperator(
            task_id='If_startdate_not_present',
            test='''{{ dag_run.conf.startdate | is_falsy }}''',
            yes_task="log_startdate_is_not_present",
            no_task="if_integrationdate_not_present",
        )

        log_startdate_is_not_present = rail.WriteLogOperator(
            task_id='log_startdate_is_not_present',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added - Start Date is required",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        if_integrationdate_not_present = rail.IfOperator(
            task_id='if_integrationdate_not_present',
            test='''{{ dag_run.conf.integrationdate | is_falsy }}''',
            yes_task="log_integrationdate_not_present",
            no_task="if_supervisorloginname_not_present",
        )

        log_integrationdate_not_present = rail.WriteLogOperator(
            task_id='log_integrationdate_not_present',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added - Integration Date is required",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        if_supervisorloginname_not_present = rail.IfOperator(
            task_id='if_supervisorloginname_not_present',
            test='''{{ dag_run.conf.supervisorloginname | is_falsy }}''',
            yes_task="log_supervisorloginname_not_present",
            no_task="if_firstname_not_present",
        )

        log_supervisorloginname_not_present = rail.WriteLogOperator(
            task_id='log_supervisorloginname_not_present',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added - Supervisor Loginname is required",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        if_firstname_not_present = rail.IfOperator(
            task_id='if_firstname_not_present',
            test='''{{ dag_run.conf.firstname | is_falsy }}''',
            yes_task="log_firstname_not_present",
            no_task="if_lastname_not_present",
        )

        log_firstname_not_present = rail.WriteLogOperator(
            task_id='log_firstname_not_present',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added - First Name is required",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        if_lastname_not_present = rail.IfOperator(
            task_id='if_lastname_not_present',
            test='''{{ dag_run.conf.lastname | is_falsy }}''',
            yes_task="log_lastname_not_present",
            no_task="if_employeetype_not_present",
        )

        log_lastname_not_present = rail.WriteLogOperator(
            task_id='log_lastname_not_present',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added - Last Name is required",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        if_employeetype_not_present = rail.IfOperator(
            task_id='if_employeetype_not_present',
            test='''{{ dag_run.conf.employeetype | is_falsy }}''',
            yes_task="log_employeetype_not_present",
            no_task="if_integrationdate_earlier_than_startdate",
        )

        log_employeetype_not_present = rail.WriteLogOperator(
            task_id='log_employeetype_not_present',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added - Employee Type is required",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        if_integrationdate_earlier_than_startdate = rail.IfOperator(
            task_id='if_integrationdate_earlier_than_startdate',
            test=lambda dag_run: bool(dag_run.conf['integrationdate'] and dag_run.conf['startdate'] and (datetime.strptime(
                dag_run.conf['integrationdate'], '%m/%d/%Y') < datetime.strptime(dag_run.conf['startdate'], '%m/%d/%Y'))),
            yes_task="log_integrationdate_earlier_than_startdate",
            no_task="get_all_custom_fields",
        )

        log_integrationdate_earlier_than_startdate = rail.WriteLogOperator(
            task_id='log_integrationdate_earlier_than_startdate',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "status": "Skipped",
                "details": "User not added. Integration Date is earlier than Start Date provided.",
                "jobid": "{{ dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "department|location|team": "||"
            }
        )

        def get_required_customfields_uri(response):
            return {
                'integrationdateuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Integration Date', 'uri', null),
                'stateprovinceuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'State/Province', 'uri', null),
                'chinacareerstartdateuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'China Career Start Date', 'uri', null),
                'honkonglevelsuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'HongKong Levels', 'uri', null)
            }

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=get_required_customfields_uri
        )

        def get_date_object(datestring):
            date_obj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': date_obj.day,
                'month': date_obj.month,
                'year': date_obj.year
            }

        get_startdate_object = rail.PythonOperator(
            task_id='get_startdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['startdate'])
        )

        get_integrationdate_object = rail.PythonOperator(
            task_id='get_integrationdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['integrationdate'])
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/ImportService1.svc/PutUser3",
            data={
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": "{{ dag_run.conf.loginname }}",
                        "parameterCorrelationId": null
                    },
                    "firstname": "{{ dag_run.conf.firstname }}",
                    "lastname": "{{ dag_run.conf.lastname }}",
                    "emailAddress": null,
                    "employeeId": null,
                    "department": {
                        "uri": "urn:replicon-tenant:{{ dag_run.conf.slug }}:department:1",
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": null,
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": "{{ result('get_startdate_object').year }}",
                            "month": "{{ result('get_startdate_object').month }}",
                            "day": "{{ result('get_startdate_object').day }}"
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:replicon"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": "{{ dag_run.conf.loginname }}",
                        "SSOName": null,
                        "password": "Centricbrands2019"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [],
                    "policySets": [],
                    "employeeType": {
                        "uri": null,
                        "name": "{{ dag_run.conf.employeetype }}"
                    },
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": [
                        {
                            "customField": {
                                "uri": "{{ result('get_all_custom_fields').integrationdateuri }}",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": {
                                "year": "{{ result('get_integrationdate_object').year }}",
                                "month": "{{ result('get_integrationdate_object').month }}",
                                "day": "{{ result('get_integrationdate_object').day }}"
                            },
                            "dropDownOption": null,
                            "number": null
                        }
                    ],
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": [],
                    "employeeTypeGroupSchedule": [],
                    "timesheetPeriodSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        if_departmentname_not_present = rail.IfOperator(
            task_id='if_departmentname_not_present',
            test='''{{ dag_run.conf.departmentname | is_falsy }}''',
            yes_task="log_departmentname_not_present",
            no_task="if_departmentname_present",
        )

        log_departmentname_not_present = rail.PythonOperator(
            task_id='log_departmentname_not_present',
            python_callable=lambda:  "Department name is required." +
            "User added to 'Centric Brands' department;"
        )

        if_departmentname_present = rail.IfOperator(
            task_id='if_departmentname_present',
            test='''{{ dag_run.conf.departmentname | is_truthy }}''',
            yes_task="if_inputdepartmenturi_present",
            no_task="unassign_all_timeoff_types",
        )

        if_inputdepartmenturi_present = rail.IfOperator(
            task_id='if_inputdepartmenturi_present',
            test=lambda dag_run: dag_run.conf['inputdepartmenturi'] and 'urn' in dag_run.conf['inputdepartmenturi'],
            yes_task="update_department_for_user",
            no_task="log_departmentname_not_found",
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/departmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "departmentUri": "{{ dag_run.conf.inputdepartmenturi }}"
            }
        )

        log_departmentname_not_found = rail.PythonOperator(
            task_id='log_departmentname_not_found',
            python_callable=lambda dag_run:  "Department name '" +
            dag_run.conf['departmentname'] + "' not found while Adding user;"
        )

        unassign_all_timeoff_types = rail.RepliconServiceOperator(
            task_id='unassign_all_timeoff_types',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timeOffTypeUris": []
            }
        )

        if_email_present_and_valid = rail.IfOperator(
            task_id='if_email_present_and_valid',
            test=lambda dag_run: dag_run.conf['email'] and '@' in dag_run.conf['email'],
            yes_task="update_email",
            no_task="log_email_format_incorrect",
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "email": "{{ dag_run.conf.email }}"
            }
        )

        log_email_format_incorrect = rail.PythonOperator(
            task_id='log_email_format_incorrect',
            python_callable=lambda dag_run:  "Email address not updated - Incorrect email format '" +
            dag_run.conf['email'] + "';"
        )

        if_employeeid_present = rail.IfOperator(
            task_id='if_employeeid_present',
            test='''{{ dag_run.conf.employeeid | is_truthy }}''',
            yes_task="update_employee_id",
            no_task="if_authenticationtype_present_is_sso",
        )

        update_employee_id = rail.RepliconServiceOperator(
            task_id='update_employee_id',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "employeeId": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_authenticationtype_present_is_sso = rail.IfOperator(
            task_id='if_authenticationtype_present_is_sso',
            test=lambda dag_run: dag_run.conf['authenticationtype'] and 'sso' in (
                dag_run.conf['authenticationtype']).lower(),
            yes_task="set_sso_authentication_for_user",
            no_task="if_authenticationtype_present_is_replicon",
        )

        set_sso_authentication_for_user = rail.RepliconServiceOperator(
            task_id='set_sso_authentication_for_user',
            endpoint="/services/securityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "loginName": "{{ dag_run.conf.loginname }}"
            }
        )

        if_authenticationtype_present_is_replicon = rail.IfOperator(
            task_id='if_authenticationtype_present_is_replicon',
            test=lambda dag_run: dag_run.conf['authenticationtype'] and 'replicon' in (
                dag_run.conf['authenticationtype']).lower(),
            yes_task="set_replicon_authentication_for_user",
            no_task="if_licenseseats_contains_timeoffenterprise",
        )

        set_replicon_authentication_for_user = rail.RepliconServiceOperator(
            task_id='set_replicon_authentication_for_user',
            endpoint="/services/securityService1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "loginName": "{{ dag_run.conf.loginname }}",
                "password": "Centricbrands2019",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        if_licenseseats_contains_timeoffenterprise = rail.IfOperator(
            task_id='if_licenseseats_contains_timeoffenterprise',
            test=lambda dag_run: dag_run.conf['licenseseats'] and 'timeoff enterprise' in (
                dag_run.conf['licenseseats']).lower(),
            yes_task="put_product_assignments_for_user",
            no_task="if_userpermission_or_supervisorpermission_present",
        )

        put_product_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "productUris": [
                    "urn:replicon-saas:product:time-off-enterprise"
                ]
            }
        )

        if_userpermission_or_supervisorpermission_present = rail.IfOperator(
            task_id='if_userpermission_or_supervisorpermission_present',
            test='''{{ dag_run.conf.userpermission | is_truthy  or dag_run.conf.supervisorpermission | is_truthy }}''',
            yes_task="get_all_permission_sets",
            no_task="get_all_cost_centers_locations",
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_required_permissions_uri = rail.PythonOperator(
            task_id='get_required_permissions_uri',
            python_callable=lambda dag_run: {
                'userpermission': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets'), 'name', dag_run.conf['userpermission'], 'uri', '') if dag_run.conf['userpermission'] else null,
                'supervisorpermission': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets'), 'name', dag_run.conf['supervisorpermission'], 'uri', '') if dag_run.conf['supervisorpermission'] else null
            }
        )

        if_userpermission_uri_present = rail.IfOperator(
            task_id='if_userpermission_uri_present',
            test='''{{ result('get_required_permissions_uri').userpermission | is_truthy }}''',
            yes_task="assign_permission_set_to_user",
            no_task="if_supervisorpermission_uri_present",
        )

        assign_permission_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "permissionSetUri": "{{ result('get_required_permissions_uri').userpermission }}"
            }
        )

        if_supervisorpermission_uri_present = rail.IfOperator(
            task_id='if_supervisorpermission_uri_present',
            test='''{{ result('get_required_permissions_uri').supervisorpermission | is_truthy }}''',
            yes_task="assign_supervisorpermission_set_to_user",
            no_task="get_all_cost_centers_locations",
        )

        assign_supervisorpermission_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_supervisorpermission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "permissionSetUri": "{{ result('get_required_permissions_uri').supervisorpermission }}"
            }
        )

        get_all_cost_centers_locations = rail.RepliconServiceOperator(
            task_id='get_all_cost_centers_locations',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        if_location_present = rail.IfOperator(
            task_id='if_location_present',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task="if_inputlocationuri_starts_with_urn",
            no_task="if_team_present",
        )

        if_inputlocationuri_starts_with_urn = rail.IfOperator(
            task_id='if_inputlocationuri_starts_with_urn',
            test=lambda dag_run: 'urn' in dag_run.conf['inputlocationuri'],
            yes_task="if_locationeffectivedate_present",
            no_task="log_location_not_found",
        )

        if_locationeffectivedate_present = rail.IfOperator(
            task_id='if_locationeffectivedate_present',
            test='''{{ dag_run.conf.locationeffectivedate | is_truthy }}''',
            yes_task="get_locationeffectivedate_object",
            no_task="put_cost_center_schedule_for_user_without_effectivedate",
        )

        get_locationeffectivedate_object = rail.PythonOperator(
            task_id='get_locationeffectivedate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['locationeffectivedate'])
        )

        put_cost_center_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "costCenter": {
                            "uri": "{{ dag_run.conf.inputlocationuri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": "{{ result('get_locationeffectivedate_object').year }}",
                            "month": "{{ result('get_locationeffectivedate_object').month }}",
                            "day": "{{ result('get_locationeffectivedate_object').day }}"
                        }
                    }
                ]
            }
        )

        put_cost_center_schedule_for_user_without_effectivedate = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_without_effectivedate',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "costCenter": {
                            "uri": "{{ dag_run.conf.inputlocationuri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_location_not_found = rail.PythonOperator(
            task_id='log_location_not_found',
            python_callable=lambda dag_run:  "Location '" +
            dag_run.conf['location'] + "' not found while adding user;"
        )

        if_team_present = rail.IfOperator(
            task_id='if_team_present',
            test='''{{ dag_run.conf.team | is_truthy }}''',
            yes_task="if_inputteamuri_starts_with_urn",
            no_task="if_stateprovince_present",
        )

        if_inputteamuri_starts_with_urn = rail.IfOperator(
            task_id='if_inputteamuri_starts_with_urn',
            test=lambda dag_run: 'urn' in dag_run.conf['inputteamuri'],
            yes_task="if_teameffectivedate_present",
            no_task="log_team_not_found",
        )

        if_teameffectivedate_present = rail.IfOperator(
            task_id='if_teameffectivedate_present',
            test='''{{ dag_run.conf.teameffectivedate | is_truthy }}''',
            yes_task="get_team_effectivedate_object",
            no_task="put_location_schedule_for_user_without_effectivedate",
        )

        get_team_effectivedate_object = rail.PythonOperator(
            task_id='get_team_effectivedate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['teameffectivedate'])
        )

        put_location_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ dag_run.conf.inputteamuri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": "{{ result('get_team_effectivedate_object').year }}",
                            "month": "{{ result('get_team_effectivedate_object').month }}",
                            "day": "{{ result('get_team_effectivedate_object').day }}"
                        }
                    }
                ]
            }
        )

        put_location_schedule_for_user_without_effectivedate = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_without_effectivedate',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ dag_run.conf.inputteamuri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_team_not_found = rail.PythonOperator(
            task_id='log_team_not_found',
            python_callable=lambda dag_run:  "Team '" +
            dag_run.conf['team'] + "' not found while Adding user;"
        )

        if_stateprovince_present = rail.IfOperator(
            task_id='if_stateprovince_present',
            test='''{{ dag_run.conf.stateprovince | is_truthy }}''',
            yes_task="get_all_custom_field_drop_down_options",
            no_task="if_supervisorloginname_present",
        )

        get_all_custom_field_drop_down_options = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_all_custom_fields')['stateprovinceuri']
            }
        )

        get_required_dropdownoption_uri = rail.PythonOperator(
            task_id='get_required_dropdownoption_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_field_drop_down_options'), 'displayText', dag_run.conf['stateprovince'], 'uri', '')
        )

        if_required_dropdownoption_present = rail.IfOperator(
            task_id='if_required_dropdownoption_present',
            test='''{{ result('get_required_dropdownoption_uri') | is_truthy }}''',
            yes_task="update_stateprovince_dropdown_value",
            no_task="log_stateprovince_not_found",
        )

        update_stateprovince_dropdown_value = rail.RepliconServiceOperator(
            task_id='update_stateprovince_dropdown_value',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_all_custom_fields').stateprovinceuri }}",
                "customFieldDropDownOptionUri": "{{ result('get_required_dropdownoption_uri') }}"
            }
        )

        log_stateprovince_not_found = rail.PythonOperator(
            task_id='log_stateprovince_not_found',
            python_callable=lambda dag_run:  "State/Province '" +
            dag_run.conf['stateprovince'] + "' not found while Adding user;"
        )

        if_supervisorloginname_present = rail.IfOperator(
            task_id='if_supervisorloginname_present',
            test='''{{ dag_run.conf.supervisorloginname | is_truthy }}''',
            yes_task="search_supervisoruser_by_loginname",
            no_task="if_timeofftemplate_present",
        )

        def find_matching_user_uri_and_status(response, dag_run):
            users_found = response['rows']
            matching_user = list(filter(
                lambda user: user['cells'][1]['textValue'] == dag_run.conf['supervisorloginname'], users_found))
            return {
                'uri': matching_user[0]['cells'][0]['uri'] if matching_user else '',
                'status': matching_user[0]['cells'][2]['textValue'] if matching_user else ''
            }

        search_supervisoruser_by_loginname = rail.RepliconServiceOperator(
            task_id='search_supervisoruser_by_loginname',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{ dag_run.conf.supervisorloginname }}"
                        }
                    }
                }
            },
            data_handler=find_matching_user_uri_and_status
        )

        if_supervisor_uri_present = rail.IfOperator(
            task_id='if_supervisor_uri_present',
            test='''{{ result('search_supervisoruser_by_loginname').uri | is_truthy }}''',
            yes_task="if_supervisor_status_equals_true",
            no_task="add_entry_to_supervisorassignment_lookup",
        )

        if_supervisor_status_equals_true = rail.IfOperator(
            task_id='if_supervisor_status_equals_true',
            test='''{{ result('search_supervisoruser_by_loginname').status == 'True' }}''',
            yes_task="get_assigned_permission_sets_for_user",
            no_task="add_entry_to_supervisorassignment_lookup",
        )

        get_assigned_permission_sets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_supervisoruser_by_loginname').uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.displayText', '')
        )

        if_supervisor_permission_present = rail.IfOperator(
            task_id='if_supervisor_permission_present',
            test='''{{ result('get_assigned_permission_sets_for_user') | is_truthy }}''',
            yes_task="if_supervisorstartdate_present",
            no_task="add_entry_to_supervisorassignment_lookup",
        )

        if_supervisorstartdate_present = rail.IfOperator(
            task_id='if_supervisorstartdate_present',
            test='''{{ dag_run.conf.supervisorstartdate | is_truthy }}''',
            yes_task="get_supervisorstartdate_object",
            no_task="put_supervisor_assignment_schedule_without_effectivedate",
        )

        get_supervisorstartdate_object = rail.PythonOperator(
            task_id='get_supervisorstartdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['supervisorstartdate'])
        )

        put_supervisor_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule2",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "supervisor": {
                            "uri": "{{ result('search_supervisoruser_by_loginname').uri }}",
                            "loginName": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": {
                            "year": "{{ result('get_supervisorstartdate_object').year }}",
                            "month": "{{ result('get_supervisorstartdate_object').month }}",
                            "day": "{{ result('get_supervisorstartdate_object').day }}"
                        }
                    }
                ]
            }
        )

        put_supervisor_assignment_schedule_without_effectivedate = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule_without_effectivedate',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule2",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "supervisor": {
                            "uri": "{{ result('search_supervisoruser_by_loginname').uri }}",
                            "loginName": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        add_entry_to_supervisorassignment_lookup = rail.WriteLogOperator(
            task_id='add_entry_to_supervisorassignment_lookup',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ result('create_user').uri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "supervisorstartdate": "{{ dag_run.conf.supervisorstartdate }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "assignedstatus": "Not assigned",
                "action": "new user"
            }
        )

        if_timeofftemplate_present = rail.IfOperator(
            task_id='if_timeofftemplate_present',
            test='''{{ dag_run.conf.timeofftemplate | is_truthy }}''',
            yes_task="get_all_policy_sets",
            no_task="if_timeoffapprovalpath_present",
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint="/services/policysetService1.svc/GetAllPolicySets",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'name', dag_run.conf['timeofftemplate'], 'uri', '')
        )

        if_time_off_template_uri_present = rail.IfOperator(
            task_id='if_time_off_template_uri_present',
            test='''{{ result('get_all_policy_sets') | is_truthy }}''',
            yes_task="assign_policy_set_to_user",
            no_task="log_timeofftemplate_not_found",
        )

        assign_policy_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user',
            endpoint="/services/policysetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "policySetUri": "{{ result('get_all_policy_sets') }}"
            }
        )

        log_timeofftemplate_not_found = rail.PythonOperator(
            task_id='log_timeofftemplate_not_found',
            python_callable=lambda dag_run:  "Time Off Template '" +
            dag_run.conf['timeofftemplate'] + "' not found while Adding user;"
        )

        if_timeoffapprovalpath_present = rail.IfOperator(
            task_id='if_timeoffapprovalpath_present',
            test='''{{ dag_run.conf.timeoffapprovalpath | is_truthy }}''',
            yes_task="get_all_approval_paths",
            no_task="if_holidaycalendar_present",
        )

        get_all_approval_paths = rail.RepliconServiceOperator(
            task_id='get_all_approval_paths',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['timeoffapprovalpath'], 'uri', '')
        )

        if_approvalpath_uri_present = rail.IfOperator(
            task_id='if_approvalpath_uri_present',
            test='''{{ result('get_all_approval_paths') | is_truthy }}''',
            yes_task="update_approval_path_for_user",
            no_task="log_timeoffapprovalpath_not_found",
        )

        update_approval_path_for_user = rail.RepliconServiceOperator(
            task_id='update_approval_path_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "approvalPathUri": "{{ result('get_all_approval_paths') }}"
            }
        )

        log_timeoffapprovalpath_not_found = rail.PythonOperator(
            task_id='log_timeoffapprovalpath_not_found',
            python_callable=lambda dag_run:  "Time Off approval path '" +
            dag_run.conf['timeoffapprovalpath'] +
            "' not found while Adding user;"
        )

        if_holidaycalendar_present = rail.IfOperator(
            task_id='if_holidaycalendar_present',
            test='''{{ dag_run.conf.holidaycalendar | is_truthy }}''',
            yes_task="get_all_holiday_calendars",
            no_task="if_chinacareerstartdate_present",
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'name', dag_run.conf['holidaycalendar'], 'uri', '')
        )

        if_holiday_calendar_uri_present = rail.IfOperator(
            task_id='if_holiday_calendar_uri_present',
            test='''{{ result('get_all_holiday_calendars') | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user",
            no_task="log_holiday_calendar_not_found",
        )

        update_holiday_calendar_for_user = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "holidayCalendarUri": "{{ result('get_all_holiday_calendars') }}"
            }
        )

        log_holiday_calendar_not_found = rail.PythonOperator(
            task_id='log_holiday_calendar_not_found',
            python_callable=lambda dag_run:  "Holiday Calendar '" +
            dag_run.conf['holidaycalendar'] + "' not found while Adding user;"
        )

        if_chinacareerstartdate_present = rail.IfOperator(
            task_id='if_chinacareerstartdate_present',
            test='''{{ dag_run.conf.chinacareerstartdate | is_truthy }}''',
            yes_task="if_chinacareerstartdate_uri_not_present",
            no_task="if_hongkonglevels_present",
        )

        if_chinacareerstartdate_uri_not_present = rail.IfOperator(
            task_id='if_chinacareerstartdate_uri_not_present',
            test="{{result('get_all_custom_fields').chinacareerstartdateuri | is_truthy}}",
            yes_task='update_chinacareerstartdate_for_user',
            no_task='if_hongkonglevels_present'
        )

        update_chinacareerstartdate_for_user = rail.RepliconServiceOperator(
            task_id='update_chinacareerstartdate_for_user',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.result('get_all_custom_fields')['chinacareerstartdateuri'],
                "value": {
                    "year": int(dag_run.conf['chinacareerstartdate'].split('/')[2]),
                    "month": int(dag_run.conf['chinacareerstartdate'].split('/')[0]),
                    "day": int(dag_run.conf['chinacareerstartdate'].split('/')[1])
                }
            }
        )

        if_hongkonglevels_present = rail.IfOperator(
            task_id='if_hongkonglevels_present',
            test='''{{ dag_run.conf.hongkonglevels | is_truthy }}''',
            yes_task='if_hongkonglevels_uri_not_present',
            no_task='if_schedule_present',
        )

        if_hongkonglevels_uri_not_present = rail.IfOperator(
            task_id='if_hongkonglevels_uri_not_present',
            test="{{result('get_all_custom_fields').honkonglevelsuri | is_truthy}}",
            yes_task='update_hongkonglevels_for_user',
            no_task='if_schedule_present'
        )

        update_hongkonglevels_for_user = rail.RepliconServiceOperator(
            task_id='update_hongkonglevels_for_user',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{result('create_user').uri}}",
                "customFieldUri": "{{result('get_all_custom_fields').honkonglevelsuri}}",
                "value": "{{ dag_run.conf.hongkonglevels}}"
            }
        )

        if_schedule_present = rail.IfOperator(
            task_id='if_schedule_present',
            test='''{{ dag_run.conf.schedule | is_truthy }}''',
            yes_task="get_all_office_schedules",
            no_task="trigger_child_to_add_user_timeoff",
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['schedule'], 'uri', '')
        )

        if_office_schedule_uri_present = rail.IfOperator(
            task_id='if_office_schedule_uri_present',
            test='''{{ result('get_all_office_schedules') | is_truthy }}''',
            yes_task="if_scheduleeffectivedate_present",
            no_task="log_office_schedule_not_found",
        )

        if_scheduleeffectivedate_present = rail.IfOperator(
            task_id='if_scheduleeffectivedate_present',
            test='''{{ dag_run.conf.scheduleeffectivedate | is_truthy }}''',
            yes_task="get_scheduleeffectivedate_object",
            no_task="put_schedule_policy_schedule_for_user_without_effectivedate",
        )

        get_scheduleeffectivedate_object = rail.PythonOperator(
            task_id='get_scheduleeffectivedate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['scheduleeffectivedate'])
        )

        put_schedule_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": "{{ result('get_all_office_schedules') }}",
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": {
                            "year": "{{ result('get_scheduleeffectivedate_object').year }}",
                            "month": "{{ result('get_scheduleeffectivedate_object').month }}",
                            "day": "{{ result('get_scheduleeffectivedate_object').day }}"
                        }
                    }
                ]
            }
        )

        put_schedule_policy_schedule_for_user_without_effectivedate = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_without_effectivedate',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": "{{ result('get_all_office_schedules') }}",
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_office_schedule_not_found = rail.PythonOperator(
            task_id='log_office_schedule_not_found',
            python_callable=lambda dag_run:  "Office Schedule '" +
            dag_run.conf['schedule'] + "' not found while Adding user;"
        )

        trigger_child_to_add_user_timeoff = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_add_user_timeoff',
            retries=0,
            trigger_dag_id=config.child_add_user_time_off_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "uri": "{{ result('create_user').uri }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "authenticationtype": "{{ dag_run.conf.authenticationtype }}",
                "departmentname": "{{ dag_run.conf.departmentname }}",
                "licenseseats": "{{ dag_run.conf.licenseseats }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "integrationdate": "{{ dag_run.conf.integrationdate }}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "userpermission": "{{ dag_run.conf.userpermission }}",
                "supervisorpermission": "{{ dag_run.conf.supervisorpermission }}",
                "teammanager": "{{ dag_run.conf.teammanager }}",
                "payrollmanager": "{{ dag_run.conf.payrollmanager }}",
                "administratorpermission": "{{ dag_run.conf.administratorpermission }}",
                "location": "{{ dag_run.conf.location }}",
                "locationeffectivedate": "{{ dag_run.conf.locationeffectivedate }}",
                "team": "{{ dag_run.conf.team }}",
                "teameffectivedate": "{{ dag_run.conf.teameffectivedate }}",
                "stateprovince": "{{ dag_run.conf.stateprovince }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "supervisorstartdate": "{{ dag_run.conf.supervisorstartdate }}",
                "timeofftemplate": "{{ dag_run.conf.timeofftemplate }}",
                "timeoffapprovalpath": "{{ dag_run.conf.timeoffapprovalpath }}",
                "holidaycalendar": "{{ dag_run.conf.holidaycalendar }}",
                "schedule": "{{ dag_run.conf.schedule }}",
                "scheduleeffectivedate": "{{ dag_run.conf.scheduleeffectivedate }}",
                "chinacareerstartdate": "{{dag_run.conf.chinacareerstartdate}}",
                "hongkonglevels": "{{dag_run.conf.hongkonglevels}}"
            }
        )

        wait_for_child_to_add_user_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_add_user_timeoff',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_add_user_timeoff") }}'
        )

        def get_exception_logs():
            return (rail.result('log_departmentname_not_present') if rail.result('log_departmentname_not_present') else '') + (rail.result(
                'log_departmentname_not_found') if rail.result('log_departmentname_not_found') else '') + (rail.result(
                    'log_email_format_incorrect') if rail.result('log_email_format_incorrect') else '') + (rail.result(
                        'log_location_not_found') if rail.result('log_location_not_found') else '') + (rail.result(
                            'log_team_not_found') if rail.result('log_team_not_found') else '') + (rail.result(
                                'log_stateprovince_not_found') if rail.result('log_stateprovince_not_found') else '') + (rail.result(
                                    'log_timeofftemplate_not_found') if rail.result('log_timeofftemplate_not_found') else '') + (rail.result(
                                        'log_timeoffapprovalpath_not_found') if rail.result('log_timeoffapprovalpath_not_found') else '') + (rail.result(
                                            'log_holiday_calendar_not_found') if rail.result('log_holiday_calendar_not_found') else '') + (rail.result(
                                                'log_office_schedule_not_found') if rail.result('log_office_schedule_not_found') else '')

        log_all_exceptions = rail.PythonOperator(
            task_id='log_all_exceptions',
            python_callable=get_exception_logs
        )

        add_final_log_for_user = rail.WriteLogOperator(
            task_id='add_final_log_for_user',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity=lambda: 'Exception' if rail.result(
                'log_all_exceptions') else 'Success',
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "empid": dag_run.conf['employeeid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": 'Exception' if rail.result('log_all_exceptions') else 'Success',
                "details": "User added." + rail.result('log_all_exceptions'),
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": dag_run.conf['departmentname'] + "|" + dag_run.conf['location'] + "|" + dag_run.conf['team']
            }
        )

        centricbrands_trial_user_import_logs_add_entry_157 = rail.WriteLogOperator(
            task_id='centricbrands_trial_user_import_logs_add_entry_157',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "empid": dag_run.conf['employeeid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": 'Error',
                "details": "Error - " + rail.render_template("{{get_error_message()}}") + get_exception_logs(),
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": dag_run.conf['departmentname'] + "|" + dag_run.conf['location'] + "|" + dag_run.conf['team']
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> if_loginname_not_present
        if_loginname_not_present >> rail.Label(
            'Yes') >> log_username_not_present >> finish
        if_loginname_not_present >> rail.Label(
            'No') >> if_isloginenabled_not_equals_to_yes
        if_isloginenabled_not_equals_to_yes >> rail.Label(
            'Yes') >> log_isloginenabled_not_set_yes >> finish
        if_isloginenabled_not_equals_to_yes >> rail.Label(
            'No') >> search_user_by_loginname >> if_useruri_present
        if_useruri_present >> rail.Label(
            'Yes') >> log_user_already_exists >> finish
        if_useruri_present >> rail.Label('No') >> If_startdate_not_present
        If_startdate_not_present >> rail.Label(
            'Yes') >> log_startdate_is_not_present >> finish
        If_startdate_not_present >> rail.Label(
            'No') >> if_integrationdate_not_present
        if_integrationdate_not_present >> rail.Label(
            'Yes') >> log_integrationdate_not_present >> finish
        if_integrationdate_not_present >> rail.Label(
            'No') >> if_supervisorloginname_not_present
        if_supervisorloginname_not_present >> rail.Label(
            'Yes') >> log_supervisorloginname_not_present >> finish
        if_supervisorloginname_not_present >> rail.Label(
            'No') >> if_firstname_not_present
        if_firstname_not_present >> rail.Label(
            'Yes') >> log_firstname_not_present >> finish
        if_firstname_not_present >> rail.Label('No') >> if_lastname_not_present
        if_lastname_not_present >> rail.Label(
            'Yes') >> log_lastname_not_present >> finish
        if_lastname_not_present >> rail.Label(
            'No') >> if_employeetype_not_present
        if_employeetype_not_present >> rail.Label(
            'Yes') >> log_employeetype_not_present >> finish
        if_employeetype_not_present >> rail.Label(
            'No') >> if_integrationdate_earlier_than_startdate
        if_integrationdate_earlier_than_startdate >> rail.Label(
            'Yes') >> log_integrationdate_earlier_than_startdate >> finish
        if_integrationdate_earlier_than_startdate >> rail.Label(
            'No') >> get_all_custom_fields >> get_startdate_object >> get_integrationdate_object >> create_user >> if_departmentname_not_present
        if_departmentname_not_present >> rail.Label(
            'Yes') >> log_departmentname_not_present >> if_departmentname_present
        if_departmentname_not_present >> rail.Label(
            'No') >> if_departmentname_present
        if_departmentname_present >> rail.Label(
            'Yes') >> if_inputdepartmenturi_present
        if_inputdepartmenturi_present >> rail.Label(
            'Yes') >> update_department_for_user >> unassign_all_timeoff_types
        if_inputdepartmenturi_present >> rail.Label(
            'No') >> log_departmentname_not_found >> unassign_all_timeoff_types
        if_departmentname_present >> rail.Label(
            'No') >> unassign_all_timeoff_types >> if_email_present_and_valid
        if_email_present_and_valid >> rail.Label(
            'Yes') >> update_email >> if_employeeid_present
        if_email_present_and_valid >> rail.Label(
            'No') >> log_email_format_incorrect >> if_employeeid_present
        if_employeeid_present >> rail.Label(
            'Yes') >> update_employee_id >> if_authenticationtype_present_is_sso
        if_employeeid_present >> rail.Label(
            'No') >> if_authenticationtype_present_is_sso
        if_authenticationtype_present_is_sso >> rail.Label(
            'Yes') >> set_sso_authentication_for_user >> if_authenticationtype_present_is_replicon
        if_authenticationtype_present_is_sso >> rail.Label(
            'No') >> if_authenticationtype_present_is_replicon
        if_authenticationtype_present_is_replicon >> rail.Label(
            'Yes') >> set_replicon_authentication_for_user >> if_licenseseats_contains_timeoffenterprise
        if_authenticationtype_present_is_replicon >> rail.Label(
            'No') >> if_licenseseats_contains_timeoffenterprise
        if_licenseseats_contains_timeoffenterprise >> rail.Label(
            'Yes') >> put_product_assignments_for_user >> if_userpermission_or_supervisorpermission_present
        if_licenseseats_contains_timeoffenterprise >> rail.Label(
            'No') >> if_userpermission_or_supervisorpermission_present
        if_userpermission_or_supervisorpermission_present >> rail.Label(
            'Yes') >> get_all_permission_sets >> get_required_permissions_uri >> if_userpermission_uri_present
        if_userpermission_uri_present >> rail.Label(
            'Yes') >> assign_permission_set_to_user >> if_supervisorpermission_uri_present
        if_userpermission_uri_present >> rail.Label(
            'No') >> if_supervisorpermission_uri_present
        if_supervisorpermission_uri_present >> rail.Label(
            'Yes') >> assign_supervisorpermission_set_to_user >> get_all_cost_centers_locations
        if_supervisorpermission_uri_present >> rail.Label(
            'No') >> get_all_cost_centers_locations
        if_userpermission_or_supervisorpermission_present >> rail.Label(
            'No') >> get_all_cost_centers_locations >> if_location_present
        if_location_present >> rail.Label(
            'Yes') >> if_inputlocationuri_starts_with_urn
        if_inputlocationuri_starts_with_urn >> rail.Label(
            'Yes') >> if_locationeffectivedate_present
        if_locationeffectivedate_present >> rail.Label(
            'Yes') >> get_locationeffectivedate_object >> put_cost_center_schedule_for_user >> if_team_present
        if_locationeffectivedate_present >> rail.Label(
            'No') >> put_cost_center_schedule_for_user_without_effectivedate >> if_team_present
        if_inputlocationuri_starts_with_urn >> rail.Label(
            'No') >> log_location_not_found >> if_team_present
        if_location_present >> rail.Label('No') >> if_team_present
        if_team_present >> rail.Label('Yes') >> if_inputteamuri_starts_with_urn
        if_inputteamuri_starts_with_urn >> rail.Label(
            'Yes') >> if_teameffectivedate_present
        if_teameffectivedate_present >> rail.Label(
            'Yes') >> get_team_effectivedate_object >> put_location_schedule_for_user >> if_stateprovince_present
        if_teameffectivedate_present >> rail.Label(
            'No') >> put_location_schedule_for_user_without_effectivedate >> if_stateprovince_present
        if_inputteamuri_starts_with_urn >> rail.Label(
            'No') >> log_team_not_found >> if_stateprovince_present
        if_team_present >> rail.Label('No') >> if_stateprovince_present
        if_stateprovince_present >> rail.Label(
            'Yes') >> get_all_custom_field_drop_down_options >> get_required_dropdownoption_uri >> if_required_dropdownoption_present
        if_required_dropdownoption_present >> rail.Label(
            'Yes') >> update_stateprovince_dropdown_value >> if_supervisorloginname_present
        if_required_dropdownoption_present >> rail.Label(
            'No') >> log_stateprovince_not_found >> if_supervisorloginname_present
        if_stateprovince_present >> rail.Label(
            'No') >> if_supervisorloginname_present
        if_supervisorloginname_present >> rail.Label(
            'Yes') >> search_supervisoruser_by_loginname >> if_supervisor_uri_present
        if_supervisor_uri_present >> rail.Label(
            'Yes') >> if_supervisor_status_equals_true
        if_supervisor_status_equals_true >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user >> if_supervisor_permission_present
        if_supervisor_permission_present >> rail.Label(
            'Yes') >> if_supervisorstartdate_present
        if_supervisorstartdate_present >> rail.Label(
            'Yes') >> get_supervisorstartdate_object >> put_supervisor_assignment_schedule >> if_timeofftemplate_present
        if_supervisorstartdate_present >> rail.Label(
            'No') >> put_supervisor_assignment_schedule_without_effectivedate >> if_timeofftemplate_present
        if_supervisor_permission_present >> rail.Label(
            'No') >> add_entry_to_supervisorassignment_lookup
        if_supervisor_status_equals_true >> rail.Label(
            'No') >> add_entry_to_supervisorassignment_lookup >> if_timeofftemplate_present
        if_supervisor_uri_present >> rail.Label(
            'No') >> add_entry_to_supervisorassignment_lookup >> if_timeofftemplate_present
        if_supervisorloginname_present >> rail.Label(
            'No') >> if_timeofftemplate_present
        if_timeofftemplate_present >> rail.Label(
            'Yes') >> get_all_policy_sets >> if_time_off_template_uri_present
        if_time_off_template_uri_present >> rail.Label(
            'Yes') >> assign_policy_set_to_user >> if_timeoffapprovalpath_present
        if_time_off_template_uri_present >> rail.Label(
            'No') >> log_timeofftemplate_not_found >> if_timeoffapprovalpath_present
        if_timeofftemplate_present >> rail.Label(
            'No') >> if_timeoffapprovalpath_present
        if_timeoffapprovalpath_present >> rail.Label(
            'Yes') >> get_all_approval_paths >> if_approvalpath_uri_present
        if_approvalpath_uri_present >> rail.Label(
            'Yes') >> update_approval_path_for_user >> if_holidaycalendar_present
        if_approvalpath_uri_present >> rail.Label(
            'No') >> log_timeoffapprovalpath_not_found >> if_holidaycalendar_present
        if_timeoffapprovalpath_present >> rail.Label(
            'No') >> if_holidaycalendar_present
        if_holidaycalendar_present >> rail.Label(
            'Yes') >> get_all_holiday_calendars >> if_holiday_calendar_uri_present
        if_holiday_calendar_uri_present >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user >> if_chinacareerstartdate_present
        if_holiday_calendar_uri_present >> rail.Label(
            'No') >> log_holiday_calendar_not_found >> if_chinacareerstartdate_present
        if_holidaycalendar_present >> rail.Label(
            'No') >> if_chinacareerstartdate_present
        if_chinacareerstartdate_present >> rail.Label(
            'No') >> if_hongkonglevels_present
        if_chinacareerstartdate_present >> rail.Label(
            'Yes') >> if_chinacareerstartdate_uri_not_present
        if_chinacareerstartdate_uri_not_present >> rail.Label(
            'No') >> if_hongkonglevels_present
        if_chinacareerstartdate_uri_not_present >> rail.Label(
            'Yes') >> update_chinacareerstartdate_for_user >> if_hongkonglevels_present
        if_hongkonglevels_present >> rail.Label('No') >> if_schedule_present
        if_hongkonglevels_present >> rail.Label(
            'Yes') >> if_hongkonglevels_uri_not_present
        if_hongkonglevels_uri_not_present >> rail.Label(
            'No') >> if_schedule_present
        if_hongkonglevels_uri_not_present >> rail.Label(
            'Yes') >> update_hongkonglevels_for_user >> if_schedule_present
        if_schedule_present >> rail.Label(
            'Yes') >> get_all_office_schedules >> if_office_schedule_uri_present
        if_office_schedule_uri_present >> rail.Label(
            'Yes') >> if_scheduleeffectivedate_present
        if_scheduleeffectivedate_present >> rail.Label(
            'Yes') >> get_scheduleeffectivedate_object >> put_schedule_policy_schedule_for_user >> trigger_child_to_add_user_timeoff
        if_scheduleeffectivedate_present >> rail.Label(
            'No') >> put_schedule_policy_schedule_for_user_without_effectivedate >> trigger_child_to_add_user_timeoff
        if_office_schedule_uri_present >> rail.Label(
            'No') >> log_office_schedule_not_found >> trigger_child_to_add_user_timeoff
        if_schedule_present >> rail.Label(
            'No') >> trigger_child_to_add_user_timeoff >> wait_for_child_to_add_user_timeoff >> log_all_exceptions >> add_final_log_for_user
        add_final_log_for_user >> centricbrands_trial_user_import_logs_add_entry_157 >> finish

    return dag


rail.for_each_instance(create_dag)
