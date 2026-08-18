
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_update_user_dag_id,
        description=f'CentricBrands User Import Update_User Child',
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
            no_task='declare_locationandemployeetypebasedchange_variable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_locationandemployeetypebasedchange_variable',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_locationandemployeetypebasedchange_variable = rail.SetVariableOperator(
            task_id='declare_locationandemployeetypebasedchange_variable',
            append=False,
            name='locationandemployeetypebasedchange',
            value='no'
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.uri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            }
        )

        if_user_currently_enabled_but_tobe_disabled = rail.IfOperator(
            task_id='if_user_currently_enabled_but_tobe_disabled',
            test=lambda dag_run: rail.result('get_user_details')[0]['userDetails']['isEnabled'] and dag_run.conf['isloginenabled'] and (
                dag_run.conf['isloginenabled']).lower() == 'no',
            yes_task="trigger_dag_to_disable_user",
            no_task="if_user_currently_disabled_and_tobe_disabled",
        )

        trigger_dag_to_disable_user = rail.TriggerDagRunOperator(
            task_id='trigger_dag_to_disable_user',
            retries=0,
            trigger_dag_id=config.child_disable_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run.conf.callerjobid}}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ dag_run.conf.uri }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "empid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "userimportlogslookuptable": "{{dag_run.conf.userimportlogslookuptable}}",
                "callerjobid": "{{dag_run_ecid()}}"
            }
        )

        wait_for_child_to_disable_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_disable_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_to_disable_user") }}'
        )

        if_user_currently_disabled_and_tobe_disabled = rail.IfOperator(
            task_id='if_user_currently_disabled_and_tobe_disabled',
            test=lambda dag_run: not (rail.result('get_user_details')[
                                      0]['userDetails']['isEnabled']) and dag_run.conf['isloginenabled'] and (dag_run.conf['isloginenabled']).lower() == 'no',
            yes_task="log_user_already_disabled",
            no_task="if_integrationdate_less_than_startdate",
        )

        log_user_already_disabled = rail.WriteLogOperator(
            task_id='log_user_already_disabled',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "empid": dag_run.conf['employeeid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": 'Skipped',
                "details": "User is disabled already",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": '||'
            }
        )

        if_integrationdate_less_than_startdate = rail.IfOperator(
            task_id='if_integrationdate_less_than_startdate',
            test=lambda dag_run: bool(dag_run.conf['integrationdate'] and dag_run.conf['startdate'] and (datetime.strptime(
                dag_run.conf['integrationdate'], '%m/%d/%Y') < datetime.strptime(dag_run.conf['startdate'], '%m/%d/%Y'))),
            yes_task="log_integrationdate_earlier_than_startdate",
            no_task="get_data_for_user",
        )

        log_integrationdate_earlier_than_startdate = rail.WriteLogOperator(
            task_id='log_integrationdate_earlier_than_startdate',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "empid": dag_run.conf['employeeid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": 'Skipped',
                "details": "Integration Date is earlier than Start Date provided. User not updated.",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": '||'
            }
        )

        get_data_for_user = rail.RepliconServiceOperator(
            task_id='get_data_for_user',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:supervisor",
                    "urn:replicon:user-list-column:location",
                    "urn:replicon:user-list-column:cost-center"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:user"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ dag_run.conf.uri }}",
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
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
            }
        )

        if_firstname_unequal_current = rail.IfOperator(
            task_id='if_firstname_unequal_current',
            test=lambda dag_run: dag_run.conf['firstname'] and dag_run.conf['firstname'] != rail.result(
                'get_user_details')[0]['userDetails']['firstName'],
            yes_task="update_first_name",
            no_task="if_lastname_unequal_current",
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        log_firstname_updated = rail.PythonOperator(
            task_id='log_firstname_updated',
            python_callable=lambda dag_run:  "First name updated to '" +
            dag_run.conf['firstname'] + "';"
        )

        if_lastname_unequal_current = rail.IfOperator(
            task_id='if_lastname_unequal_current',
            test=lambda dag_run: dag_run.conf['lastname'] and dag_run.conf['lastname'] != rail.result(
                'get_user_details')[0]['userDetails']['lastName'],
            yes_task="update_last_name",
            no_task="if_employeeid_unequal_current",
        )

        update_last_name = rail.RepliconServiceOperator(
            task_id='update_last_name',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        log_lastname_updated = rail.PythonOperator(
            task_id='log_lastname_updated',
            python_callable=lambda dag_run:  "Last name updated to '" +
            dag_run.conf['lastname'] + "';"
        )

        if_employeeid_unequal_current = rail.IfOperator(
            task_id='if_employeeid_unequal_current',
            test=lambda dag_run: dag_run.conf['employeeid'] and dag_run.conf['employeeid'] != rail.result(
                'get_user_details')[0]['userDetails']['employeeId'],
            yes_task="update_employeeid",
            no_task="if_email_unequal_current",
        )

        update_employeeid = rail.RepliconServiceOperator(
            task_id='update_employeeid',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "employeeId": "{{ dag_run.conf.employeeid }}"
            }
        )

        log_employeeid_updated = rail.PythonOperator(
            task_id='log_employeeid_updated',
            python_callable=lambda dag_run:  "EmployeeID updated to '" +
            dag_run.conf['employeeid'] + "';"
        )

        if_email_unequal_current = rail.IfOperator(
            task_id='if_email_unequal_current',
            test=lambda dag_run: dag_run.conf['email'] and dag_run.conf['email'] != rail.result(
                'get_user_details')[0]['userDetails']['emailAddress'],
            yes_task="if_valid_email",
            no_task="if_employeetype_unequal_current",
        )

        if_valid_email = rail.IfOperator(
            task_id='if_valid_email',
            test=lambda dag_run: '@' in dag_run.conf['email'],
            yes_task="update_email",
            no_task="log_incorrect_email_format",
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "email": "{{ dag_run.conf.email }}"
            }
        )

        log_email_updated = rail.PythonOperator(
            task_id='log_email_updated',
            python_callable=lambda dag_run:  "Email address updated to '" +
            dag_run.conf['email'] + "';"
        )

        log_incorrect_email_format = rail.PythonOperator(
            task_id='log_incorrect_email_format',
            python_callable=lambda dag_run:  "Email address not updated - Incorrect email format '" +
            dag_run.conf['email'] + "';"
        )

        if_employeetype_unequal_current = rail.IfOperator(
            task_id='if_employeetype_unequal_current',
            test=lambda dag_run: dag_run.conf['employeetype'] and dag_run.conf['employeetype'] != (rail.result(
                'get_user_details')[0]['employeeType']['name'] if rail.result(
                'get_user_details')[0]['employeeType'] else ''),
            yes_task="get_all_employee_type_details",
            no_task="get_current_authentication_type",
        )

        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['employeetype'], 'uri')
        )

        if_employeetype_uri_present = rail.IfOperator(
            task_id='if_employeetype_uri_present',
            test='''{{ result('get_all_employee_type_details') | is_truthy }}''',
            yes_task="update_employee_type_for_user",
            no_task="log_employeetype_notfound",
        )

        update_employee_type_for_user = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "employeeTypeUri": "{{ result('get_all_employee_type_details') }}"
            }
        )

        if_location_is_canada = rail.IfOperator(
            task_id='if_location_is_canada',
            test=lambda dag_run: dag_run.conf['location'] == 'Canada',
            yes_task="update_locationandemployeetypebasedchange_variable",
            no_task="log_employeetype_changed",
        )

        update_locationandemployeetypebasedchange_variable = rail.SetVariableOperator(
            task_id='update_locationandemployeetypebasedchange_variable',
            append=False,
            name='{{ result("declare_locationandemployeetypebasedchange_variable").name }}',
            value='yes'
        )

        log_employeetype_changed = rail.PythonOperator(
            task_id='log_employeetype_changed',
            python_callable=lambda dag_run:  "Employee Type changed to '" +
            dag_run.conf['employeetype'] + "';"
        )

        log_employeetype_notfound = rail.PythonOperator(
            task_id='log_employeetype_notfound',
            python_callable=lambda dag_run:  "User not updated - Employee type '" +
            dag_run.conf.employeetype + "' not found;"
        )

        get_current_authentication_type = rail.PythonOperator(
            task_id='get_current_authentication_type',
            python_callable=lambda:  (rail.result('get_user_details')[
                                      0]['securityConfiguration']['enabledAuthenticationTypeUris'][0].split(":"))[-1]
        )

        if_authenticationtype_not_equal_current = rail.IfOperator(
            task_id='if_authenticationtype_not_equal_current',
            test=lambda dag_run: dag_run.conf['authenticationtype'] and (
                dag_run.conf['authenticationtype']).lower() != rail.result('get_current_authentication_type'),
            yes_task="if_authenticationtype_is_sso",
            no_task="if_departmentname_present",
        )

        if_authenticationtype_is_sso = rail.IfOperator(
            task_id='if_authenticationtype_is_sso',
            test=lambda dag_run: 'sso' in (
                dag_run.conf['authenticationtype']).lower(),
            yes_task="set_sso_authentication_for_user",
            no_task="if_authenticationtype_is_replicon",
        )

        set_sso_authentication_for_user = rail.RepliconServiceOperator(
            task_id='set_sso_authentication_for_user',
            endpoint="/services/securityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "loginName": "{{ dag_run.conf.loginname }}"
            }
        )

        log_authentication_type_updated = rail.PythonOperator(
            task_id='log_authentication_type_updated',
            python_callable=lambda dag_run:  "Authentication type updated to " +
            dag_run.conf['authenticationtype'] + ";"
        )

        if_authenticationtype_is_replicon = rail.IfOperator(
            task_id='if_authenticationtype_is_replicon',
            test=lambda dag_run: 'replicon' in (
                dag_run.conf['authenticationtype']).lower(),
            yes_task="set_replicon_authentication_for_user",
            no_task="if_departmentname_present",
        )

        set_replicon_authentication_for_user = rail.RepliconServiceOperator(
            task_id='set_replicon_authentication_for_user',
            endpoint="/services/securityService1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "loginName": "{{ dag_run.conf.loginname }}",
                "password": "Centricbrands2019",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        log_authenticationtype_updated = rail.PythonOperator(
            task_id='log_authenticationtype_updated',
            python_callable=lambda dag_run:  "Authentication type updated to " +
            dag_run.conf['authenticationtype'] + ";"
        )
        if_departmentname_present = rail.IfOperator(
            task_id='if_departmentname_present',
            test=lambda dag_run: bool(dag_run.conf['departmentname']),
            yes_task="if_inputdepartmenturi_present",
            no_task="get_all_custom_fields",
        )

        if_inputdepartmenturi_present = rail.IfOperator(
            task_id='if_inputdepartmenturi_present',
            test=lambda dag_run: bool(dag_run.conf['inputdepartmenturi']),
            yes_task="if_inputdepartmenturi_unequal_current",
            no_task="log_department_notfound",
        )

        if_inputdepartmenturi_unequal_current = rail.IfOperator(
            task_id='if_inputdepartmenturi_unequal_current',
            test=lambda dag_run: dag_run.conf['inputdepartmenturi'] != (rail.result(
                'get_user_details')[0]['userDetails']['department']['uri'] if rail.result(
                'get_user_details')[0]['userDetails']['department'] else ''),
            yes_task="update_department_for_user",
            no_task="get_all_custom_fields",
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "departmentUri": "{{ dag_run.conf.inputdepartmenturi }}"
            }
        )

        log_department_updated = rail.PythonOperator(
            task_id='log_department_updated',
            python_callable=lambda dag_run:  "User Department updated to '" +
            dag_run.conf['departmentname'] + "';"
        )

        log_department_notfound = rail.PythonOperator(
            task_id='log_department_notfound',
            python_callable=lambda dag_run:  "User department not updated as Department - " +
            dag_run.conf['departmentname'] + " not found in instance ;"
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'integrationdatefield': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Integration Date', 'uri', ''),
                'stateprovincefield': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'State/Province', 'uri', ''),
                'chinacareerstartdateuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'China Career Start Date', 'uri', null),
                'honkonglevelsuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'HongKong Levels', 'uri', null)
            }
        )

        get_current_chinacareerstartdate = rail.PythonOperator(
            task_id='get_current_chinacareerstartdate',
            python_callable=lambda: datetime.strptime(rail.find_first_by_attr_and_get_attr(rail.result('get_user_details')[0]['userDetails'][
                    'customFieldValues'], 'customField.displayText', 'China Career Start Date', 'text', ''), "%B %d, %Y").strftime(
                "%m/%d/%Y") if rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_user_details')[0]['userDetails']['customFieldValues'], 'customField.displayText', 'China Career Start Date', 'text', '') else ''
        )

        if_chinacareerstartdate_present = rail.IfOperator(
            task_id='if_chinacareerstartdate_present',
            test='''{{ dag_run.conf.chinacareerstartdate | is_truthy }}''',
            yes_task="if_chinacareerstartdate_equal_current",
            no_task="get_current_hongkonglevels",
        )

        if_chinacareerstartdate_equal_current = rail.IfOperator(
            task_id='if_chinacareerstartdate_equal_current',
            test=lambda dag_run: dag_run.conf['chinacareerstartdate'] == rail.result(
                'get_current_chinacareerstartdate'),
            yes_task='get_current_hongkonglevels',
            no_task='update_chinacareerstartdate_for_user'
        )

        update_chinacareerstartdate_for_user = rail.RepliconServiceOperator(
            task_id='update_chinacareerstartdate_for_user',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['uri'],
                "customFieldUri": rail.result('get_all_custom_fields')['chinacareerstartdateuri'],
                "value": {
                    "year": int(dag_run.conf['chinacareerstartdate'].split('/')[2]),
                    "month": int(dag_run.conf['chinacareerstartdate'].split('/')[0]),
                    "day": int(dag_run.conf['chinacareerstartdate'].split('/')[1])
                }
            }
        )

        get_current_hongkonglevels = rail.PythonOperator(
            task_id='get_current_hongkonglevels',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_details')[0]['userDetails']['customFieldValues'], 'customField.displayText', 'HongKong Levels', 'text', '')
        )

        if_hongkonglevels_present = rail.IfOperator(
            task_id='if_hongkonglevels_present',
            test='''{{ dag_run.conf.hongkonglevels | is_truthy }}''',
            yes_task='if_hongkonglevels_equal_current',
            no_task='if_currently_disabled_and_tobe_enabled',
        )

        if_hongkonglevels_equal_current = rail.IfOperator(
            task_id='if_hongkonglevels_equal_current',
            test="{{dag_run.conf.hongkonglevels == result('get_current_hongkonglevels')}}",
            yes_task='if_currently_disabled_and_tobe_enabled',
            no_task='update_hongkonglevels_for_user'
        )

        update_hongkonglevels_for_user = rail.RepliconServiceOperator(
            task_id='update_hongkonglevels_for_user',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.uri }}",
                "customFieldUri": "{{result('get_all_custom_fields').honkonglevelsuri}}",
                "value": "{{ dag_run.conf.hongkonglevels}}"
            }
        )

        if_currently_disabled_and_tobe_enabled = rail.IfOperator(
            task_id='if_currently_disabled_and_tobe_enabled',
            test=lambda dag_run: dag_run.conf['isloginenabled'] == 'Yes' and not (
                rail.result('get_user_details')[0]['securityConfiguration']['isLoginEnabled']),
            yes_task="get_integration_dateobject",
            no_task="get_assigned_userpermissionset",
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year,
                'datestring': datestring
            }

        get_integration_dateobject = rail.PythonOperator(
            task_id='get_integration_dateobject',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['integrationdate'])
        )

        get_startdate_object = rail.PythonOperator(
            task_id='get_startdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['startdate'])
        )

        update_employment_daterange_remove_enddate = rail.RepliconServiceOperator(
            task_id='update_employment_daterange_remove_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('get_user_details')[0].userDetails.employmentDateRange.startDate.year }}",
                        "month": "{{ result('get_user_details')[0].userDetails.employmentDateRange.startDate.month }}",
                        "day": "{{ result('get_user_details')[0].userDetails.employmentDateRange.startDate.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        update_integration_date = rail.RepliconServiceOperator(
            task_id='update_integration_date',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.uri }}",
                "customFieldUri": "{{ result('get_all_custom_fields').integrationdatefield }}",
                "value": {
                    "year": "{{ result('get_integration_dateobject').year }}",
                    "month": "{{ result('get_integration_dateobject').month }}",
                    "day": "{{ result('get_integration_dateobject').day }}"
                }
            }
        )

        log_integration_date_updated = rail.PythonOperator(
            task_id='log_integration_date_updated',
            python_callable=lambda dag_run:  "User Integration Date updated to '" +
            dag_run.conf['integrationdate'] + "';"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
            }
        )

        log_user_rehired_and_enabled = rail.PythonOperator(
            task_id='log_user_rehired_and_enabled',
            python_callable=lambda:  "User rehired and" + "enabled ;"
        )

        update_location_and_employeetypebasedchange_variable = rail.SetVariableOperator(
            task_id='update_location_and_employeetypebasedchange_variable',
            append=False,
            name='{{ result("declare_locationandemployeetypebasedchange_variable").name }}',
            value='rehire'
        )

        get_assigned_userpermissionset = rail.PythonOperator(
            task_id='get_assigned_userpermissionset',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_details')[0]['permissionSets'], 'name', dag_run.conf['userpermission'], 'name', '')
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response, dag_run: {
                'userpermissionuri': rail.find_first_by_attr_and_get_attr(response, 'name', dag_run.conf['userpermission'], 'uri', ''),
                'supervisorpermissionuri': rail.find_first_by_attr_and_get_attr(response, 'name', dag_run.conf['supervisorpermission'], 'uri', '')
            }
        )

        if_userpermission_unequal_current = rail.IfOperator(
            task_id='if_userpermission_unequal_current',
            test=lambda dag_run: dag_run.conf['userpermission'] and dag_run.conf['userpermission'] != rail.result(
                'get_assigned_userpermissionset'),
            yes_task="if_userpermission_uri_present",
            no_task="get_assigned_supervisorpermissionset",
        )

        if_userpermission_uri_present = rail.IfOperator(
            task_id='if_userpermission_uri_present',
            test=lambda: bool(rail.result('get_all_permission_sets')[
                              'userpermissionuri']),
            yes_task="assign_permission_set_to_user",
            no_task="log_userpermissionset_notfound",
        )

        assign_permission_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "permissionSetUri": "{{ result('get_all_permission_sets').userpermissionuri }}"
            }
        )

        log_userpermission_updated = rail.PythonOperator(
            task_id='log_userpermission_updated',
            python_callable=lambda dag_run:  "User permission updated to '" +
            dag_run.conf['userpermission'] + "';"
        )

        log_userpermissionset_notfound = rail.PythonOperator(
            task_id='log_userpermissionset_notfound',
            python_callable=lambda dag_run:  "User not updated - User permission set '" +
            dag_run.conf['userpermission'] + "' not found;"
        )

        get_assigned_supervisorpermissionset = rail.PythonOperator(
            task_id='get_assigned_supervisorpermissionset',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_details')[0]['permissionSets'], 'name', dag_run.conf['supervisorpermission'], 'name', '')
        )

        if_supervisorpermission_unequal_current = rail.IfOperator(
            task_id='if_supervisorpermission_unequal_current',
            test=lambda dag_run: dag_run.conf['supervisorpermission'] and dag_run.conf['supervisorpermission'] != rail.result(
                'get_assigned_supervisorpermissionset'),
            yes_task="if_supervisorpermission_uri_present",
            no_task="get_assigned_products",
        )

        if_supervisorpermission_uri_present = rail.IfOperator(
            task_id='if_supervisorpermission_uri_present',
            test=lambda: bool(rail.result('get_all_permission_sets')[
                              'supervisorpermissionuri']),
            yes_task="assign_permission_set_touser",
            no_task="log_supervisorpermission_notfound",
        )

        assign_permission_set_touser = rail.RepliconServiceOperator(
            task_id='assign_permission_set_touser',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "permissionSetUri": "{{ result('get_all_permission_sets').supervisorpermissionuri }}"
            }
        )

        log_supervisorpermission_updated = rail.PythonOperator(
            task_id='log_supervisorpermission_updated',
            python_callable=lambda dag_run:  "Supervisor permission updated to '" +
            dag_run.conf['supervisorpermission'] + "';"
        )

        log_supervisorpermission_notfound = rail.PythonOperator(
            task_id='log_supervisorpermission_notfound',
            python_callable=lambda dag_run:  "Supervisor permission set '" +
            dag_run.conf['supervisorpermission'] + "' not found;"
        )

        get_assigned_products = rail.PythonOperator(
            task_id='get_assigned_products',
            python_callable=lambda: "".join([(product['displayText']).lower(
            ) for product in rail.result('get_user_details')[0]['assignedProducts']])
        )

        if_licenseseats_notin_assigned_products = rail.IfOperator(
            task_id='if_licenseseats_notin_assigned_products',
            test=lambda dag_run: dag_run.conf['licenseseats'] and (
                dag_run.conf['licenseseats']).lower() not in rail.result('get_assigned_products'),
            yes_task="get_all_products_available_for_user_assignment",
            no_task="checkifany_cost_center_locationisassigned",
        )

        get_all_products_available_for_user_assignment = rail.RepliconServiceOperator(
            task_id='get_all_products_available_for_user_assignment',
            endpoint="/services/AccountManagementService1.svc/GetAllProductsAvailableForUserAssignment",
            data_handler=lambda repsonse, dag_run: rail.find_first_by_attr_and_get_attr(
                repsonse, 'displayText', dag_run.conf['licenseseats'], 'uri', '')
        )

        if_product_uri_present = rail.IfOperator(
            task_id='if_product_uri_present',
            test='''{{ result('get_all_products_available_for_user_assignment') | is_truthy }}''',
            yes_task="put_product_assignments_for_user",
            no_task="log_licenseseat_notfound",
        )

        put_product_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "productUris": [
                    "{{ result('get_all_products_available_for_user_assignment') }}"
                ]
            }
        )

        log_licenseseat_updated = rail.PythonOperator(
            task_id='log_licenseseat_updated',
            python_callable=lambda dag_run:  "License seat updated to '" +
            dag_run.conf['licenseseats'] + "';"
        )

        log_licenseseat_notfound = rail.PythonOperator(
            task_id='log_licenseseat_notfound',
            python_callable=lambda dag_run:  "License seats '" +
            dag_run.conf['licenseseats'] + "' not found in instance;"
        )

        checkifany_cost_center_locationisassigned = rail.PythonOperator(
            task_id='checkifany_cost_center_locationisassigned',
            python_callable=lambda:  rail.result('get_data_for_user')[
                'rows'][0]['cells'][2]['dataType']
        )

        get_all_cost_centers_cost_centerislabelled_location = rail.RepliconServiceOperator(
            task_id='get_all_cost_centers_cost_centerislabelled_location',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        get_locationeffective_dateobject = rail.PythonOperator(
            task_id='get_locationeffective_dateobject',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['locationeffectivedate'] if dag_run.conf['locationeffectivedate'] else datetime.now().strftime('%m/%d/%Y'))
        )

        if_no_costcenter_assigned = rail.IfOperator(
            task_id='if_no_costcenter_assigned',
            test='''{{ result('checkifany_cost_center_locationisassigned') == 'urn:replicon:list-type:null' }}''',
            yes_task="if_location_present",
            no_task="if_currently_costcenter_assigned",
        )

        if_location_present = rail.IfOperator(
            task_id='if_location_present',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task="if_inputlocationuri_present",
            no_task="if_currently_costcenter_assigned",
        )

        if_inputlocationuri_present = rail.IfOperator(
            task_id='if_inputlocationuri_present',
            test='''{{ dag_run.conf.inputlocationuri | is_truthy }}''',
            yes_task="put_cost_center_schedule_for_user",
            no_task="log_location_not_found",
        )

        put_cost_center_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "scheduleEntries": [
                    {
                        "costCenter": {
                            "uri": "{{ dag_run.conf.inputlocationuri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": "{{ result('get_locationeffective_dateobject').year }}",
                            "month": "{{ result('get_locationeffective_dateobject').month }}",
                            "day": "{{ result('get_locationeffective_dateobject').day }}"
                        }
                    }
                ]
            }
        )

        add_log_location_updated = rail.WriteLogOperator(
            task_id='add_log_location_updated',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "empid": dag_run.conf['employeeid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": 'Success',
                "details": "Location updated '" + dag_run.conf['location'] + "'",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": '||'
            }
        )

        log_location_changed = rail.PythonOperator(
            task_id='log_location_changed',
            python_callable=lambda dag_run:  "User Location changed to '" +
            dag_run.conf['location'] + "';"
        )

        update_location_and_employeetype_basedchange_variable = rail.SetVariableOperator(
            task_id='update_location_and_employeetype_basedchange_variable',
            append=False,
            name='{{ result("declare_locationandemployeetypebasedchange_variable").name }}',
            value='Location change'
        )

        log_location_not_found = rail.PythonOperator(
            task_id='log_location_not_found',
            python_callable=lambda dag_run:  "Location not found '" +
            dag_run.conf['location'] + "';"
        )

        if_currently_costcenter_assigned = rail.IfOperator(
            task_id='if_currently_costcenter_assigned',
            test='''{{ result('checkifany_cost_center_locationisassigned') != 'urn:replicon:list-type:null' }}''',
            yes_task="get_uri_and_parent_location_name",
            no_task="check_if_any_team_assigned",
        )

        get_uri_and_parent_location_name = rail.PythonOperator(
            task_id='get_uri_and_parent_location_name',
            python_callable=lambda: {
                'parent': rail.result('get_data_for_user')['rows'][0]['cells'][2]['cellCollection'][0]['textValue'],
                'uri': rail.result('get_data_for_user')['rows'][0]['cells'][2]['cellCollection'][-1]['uri']
            }
        )

        if_location_unequal_current = rail.IfOperator(
            task_id='if_location_unequal_current',
            test=lambda dag_run: dag_run.conf['inputlocationuri'] and dag_run.conf['inputlocationuri'] != rail.result(
                'get_uri_and_parent_location_name')['uri'],
            yes_task="if_input_location_uri_present",
            no_task="check_if_any_team_assigned",
        )

        if_input_location_uri_present = rail.IfOperator(
            task_id='if_input_location_uri_present',
            test='''{{ dag_run.conf.inputlocationuri | is_truthy }}''',
            yes_task="create_locationschedule_list",
            no_task="log_location_notfound",
        )

        def get_date_string(dateobj, dateformat=True):
            print(dateobj)
            return str(dateobj['month']) + "/" + str(dateobj['day']) + "/" + str(dateobj['year']) if dateformat else (
                str(dateobj['year']) + "-" + str(dateobj['month']) + "-" + str(dateobj['day']))

        def get_locationschedule_entries(dag_run):
            current_schedule = rail.result('get_user_details')[
                0]['costCenterSchedule']
            location_schedule = [{
                "costCenter": {
                    "uri": schedule['costCenter']['uri'],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null if not (schedule['effectiveDate'] and schedule['effectiveDate']['day']) else {
                    "year": schedule['effectiveDate']['year'],
                    "month": schedule['effectiveDate']['month'],
                    "day": schedule['effectiveDate']['day']
                }
            } for schedule in current_schedule if (not (schedule['effectiveDate'] and schedule['effectiveDate']['day']) or (
                schedule['effectiveDate'] and datetime.strptime(get_date_string(schedule['effectiveDate']), "%m/%d/%Y") != datetime.strptime(rail.result(
                    'get_locationeffective_dateobject')['datestring'], "%m/%d/%Y")))]
            location_schedule.append({
                "costCenter": {
                    "uri": dag_run.conf['inputlocationuri'],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": {
                    "year": rail.result('get_locationeffective_dateobject')['year'],
                    "month": rail.result('get_locationeffective_dateobject')['month'],
                    "day": rail.result('get_locationeffective_dateobject')['day']
                }
            })
            return location_schedule

        create_locationschedule_list = rail.PythonOperator(
            task_id='create_locationschedule_list',
            python_callable=get_locationschedule_entries
        )

        if_location_schedule_present = rail.IfOperator(
            task_id='if_location_schedule_present',
            test='''{{ result('create_locationschedule_list') | is_truthy }}''',
            yes_task="put_costcenterschedule_for_user",
            no_task="check_if_any_team_assigned",
        )

        put_costcenterschedule_for_user = rail.RepliconServiceOperator(
            task_id='put_costcenterschedule_for_user',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['uri'],
                "scheduleEntries": rail.result('create_locationschedule_list')
            }
        )

        log_location_is_changed = rail.PythonOperator(
            task_id='log_location_is_changed',
            python_callable=lambda dag_run:  "User Location changed to '" +
            dag_run.conf['location'] + "';"
        )

        if_parent_location_unequal_location = rail.IfOperator(
            task_id='if_parent_location_unequal_location',
            test='''{{ result('get_uri_and_parent_location_name').parent != dag_run.conf.location }}''',
            yes_task="update_location_and_employeetype_based_change_variable",
            no_task="check_if_any_team_assigned",
        )

        update_location_and_employeetype_based_change_variable = rail.SetVariableOperator(
            task_id='update_location_and_employeetype_based_change_variable',
            append=False,
            name='{{ result("declare_locationandemployeetypebasedchange_variable").name }}',
            value='Location change'
        )

        log_location_notfound = rail.PythonOperator(
            task_id='log_location_notfound',
            python_callable=lambda dag_run:  "Location not found '" +
            dag_run.conf['location'] + "';"
        )

        check_if_any_team_assigned = rail.PythonOperator(
            task_id='check_if_any_team_assigned',
            python_callable=lambda: rail.result('get_data_for_user')[
                'rows'][0]['cells'][1]['dataType']
        )

        get_all_locations_locationislabelled_team = rail.RepliconServiceOperator(
            task_id='get_all_locations_locationislabelled_team',
            endpoint="/services/LocationService1.svc/GetAllLocations",
        )

        get_team_effectivedate_object = rail.PythonOperator(
            task_id='get_team_effectivedate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['teameffectivedate'] if dag_run.conf['teameffectivedate'] else datetime.now().strftime('%m/%d/%Y'))
        )

        if_no_team_assigned_currently = rail.IfOperator(
            task_id='if_no_team_assigned_currently',
            test='''{{ result('check_if_any_team_assigned') == 'urn:replicon:list-type:null' }}''',
            yes_task="if_team_present",
            no_task="if_currently_team_is_assigned",
        )

        if_team_present = rail.IfOperator(
            task_id='if_team_present',
            test='''{{ dag_run.conf.team | is_truthy }}''',
            yes_task="if_inputteamuri_present",
            no_task="if_currently_team_is_assigned",
        )

        if_inputteamuri_present = rail.IfOperator(
            task_id='if_inputteamuri_present',
            test='''{{ dag_run.conf.inputteamuri | is_truthy }}''',
            yes_task="put_location_schedule_for_user",
            no_task="log_team_not_found",
        )

        put_location_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ dag_run.conf.inputteamuri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": "{{ result('get_locationeffective_dateobject').year }}",
                            "month": "{{ result('get_locationeffective_dateobject').month }}",
                            "day": "{{ result('get_locationeffective_dateobject').day }}"
                        }
                    }
                ]
            }
        )

        log_team_changed = rail.PythonOperator(
            task_id='log_team_changed',
            python_callable=lambda dag_run:  "User Team changed to '" +
            dag_run.conf['team'] + "';"
        )

        log_team_not_found = rail.PythonOperator(
            task_id='log_team_not_found',
            python_callable=lambda dag_run:  "Team not found '" +
            dag_run.conf['team'] + "';"
        )

        if_currently_team_is_assigned = rail.IfOperator(
            task_id='if_currently_team_is_assigned',
            test='''{{ result('check_if_any_team_assigned') != 'urn:replicon:list-type:null' }}''',
            yes_task="get_current_team_uri",
            no_task="get_current_state_provincevalue",
        )

        get_current_team_uri = rail.PythonOperator(
            task_id='get_current_team_uri',
            python_callable=lambda: rail.result('get_data_for_user')[
                'rows'][0]['cells'][1]['cellCollection'][-1]['uri']
        )

        if_team_unequal_current = rail.IfOperator(
            task_id='if_team_unequal_current',
            test='''{{ dag_run.conf.inputteamuri | is_truthy  and dag_run.conf.inputteamuri != result('get_current_team_uri') }}''',
            yes_task="if_input_teamuri_present",
            no_task="get_current_state_provincevalue",
        )

        if_input_teamuri_present = rail.IfOperator(
            task_id='if_input_teamuri_present',
            test='''{{ dag_run.conf.inputteamuri | is_truthy }}''',
            yes_task="create_teamschedule_list",
            no_task="log_team_notfound",
        )

        def get_teamschedule_entries(dag_run):
            current_schedule = rail.result('get_user_details')[
                0]['locationSchedule']
            location_schedule = [{
                "location": {
                    "uri": schedule['location']['uri'],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null if not (schedule['effectiveDate'] and schedule['effectiveDate']['day']) else {
                    "year": schedule['effectiveDate']['year'],
                    "month": schedule['effectiveDate']['month'],
                    "day": schedule['effectiveDate']['day']
                }
            } for schedule in current_schedule if (not (schedule['effectiveDate'] and schedule['effectiveDate']['day']) or (
                schedule['effectiveDate'] and datetime.strptime(get_date_string(schedule['effectiveDate']), "%m/%d/%Y") != datetime.strptime(rail.result(
                    'get_team_effectivedate_object')['datestring'], "%m/%d/%Y")))]
            location_schedule.append({
                "location": {
                    "uri": dag_run.conf['inputteamuri'],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": {
                    "year": rail.result('get_team_effectivedate_object')['year'],
                    "month": rail.result('get_team_effectivedate_object')['month'],
                    "day": rail.result('get_team_effectivedate_object')['day']
                }
            })
            return location_schedule

        create_teamschedule_list = rail.PythonOperator(
            task_id='create_teamschedule_list',
            python_callable=get_teamschedule_entries
        )

        if_team_schedule_present = rail.IfOperator(
            task_id='if_team_schedule_present',
            test='''{{ result('create_teamschedule_list') | is_truthy }}''',
            yes_task="put_locationschedule_for_user",
            no_task="get_current_state_provincevalue",
        )

        put_locationschedule_for_user = rail.RepliconServiceOperator(
            task_id='put_locationschedule_for_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['uri'],
                "scheduleEntries": rail.result('create_teamschedule_list')
            }
        )

        log_teamchanged = rail.PythonOperator(
            task_id='log_teamchanged',
            python_callable=lambda dag_run:  "User Team changed to '" +
            dag_run.conf['team'] + "';"
        )

        log_team_notfound = rail.PythonOperator(
            task_id='log_team_notfound',
            python_callable=lambda dag_run:  "Team not found '" +
            dag_run.conf['team'] + "';"
        )

        get_current_state_provincevalue = rail.PythonOperator(
            task_id='get_current_state_provincevalue',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_details')[0]['userDetails']['customFieldValues'], 'customField.displayText', 'State/Province', 'text', '')
        )

        if_stateprovince_unequal_current = rail.IfOperator(
            task_id='if_stateprovince_unequal_current',
            test=lambda dag_run: dag_run.conf['stateprovince'] and dag_run.conf['stateprovince'] != rail.result(
                'get_current_state_provincevalue'),
            yes_task="get_required_dropdown_uri",
            no_task="if_supervisorloginname_present",
        )

        get_required_dropdown_uri = rail.RepliconServiceOperator(
            task_id='get_required_dropdown_uri',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields').stateprovincefield }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['stateprovince'], 'uri', '')
        )

        if_required_stateprovince_present = rail.IfOperator(
            task_id='if_required_stateprovince_present',
            test='''{{ result('get_required_dropdown_uri') | is_truthy }}''',
            yes_task="update_dropdown_value",
            no_task="log_stateprovince_notfound",
        )

        update_dropdown_value = rail.RepliconServiceOperator(
            task_id='update_dropdown_value',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.uri }}",
                "customFieldUri": "{{ result('get_all_custom_fields').stateprovincefield }}",
                "customFieldDropDownOptionUri": "{{ result('get_required_dropdown_uri') }}"
            }
        )

        if_location_equals_usa = rail.IfOperator(
            task_id='if_location_equals_usa',
            test='''{{ dag_run.conf.location == 'USA' }}''',
            yes_task="updatelocation_and_employeetype_based_change_variable",
            no_task="log_state_province_changed",
        )

        updatelocation_and_employeetype_based_change_variable = rail.SetVariableOperator(
            task_id='updatelocation_and_employeetype_based_change_variable',
            append=False,
            name='{{ result("declare_locationandemployeetypebasedchange_variable").name }}',
            value='State Change'
        )

        log_state_province_changed = rail.PythonOperator(
            task_id='log_state_province_changed',
            python_callable=lambda dag_run:  "User State/Province changed to '" +
            dag_run.conf['stateprovince'] + "';"
        )

        log_stateprovince_notfound = rail.PythonOperator(
            task_id='log_stateprovince_notfound',
            python_callable=lambda dag_run:  "State/Province not found '" +
            dag_run.conf['stateprovince'] + "';"
        )

        if_supervisorloginname_present = rail.IfOperator(
            task_id='if_supervisorloginname_present',
            test='''{{ dag_run.conf.supervisorloginname | is_truthy }}''',
            yes_task="if_supervisorloginname_equal_userloginname",
            no_task="if_timeofftemplate_unequal_current",
        )

        if_supervisorloginname_equal_userloginname = rail.IfOperator(
            task_id='if_supervisorloginname_equal_userloginname',
            test='''{{ dag_run.conf.supervisorloginname == dag_run.conf.loginname }}''',
            yes_task="log_supervisor_not_updated",
            no_task="get_userdetails",
        )

        log_supervisor_not_updated = rail.PythonOperator(
            task_id='log_supervisor_not_updated',
            python_callable=lambda:  "Supervisor login name and User login name is same - Supervisor" + "not updated;"
        )

        get_userdetails = rail.RepliconServiceOperator(
            task_id='get_userdetails',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            }
        )

        if_supervisorloginname_unequal_current = rail.IfOperator(
            task_id='if_supervisorloginname_unequal_current',
            test=lambda dag_run: dag_run.conf['supervisorloginname'] != (rail.result('get_userdetails')['supervisor']['user']['loginName'] if (rail.result(
                'get_userdetails')['supervisor'] and rail.result('get_userdetails')['supervisor']['user']) else ''),
            yes_task="search_supervisor_user",
            no_task="if_timeofftemplate_unequal_current",
        )

        def get_supervisor_uri_and_status(response, dag_run):
            users_found = response['rows']
            matching_user = list(filter(
                lambda user: user['cells'][0]['textValue'] == dag_run.conf['supervisorloginname'], users_found))
            return {
                'uri': matching_user[0]['cells'][0]['uri'] if matching_user else '',
                'status': matching_user[0]['cells'][1]['textValue'] if matching_user else ''
            }

        search_supervisor_user = rail.RepliconServiceOperator(
            task_id='search_supervisor_user',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{dag_run.conf.supervisorloginname}}"
                        }
                    }
                }
            },
            data_handler=get_supervisor_uri_and_status
        )

        if_supervisoruri_present_in_replicon = rail.IfOperator(
            task_id='if_supervisoruri_present_in_replicon',
            test='''{{ result('search_supervisor_user').uri | is_truthy }}''',
            yes_task="if_supervisorstatus_is_true",
            no_task="add_to_supervisorassignment_lookup",
        )

        if_supervisorstatus_is_true = rail.IfOperator(
            task_id='if_supervisorstatus_is_true',
            test=lambda: 'True' in rail.result(
                'search_supervisor_user')['status'],
            yes_task="get_assigned_permission_sets_for_user",
            no_task="add_to_supervisorassignment_lookup",
        )

        get_assigned_permission_sets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_supervisor_user').uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.displayText', '')
        )

        if_supervisorpermission_present = rail.IfOperator(
            task_id='if_supervisorpermission_present',
            test=lambda: bool(rail.result(
                'get_assigned_permission_sets_for_user')),
            yes_task="get_supervisoreffectivedate_object",
            no_task="add_to_supervisorassignment_lookup",
        )

        get_supervisoreffectivedate_object = rail.PythonOperator(
            task_id='get_supervisoreffectivedate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['supervisorstartdate'] if dag_run.conf['supervisorstartdate'] else datetime.now().strftime('%m/%d/%Y'))
        )

        update_supervisor_assignment_schedule_over_date_range = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "supervisorUri": "{{ result('search_supervisor_user').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('get_supervisoreffectivedate_object').year }}",
                        "month": "{{ result('get_supervisoreffectivedate_object').month }}",
                        "day": "{{ result('get_supervisoreffectivedate_object').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_supervisor_updated = rail.PythonOperator(
            task_id='log_supervisor_updated',
            python_callable=lambda dag_run:  "Supervisor updated to '" +
            dag_run.conf['supervisorloginname'] + "';"
        )

        add_to_supervisorassignment_lookup = rail.WriteLogOperator(
            task_id='add_to_supervisorassignment_lookup',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ dag_run.conf.uri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "supervisorstartdate": "{{ dag_run.conf.supervisorstartdate }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "assignedstatus": "Not assigned",
                "action": "update user"
            }
        )

        if_timeofftemplate_unequal_current = rail.IfOperator(
            task_id='if_timeofftemplate_unequal_current',
            test=lambda dag_run: dag_run.conf['timeofftemplate'] and dag_run.conf['timeofftemplate'] != (rail.result(
                'get_user_details')[0]['timeOffTemplate']['name'] if rail.result(
                'get_user_details')[0]['timeOffTemplate'] else ''),
            yes_task="get_required_timeoff_template_uri",
            no_task="if_timeoffaprrovalpath_unequal_current",
        )

        get_required_timeoff_template_uri = rail.RepliconServiceOperator(
            task_id='get_required_timeoff_template_uri',
            endpoint="/services/policysetService1.svc/GetAllPolicySets",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'name', dag_run.conf['timeofftemplate'], 'uri', '')
        )

        if_timeofftemplate_uri_present = rail.IfOperator(
            task_id='if_timeofftemplate_uri_present',
            test='''{{ result('get_required_timeoff_template_uri') | is_truthy }}''',
            yes_task="assign_policy_set_to_user",
            no_task="log_timeofftemplate_notfound",
        )

        assign_policy_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user',
            endpoint="/services/policysetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "policySetUri": "{{ result('get_required_timeoff_template_uri') }}"
            }
        )

        log_timeofftemplate_updated = rail.PythonOperator(
            task_id='log_timeofftemplate_updated',
            python_callable=lambda dag_run:  "Time off template updated to '" +
            dag_run.conf['timeofftemplate'] + "';"
        )

        log_timeofftemplate_notfound = rail.PythonOperator(
            task_id='log_timeofftemplate_notfound',
            python_callable=lambda dag_run:  "Time off template not found - '" +
            dag_run.conf['timeofftemplate'] + "';"
        )

        if_timeoffaprrovalpath_unequal_current = rail.IfOperator(
            task_id='if_timeoffaprrovalpath_unequal_current',
            test=lambda dag_run: dag_run.conf['timeoffapprovalpath'] and dag_run.conf['timeoffapprovalpath'] != (rail.result(
                'get_user_details')[0]['timeOffApprovalPath']['displayText'] if rail.result(
                'get_user_details')[0]['timeOffApprovalPath'] else ''),
            yes_task="get_required_timeoffapproval_path_uri",
            no_task="if_holidaycalendar_unequal_current",
        )

        get_required_timeoffapproval_path_uri = rail.RepliconServiceOperator(
            task_id='get_required_timeoffapproval_path_uri',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['timeoffapprovalpath'], 'uri', '')
        )

        if_timeoffapprovalpath_uri_present = rail.IfOperator(
            task_id='if_timeoffapprovalpath_uri_present',
            test='''{{ result('get_required_timeoffapproval_path_uri') | is_truthy }}''',
            yes_task="update_approval_path_for_user",
            no_task="log_timeoffapproval_path_notfound",
        )

        update_approval_path_for_user = rail.RepliconServiceOperator(
            task_id='update_approval_path_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "approvalPathUri": "{{ result('get_required_timeoffapproval_path_uri') }}"
            }
        )

        log_timeoffapproval_path_updated = rail.PythonOperator(
            task_id='log_timeoffapproval_path_updated',
            python_callable=lambda dag_run:  "Time off approval path updated to '" +
            dag_run.conf['timeoffapprovalpath'] + "';"
        )

        log_timeoffapproval_path_notfound = rail.PythonOperator(
            task_id='log_timeoffapproval_path_notfound',
            python_callable=lambda dag_run:  "Time off approval path not found - '" +
            dag_run.conf['timeoffapprovalpath'] + "';"
        )

        if_holidaycalendar_unequal_current = rail.IfOperator(
            task_id='if_holidaycalendar_unequal_current',
            test=lambda dag_run: dag_run.conf['holidaycalendar'] and dag_run.conf['holidaycalendar'] != (rail.result(
                'get_user_details')[0]['holidayCalendar']['name'] if rail.result(
                'get_user_details')[0]['holidayCalendar'] else ''),
            yes_task="get_required_holidaycalendar_uri",
            no_task="get_scheduleeffectivedate_object",
        )

        get_required_holidaycalendar_uri = rail.RepliconServiceOperator(
            task_id='get_required_holidaycalendar_uri',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'name', dag_run.conf['holidaycalendar'], 'uri', '')
        )

        if_required_holiday_calendar_uri_present = rail.IfOperator(
            task_id='if_required_holiday_calendar_uri_present',
            test='''{{ result('get_required_holidaycalendar_uri') | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user",
            no_task="log_holidaycalendar_notfound",
        )

        update_holiday_calendar_for_user = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}",
                "holidayCalendarUri": "{{ result('get_required_holidaycalendar_uri') }}"
            }
        )

        log_holidaycalendar_updated = rail.PythonOperator(
            task_id='log_holidaycalendar_updated',
            python_callable=lambda dag_run:  "Holiday Calendar updated to '" +
            dag_run.conf['holidaycalendar'] + "';"
        )

        log_holidaycalendar_notfound = rail.PythonOperator(
            task_id='log_holidaycalendar_notfound',
            python_callable=lambda dag_run:  "Holiday Calendar not found '" +
            dag_run.conf['holidaycalendar'] + "';"
        )

        get_scheduleeffectivedate_object = rail.PythonOperator(
            task_id='get_scheduleeffectivedate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['scheduleeffectivedate'] if dag_run.conf['scheduleeffectivedate'] else datetime.now().strftime('%m/%d/%Y'))
        )

        get_schedule_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_schedule_policy_schedule_for_user',
            endpoint="/services/SchedulingService2.svc/GetSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            }
        )

        def get_schedule_entries_list():
            schedule_policy_schedule = rail.result(
                'get_schedule_policy_schedule_for_user')
            schedule_entries = []
            schedule_entries_for_comparison = []
            for schedule in schedule_policy_schedule:
                if 'office' in schedule['scheduleTypeUri']:
                    if not (schedule['effectiveDate'] and schedule['effectiveDate']['day']):
                        schedule_entries.append({
                            "schedulePolicy": {
                                "officeScheduleUri": schedule['officeSchedule']['uri'],
                                "scheduleTypeUri": schedule['scheduleTypeUri'],
                                "name": null,
                                "officeSchedule": null
                            },
                            "effectiveDate": null
                        })
                        schedule_entries_for_comparison.append({
                            "schedulePolicy": {
                                "officeScheduleUri": schedule['officeSchedule']['uri'],
                                "scheduleTypeUri": schedule['scheduleTypeUri'],
                                "name": null,
                                "officeSchedule": null
                            },
                            "effectiveDate": null,
                            "effectivedateforcomparison": get_date_string(rail.result(
                                'get_user_details')[0]['userDetails']['employmentDateRange']['startDate'], False)
                        })
                    elif datetime.strptime(get_date_string(schedule['effectiveDate']), '%m/%d/%Y') != datetime.strptime(rail.result(
                            'get_scheduleeffectivedate_object')['datestring'], '%m/%d/%Y'):
                        schedule_entries.append({
                            "schedulePolicy": {
                                "officeScheduleUri": schedule['officeSchedule']['uri'],
                                "scheduleTypeUri": schedule['scheduleTypeUri'],
                                "name": null,
                                "officeSchedule": null
                            },
                            "effectiveDate": {
                                "year": schedule['effectiveDate']['year'],
                                "month": schedule['effectiveDate']['month'],
                                "day": schedule['effectiveDate']['day']
                            }
                        })
                        if datetime.strptime(get_date_string(schedule['effectiveDate']), '%m/%d/%Y') < datetime.strptime(rail.result(
                                'get_scheduleeffectivedate_object')['datestring'], '%m/%d/%Y'):
                            schedule_entries_for_comparison.append({
                                "schedulePolicy": {
                                    "officeScheduleUri": schedule['officeSchedule']['uri'],
                                    "scheduleTypeUri": schedule['scheduleTypeUri'],
                                    "name": null,
                                    "officeSchedule": null
                                },
                                "effectiveDate": {
                                    "year": schedule['effectiveDate']['year'],
                                    "month": schedule['effectiveDate']['month'],
                                    "day": schedule['effectiveDate']['day']
                                },
                                "effectivedateforcomparison": get_date_string(schedule['effectiveDate'], False)
                            })
            return {
                'scheduleentries': schedule_entries,
                'entriesforcomparison': schedule_entries_for_comparison
            }

        create_schedule_entries_list = rail.PythonOperator(
            task_id='create_schedule_entries_list',
            python_callable=get_schedule_entries_list
        )

        def get_existing_schedule():
            entriesforcomparison = rail.result('create_schedule_entries_list')[
                'entriesforcomparison']
            max_date = max(entry['effectivedateforcomparison']
                           for entry in entriesforcomparison) if entriesforcomparison else ''
            return rail.find_first_by_attr_and_get_attr(entriesforcomparison, 'effectivedateforcomparison',
                                                        max_date, 'schedulePolicy.officeScheduleUri') if entriesforcomparison else ''

        get_existing_schedule_uri = rail.PythonOperator(
            task_id='get_existing_schedule_uri',
            python_callable=get_existing_schedule
        )

        if_schedule_present = rail.IfOperator(
            task_id='if_schedule_present',
            test='''{{ dag_run.conf.schedule | is_truthy }}''',
            yes_task="get_required_office_schedule_uri",
            no_task="if_location_uk",
        )

        get_required_office_schedule_uri = rail.RepliconServiceOperator(
            task_id='get_required_office_schedule_uri',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['schedule'], 'uri', '')
        )

        if_officeschedule_unequal_current = rail.IfOperator(
            task_id='if_officeschedule_unequal_current',
            test='''{{ result('get_required_office_schedule_uri') != result('get_existing_schedule_uri') }}''',
            yes_task="if_required_officeschedule_uri_present",
            no_task="if_location_uk",
        )

        if_required_officeschedule_uri_present = rail.IfOperator(
            task_id='if_required_officeschedule_uri_present',
            test='''{{ result('get_required_office_schedule_uri') | is_truthy }}''',
            yes_task="get_final_office_schedule",
            no_task="if_required_officeschedule_uri_not_present",
        )

        get_final_office_schedule = rail.PythonOperator(
            task_id='get_final_office_schedule',
            python_callable=lambda: rail.result('create_schedule_entries_list')['scheduleentries'] + [{
                "schedulePolicy": {
                    "officeScheduleUri": rail.result('get_required_office_schedule_uri'),
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                    "name": null,
                    "officeSchedule": null
                },
                "effectiveDate": {
                    "year": rail.result('get_scheduleeffectivedate_object')['year'],
                    "month": rail.result('get_scheduleeffectivedate_object')['month'],
                    "day": rail.result('get_scheduleeffectivedate_object')['day']
                }
            }]
        )

        put_schedule_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['uri'],
                "scheduleEntries": rail.result('get_final_office_schedule')
            }
        )

        log_office_schedule_updated = rail.PythonOperator(
            task_id='log_office_schedule_updated',
            python_callable=lambda dag_run:  "Office schedule updated - '" +
            dag_run.conf['schedule'] + "';"
        )

        if_required_officeschedule_uri_not_present = rail.IfOperator(
            task_id='if_required_officeschedule_uri_not_present',
            test='''{{ result('get_required_office_schedule_uri') | is_falsy }}''',
            yes_task="log_office_schedule_notfound",
            no_task="if_location_uk",
        )

        log_office_schedule_notfound = rail.PythonOperator(
            task_id='log_office_schedule_notfound',
            python_callable=lambda dag_run:  "Office schedule not found - '" +
            dag_run.conf['schedule'] + "';"
        )

        if_location_uk = rail.IfOperator(
            task_id='if_location_uk',
            test='''{{dag_run.conf.location | lower == 'united kingdom'}}''',
            yes_task='get_final_exceptions',
            no_task='if_location_china_hongkong'
        )

        if_location_china_hongkong = rail.IfOperator(
            task_id='if_location_china_hongkong',
            test='''{{dag_run.conf.location | lower == 'china' or dag_run.conf.location | lower == 'hong kong'}}''',
            yes_task='if_user_rehired_and_enabled',
            no_task='if_locationandemployeetypebasedchange_variable_notequal_no'
        )

        if_locationandemployeetypebasedchange_variable_notequal_no = rail.IfOperator(
            task_id='if_locationandemployeetypebasedchange_variable_notequal_no',
            test=lambda: rail.get_dag_run_var(
                'locationandemployeetypebasedchange') != 'no',
            yes_task="if_user_rehired_and_enabled",
            no_task="get_final_exceptions",
        )

        if_user_rehired_and_enabled = rail.IfOperator(
            task_id='if_user_rehired_and_enabled',
            test='''{{ result('log_user_rehired_and_enabled') | is_truthy }}''',
            yes_task="trigger_child_timeofftype_assignment_for_rehireduser",
            no_task="trigger_child_timeofftype_assignment_for_updateuser",
        )

        trigger_child_timeofftype_assignment_for_rehireduser = rail.TriggerDagRunOperator(
            task_id='trigger_child_timeofftype_assignment_for_rehireduser',
            retries=0,
            trigger_dag_id=config.child_rehire_user_time_off_type_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "uri": "{{ dag_run.conf.uri }}",
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

        wait_for_child_timeofftype_assignment_for_rehireduser = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_timeofftype_assignment_for_rehireduser',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_timeofftype_assignment_for_rehireduser") }}'
        )

        trigger_child_timeofftype_assignment_for_updateuser = rail.TriggerDagRunOperator(
            task_id='trigger_child_timeofftype_assignment_for_updateuser',
            retries=0,
            trigger_dag_id=config.child_update_user_time_off_type_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "uri": "{{ dag_run.conf.uri }}",
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
                "previousloginstatus": "{{ result('get_user_details')[0].securityConfiguration.isLoginEnabled }}",
                "parentjobid": "{{ dag_run.conf.callerjobid }}",
                "previouscountry": "{{ result('get_uri_and_parent_location_name').parent }}",
                "previousemployeetype": "{{ result('get_user_details')[0].employeeType.name }}",
                "previousstateprovince": "{{ result('get_current_state_provincevalue') }}",
                "chinacareerstartdate": "{{dag_run.conf.chinacareerstartdate}}",
                "hongkonglevels": "{{dag_run.conf.hongkonglevels}}"
            }
        )

        wait_for_child_timeofftype_assignment_for_updateuser = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_timeofftype_assignment_for_updateuser',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_timeofftype_assignment_for_updateuser") }}'
        )

        def get_exceptions():
            return (rail.result('log_incorrect_email_format') if rail.result('log_incorrect_email_format') else '') + (
                rail.result('log_employeetype_notfound') if rail.result('log_employeetype_notfound') else '') + (
                rail.result('log_department_notfound') if rail.result('log_department_notfound') else '') + (
                rail.result('log_userpermissionset_notfound') if rail.result('log_userpermissionset_notfound') else '') + (
                rail.result('log_supervisorpermission_notfound') if rail.result('log_supervisorpermission_notfound') else '') + (
                rail.result('log_licenseseat_notfound') if rail.result('log_licenseseat_notfound') else '') + (
                rail.result('log_location_not_found') if rail.result('log_location_not_found') else '') + (
                rail.result('log_location_notfound') if rail.result('log_location_notfound') else '') + (
                rail.result('log_team_not_found') if rail.result('log_team_not_found') else '') + (
                rail.result('log_team_notfound') if rail.result('log_team_notfound') else '') + (
                rail.result('log_stateprovince_notfound') if rail.result('log_stateprovince_notfound') else '') + (
                rail.result('log_supervisor_not_updated') if rail.result('log_supervisor_not_updated') else '') + (
                rail.result('log_timeofftemplate_notfound') if rail.result('log_timeofftemplate_notfound') else '') + (
                rail.result('log_timeoffapproval_path_notfound') if rail.result('log_timeoffapproval_path_notfound') else '') + (
                rail.result('log_holidaycalendar_notfound') if rail.result('log_holidaycalendar_notfound') else '') + (
                rail.result('log_office_schedule_notfound') if rail.result('log_office_schedule_notfound') else '')

        get_final_exceptions = rail.PythonOperator(
            task_id='get_final_exceptions',
            python_callable=get_exceptions
        )

        def get_logs():
            return (rail.result('log_employeetype_changed') if rail.result('log_employeetype_changed') else '') + (
                rail.result('log_integration_date_updated') if rail.result('log_integration_date_updated') else '') + (
                rail.result('log_user_rehired_and_enabled') if rail.result('log_user_rehired_and_enabled') else '') + (
                rail.result('log_location_changed') if rail.result('log_location_changed') else '') + (
                rail.result('log_location_is_changed') if rail.result('log_location_is_changed') else '') + (
                rail.result('log_team_changed') if rail.result('log_team_changed') else '') + (
                rail.result('log_teamchanged') if rail.result('log_teamchanged') else '') + (
                rail.result('log_state_province_changed') if rail.result('log_state_province_changed') else '') + (
                rail.result('log_incorrect_email_format') if rail.result('log_incorrect_email_format') else '') + (
                rail.result('log_employeetype_notfound') if rail.result('log_employeetype_notfound') else '') + (
                rail.result('log_department_notfound') if rail.result('log_department_notfound') else '') + (
                rail.result('log_userpermissionset_notfound') if rail.result('log_userpermissionset_notfound') else '') + (
                rail.result('log_supervisorpermission_notfound') if rail.result('log_supervisorpermission_notfound') else '') + (
                rail.result('log_licenseseat_notfound') if rail.result('log_licenseseat_notfound') else '') + (
                rail.result('log_location_not_found') if rail.result('log_location_not_found') else '') + (
                rail.result('log_location_notfound') if rail.result('log_location_notfound') else '') + (
                rail.result('log_team_not_found') if rail.result('log_team_not_found') else '') + (
                rail.result('log_team_notfound') if rail.result('log_team_notfound') else '') + (
                rail.result('log_stateprovince_notfound') if rail.result('log_stateprovince_notfound') else '') + (
                rail.result('log_supervisor_not_updated') if rail.result('log_supervisor_not_updated') else '') + (
                rail.result('log_timeofftemplate_notfound') if rail.result('log_timeofftemplate_notfound') else '') + (
                rail.result('log_timeoffapproval_path_notfound') if rail.result('log_timeoffapproval_path_notfound') else '') + (
                rail.result('log_holidaycalendar_notfound') if rail.result('log_holidaycalendar_notfound') else '') + (
                rail.result('log_office_schedule_notfound') if rail.result('log_office_schedule_notfound') else '') + (
                rail.result('log_firstname_updated') if rail.result('log_firstname_updated') else '') + (
                rail.result('log_lastname_updated') if rail.result('log_lastname_updated') else '') + (
                rail.result('log_employeeid_updated') if rail.result('log_employeeid_updated') else '') + (
                rail.result('log_email_updated') if rail.result('log_email_updated') else '') + (
                rail.result('log_authentication_type_updated') if rail.result('log_authentication_type_updated') else '') + (
                rail.result('log_authenticationtype_updated') if rail.result('log_authenticationtype_updated') else '') + (
                rail.result('log_department_updated') if rail.result('log_department_updated') else '') + (
                rail.result('log_userpermission_updated') if rail.result('log_userpermission_updated') else '') + (
                rail.result('log_supervisorpermission_updated') if rail.result('log_supervisorpermission_updated') else '') + (
                rail.result('log_licenseseat_updated') if rail.result('log_licenseseat_updated') else '') + (
                rail.result('log_supervisor_updated') if rail.result('log_supervisor_updated') else '') + (
                rail.result('log_timeofftemplate_updated') if rail.result('log_timeofftemplate_updated') else '') + (
                rail.result('log_timeoffapproval_path_updated') if rail.result('log_timeoffapproval_path_updated') else '') + (
                rail.result('log_holidaycalendar_updated') if rail.result('log_holidaycalendar_updated') else '') + (
                rail.result('log_office_schedule_updated') if rail.result('log_office_schedule_updated') else '')

        get_final_logs = rail.PythonOperator(
            task_id='get_final_logs',
            python_callable=get_logs
        )

        add_final_log_for_user = rail.WriteLogOperator(
            task_id='add_final_log_for_user',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity=lambda: ("Exception" if rail.result(
                'get_final_exceptions') else 'Success') if rail.result('get_final_logs') else "Skipped",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "empid": dag_run.conf['employeeid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": ("Exception" if rail.result('get_final_exceptions') else 'Success') if rail.result('get_final_logs') else "Skipped",
                "details": ("User Updated - " + rail.result('get_final_logs')) if rail.result('get_final_logs') else "No changes Received",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": '||'
            }
        )

        catch_log_error = rail.WriteLogOperator(
            task_id='catch_log_error',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "empid": dag_run.conf['employeeid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": "Error",
                "details": "Error - " + rail.render_template("{{get_error_message()}}") + ";" + get_logs(),
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": '||'
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> declare_locationandemployeetypebasedchange_variable
        declare_locationandemployeetypebasedchange_variable >> get_user_details >> if_user_currently_enabled_but_tobe_disabled
        if_user_currently_enabled_but_tobe_disabled >> rail.Label(
            'Yes') >> trigger_dag_to_disable_user >> wait_for_child_to_disable_user >> finish
        if_user_currently_enabled_but_tobe_disabled >> rail.Label(
            'No') >> if_user_currently_disabled_and_tobe_disabled
        if_user_currently_disabled_and_tobe_disabled >> rail.Label(
            'Yes') >> log_user_already_disabled >> finish
        if_user_currently_disabled_and_tobe_disabled >> rail.Label(
            'No') >> if_integrationdate_less_than_startdate
        if_integrationdate_less_than_startdate >> rail.Label(
            'Yes') >> log_integrationdate_earlier_than_startdate >> finish
        if_integrationdate_less_than_startdate >> rail.Label(
            'No') >> get_data_for_user >> if_firstname_unequal_current
        if_firstname_unequal_current >> rail.Label(
            'Yes') >> update_first_name >> log_firstname_updated >> if_lastname_unequal_current
        if_firstname_unequal_current >> rail.Label(
            'No') >> if_lastname_unequal_current
        if_lastname_unequal_current >> rail.Label(
            'Yes') >> update_last_name >> log_lastname_updated >> if_employeeid_unequal_current
        if_lastname_unequal_current >> rail.Label(
            'No') >> if_employeeid_unequal_current
        if_employeeid_unequal_current >> rail.Label(
            'Yes') >> update_employeeid >> log_employeeid_updated >> if_email_unequal_current
        if_employeeid_unequal_current >> rail.Label(
            'No') >> if_email_unequal_current
        if_email_unequal_current >> rail.Label('Yes') >> if_valid_email
        if_valid_email >> rail.Label(
            'Yes') >> update_email >> log_email_updated >> if_employeetype_unequal_current
        if_valid_email >> rail.Label(
            'No') >> log_incorrect_email_format >> if_employeetype_unequal_current
        if_email_unequal_current >> rail.Label(
            'No') >> if_employeetype_unequal_current
        if_employeetype_unequal_current >> rail.Label(
            'Yes') >> get_all_employee_type_details >> if_employeetype_uri_present
        if_employeetype_uri_present >> rail.Label(
            'Yes') >> update_employee_type_for_user >> if_location_is_canada
        if_location_is_canada >> rail.Label(
            'Yes') >> update_locationandemployeetypebasedchange_variable >> log_employeetype_changed
        if_location_is_canada >> rail.Label(
            'No') >> log_employeetype_changed >> get_current_authentication_type
        if_employeetype_uri_present >> rail.Label(
            'No') >> log_employeetype_notfound >> get_current_authentication_type
        if_employeetype_unequal_current >> rail.Label(
            'No') >> get_current_authentication_type >> if_authenticationtype_not_equal_current
        if_authenticationtype_not_equal_current >> rail.Label(
            'Yes') >> if_authenticationtype_is_sso
        if_authenticationtype_is_sso >> rail.Label(
            'Yes') >> set_sso_authentication_for_user >> log_authentication_type_updated >> if_authenticationtype_is_replicon
        if_authenticationtype_is_sso >> rail.Label(
            'No') >> if_authenticationtype_is_replicon
        if_authenticationtype_is_replicon >> rail.Label(
            'Yes') >> set_replicon_authentication_for_user >> log_authenticationtype_updated >> if_departmentname_present
        if_authenticationtype_is_replicon >> rail.Label(
            'No') >> if_departmentname_present
        if_authenticationtype_not_equal_current >> rail.Label(
            'No') >> if_departmentname_present
        if_departmentname_present >> rail.Label(
            'Yes') >> if_inputdepartmenturi_present
        if_inputdepartmenturi_present >> rail.Label(
            'Yes') >> if_inputdepartmenturi_unequal_current
        if_inputdepartmenturi_unequal_current >> rail.Label(
            'Yes') >> update_department_for_user >> log_department_updated >> get_all_custom_fields
        if_inputdepartmenturi_unequal_current >> rail.Label(
            'No') >> get_all_custom_fields
        if_inputdepartmenturi_present >> rail.Label(
            'No') >> log_department_notfound >> get_all_custom_fields
        if_departmentname_present >> rail.Label(
            'No') >> get_all_custom_fields >> get_current_chinacareerstartdate >> if_chinacareerstartdate_present
        if_chinacareerstartdate_present >> rail.Label(
            'No') >> get_current_hongkonglevels
        if_chinacareerstartdate_present >> rail.Label(
            'Yes') >> if_chinacareerstartdate_equal_current
        if_chinacareerstartdate_equal_current >> rail.Label(
            'Yes') >> get_current_hongkonglevels
        if_chinacareerstartdate_equal_current >> rail.Label(
            'No') >> update_chinacareerstartdate_for_user >> get_current_hongkonglevels >> if_hongkonglevels_present
        if_hongkonglevels_present >> rail.Label(
            'No') >> if_currently_disabled_and_tobe_enabled
        if_hongkonglevels_present >> rail.Label(
            'Yes') >> if_hongkonglevels_equal_current
        if_hongkonglevels_equal_current >> rail.Label(
            'No') >> update_hongkonglevels_for_user >> if_currently_disabled_and_tobe_enabled
        if_hongkonglevels_equal_current >> rail.Label(
            'Yes') >> if_currently_disabled_and_tobe_enabled

        if_currently_disabled_and_tobe_enabled >> rail.Label(
            'Yes') >> get_integration_dateobject >> get_startdate_object
        get_startdate_object >> update_employment_daterange_remove_enddate >> update_integration_date >> log_integration_date_updated >> enable_login
        enable_login >> log_user_rehired_and_enabled >> update_location_and_employeetypebasedchange_variable >> get_assigned_userpermissionset
        if_currently_disabled_and_tobe_enabled >> rail.Label(
            'No') >> get_assigned_userpermissionset >> get_all_permission_sets >> if_userpermission_unequal_current
        if_userpermission_unequal_current >> rail.Label(
            'Yes') >> if_userpermission_uri_present
        if_userpermission_uri_present >> rail.Label(
            'Yes') >> assign_permission_set_to_user >> log_userpermission_updated >> get_assigned_supervisorpermissionset
        if_userpermission_uri_present >> rail.Label(
            'No') >> log_userpermissionset_notfound >> get_assigned_supervisorpermissionset >> if_supervisorpermission_unequal_current
        if_userpermission_unequal_current >> rail.Label(
            'No') >> get_assigned_supervisorpermissionset >> if_supervisorpermission_unequal_current
        if_supervisorpermission_unequal_current >> rail.Label(
            'Yes') >> if_supervisorpermission_uri_present
        if_supervisorpermission_uri_present >> rail.Label(
            'Yes') >> assign_permission_set_touser >> log_supervisorpermission_updated >> get_assigned_products
        if_supervisorpermission_uri_present >> rail.Label(
            'No') >> log_supervisorpermission_notfound >> get_assigned_products
        if_supervisorpermission_unequal_current >> rail.Label(
            'No') >> get_assigned_products >> if_licenseseats_notin_assigned_products
        if_licenseseats_notin_assigned_products >> rail.Label(
            'Yes') >> get_all_products_available_for_user_assignment >> if_product_uri_present
        if_product_uri_present >> rail.Label(
            'Yes') >> put_product_assignments_for_user >> log_licenseseat_updated >> checkifany_cost_center_locationisassigned
        if_product_uri_present >> rail.Label(
            'No') >> log_licenseseat_notfound >> checkifany_cost_center_locationisassigned
        if_licenseseats_notin_assigned_products >> rail.Label(
            'No') >> checkifany_cost_center_locationisassigned >> get_all_cost_centers_cost_centerislabelled_location >> get_locationeffective_dateobject
        get_locationeffective_dateobject >> if_no_costcenter_assigned
        if_no_costcenter_assigned >> rail.Label('Yes') >> if_location_present
        if_location_present >> rail.Label('Yes') >> if_inputlocationuri_present
        if_inputlocationuri_present >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user >> add_log_location_updated >> log_location_changed
        log_location_changed >> update_location_and_employeetype_basedchange_variable >> if_currently_costcenter_assigned
        if_no_costcenter_assigned >> rail.Label(
            'No') >> if_currently_costcenter_assigned
        if_inputlocationuri_present >> rail.Label(
            'No') >> log_location_not_found >> if_currently_costcenter_assigned
        if_location_present >> rail.Label(
            'No') >> if_currently_costcenter_assigned
        if_no_costcenter_assigned >> rail.Label(
            'No') >> if_currently_costcenter_assigned
        if_currently_costcenter_assigned >> rail.Label(
            'Yes') >> get_uri_and_parent_location_name >> if_location_unequal_current
        if_location_unequal_current >> rail.Label(
            'Yes') >> if_input_location_uri_present
        if_input_location_uri_present >> rail.Label(
            'Yes') >> create_locationschedule_list >> if_location_schedule_present
        if_location_schedule_present >> rail.Label(
            'Yes') >> put_costcenterschedule_for_user >> log_location_is_changed >> if_parent_location_unequal_location
        if_parent_location_unequal_location >> rail.Label(
            'Yes') >> update_location_and_employeetype_based_change_variable >> check_if_any_team_assigned
        if_parent_location_unequal_location >> rail.Label(
            'No') >> check_if_any_team_assigned
        if_location_schedule_present >> rail.Label(
            'No') >> check_if_any_team_assigned
        if_input_location_uri_present >> rail.Label(
            'No') >> log_location_notfound >> check_if_any_team_assigned
        if_location_unequal_current >> rail.Label(
            'No') >> check_if_any_team_assigned
        if_currently_costcenter_assigned >> rail.Label(
            'No') >> check_if_any_team_assigned >> get_all_locations_locationislabelled_team >> get_team_effectivedate_object >> if_no_team_assigned_currently
        if_no_team_assigned_currently >> rail.Label('Yes') >> if_team_present
        if_team_present >> rail.Label('Yes') >> if_inputteamuri_present
        if_inputteamuri_present >> rail.Label(
            'Yes') >> put_location_schedule_for_user >> log_team_changed >> if_currently_team_is_assigned
        if_inputteamuri_present >> rail.Label(
            'No') >> log_team_not_found >> if_currently_team_is_assigned
        if_team_present >> rail.Label('No') >> if_currently_team_is_assigned
        if_no_team_assigned_currently >> rail.Label(
            'No') >> if_currently_team_is_assigned
        if_currently_team_is_assigned >> rail.Label(
            'Yes') >> get_current_team_uri >> if_team_unequal_current
        if_team_unequal_current >> rail.Label(
            'Yes') >> if_input_teamuri_present
        if_input_teamuri_present >> rail.Label(
            'Yes') >> create_teamschedule_list >> if_team_schedule_present
        if_team_schedule_present >> rail.Label(
            'Yes') >> put_locationschedule_for_user >> log_teamchanged >> get_current_state_provincevalue
        if_team_schedule_present >> rail.Label(
            'No') >> get_current_state_provincevalue
        if_input_teamuri_present >> rail.Label(
            'No') >> log_team_notfound >> get_current_state_provincevalue
        if_team_unequal_current >> rail.Label(
            'No') >> get_current_state_provincevalue
        if_currently_team_is_assigned >> rail.Label(
            'No') >> get_current_state_provincevalue >> if_stateprovince_unequal_current
        if_stateprovince_unequal_current >> rail.Label(
            'Yes') >> get_required_dropdown_uri >> if_required_stateprovince_present
        if_required_stateprovince_present >> rail.Label(
            'Yes') >> update_dropdown_value >> if_location_equals_usa
        if_location_equals_usa >> rail.Label(
            'Yes') >> updatelocation_and_employeetype_based_change_variable >> log_state_province_changed
        if_location_equals_usa >> rail.Label(
            'No') >> log_state_province_changed >> if_supervisorloginname_present
        if_required_stateprovince_present >> rail.Label(
            'No') >> log_stateprovince_notfound >> if_supervisorloginname_present
        if_stateprovince_unequal_current >> rail.Label(
            'No') >> if_supervisorloginname_present
        if_supervisorloginname_present >> rail.Label(
            'Yes') >> if_supervisorloginname_equal_userloginname
        if_supervisorloginname_equal_userloginname >> rail.Label(
            'Yes') >> log_supervisor_not_updated >> if_timeofftemplate_unequal_current
        if_supervisorloginname_equal_userloginname >> rail.Label(
            'No') >> get_userdetails >> if_supervisorloginname_unequal_current
        if_supervisorloginname_unequal_current >> rail.Label(
            'Yes') >> search_supervisor_user >> if_supervisoruri_present_in_replicon
        if_supervisoruri_present_in_replicon >> rail.Label(
            'Yes') >> if_supervisorstatus_is_true
        if_supervisorstatus_is_true >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user >> if_supervisorpermission_present
        if_supervisorpermission_present >> rail.Label(
            'Yes') >> get_supervisoreffectivedate_object >> update_supervisor_assignment_schedule_over_date_range >> log_supervisor_updated
        log_supervisor_updated >> if_timeofftemplate_unequal_current
        if_supervisorpermission_present >> rail.Label(
            'No') >> add_to_supervisorassignment_lookup
        if_supervisoruri_present_in_replicon >> rail.Label(
            'No') >> add_to_supervisorassignment_lookup >> if_timeofftemplate_unequal_current
        if_supervisorstatus_is_true >> rail.Label(
            'No') >> add_to_supervisorassignment_lookup
        if_supervisorloginname_unequal_current >> rail.Label(
            'No') >> if_timeofftemplate_unequal_current
        if_supervisorloginname_present >> rail.Label(
            'No') >> if_timeofftemplate_unequal_current
        if_timeofftemplate_unequal_current >> rail.Label(
            'Yes') >> get_required_timeoff_template_uri >> if_timeofftemplate_uri_present
        if_timeofftemplate_uri_present >> rail.Label(
            'Yes') >> assign_policy_set_to_user >> log_timeofftemplate_updated >> if_timeoffaprrovalpath_unequal_current
        if_timeofftemplate_uri_present >> rail.Label(
            'No') >> log_timeofftemplate_notfound >> if_timeoffaprrovalpath_unequal_current
        if_timeofftemplate_unequal_current >> rail.Label(
            'No') >> if_timeoffaprrovalpath_unequal_current
        if_timeoffaprrovalpath_unequal_current >> rail.Label(
            'Yes') >> get_required_timeoffapproval_path_uri >> if_timeoffapprovalpath_uri_present
        if_timeoffapprovalpath_uri_present >> rail.Label(
            'Yes') >> update_approval_path_for_user >> log_timeoffapproval_path_updated >> if_holidaycalendar_unequal_current
        if_timeoffapprovalpath_uri_present >> rail.Label(
            'No') >> log_timeoffapproval_path_notfound >> if_holidaycalendar_unequal_current
        if_timeoffaprrovalpath_unequal_current >> rail.Label(
            'No') >> if_holidaycalendar_unequal_current
        if_holidaycalendar_unequal_current >> rail.Label(
            'Yes') >> get_required_holidaycalendar_uri >> if_required_holiday_calendar_uri_present
        if_required_holiday_calendar_uri_present >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user >> log_holidaycalendar_updated >> get_scheduleeffectivedate_object
        if_required_holiday_calendar_uri_present >> rail.Label(
            'No') >> log_holidaycalendar_notfound >> get_scheduleeffectivedate_object
        if_holidaycalendar_unequal_current >> rail.Label(
            'No') >> get_scheduleeffectivedate_object >> get_schedule_policy_schedule_for_user >> create_schedule_entries_list >> get_existing_schedule_uri
        get_existing_schedule_uri >> if_schedule_present
        if_schedule_present >> rail.Label(
            'Yes') >> get_required_office_schedule_uri >> if_officeschedule_unequal_current
        if_officeschedule_unequal_current >> rail.Label(
            'Yes') >> if_required_officeschedule_uri_present
        if_required_officeschedule_uri_present >> rail.Label(
            'Yes') >> get_final_office_schedule >> put_schedule_policy_schedule_for_user >> log_office_schedule_updated
        log_office_schedule_updated >> if_required_officeschedule_uri_not_present
        if_required_officeschedule_uri_present >> rail.Label(
            'No') >> if_required_officeschedule_uri_not_present
        if_required_officeschedule_uri_not_present >> rail.Label(
            'Yes') >> log_office_schedule_notfound >> if_location_uk
        if_required_officeschedule_uri_not_present >> rail.Label(
            'No') >> if_location_uk
        if_officeschedule_unequal_current >> rail.Label(
            'No') >> if_location_uk
        if_schedule_present >> rail.Label(
            'No') >> if_location_uk
        if_location_uk >> rail.Label(
            'No') >> if_location_china_hongkong
        if_location_uk >> rail.Label(
            'Yes') >> get_final_exceptions
        if_location_china_hongkong >> rail.Label(
            'No') >> if_locationandemployeetypebasedchange_variable_notequal_no
        if_location_china_hongkong >> rail.Label(
            'Yes') >> if_user_rehired_and_enabled
        if_locationandemployeetypebasedchange_variable_notequal_no >> rail.Label(
            'Yes') >> if_user_rehired_and_enabled
        if_user_rehired_and_enabled >> rail.Label(
            'Yes') >> trigger_child_timeofftype_assignment_for_rehireduser >> wait_for_child_timeofftype_assignment_for_rehireduser >> get_final_exceptions
        if_user_rehired_and_enabled >> rail.Label(
            'No') >> trigger_child_timeofftype_assignment_for_updateuser >> wait_for_child_timeofftype_assignment_for_updateuser >> get_final_exceptions
        if_locationandemployeetypebasedchange_variable_notequal_no >> rail.Label(
            'No') >> get_final_exceptions

        get_final_exceptions >> get_final_logs >> add_final_log_for_user >> catch_log_error >> finish

    return dag


rail.for_each_instance(create_dag)
