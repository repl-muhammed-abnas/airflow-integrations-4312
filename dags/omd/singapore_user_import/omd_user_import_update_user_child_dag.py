
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'omd_singapore_user_import_update_user_child_{config.instance}',
        description=f'OMD User Update Singapore Update User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_logs_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_logs_list',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_logs_list=rail.SetVariableOperator(
            task_id='create_logs_list',
            append=False,
            name='logs',
            value=[]
        )

        create_exception_list=rail.SetVariableOperator(
            task_id='create_exception_list',
            append=False,
            name='Exception',
            value=[]
        )

        create_updatetype_variable=rail.SetVariableOperator(
            task_id='create_updatetype_variable',
            append=False,
            name='updattype',
            value=None
        )

        bulk_get_user=rail.RepliconServiceOperator(
            task_id='bulk_get_user',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_all_products_available_for_user_assignment=rail.RepliconServiceOperator(
            task_id='get_all_products_available_for_user_assignment',
            endpoint="/services/AccountManagementService1.svc/GetAllProductsAvailableForUserAssignment",
        )

        def get_licenselist():
            licenses = rail.result('get_all_products_available_for_user_assignment')
            licenselist = []
            licenselist = [license['uri'] for license in licenses ]
            return licenselist

        get_license_list = rail.PythonOperator(
            task_id = 'get_license_list',
            python_callable= get_licenselist
        )

        log_custom_field_values=rail.PythonOperator(
            task_id='log_custom_field_values',
            python_callable= lambda: rail.result('bulk_get_user')[0]['userDetails']['customFieldValues']
        )

        def get_datestring_from_datetime_object(string):
            return datetime.strptime(string,'%d/%m/%Y').strftime('%d/%m/%Y')

        log_eestatus_value=rail.PythonOperator(
            task_id='log_eestatus_value',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('bulk_get_user')[0]['userDetails']['customFieldValues'],'customField.textValue','EEstatus','text','')
        )

        def get_currently_set_date(datetype):
            date = rail.result('bulk_get_user')[0]['userDetails']['employmentDateRange'][datetype]
            datestring = str(date['day']) + '/' + str(date['month']) + '/' + str(date['year']) if date else null
            return get_datestring_from_datetime_object(datestring) if datestring else null

        get_current_start_date=rail.PythonOperator(
            task_id='get_current_start_date',
            python_callable=lambda: get_currently_set_date('startDate')
        )

        get_today_date=rail.PythonOperator(
            task_id='get_today_date',
            python_callable= lambda: {
                "day": datetime.now().day,
                "month": datetime.now().month,
                "year": datetime.now().year
            }
        )

        get_timeoff_schedule=rail.PythonOperator(
            task_id='get_timeoff_schedule',
            python_callable= lambda: rail.result('bulk_get_user')[0]['timeOffTypePolicySummary']['policiesByTimeOffType']
        )

        if_request_startdate_present=rail.IfOperator(
            task_id='if_request_startdate_present',
            test='''{{ dag_run.conf.startdate | is_truthy }}''',
            yes_task="get_hire_date",
            no_task="if_user_currently_not_enabled",
        )

        def get_date_object(datestring):
            date = datetime.strptime(datestring,'%d/%m/%Y')
            return {
                "day": date.day,
                "month": date.month,
                "year": date.year

            }

        get_hire_date=rail.PythonOperator(
            task_id='get_hire_date',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['startdate'])
        )

        if_startdate_present_and_not_equal_request=rail.IfOperator(
            task_id='if_startdate_present_and_not_equal_request',
            test=lambda dag_run: not bool((datetime.strptime(rail.result('get_current_start_date')
                                    if rail.result('get_current_start_date')
                                    else '01/01/1999','%d/%m/%Y')) == datetime.strptime(dag_run.conf['startdate'],'%d/%m/%Y')),
            yes_task="update_start_date",
            no_task="if_user_currently_not_enabled",
        )

        update_start_date=rail.RepliconServiceOperator(
            task_id='update_start_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                    "year": "{{ result('get_hire_date').year }}",
                    "month": "{{ result('get_hire_date').month }}",
                    "day": "{{ result('get_hire_date').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_newhiredate=rail.PythonOperator(
            task_id='log_newhiredate',
            python_callable=lambda: get_datestring_from_datetime_object(str(rail.result('get_hire_date')['day']) + "/" +
                                    str(rail.result('get_hire_date')['month']) + "/" + str(rail.result('get_hire_date')['year']))
        )

        log_startdate_updated=rail.SetVariableOperator(
            task_id='log_startdate_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "Start date updated"
            }
        )

        if_user_currently_not_enabled=rail.IfOperator(
            task_id='if_user_currently_not_enabled',
            test=lambda dag_run: bool( not(rail.result('bulk_get_user')[0]['userDetails']['isEnabled']) and dag_run.conf['enabled'].lower() == 'yes'),
            yes_task="enable_login",
            no_task="if_user_currently_not_enabled_and_enddate_present",
        )

        enable_login=rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint="/services/securityservice1.svc/EnableLogin",
            data={
               "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        remove_end_date=rail.RepliconServiceOperator(
            task_id='remove_end_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                    "year": "{{ result('get_hire_date').year }}",
                    "month": "{{ result('get_hire_date').month }}",
                    "day": "{{ result('get_hire_date').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        put_product_assignments_for_user=rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda dag_run:{
                "userUri": dag_run.conf['useruri'],
                "productUris": rail.result('get_license_list')
            }
        )

        log_user_re_enabled=rail.SetVariableOperator(
            task_id='log_user_re_enabled',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "User re-enabled"
            }
        )

        if_user_currently_not_enabled_and_enddate_present=rail.IfOperator(
            task_id='if_user_currently_not_enabled_and_enddate_present',
            test=lambda dag_run: bool( not(rail.result('bulk_get_user')[0]['userDetails']['isEnabled']) and
                                    dag_run.conf['enddate'] and dag_run.conf['enabled'].lower() == 'no' ),
            yes_task="log_user_already_disabled",
            no_task="if_request_enddate_present",
        )

        log_user_already_disabled=rail.WriteLogOperator(
            task_id='log_user_already_disabled',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['loginname'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "status": "Success",
                "action": "Update",
                "details": ("User already disabled" + ';'.join([ log['value'] for log in rail.get_dag_run_var('logs')]) )
                            if rail.get_dag_run_var('logs') else "User already disabled",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        if_request_enddate_present=rail.IfOperator(
            task_id='if_request_enddate_present',
            test='''{{ dag_run.conf.enddate | is_truthy }}''',
            yes_task="get_current_enddate",
            no_task="if_user_currenlty_enabled_and_enddate_present",
        )

        get_current_enddate=rail.PythonOperator(
            task_id='get_current_enddate',
            python_callable=lambda: get_currently_set_date('endDate')
        )

        if_current_enddate_present_and_not_equal_request=rail.IfOperator(
            task_id='if_current_enddate_present_and_not_equal_request',
            test=lambda dag_run: bool( datetime.strptime((rail.result('get_current_enddate') if
                                    rail.result('get_current_enddate')
                                    else '01/01/1999'),'%d/%m/%Y') != datetime.strptime(dag_run.conf['enddate'],'%d/%m/%Y') ),
            yes_task="get_termination_date",
            no_task="if_user_currenlty_enabled_and_enddate_present",
        )

        get_termination_date=rail.PythonOperator(
            task_id='get_termination_date',
            python_callable= lambda dag_run: get_date_object(dag_run.conf['enddate'])
        )

        if_newhire_date_present=rail.IfOperator(
            task_id='if_newhire_date_present',
            test='''{{ result('log_newhiredate') | is_truthy }}''',
            yes_task="update_hire_and_termination_date",
            no_task="update_termination_date",
        )

        update_hire_and_termination_date=rail.RepliconServiceOperator(
            task_id='update_hire_and_termination_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                    "year": "{{ result('get_hire_date').year }}",
                    "month": "{{ result('get_hire_date').month }}",
                    "day": "{{ result('get_hire_date').day }}"
                    },
                    "endDate": {
                    "year": "{{ result('get_termination_date').year }}",
                    "month": "{{ result('get_termination_date').month }}",
                    "day": "{{ result('get_termination_date').day }}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        update_termination_date=rail.RepliconServiceOperator(
            task_id='update_termination_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                    "year": "{{ result('bulk_get_user')[0].userDetails.employmentDateRange.startDate.year }}",
                    "month": "{{ result('bulk_get_user')[0].userDetails.employmentDateRange.startDate.month }}",
                    "day": "{{ result('bulk_get_user')[0].userDetails.employmentDateRange.startDate.day }}"
                    },
                    "endDate": {
                    "year": "{{ result('get_termination_date').year }}",
                    "month": "{{ result('get_termination_date').month }}",
                    "day": "{{ result('get_termination_date').day }}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_enddate_updated=rail.SetVariableOperator(
            task_id='log_enddate_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "End Date updated"
            }
        )

        if_user_currenlty_enabled_and_enddate_present=rail.IfOperator(
            task_id='if_user_currenlty_enabled_and_enddate_present',
            test=lambda dag_run: bool( rail.result('bulk_get_user')[0]['userDetails']['isEnabled'] and
                                    dag_run.conf['enddate'] and dag_run.conf['enabled'].lower() == 'no' ),
            yes_task="disable_login",
            no_task="if_firstname_present_not_equal_current",
        )

        disable_login=rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_user_disabled=rail.WriteLogOperator(
            task_id='log_user_disabled',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['loginname'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'] ,
                "status": "Success",
                "action": "Update",
                "details": ("User disabled|" + ';'.join([ log['value'] for log in rail.get_dag_run_var('logs')]) )
                            if rail.get_dag_run_var('logs') else "User disabled",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        if_firstname_present_not_equal_current=rail.IfOperator(
            task_id='if_firstname_present_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['firstname'] and
                                    rail.result('bulk_get_user')[0]['userDetails']['firstName'].lower() != dag_run.conf['firstname'].lower()),
            yes_task="update_firstname",
            no_task="if_lastname_present_not_equal_current",
        )

        update_firstname=rail.RepliconServiceOperator(
            task_id='update_firstname',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        log_firstname_updated=rail.SetVariableOperator(
            task_id='log_firstname_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "First name updated"
            }
        )

        if_lastname_present_not_equal_current=rail.IfOperator(
            task_id='if_lastname_present_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['lastname'] and
                                    rail.result('bulk_get_user')[0]['userDetails']['lastName'].lower() != dag_run.conf['lastname'].lower()),
            yes_task="update_last_name",
            no_task="if_email_present_and_not_equal_current",
        )

        update_last_name=rail.RepliconServiceOperator(
            task_id='update_last_name',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        log_lastname_updated=rail.SetVariableOperator(
            task_id='log_lastname_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "Last name updated"
            }
        )

        if_email_present_and_not_equal_current=rail.IfOperator(
            task_id='if_email_present_and_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['email'] and ( rail.result('bulk_get_user')[0]['userDetails']['emailAddress'].lower()
                                    if rail.result('bulk_get_user')[0]['userDetails']['emailAddress'] else null ) != dag_run.conf['email'].lower()),
            yes_task="update_email",
            no_task="log_current_customfield1",
        )

        update_email=rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.email }}"
            }
        )

        log_email_updated=rail.SetVariableOperator(
            task_id='log_email_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "Email updated"
            }
        )

        log_current_customfield1=rail.PythonOperator(
            task_id='log_current_customfield1',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                                rail.result('bulk_get_user')[0]['userDetails']['customFieldValues'],'customField.displayText',
                                dag_run.conf['customfield1name'],'text')
        )

        if_request_customfield1_present_not_equal_current=rail.IfOperator(
            task_id='if_request_customfield1_present_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['customfield1'] and
                                    rail.result('log_current_customfield1').lower() != dag_run.conf['customfield1'].lower()),
            yes_task="get_all_custom_field_drop_down_options",
            no_task="if_employeetype_present",
        )

        get_all_custom_field_drop_down_options=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.customfield1_uri }}"
            }
        )

        get_required_customfield1_dropdown_option=rail.PythonOperator(
            task_id='get_required_customfield1_dropdown_option',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_custom_field_drop_down_options'),'displayText',dag_run.conf['customfield1'],'uri','')
        )

        if_reqeuired_customfield1_dropdown_present=rail.IfOperator(
            task_id='if_reqeuired_customfield1_dropdown_present',
            test='''{{ result('get_required_customfield1_dropdown_option') | is_truthy }}''',
            yes_task="update_dropdown_value",
            no_task="log_customfield1_not_available",
        )

        update_dropdown_value=rail.RepliconServiceOperator(
            task_id='update_dropdown_value',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.customfield1_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_required_customfield1_dropdown_option') }}"
            }
        )

        log_profile_status_updated=rail.SetVariableOperator(
            task_id='log_profile_status_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "Profile Status updated"
            }
        )

        log_customfield1_not_available=rail.SetVariableOperator(
            task_id='log_customfield1_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
                "value": "Profile Status not updated since {{ dag_run.conf.customfield1 }} is not available"
            }
        )

        if_employeetype_present=rail.IfOperator(
            task_id='if_employeetype_present',
            test='''{{ dag_run.conf.employeetype | is_truthy }}''',
            yes_task="create_employeetype_schedule_list",
            no_task="if_request_department_present",
        )

        create_employeetype_schedule_list=rail.SetVariableOperator(
            task_id='create_employeetype_schedule_list',
            append=False,
            name='employee type schedule',
            value=[]
        )

        create_employee_typelist=rail.SetVariableOperator(
            task_id='create_employee_typelist',
            append=False,
            name='employee typelist',
            value=[]
        )

        get_employeetypeschedule=rail.PythonOperator(
            task_id='get_employeetypeschedule',
            python_callable= lambda:  json.dumps(rail.result('bulk_get_user')[0]['employeeTypeGroupSchedule'])
        )

        if_employeetypeschedule_has_urn=rail.IfOperator(
            task_id='if_employeetypeschedule_has_urn',
            test='''{{ result('get_employeetypeschedule') | matches('urn') }}''',
            yes_task="parse_employeetypeschedule",
            no_task="if_employeetypeschedule_list_has_uri",
        )

        parse_employeetypeschedule=rail.PythonOperator(
            task_id='parse_employeetypeschedule',
            python_callable=lambda: json.loads(rail.result('get_employeetypeschedule'))
        )

        foreach_schedule=rail.ForEachOperator(
            task_id='foreach_schedule',
            items=lambda: rail.result('parse_employeetypeschedule'),
            start_task = 'if_effectivedate_not_present',
            end_task = 'foreach_schedule_end'
        )

        if_effectivedate_not_present=rail.IfOperator(
            task_id='if_effectivedate_not_present',
            test=lambda: not bool( rail.result('foreach_schedule') and rail.result('foreach_schedule')['effectiveDate'] and
                                rail.result('foreach_schedule')['effectiveDate']['day']),
            yes_task="insert_to_employeetypeschedule_list",
            no_task="log_effectivedate",
        )

        insert_to_employeetypeschedule_list=rail.SetVariableOperator(
            task_id='insert_to_employeetypeschedule_list',
            append=True,
            name='{{ result("create_employeetype_schedule_list").name }}',
            value={
                "uri": "{{ result('foreach_schedule').employeeTypeGroup.uri }}",
                "effectivedate": "{{result('get_current_start_date')}}",
                "name": "{{ result('foreach_schedule').employeeTypeGroup.displayText }}"
            }
        )

        insert_to_employeetype_list=rail.SetVariableOperator(
            task_id='insert_to_employeetype_list',
            append=True,
            name='{{ result("create_employee_typelist").name }}',
            value={
                "effectiveDate": {},
                "employeeTypeGroup": {
                    "uri": "{{ result('foreach_schedule').employeeTypeGroup.uri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        log_effectivedate=rail.PythonOperator(
            task_id='log_effectivedate',
            python_callable= lambda:  get_datestring_from_datetime_object(str(rail.result('foreach_schedule')['effectiveDate']['day']) + "/" +
                                        str(rail.result('foreach_schedule')['effectiveDate']['month']) + "/" +
                                        str(rail.result('foreach_schedule')['effectiveDate']['year']))
        )

        if_effectivedate_less_than_today=rail.IfOperator(
            task_id='if_effectivedate_less_than_today',
            test=lambda: bool(datetime.strptime(rail.result('log_effectivedate'),'%d/%m/%Y') < datetime.strptime(
                            datetime.now().strftime('%d/%m/%Y'),'%d/%m/%Y')),
            yes_task="insertto_employeetypeschedule_list",
            no_task="if_effectivedate_not_equal_today",
        )

        insertto_employeetypeschedule_list=rail.SetVariableOperator(
            task_id='insertto_employeetypeschedule_list',
            append=True,
            name='{{ result("create_employeetype_schedule_list").name }}',
            value={
                "uri": "{{ result('foreach_schedule').employeeTypeGroup.uri }}",
                "effectivedate": "{{result('log_effectivedate')}}",
                "name": "{{ result('foreach_schedule').employeeTypeGroup.displayText }}"
            }
        )

        if_effectivedate_not_equal_today=rail.IfOperator(
            task_id='if_effectivedate_not_equal_today',
            test=lambda: bool(datetime.strptime(rail.result('log_effectivedate'),'%d/%m/%Y') != datetime.strptime(
                            datetime.now().strftime('%d/%m/%Y'),'%d/%m/%Y')),
            yes_task="insertto_employeetype_list",
            no_task="foreach_schedule_end",
        )

        insertto_employeetype_list=rail.SetVariableOperator(
            task_id='insertto_employeetype_list',
            append=True,
            name='{{ result("create_employee_typelist").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('foreach_schedule').effectiveDate.year }}",
                    "month": "{{ result('foreach_schedule').effectiveDate.month }}",
                    "day": "{{ result('foreach_schedule').effectiveDate.day }}"
                },
                "employeeTypeGroup": {
                    "parentUri": null,
                    "uri": "{{ result('foreach_schedule').employeeTypeGroup.uri }}",
                    "name": null
                }
            }
        )

        foreach_schedule_end=rail.EmptyOperator(
            task_id='foreach_schedule_end',
        )

        if_employeetypeschedule_list_has_uri=rail.IfOperator(
            task_id='if_employeetypeschedule_list_has_uri',
            test=lambda: rail.get_dag_run_var('employee type schedule')[0]['uri'],
            yes_task="get_maximum_effectivedate",
            no_task="if_current_employeetype_not_present_or_notequal_request",
        )

        def get_max_date(schedules):
            print(rail.get_dag_run_var('employee type schedule'))
            effectivedates = []
            effectivedates = [ schedule['effectivedate'] for schedule in schedules]
            effectivedateobjects = [ datetime.strptime(date,'%d/%m/%Y') for date in effectivedates]
            return (max(effectivedateobjects)).strftime('%d/%m/%Y')

        get_maximum_effectivedate=rail.PythonOperator(
            task_id='get_maximum_effectivedate',
            python_callable=lambda: get_max_date(rail.get_dag_run_var('employee type schedule'))
        )

        get_current_employee_type_name=rail.PythonOperator(
            task_id='get_current_employee_type_name',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.get_dag_run_var('employee type schedule'),'effectivedate',rail.result('get_maximum_effectivedate'),'name','')
        )

        if_current_employeetype_not_present_or_notequal_request=rail.IfOperator(
            task_id='if_current_employeetype_not_present_or_notequal_request',
            test=lambda dag_run: bool( (not rail.result('get_current_employee_type_name')) or
                                    (rail.result('get_current_employee_type_name')).lower() != dag_run.conf['employeetype'].lower() ),
            yes_task="if_request_employeetype_uri_present",
            no_task="if_request_department_present",
        )

        if_request_employeetype_uri_present=rail.IfOperator(
            task_id='if_request_employeetype_uri_present',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy  and dag_run.conf.employeetypeuri | matches('urn') }}''',
            yes_task="log_to_employeetype_list",
            no_task="log_employeetype_not_available",
        )

        log_to_employeetype_list=rail.SetVariableOperator(
            task_id='log_to_employeetype_list',
            append=True,
            name='{{ result("create_employee_typelist").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('get_today_date').year }}",
                    "month": "{{ result('get_today_date').month }}",
                    "day": "{{ result('get_today_date').day }}"
                },
                "employeeTypeGroup": {
                    "parentUri": null,
                    "name": null,
                    "uri": "{{ dag_run.conf.employeetypeuri }}"
                }
            }
        )

        log_employee_type_schedule=rail.PythonOperator(
            task_id='log_employee_type_schedule',
            python_callable= lambda: (json.dumps(rail.get_dag_run_var('employee typelist'))).replace('effectiveDate": {}','effectiveDate":null').replace(
                                '{"uri": "","parentUri": null,"name": null}','null').replace('{"parentUri": null,"uri": "","name": null}','null').replace(
                                '{"name": null,"parentUri": null,"uri": ""}','null').replace('{"uri": ""}','null')
        )

        put_employee_type_group_schedule_for_user=rail.RepliconServiceOperator(
            task_id='put_employee_type_group_schedule_for_user',
            endpoint="/services/EmployeeTypeGroupService1.svc/PutEmployeeTypeGroupScheduleForUser",
            data=lambda dag_run: {
            "userUri": dag_run.conf['useruri'],
            "scheduleEntries": json.loads(rail.result('log_employee_type_schedule'))
            }
        )

        log_employeetype_updated=rail.SetVariableOperator(
            task_id='log_employeetype_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "Employee type updated"
            }
        )

        log_employeetype_not_available=rail.SetVariableOperator(
            task_id='log_employeetype_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
                "value": "Employee type {{ dag_run.conf.employeetype }} not present/ is disabled in Replicon"
            }
        )

        if_request_department_present=rail.IfOperator(
            task_id='if_request_department_present',
            test='''{{ dag_run.conf.department | is_truthy }}''',
            yes_task="create_department_schedule_list",
            no_task="if_request_supervisor_present",
        )

        create_department_schedule_list=rail.SetVariableOperator(
            task_id='create_department_schedule_list',
            append=False,
            name='department schedule',
            value=[]
        )

        create_departmentlist=rail.SetVariableOperator(
            task_id='create_departmentlist',
            append=False,
            name='departmentlist',
            value=[]
        )

        log_departmentschedule=rail.PythonOperator(
            task_id='log_departmentschedule',
            python_callable= lambda: json.dumps(rail.result('bulk_get_user')[0]['departmentGroupSchedule'])
        )

        if_departmentschedule_contains_urn=rail.IfOperator(
            task_id='if_departmentschedule_contains_urn',
            test='''{{ result('log_departmentschedule') | matches('urn') }}''',
            yes_task="parse_departmentgroup_schedule",
            no_task="if_departmentschedule_list_has_uri",
        )

        parse_departmentgroup_schedule=rail.PythonOperator(
            task_id='parse_departmentgroup_schedule',
            python_callable=lambda: json.loads(rail.result('log_departmentschedule'))
        )

        foreach_department_schedule=rail.ForEachOperator(
            task_id='foreach_department_schedule',
            items=lambda: rail.result('parse_departmentgroup_schedule'),
            start_task = 'if_effective_date_not_present',
            end_task = 'foreach_department_schedule_end'
        )

        if_effective_date_not_present=rail.IfOperator(
            task_id='if_effective_date_not_present',
            test='''{{ result('foreach_department_schedule').effectiveDate | is_falsy }}''',
            yes_task="insert_to_departmentschedule_list",
            no_task="log_effective_date",
        )

        insert_to_departmentschedule_list=rail.SetVariableOperator(
            task_id='insert_to_departmentschedule_list',
            append=True,
            name='{{ result("create_department_schedule_list").name }}',
            value={
                "uri": "{{ result('foreach_department_schedule').departmentGroup.uri }}",
                "effectivedate": "{{result('get_current_start_date')}}", 
                "name": "{{ result('foreach_department_schedule').departmentGroup.displayText }}"
            }
        )

        insert_to_department_list=rail.SetVariableOperator(
            task_id='insert_to_department_list',
            append=True,
            name='{{ result("create_departmentlist").name }}',
            value={
                "effectiveDate": {},
                "departmentGroup": {
                    "parentUri": null,
                    "uri": "{{ result('foreach_department_schedule').departmentGroup.uri }}",
                    "name": null
                }
            }
        )

        log_effective_date=rail.PythonOperator(
            task_id='log_effective_date',
            python_callable= lambda: get_datestring_from_datetime_object(str(rail.result('foreach_department_schedule')['effectiveDate']['day']) + "/" +
                                        str(rail.result('foreach_department_schedule')['effectiveDate']['month']) + "/" +
                                        str(rail.result('foreach_department_schedule')['effectiveDate']['year']))
        )

        if_effective_date_lessthan_today=rail.IfOperator(
            task_id='if_effective_date_lessthan_today',
            test=lambda: bool(datetime.strptime(rail.result('log_effective_date'),'%d/%m/%Y') < datetime.strptime(
                            datetime.now().strftime('%d/%m/%Y'),'%d/%m/%Y')),
            yes_task="insertto_departmentschedule_list",
            no_task="if_effective_date_notequal_today",
        )

        insertto_departmentschedule_list=rail.SetVariableOperator(
            task_id='insertto_departmentschedule_list',
            append=True,
            name='{{ result("create_department_schedule_list").name }}',
            value={
                "uri": "{{ result('foreach_department_schedule').departmentGroup.uri }}",
                "effectivedate": "{{result('log_effective_date')}}",
                "name": "{{ result('foreach_department_schedule').departmentGroup.displayText }}"
            }
        )

        if_effective_date_notequal_today=rail.IfOperator(
            task_id='if_effective_date_notequal_today',
            test=lambda: bool(datetime.strptime(rail.result('log_effective_date'),'%d/%m/%Y') != datetime.strptime(
                            datetime.now().strftime('%d/%m/%Y'),'%d/%m/%Y')),
            yes_task="insertto_department_list",
            no_task="foreach_department_schedule_end",
        )

        insertto_department_list=rail.SetVariableOperator(
            task_id='insertto_department_list',
            append=True,
            name='{{ result("create_departmentlist").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('foreach_department_schedule').effectiveDate.year }}",
                    "month": "{{ result('foreach_department_schedule').effectiveDate.month }}",
                    "day": "{{ result('foreach_department_schedule').effectiveDate.day }}"
                },
                "departmentGroup": {
                    "uri": "{{ result('foreach_department_schedule').departmentGroup.uri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        foreach_department_schedule_end=rail.EmptyOperator(
            task_id='foreach_department_schedule_end',
        )

        if_departmentschedule_list_has_uri=rail.IfOperator(
            task_id='if_departmentschedule_list_has_uri',
            test=lambda: rail.get_dag_run_var('department schedule')[0]['uri'],
            yes_task="get_max_effective_date",
            no_task="if_current_department_not_present_or_notequal_request",
        )

        get_max_effective_date=rail.PythonOperator(
            task_id='get_max_effective_date',
            python_callable=lambda: get_max_date(rail.get_dag_run_var('department schedule'))
        )

        get_current_department_name=rail.PythonOperator(
            task_id='get_current_department_name',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.get_dag_run_var('department schedule'),'effectivedate',rail.result('get_max_effective_date'),'uri','')
        )

        if_current_department_not_present_or_notequal_request=rail.IfOperator(
            task_id='if_current_department_not_present_or_notequal_request',
            test=lambda dag_run: bool( (not rail.result('get_current_department_name')) or
                                    (rail.result('get_current_department_name')).lower() != dag_run.conf['departmenturi'] ),
            yes_task="if_request_department_uri_present",
            no_task="if_request_supervisor_present",
        )

        if_request_department_uri_present=rail.IfOperator(
            task_id='if_request_department_uri_present',
            test='''{{ dag_run.conf.departmenturi | matches('urn') }}''',
            yes_task="add_to_department_list",
            no_task="log_department_not_available",
        )

        add_to_department_list=rail.SetVariableOperator(
            task_id='add_to_department_list',
            append=True,
            name='{{ result("create_departmentlist").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('get_today_date').year }}",
                    "month": "{{ result('get_today_date').month }}",
                    "day": "{{ result('get_today_date').day }}"
                },
                "departmentGroup": {
                    "uri": "{{ dag_run.conf.departmenturi }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        log_department_schedule=rail.PythonOperator(
            task_id='log_department_schedule',
            python_callable= lambda: (json.dumps(rail.get_dag_run_var('departmentlist'))).replace(
                                'effectiveDate": {}','effectiveDate":null').replace('{"uri": "","parentUri": null,"name": null}','null').replace(
                                '{"parentUri": null,"uri": "","name": null}','null').replace('{"name": null,"parentUri": null,"uri": ""}','null').replace(
                                '{"uri": ""}','null')
        )

        put_department_group_schedule_for_user=rail.RepliconServiceOperator(
            task_id='put_department_group_schedule_for_user',
            endpoint="/services/DepartmentGroupService1.svc/PutDepartmentGroupScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": json.loads(rail.result('log_department_schedule'))
            }
        )

        log_department_group_updated=rail.SetVariableOperator(
            task_id='log_department_group_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "Agencies (Department group) updated"
            }
        )

        log_department_not_available=rail.SetVariableOperator(
            task_id='log_department_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
                "value": "Department not updated since {{ dag_run.conf.department }} not available/ is disabled in Replicon"
            }
        )

        if_request_supervisor_present=rail.IfOperator(
            task_id='if_request_supervisor_present',
            test='''{{ dag_run.conf.supervisor | is_truthy }}''',
            yes_task="get_user_id",
            no_task="if_request_timesheettemplate_present",
        )

        get_user_id=rail.PythonOperator(
            task_id='get_user_id',
            python_callable= lambda dag_run: dag_run.conf['employeeId'] if ('employee_id' in dag_run.conf['identifier']) else dag_run.conf['loginname']
        )

        if_supervisor_equals_userid=rail.IfOperator(
            task_id='if_supervisor_equals_userid',
            test=lambda dag_run: dag_run.conf['supervisor'] == rail.result('get_userid'),
            yes_task="log_supervisor_not_updated",
            no_task="search_supervisor_user_by_id",
        )

        log_supervisor_not_updated=rail.SetVariableOperator(
            task_id='log_supervisor_not_updated',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
                "value": "Supervisor not updated since Supervisor ID and user' ID are the same"
            }
        )

        search_supervisor_user_by_id=rail.RepliconServiceOperator(
            task_id='search_supervisor_user_by_id',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "100",
              "columnUris": [
                  "urn:replicon:user-list-column:login-name",
                  "urn:replicon:user-list-column:employee-id",
                  "urn:replicon:user-list-column:enabled"
              ],
              "sort": [],
              "filterExpression": {
                  "leftExpression": {
                      "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                  },
                  "operatorUri": "urn:replicon:filter-operator:text-search",
                  "rightExpression": {
                      "value": {
                          "text": "{{dag_run.conf.supervisor}}"
                      }
                  }
              }
            },
            data_handler=lambda response,dag_run: list(filter(lambda x: x['cells'][1]['textValue'] == dag_run.conf['supervisor'],response['rows']))
        )

        if_multiple_profiles_found=rail.IfOperator(
            task_id='if_multiple_profiles_found',
            test=lambda: bool(rail.result('search_supervisor_user_by_id') and len(rail.result('search_supervisor_user_by_id')) > 1),
            yes_task="log_multiple_profiles_found",
            no_task="get_supervisor_uri",
        )

        log_multiple_profiles_found=rail.SetVariableOperator(
            task_id='log_multiple_profiles_found',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
                "value": "Supervisor not assigned since the multiple profiles found with same Supervisor ID {{ dag_run.conf.supervisor }}"
            }
        )

        get_supervisor_uri=rail.PythonOperator(
            task_id='get_supervisor_uri',
            python_callable= lambda: rail.result('search_supervisor_user_by_id')[0]['cells'][0]['uri'] if rail.result('search_supervisor_user_by_id') and
                                rail.result('search_supervisor_user_by_id')[0]['cells'][0]['textValue'] else null
        )

        if_supervisor_uri_present=rail.IfOperator(
            task_id='if_supervisor_uri_present',
            test='''{{ result('get_supervisor_uri') | is_truthy }}''',
            yes_task="get_supervisor_status",
            no_task="log_supervisorschedule",
        )

        get_supervisor_status=rail.PythonOperator(
            task_id='get_supervisor_status',
            python_callable= lambda: rail.result('search_supervisor_user_by_id')[0]['cells'][2]['textValue']
        )

        log_supervisorschedule=rail.PythonOperator(
            task_id='log_supervisorschedule',
            python_callable= lambda:  json.dumps(rail.result('bulk_get_user')[0]['supervisorAssignmentSchedule'])
        )

        create_supervisor_schedule_list=rail.SetVariableOperator(
            task_id='create_supervisor_schedule_list',
            append=False,
            name='supervisor schedule',
            value=[]
        )

        if_supervisor_schedule_contains_urn=rail.IfOperator(
            task_id='if_supervisor_schedule_contains_urn',
            test='''{{ result('log_supervisorschedule') | matches('urn') }}''',
            yes_task="parse_supervisor_assignment_schedule",
            no_task="if_supervisor_schedule_list_has_uri",
        )

        parse_supervisor_assignment_schedule=rail.PythonOperator(
            task_id='parse_supervisor_assignment_schedule',
            python_callable= lambda: json.loads(rail.result('log_supervisorschedule'))
        )

        foreach_supervisor_schedule=rail.ForEachOperator(
            task_id='foreach_supervisor_schedule',
            items=lambda: rail.result('parse_supervisor_assignment_schedule'),
            start_task = 'if_effective_date_notpresent',
            end_task = 'foreach_supervisor_schedule_end'
        )

        if_effective_date_notpresent=rail.IfOperator(
            task_id='if_effective_date_notpresent',
            test='''{{ result('foreach_supervisor_schedule').effectiveDate | is_falsy }}''',
            yes_task="insertto_supervisor_schedule_list",
            no_task="get_effectivedate",
        )

        insertto_supervisor_schedule_list=rail.SetVariableOperator(
            task_id='insertto_supervisor_schedule_list',
            append=True,
            name='{{ result("create_supervisor_schedule_list").name }}',
            value={
                "loginname": "{{ result('foreach_supervisor_schedule').supervisor.user.loginName }}",
                "uri": "{{ result('foreach_supervisor_schedule').supervisor.user.uri }}",
                "effectivedate": "{{result('get_current_start_date')}}",
                "name": "{{ result('foreach_supervisor_schedule').supervisor.displayText }}"
            }
        )

        get_effectivedate=rail.PythonOperator(
            task_id='get_effectivedate',
            python_callable= lambda:  get_datestring_from_datetime_object(str(rail.result('foreach_supervisor_schedule')['effectiveDate']['day']) + "/" +
                                        str(rail.result('foreach_supervisor_schedule')['effectiveDate']['month']) + "/" +
                                        str(rail.result('foreach_supervisor_schedule')['effectiveDate']['year']))
        )

        if_effectivedate_lessthan_today=rail.IfOperator(
            task_id='if_effectivedate_lessthan_today',
            test=lambda: bool(datetime.strptime(rail.result('get_effectivedate'),'%d/%m/%Y') < datetime.strptime(
                            datetime.now().strftime('%d/%m/%Y'),'%d/%m/%Y')),
            yes_task="insert_to_supervisor_schedule_list",
            no_task="foreach_supervisor_schedule_end",
        )

        insert_to_supervisor_schedule_list=rail.SetVariableOperator(
            task_id='insert_to_supervisor_schedule_list',
            append=True,
            name='{{ result("create_supervisor_schedule_list").name }}',
            value={
                "loginname": "{{ result('foreach_supervisor_schedule').supervisor.user.loginName }}",
                "uri": "{{ result('foreach_supervisor_schedule').supervisor.user.uri }}",
                "effectivedate": "{{result('get_effectivedate')}}",
                "name": "{{ result('foreach_supervisor_schedule').supervisor.displayText }}"
            }
        )

        foreach_supervisor_schedule_end=rail.EmptyOperator(
            task_id='foreach_supervisor_schedule_end',
        )

        if_supervisor_schedule_list_has_uri=rail.IfOperator(
            task_id='if_supervisor_schedule_list_has_uri',
            test=lambda: rail.get_dag_run_var('supervisor schedule') and rail.get_dag_run_var('supervisor schedule')[0]['uri'],
            yes_task="get_maximum_effective_date",
            no_task="if_current_supervisor_not_present_or_notequal_request",
        )

        get_maximum_effective_date=rail.PythonOperator(
            task_id='get_maximum_effective_date',
            python_callable=lambda: get_max_date(rail.get_dag_run_var('supervisor schedule'))
        )

        get_current_supervisor_uri=rail.PythonOperator(
            task_id='get_current_supervisor_uri',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.get_dag_run_var('supervisor schedule'),'effectivedate',rail.result('get_maximum_effective_date'),'uri','')
        )

        if_current_supervisor_not_present_or_notequal_request=rail.IfOperator(
            task_id='if_current_supervisor_not_present_or_notequal_request',
            test=lambda : bool( (not rail.result('get_current_supervisor_uri')) or
                            (rail.result('get_current_supervisor_uri')).lower() != rail.result('get_supervisor_uri') ),
            yes_task="if_supervisor_uri_not_present",
            no_task="if_request_timesheettemplate_present",
        )

        if_supervisor_uri_not_present=rail.IfOperator(
            task_id='if_supervisor_uri_not_present',
            test='''{{ result('get_supervisor_uri') | is_falsy }}''',
            yes_task="add_entry_supervisor_assignment_queued",
            no_task="if_supervisor_status_equals_false",
        )

        if_supervisor_status_equals_false=rail.IfOperator(
            task_id='if_supervisor_status_equals_false',
            test='''{{ result('get_supervisor_status') == 'False' }}''',
            yes_task="add_entry_supervisor_assignment_queued",
            no_task="if_supervisoruri_present_and_status_equals_true",
        )

        add_entry_supervisor_assignment_queued=rail.WriteLogOperator(
            task_id='add_entry_supervisor_assignment_queued',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['callerjobid'],
                "username": dag_run.conf['employeeId'] if ('employee_id' in dag_run.conf['identifier']) else dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['supervisor'],
                "action": "update",
                "status": "queued",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_supervisoruri_present_and_status_equals_true=rail.IfOperator(
            task_id='if_supervisoruri_present_and_status_equals_true',
            test='''{{ result('get_supervisor_uri') | is_truthy  and result('get_supervisor_status') == 'True' }}''',
            yes_task="get_assigned_permission_sets_for_user",
            no_task="if_request_timesheettemplate_present",
        )

        get_assigned_permission_sets_for_user=rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('get_supervisor_uri') }}"
            }
        )

        get_supervision_permissionset=rail.PythonOperator(
            task_id='get_supervision_permissionset',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_assigned_permission_sets_for_user'),'policyUri','urn:replicon:policy:supervision','permissionSet.name','')
                                if rail.result('get_assigned_permission_sets_for_user')[0]['policyUri'] else null
        )

        if_supervision_permission_not_present=rail.IfOperator(
            task_id='if_supervision_permission_not_present',
            test='''{{ result('get_supervision_permissionset') | is_falsy }}''',
            yes_task="assign_permissionset_to_supervisor_user",
            no_task="update_supervisor_assignment",
        )

        assign_permissionset_to_supervisor_user=rail.RepliconServiceOperator(
            task_id='assign_permissionset_to_supervisor_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_supervisor_uri') }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        update_supervisor_assignment=rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('get_supervisor_uri') }}",
                "dateRange": {
                    "startDate": {
                    "year": "{{ result('get_today_date').year }}",
                    "month": "{{ result('get_today_date').month }}",
                    "day": "{{ result('get_today_date').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_supervisor_updated=rail.SetVariableOperator(
            task_id='log_supervisor_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
            "value": "Supervisor updated"
        }
        )

        if_request_timesheettemplate_present=rail.IfOperator(
            task_id='if_request_timesheettemplate_present',
            test='''{{ dag_run.conf.timesheettemplate | is_truthy }}''',
            yes_task="if_request_timesheettemplate_not_equal_current",
            no_task="if_request_schedule_present",
        )

        if_request_timesheettemplate_not_equal_current=rail.IfOperator(
            task_id='if_request_timesheettemplate_not_equal_current',
            test=lambda dag_run: bool(rail.result('bulk_get_user')[0]['timesheetTemplate']['name'] != dag_run.conf['timesheettemplate']),
            yes_task="get_all_policy_sets",
            no_task="if_request_schedule_present",
        )

        get_all_policy_sets=rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_required_timesheettemplate_uri=rail.PythonOperator(
            task_id='get_required_timesheettemplate_uri',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_policy_sets'),'name', dag_run.conf['timesheettemplate'],'uri','')
        )

        if_required_timesheettemplate_uri_present=rail.IfOperator(
            task_id='if_required_timesheettemplate_uri_present',
            test='''{{ result('get_required_timesheettemplate_uri') | is_truthy }}''',
            yes_task="assign_timesheettemplate_to_user",
            no_task="log_template_not_available",
        )

        assign_timesheettemplate_to_user=rail.RepliconServiceOperator(
            task_id='assign_timesheettemplate_to_user',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_required_timesheettemplate_uri') }}"
            }
        )

        log_timesheettemplate_updated=rail.SetVariableOperator(
            task_id='log_timesheettemplate_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "Timesheet template updated"
            }
        )

        log_template_not_available=rail.SetVariableOperator(
            task_id='log_template_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
                "value": "Timesheet template {{ dag_run.conf.timesheettemplate }} not available in Replicon"
            }
        )

        if_request_schedule_present=rail.IfOperator(
            task_id='if_request_schedule_present',
            test='''{{ dag_run.conf.schedule | is_truthy }}''',
            yes_task="create_office_schedule_list",
            no_task="add_log_for_the_user",
        )

        create_office_schedule_list=rail.SetVariableOperator(
            task_id='create_office_schedule_list',
            append=False,
            name='office schedule',
            value=[]
        )

        create_officeschedulelist_list=rail.SetVariableOperator(
            task_id='create_officeschedulelist_list',
            append=False,
            name='officeschedulelist',
            value=[]
        )

        log_office_schedule=rail.PythonOperator(
            task_id='log_office_schedule',
            python_callable= lambda: json.dumps(rail.result('bulk_get_user')[0]['schedulePolicies'])
        )

        if_office_schedule_contains_urn=rail.IfOperator(
            task_id='if_office_schedule_contains_urn',
            test='''{{ result('log_office_schedule') | matches('urn') }}''',
            yes_task="parse_office_schedule",
            no_task="if_office_schedule_list_has_uri",
        )

        parse_office_schedule=rail.PythonOperator(
            task_id='parse_office_schedule',
            python_callable= lambda: json.loads(rail.result('log_office_schedule'))
        )

        foreach_office_schedule=rail.ForEachOperator(
            task_id='foreach_office_schedule',
            items=lambda: rail.result('parse_office_schedule'),
            start_task = 'if_schedule_type_equals_shift',
            end_task = 'foreach_office_schedule_end'
        )

        if_schedule_type_equals_shift=rail.IfOperator(
            task_id='if_schedule_type_equals_shift',
            test='''{{ result('foreach_office_schedule').scheduleTypeUri == 'urn:replicon:schedule-type:shift' }}''',
            yes_task="if_effective_datenot_present",
            no_task="is_effective_date_not_present",
        )

        if_effective_datenot_present=rail.IfOperator(
            task_id='if_effective_datenot_present',
            test='''{{ result('foreach_office_schedule').effectiveDate | is_falsy }}''',
            yes_task="insertto_office_schedule_list",
            no_task="get_effective_date",
        )

        insertto_office_schedule_list=rail.SetVariableOperator(
            task_id='insertto_office_schedule_list',
            append=True,
            name='{{ result("create_office_schedule_list").name }}',
            value={
                "uri": "{{ result('foreach_office_schedule').scheduleTypeUri }}",
                "effectivedate": "{{result('get_current_start_date')}}",
                "name": "shift schedule"
            }
        )

        insertto_officeschedulelist_list=rail.SetVariableOperator(
            task_id='insertto_officeschedulelist_list',
            append=True,
            name='{{ result("create_officeschedulelist_list").name }}',
            value={
                "effectiveDate": {},
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": null
                    },
                    "scheduleTypeUri": "{{ result('foreach_office_schedule').scheduleTypeUri }}"
                }
            }
        )

        get_effective_date=rail.PythonOperator(
            task_id='get_effective_date',
            python_callable= lambda:  get_datestring_from_datetime_object(str(rail.result('foreach_office_schedule')['effectiveDate']['day']) + "/" +
                                        str(rail.result('foreach_office_schedule')['effectiveDate']['month']) + "/" +
                                        str(rail.result('foreach_office_schedule')['effectiveDate']['year']))
        )

        is_effective_date_less_than_today=rail.IfOperator(
            task_id='is_effective_date_less_than_today',
            test=lambda: bool(datetime.strptime(rail.result('get_effective_date'),'%d/%m/%Y') < datetime.strptime(
                            datetime.now().strftime('%d/%m/%Y'),'%d/%m/%Y')),
            yes_task="addto_officeschedule_list",
            no_task="is_effectivedate_not_equal_today",
        )

        addto_officeschedule_list=rail.SetVariableOperator(
            task_id='addto_officeschedule_list',
            append=True,
            name='{{ result("create_office_schedule_list").name }}',
            value={
                "uri": "{{ result('foreach_office_schedule').scheduleTypeUri }}",
                "effectivedate": "{{result('get_effective_date')}}",
                "name": "shift schedule"
            }
        )

        is_effectivedate_not_equal_today=rail.IfOperator(
            task_id='is_effectivedate_not_equal_today',
            test=lambda: bool(datetime.strptime(rail.result('get_effective_date'),'%d/%m/%Y') != datetime.strptime(
                            datetime.now().strftime('%d/%m/%Y'),'%d/%m/%Y')),
            yes_task="insert_to_officeschedulelist_list",
            no_task="foreach_office_schedule_end",
        )

        insert_to_officeschedulelist_list=rail.SetVariableOperator(
            task_id='insert_to_officeschedulelist_list',
            append=True,
            name='{{ result("create_officeschedulelist_list").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('foreach_office_schedule').effectiveDate.year }}",
                    "month": "{{ result('foreach_office_schedule').effectiveDate.month }}",
                    "day": "{{ result('foreach_office_schedule').effectiveDate.day }}"
                },
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": null
                    },
                    "scheduleTypeUri": "{{ result('foreach_office_schedule').scheduleTypeUri }}"
                }
            }
        )

        is_effective_date_not_present=rail.IfOperator(
            task_id='is_effective_date_not_present',
            test='''{{ result('foreach_office_schedule').effectiveDate | is_falsy }}''',
            yes_task="add_to_officeschedule_list",
            no_task="geteffective_date",
        )

        add_to_officeschedule_list=rail.SetVariableOperator(
            task_id='add_to_officeschedule_list',
            append=True,
            name='{{ result("create_office_schedule_list").name }}',
            value={
                "uri": "{{ result('foreach_office_schedule').officeSchedule.uri }}",
                "effectivedate": "{{result('get_effective_date')}}",
                "name": "{{ result('foreach_office_schedule').officeSchedule.displayText }}"
            }
        )

        add_to_officeschedulelist_list=rail.SetVariableOperator(
            task_id='add_to_officeschedulelist_list',
            append=True,
            name='{{ result("create_officeschedulelist_list").name }}',
            value={
                "effectiveDate": {},
                "schedulePolicy": {
                    "officeScheduleUri": "{{result('foreach_office_schedule').officeSchedule.uri}}",
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": "{{result('foreach_office_schedule').officeSchedule.uri}}",
                        "name": null
                    },
                    "scheduleTypeUri": "{{ result('foreach_office_schedule').scheduleTypeUri }}"
                }
            }
        )

        geteffective_date=rail.PythonOperator(
            task_id='geteffective_date',
            python_callable= lambda:  get_datestring_from_datetime_object(str(rail.result('foreach_office_schedule')['effectiveDate']['day']) + "/" +
                                        str(rail.result('foreach_office_schedule')['effectiveDate']['month']) + "/" +
                                        str(rail.result('foreach_office_schedule')['effectiveDate']['year']))
        )

        is_effectivedate_less_than_today=rail.IfOperator(
            task_id='is_effectivedate_less_than_today',
            test=lambda: bool(datetime.strptime(rail.result('geteffective_date'),'%d/%m/%Y') < datetime.strptime(
                            datetime.now().strftime('%d/%m/%Y'),'%d/%m/%Y')),
            yes_task="addto_office_schedule_list",
            no_task="is_effectivedate_notequal_today",
        )

        addto_office_schedule_list=rail.SetVariableOperator(
            task_id='addto_office_schedule_list',
            append=True,
            name='{{ result("create_office_schedule_list").name }}',
            value={
                "uri": "{{ result('foreach_office_schedule').officeSchedule.uri }}",
                "effectivedate": "{{result('geteffective_date')}}",
                "name": "{{ result('foreach_office_schedule').officeSchedule.displayText }}"
            }
        )

        is_effectivedate_notequal_today=rail.IfOperator(
            task_id='is_effectivedate_notequal_today',
            test=lambda: bool(datetime.strptime(rail.result('geteffective_date'),'%d/%m/%Y') != datetime.strptime(
                            datetime.now().strftime('%d/%m/%Y'),'%d/%m/%Y')),
            yes_task="add_to_officeschedule_list_list",
            no_task="foreach_office_schedule_end",
        )

        add_to_officeschedule_list_list=rail.SetVariableOperator(
            task_id='add_to_officeschedule_list_list',
            append=True,
            name='{{ result("create_officeschedulelist_list").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('foreach_office_schedule').effectiveDate.year }}",
                    "month": "{{ result('foreach_office_schedule').effectiveDate.month }}",
                    "day": "{{ result('foreach_office_schedule').effectiveDate.day }}"
                },
                "schedulePolicy": {
                    "officeScheduleUri": "{{ result('foreach_office_schedule').officeSchedule.uri }}",
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": "{{ result('foreach_office_schedule').officeSchedule.uri }}",
                        "name": null
                    },
                    "scheduleTypeUri": "{{ result('foreach_office_schedule').scheduleTypeUri }}"
                }
            }
        )

        foreach_office_schedule_end=rail.EmptyOperator(
            task_id='foreach_office_schedule_end',
        )

        if_office_schedule_list_has_uri=rail.IfOperator(
            task_id='if_office_schedule_list_has_uri',
            test=lambda: rail.get_dag_run_var('office schedule')[0]['uri'],
            yes_task="log_maximum_effectivedate",
            no_task="if_current_office_schedule_not_present_or_unequal_request",
        )

        log_maximum_effectivedate=rail.PythonOperator(
            task_id='log_maximum_effectivedate',
            python_callable= lambda: get_max_date(rail.get_dag_run_var('office schedule'))
        )

        get_current_office_schedule_name=rail.PythonOperator(
            task_id='get_current_office_schedule_name',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.get_dag_run_var('office schedule'),'effectivedate', rail.result('log_maximum_effectivedate'),'name','').lower()
        )

        if_current_office_schedule_not_present_or_unequal_request=rail.IfOperator(
            task_id='if_current_office_schedule_not_present_or_unequal_request',
            test=lambda dag_run: bool( (not rail.result('get_current_office_schedule_name'))
                                    or (rail.result('get_current_office_schedule_name')).lower() != dag_run.conf['schedule'].lower() ),
            yes_task="add_to_office_schedulelist_list",
            no_task="add_log_for_the_user",
        )

        add_to_office_schedulelist_list=rail.SetVariableOperator(
            task_id='add_to_office_schedulelist_list',
            append=True,
            name='{{ result("create_officeschedulelist_list").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('get_today_date').year }}",
                    "month": "{{ result('get_today_date').month }}",
                    "day": "{{ result('get_today_date').day }}"
                },
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": "{{ dag_run.conf.schedule }}",
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": "{{ dag_run.conf.schedule }}"
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                }
            }
        )

        log_required_office_schedule=rail.PythonOperator(
            task_id='log_required_office_schedule',
            python_callable= lambda: json.dumps(rail.get_dag_run_var('officeschedulelist')).replace('effectiveDate": {}','effectiveDate":null').replace(
                                '{"uri": "","parentUri": null,"name": null}','null').replace('{"parentUri": null,"uri": "","name": null}','null').replace(
                                '{"name": null,"parentUri": null,"uri": ""}','null').replace('{"uri": ""}','null')
        )

        put_office_schedule_for_user=rail.RepliconServiceOperator(
            task_id='put_office_schedule_for_user',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run:{
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('log_required_office_schedule')
            }
        )

        log_office_schedule_updated=rail.SetVariableOperator(
            task_id='log_office_schedule_updated',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "value": "Office schedule updated"
            }
        )

        add_log_for_the_user=rail.WriteLogOperator(
            task_id='add_log_for_the_user',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity=lambda: "Exception" if rail.get_dag_run_var('Exception') else ( "Success" if rail.get_dag_run_var('logs') else "Skipped"),
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['loginname'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'] ,
                "status": "Exception" if rail.get_dag_run_var('Exception') else ( "Success" if rail.get_dag_run_var('logs') else "Skipped"),
                "action": "Update",
                "details": ("Partially updated|" + ";".join([ exception['value'] for exception in rail.get_dag_run_var('Exception')]) +
                            ";".join([ log['value'] for log in rail.get_dag_run_var('logs')])) if rail.get_dag_run_var('Exception') else
                            ( ";".join([ log['value'] for log in rail.get_dag_run_var('logs')]) if rail.get_dag_run_var('logs') else
                            "No change to the user record in Replicon"),
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.loginname }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "status": "Error",
                "action": "Update",
                "details": "{{get_error_message()}}",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_logs_list
        create_logs_list >> create_exception_list >> create_updatetype_variable >> bulk_get_user >> get_all_products_available_for_user_assignment
        get_all_products_available_for_user_assignment >> get_license_list >> log_custom_field_values >> log_eestatus_value >> get_current_start_date
        get_current_start_date >> get_today_date >> get_timeoff_schedule >> if_request_startdate_present

        if_request_startdate_present >> rail.Label('Yes')  >> get_hire_date >> if_startdate_present_and_not_equal_request
        if_startdate_present_and_not_equal_request >> rail.Label(
            'Yes')  >> update_start_date >> log_newhiredate >> log_startdate_updated >> if_user_currently_not_enabled
        if_startdate_present_and_not_equal_request >> rail.Label('No') >> if_user_currently_not_enabled
        if_request_startdate_present >> rail.Label('No') >> if_user_currently_not_enabled

        if_user_currently_not_enabled >> rail.Label('Yes')  >> enable_login >> remove_end_date >> put_product_assignments_for_user >> log_user_re_enabled
        log_user_re_enabled >> if_user_currently_not_enabled_and_enddate_present
        if_user_currently_not_enabled >> rail.Label('No') >> if_user_currently_not_enabled_and_enddate_present

        if_user_currently_not_enabled_and_enddate_present >> rail.Label('Yes')  >> log_user_already_disabled >> catch_and_log_error
        if_user_currently_not_enabled_and_enddate_present >> rail.Label('No') >> if_request_enddate_present

        if_request_enddate_present >> rail.Label('Yes')  >> get_current_enddate >> if_current_enddate_present_and_not_equal_request
        if_current_enddate_present_and_not_equal_request >> rail.Label('Yes')  >> get_termination_date >> if_newhire_date_present
        if_newhire_date_present >> rail.Label('Yes')  >> update_hire_and_termination_date >> log_enddate_updated
        if_newhire_date_present >> rail.Label('No') >> update_termination_date >> log_enddate_updated >> if_user_currenlty_enabled_and_enddate_present
        if_current_enddate_present_and_not_equal_request >> rail.Label('No') >> if_user_currenlty_enabled_and_enddate_present
        if_request_enddate_present >> rail.Label('No') >> if_user_currenlty_enabled_and_enddate_present

        if_user_currenlty_enabled_and_enddate_present >> rail.Label('Yes')  >> disable_login >> log_user_disabled >> catch_and_log_error
        if_user_currenlty_enabled_and_enddate_present >> rail.Label('No') >> if_firstname_present_not_equal_current

        if_firstname_present_not_equal_current >> rail.Label('Yes')  >> update_firstname >> log_firstname_updated >> if_lastname_present_not_equal_current
        if_firstname_present_not_equal_current >> rail.Label('No') >> if_lastname_present_not_equal_current

        if_lastname_present_not_equal_current >> rail.Label('Yes')  >> update_last_name >> log_lastname_updated >> if_email_present_and_not_equal_current
        if_lastname_present_not_equal_current >> rail.Label('No') >> if_email_present_and_not_equal_current

        if_email_present_and_not_equal_current >> rail.Label('Yes')  >> update_email >> log_email_updated >> log_current_customfield1
        if_email_present_and_not_equal_current >> rail.Label('No') >> log_current_customfield1 >> if_request_customfield1_present_not_equal_current

        if_request_customfield1_present_not_equal_current >> rail.Label(
            'Yes')  >> get_all_custom_field_drop_down_options >> get_required_customfield1_dropdown_option >> if_reqeuired_customfield1_dropdown_present
        if_reqeuired_customfield1_dropdown_present >> rail.Label('Yes')  >> update_dropdown_value >> log_profile_status_updated >> if_employeetype_present
        if_reqeuired_customfield1_dropdown_present >> rail.Label('No') >> log_customfield1_not_available >> if_employeetype_present
        if_request_customfield1_present_not_equal_current >> rail.Label('No') >> if_employeetype_present

        if_employeetype_present >> rail.Label(
            'Yes')  >> create_employeetype_schedule_list >> create_employee_typelist >> get_employeetypeschedule >> if_employeetypeschedule_has_urn
        if_employeetypeschedule_has_urn >> rail.Label('Yes')  >> parse_employeetypeschedule >> foreach_schedule >> if_effectivedate_not_present
        if_effectivedate_not_present >> rail.Label('Yes')  >> insert_to_employeetypeschedule_list >> insert_to_employeetype_list >> foreach_schedule_end
        if_effectivedate_not_present >> rail.Label('No') >> log_effectivedate >> if_effectivedate_less_than_today
        if_effectivedate_less_than_today >> rail.Label('Yes')  >> insertto_employeetypeschedule_list >> if_effectivedate_not_equal_today
        if_effectivedate_less_than_today >> rail.Label('No') >> if_effectivedate_not_equal_today
        if_effectivedate_not_equal_today >> rail.Label('Yes')  >> insertto_employeetype_list >> foreach_schedule_end
        if_effectivedate_not_equal_today >> rail.Label('No') >> foreach_schedule_end
        foreach_schedule >> foreach_schedule_end >> if_employeetypeschedule_list_has_uri
        if_employeetypeschedule_has_urn >> rail.Label('No') >> if_employeetypeschedule_list_has_uri
        if_employeetypeschedule_list_has_uri >> rail.Label(
            'Yes')  >> get_maximum_effectivedate >> get_current_employee_type_name >> if_current_employeetype_not_present_or_notequal_request
        if_employeetypeschedule_list_has_uri >> rail.Label('No') >> if_current_employeetype_not_present_or_notequal_request
        if_current_employeetype_not_present_or_notequal_request >> rail.Label('Yes')  >> if_request_employeetype_uri_present
        if_request_employeetype_uri_present >> rail.Label('Yes')  >> log_to_employeetype_list >> log_employee_type_schedule
        log_employee_type_schedule >> put_employee_type_group_schedule_for_user >> log_employeetype_updated >> if_request_department_present
        if_request_employeetype_uri_present >> rail.Label('No') >> log_employeetype_not_available >> if_request_department_present
        if_current_employeetype_not_present_or_notequal_request >> rail.Label('No') >> if_request_department_present
        if_employeetype_present >> rail.Label('No') >> if_request_department_present

        if_request_department_present >> rail.Label(
            'Yes') >> create_department_schedule_list >> create_departmentlist >> log_departmentschedule >> if_departmentschedule_contains_urn
        if_departmentschedule_contains_urn >> rail.Label(
            'Yes') >> parse_departmentgroup_schedule >> foreach_department_schedule >> if_effective_date_not_present
        if_effective_date_not_present >> rail.Label(
            'Yes')  >> insert_to_departmentschedule_list >> insert_to_department_list >> foreach_department_schedule_end
        if_effective_date_not_present >> rail.Label('No') >> log_effective_date >> if_effective_date_lessthan_today
        if_effective_date_lessthan_today >> rail.Label('Yes')  >> insertto_departmentschedule_list >> if_effective_date_notequal_today
        if_effective_date_lessthan_today >> rail.Label('No') >> if_effective_date_notequal_today
        if_effective_date_notequal_today >> rail.Label('Yes')  >> insertto_department_list >> foreach_department_schedule_end
        if_effective_date_notequal_today >> rail.Label('No') >> foreach_department_schedule_end
        foreach_department_schedule >> foreach_department_schedule_end >> if_departmentschedule_list_has_uri
        if_departmentschedule_contains_urn >> rail.Label('No') >> if_departmentschedule_list_has_uri
        if_departmentschedule_list_has_uri >> rail.Label(
            'Yes')  >> get_max_effective_date >> get_current_department_name >> if_current_department_not_present_or_notequal_request
        if_departmentschedule_list_has_uri >> rail.Label('No') >> if_current_department_not_present_or_notequal_request
        if_current_department_not_present_or_notequal_request >> rail.Label('Yes')  >> if_request_department_uri_present
        if_request_department_uri_present >> rail.Label('Yes')  >> add_to_department_list >> log_department_schedule >> put_department_group_schedule_for_user
        put_department_group_schedule_for_user >> log_department_group_updated >> if_request_supervisor_present
        if_request_department_uri_present >> rail.Label('No') >> log_department_not_available >> if_request_supervisor_present
        if_current_department_not_present_or_notequal_request >> rail.Label('No') >> if_request_supervisor_present
        if_request_department_present >> rail.Label('No') >> if_request_supervisor_present

        if_request_supervisor_present >> rail.Label('Yes')  >> get_user_id >> if_supervisor_equals_userid
        if_supervisor_equals_userid >> rail.Label('Yes')  >> log_supervisor_not_updated >> if_request_timesheettemplate_present
        if_supervisor_equals_userid >> rail.Label('No') >> search_supervisor_user_by_id >> if_multiple_profiles_found
        if_multiple_profiles_found >> rail.Label('Yes')  >> log_multiple_profiles_found >> if_request_timesheettemplate_present
        if_multiple_profiles_found >> rail.Label('No') >> get_supervisor_uri >> if_supervisor_uri_present
        if_supervisor_uri_present >> rail.Label('Yes')  >> get_supervisor_status >> log_supervisorschedule
        if_supervisor_uri_present >> rail.Label('No') >> log_supervisorschedule >> create_supervisor_schedule_list >> if_supervisor_schedule_contains_urn
        if_supervisor_schedule_contains_urn >> rail.Label(
            'Yes')  >> parse_supervisor_assignment_schedule >> foreach_supervisor_schedule >> if_effective_date_notpresent
        if_effective_date_notpresent >> rail.Label('Yes')  >> insertto_supervisor_schedule_list >> foreach_supervisor_schedule_end
        if_effective_date_notpresent >> rail.Label('No') >> get_effectivedate >> if_effectivedate_lessthan_today
        if_effectivedate_lessthan_today >> rail.Label('Yes')  >> insert_to_supervisor_schedule_list >> foreach_supervisor_schedule_end
        if_effectivedate_lessthan_today >> rail.Label('No') >> foreach_supervisor_schedule_end
        foreach_supervisor_schedule >> foreach_supervisor_schedule_end >> if_supervisor_schedule_list_has_uri
        if_supervisor_schedule_contains_urn >> rail.Label('No') >> if_supervisor_schedule_list_has_uri
        if_supervisor_schedule_list_has_uri >> rail.Label(
            'Yes')  >> get_maximum_effective_date >> get_current_supervisor_uri >> if_current_supervisor_not_present_or_notequal_request
        if_supervisor_schedule_list_has_uri >> rail.Label('No') >> if_current_supervisor_not_present_or_notequal_request
        if_current_supervisor_not_present_or_notequal_request >> rail.Label('Yes')  >> if_supervisor_uri_not_present
        if_supervisor_uri_not_present >> rail.Label('Yes')  >> add_entry_supervisor_assignment_queued >> if_supervisoruri_present_and_status_equals_true
        if_supervisor_uri_not_present >> rail.Label('No') >> if_supervisor_status_equals_false
        if_supervisor_status_equals_false >> rail.Label('Yes')  >> add_entry_supervisor_assignment_queued >> if_supervisoruri_present_and_status_equals_true
        if_supervisor_status_equals_false >> rail.Label('No') >> if_supervisoruri_present_and_status_equals_true
        if_supervisoruri_present_and_status_equals_true >> rail.Label(
            'Yes')  >> get_assigned_permission_sets_for_user >> get_supervision_permissionset >> if_supervision_permission_not_present
        if_supervision_permission_not_present >> rail.Label('Yes')  >> assign_permissionset_to_supervisor_user >> update_supervisor_assignment
        if_supervision_permission_not_present >> rail.Label(
            'No') >> update_supervisor_assignment >> log_supervisor_updated >> if_request_timesheettemplate_present
        if_supervisoruri_present_and_status_equals_true >> rail.Label('No') >> if_request_timesheettemplate_present
        if_current_supervisor_not_present_or_notequal_request >> rail.Label('No') >> if_request_timesheettemplate_present
        if_request_supervisor_present >> rail.Label('No') >> if_request_timesheettemplate_present

        if_request_timesheettemplate_present >> rail.Label('Yes')  >> if_request_timesheettemplate_not_equal_current
        if_request_timesheettemplate_not_equal_current >> rail.Label(
            'Yes') >> get_all_policy_sets >> get_required_timesheettemplate_uri >> if_required_timesheettemplate_uri_present
        if_required_timesheettemplate_uri_present >> rail.Label(
            'Yes') >> assign_timesheettemplate_to_user >> log_timesheettemplate_updated >> if_request_schedule_present
        if_required_timesheettemplate_uri_present >> rail.Label('No') >> log_template_not_available
        if_request_timesheettemplate_not_equal_current >> rail.Label('No') >> if_request_schedule_present
        if_request_timesheettemplate_present >> rail.Label('No') >> if_request_schedule_present

        if_request_schedule_present >> rail.Label(
            'Yes')  >> create_office_schedule_list >> create_officeschedulelist_list >> log_office_schedule >> if_office_schedule_contains_urn
        if_office_schedule_contains_urn >> rail.Label('Yes')  >> parse_office_schedule >> foreach_office_schedule >> if_schedule_type_equals_shift
        if_schedule_type_equals_shift >> rail.Label('Yes') >> if_effective_datenot_present
        if_effective_datenot_present >> rail.Label('Yes')  >> insertto_office_schedule_list >> insertto_officeschedulelist_list >> foreach_office_schedule_end
        if_effective_datenot_present >> rail.Label('No') >> get_effective_date >> is_effective_date_less_than_today
        is_effective_date_less_than_today >> rail.Label('Yes')  >> addto_officeschedule_list >> is_effectivedate_not_equal_today
        is_effective_date_less_than_today >> rail.Label('No') >> is_effectivedate_not_equal_today
        is_effectivedate_not_equal_today >> rail.Label('Yes')  >> insert_to_officeschedulelist_list >> foreach_office_schedule_end
        is_effectivedate_not_equal_today >> rail.Label('No') >> foreach_office_schedule_end
        if_schedule_type_equals_shift >> rail.Label('No') >> is_effective_date_not_present >> rail.Label(
            'Yes') >> add_to_officeschedule_list >> add_to_officeschedulelist_list >> foreach_office_schedule_end
        is_effective_date_not_present >> rail.Label('No') >> geteffective_date >> is_effectivedate_less_than_today
        is_effectivedate_less_than_today >> rail.Label('Yes')  >> addto_office_schedule_list >> is_effectivedate_notequal_today
        is_effectivedate_less_than_today >> rail.Label('No') >> is_effectivedate_notequal_today
        is_effectivedate_notequal_today >> rail.Label('Yes')  >> add_to_officeschedule_list_list >> foreach_office_schedule_end
        is_effectivedate_notequal_today >> rail.Label('No') >> foreach_office_schedule_end
        foreach_office_schedule >> foreach_office_schedule_end >> if_office_schedule_list_has_uri
        if_office_schedule_contains_urn >> rail.Label('No') >> if_office_schedule_list_has_uri
        if_office_schedule_list_has_uri >> rail.Label(
            'Yes')  >> log_maximum_effectivedate >> get_current_office_schedule_name >> if_current_office_schedule_not_present_or_unequal_request
        if_office_schedule_list_has_uri >> rail.Label('No') >> if_current_office_schedule_not_present_or_unequal_request
        if_current_office_schedule_not_present_or_unequal_request >> rail.Label('Yes')  >> add_to_office_schedulelist_list >> log_required_office_schedule
        log_required_office_schedule >> put_office_schedule_for_user >> log_office_schedule_updated >> add_log_for_the_user
        if_current_office_schedule_not_present_or_unequal_request >> rail.Label('No') >> add_log_for_the_user
        if_request_schedule_present >> rail.Label('No') >> add_log_for_the_user >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
