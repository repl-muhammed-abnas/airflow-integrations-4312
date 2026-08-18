
from datetime import timedelta
import rail
from rail.lib.ecid import get_dagrun_ecid
from impervainc.user_sync.utils import python_callable, request_payload, response_filter

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_usersync_update,
        description=f'impervainc User Sync Update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        mapper_depedant_field_trigger = rail.SetVariableOperator(
            task_id='mapper_depedant_field_trigger',
            name='mapperdepedantfieldtrigger',
            value='no'
        )

        timeoff_trigger = rail.SetVariableOperator(
            task_id='timeoff_trigger',
            name='timeofftrigger',
            value='no'
        )

        warnings = rail.SetVariableOperator(
            task_id='warnings',
            name='warnings',
            value='NA'
        )

        get_user_details= rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                    "uri": "{{ dag_run.conf.useruri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_status_is_disabled_and_isloginenable_true = rail.IfOperator(
            task_id='if_status_is_disabled_and_isloginenable_true',
            test='''{{ dag_run.conf.status | matches('Disabled') and result('get_user_details')[0].securityConfiguration.isLoginEnabled | is_truthy }}''',
            yes_task="trigger_disable_user_dag",
            no_task="if_status_is_disabled_and_isloginenable_false",
        )

        trigger_disable_user_dag = rail.TriggerDagRunOperator(
            task_id='trigger_disable_user_dag',
            trigger_dag_id=config.imperva_usersync_disable_user,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.disable_user_payload
        )

        if_status_is_disabled_and_isloginenable_false = rail.IfOperator(
            task_id='if_status_is_disabled_and_isloginenable_false',
            test='''{{ dag_run.conf.status | matches('Disabled') and result('get_user_details')[0].securityConfiguration.isLoginEnabled | is_falsy }}''',
            yes_task="imperva_user_import_logs_add_entry",
            no_task="if_status_is_enable_and_isloginenable_false",
        )

        imperva_user_import_logs_add_entry = rail.WriteLogOperator(
            task_id='imperva_user_import_logs_add_entry',
            message="na",
            log= "{{dag_run.conf.user_sync_log}}",
            severity="Skipped",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": get_dagrun_ecid(dag_run),
                "loginname": dag_run.conf['Username'],
                "employeeid": dag_run.conf['Employee_ID'],
                "status": "Skipped",
                "reason": "User is already disabled in Replicon",
                "action": "Update",
                "country": dag_run.conf['Work_Address_Country']
            }
        )

        if_status_is_enable_and_isloginenable_false = rail.IfOperator(
            task_id='if_status_is_enable_and_isloginenable_false',
            test='''{{ dag_run.conf.status | matches('Enabled') and result('get_user_details')[0].securityConfiguration.isLoginEnabled | is_falsy }}''',
            yes_task="enable_login_re_enable_user",
            no_task="if_day_is_present_termination_day_is_not",
        )

        enable_login_re_enable_user = rail.RepliconServiceOperator(
            task_id='enable_login_re_enable_user',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        set_sso_authentication_user = rail.RepliconServiceOperator(
            task_id='set_sso_authentication_user',
            endpoint='/services/SecurityService1.svc/SetSSOAuthenticationForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'loginName': '{{ dag_run.conf.Username }}'
            }
        )

        user_rehired_18 = rail.PythonOperator(
            task_id='user_rehired_18',
            python_callable=lambda: 'User rehired'
        )

        update_startdate_end_remove_enddate = rail.RepliconServiceOperator(
            task_id='update_startdate_end_remove_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": python_callable.get_originalhiredate(dag_run)
                }
            }
        )

        timeoff_trigger_update = rail.SetVariableOperator(
            task_id='timeoff_trigger_update',
            name='{{ result("timeoff_trigger").name }}',
            value='rehire'
        )

        mapper_depedant_field_trigger_update = rail.SetVariableOperator(
            task_id='mapper_depedant_field_trigger_update',
            name='{{ result("mapper_depedant_field_trigger").name }}',
            value='yes'
        )

        if_day_is_present_termination_day_is_not = rail.IfOperator(
            task_id='if_day_is_present_termination_day_is_not',
            test='''{{ dag_run.conf.status | matches('Disabled') and result('get_user_details')[0].securityConfiguration.isLoginEnabled | is_falsy }}''',
            yes_task="update_startdate_end_remove_enddate_29",
            no_task="get_required_user_customfields",
        )

        update_startdate_end_remove_enddate_29 = rail.RepliconServiceOperator(
            task_id='update_startdate_end_remove_enddate_29',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": python_callable.get_originalhiredate(dag_run)
                }
            }
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "urn:replicon:object-type:user"
            }
        )

        current_value_for_imperva_organization = rail.PythonOperator(
            task_id='current_value_for_imperva_organization',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_details")[0]['userDetails']['customFieldValues'], "customField.displayText",
                "Imperva Organization", "text")
        )

        if_imperva_organization_does_not_equal = rail.IfOperator(
            task_id='if_imperva_organization_does_not_equal',
            test=lambda dag_run: rail.result('current_value_for_imperva_organization') != dag_run.conf['Imperva_Organization'],
            yes_task="get_required_custom_field_uri_for_imperva_org",
            no_task="get_current_value_for_time_type",
        )

        get_required_custom_field_uri_for_imperva_org = rail.PythonOperator(
            task_id='get_required_custom_field_uri_for_imperva_org',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_required_user_customfields"), "displayText",
                "Imperva Organization", "uri") if bool(dag_run.conf['Imperva_Organization']) else null
        )

        if_imperva_organization_uri_present = rail.IfOperator(
            task_id='if_imperva_organization_uri_present',
            test=lambda: bool(rail.result('get_required_custom_field_uri_for_imperva_org')),
            yes_task="get_enabled_customfield_dropdown_options",
            no_task="get_current_value_for_time_type",
        )

        get_enabled_customfield_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_imperva_org') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Imperva_Organization'], 'uri', '')
        )

        if_customfield_dropdown_options_uri_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_uri_present',
            test=lambda: bool(rail.result('get_enabled_customfield_dropdown_options')),
            yes_task="update_dropdown_value_udf",
            no_task="get_current_value_for_time_type",
        )

        update_dropdown_value_udf = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_imperva_org') }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_customfield_dropdown_options') }}"
            }
        )

        mapper_depedant_field_trigger_update_39 = rail.SetVariableOperator(
            task_id='mapper_depedant_field_trigger_update_39',
            name='{{ result("mapper_depedant_field_trigger").name }}',
            value='yes'
        )

        get_current_value_for_time_type = rail.PythonOperator(
            task_id='get_current_value_for_time_type',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_details")[0]['userDetails']['customFieldValues'], "customField.displayText",
                "Time Type", "text")
        )

        if_time_type_does_not_equal = rail.IfOperator(
            task_id='if_time_type_does_not_equal',
            test=lambda dag_run: rail.result('get_current_value_for_time_type') != dag_run.conf['Time_Type'],
            yes_task="get_required_custom_field_uri_for_time_type",
            no_task="get_current_value_for_payrate_type",
        )

        get_required_custom_field_uri_for_time_type = rail.PythonOperator(
            task_id='get_required_custom_field_uri_for_time_type',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_required_user_customfields"), "displayText",
                "Time Type", "uri") if bool(dag_run.conf['Time_Type']) else null
        )

        if_time_type_uri_present = rail.IfOperator(
            task_id='if_time_type_uri_present',
            test=lambda: bool(rail.result('get_required_custom_field_uri_for_time_type')),
            yes_task="get_enabled_customfield_dropdown_options_44",
            no_task="get_current_value_for_payrate_type",
        )

        get_enabled_customfield_dropdown_options_44 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_44',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_time_type') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Time_Type'], 'uri', '')
        )

        if_get_enabled_customfield_dropdown_options_44_present = rail.IfOperator(
            task_id='if_get_enabled_customfield_dropdown_options_44_present',
            test=lambda: bool(rail.result('get_enabled_customfield_dropdown_options_44')),
            yes_task="update_dropdown_value_udf_47",
            no_task="get_current_value_for_payrate_type",
        )

        update_dropdown_value_udf_47 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_47',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_time_type') }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_customfield_dropdown_options_44') }}"
            }
        )

        mapper_depedant_field_trigger_update_48 = rail.SetVariableOperator(
            task_id='mapper_depedant_field_trigger_update_48',
            name='{{ result("mapper_depedant_field_trigger").name }}',
            value='yes'
        )

        get_current_value_for_payrate_type = rail.PythonOperator(
            task_id='get_current_value_for_payrate_type',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_details")[0]['userDetails']['customFieldValues'], "customField.displayText",
                "PayRate Type", "text")
        )

        if_payrate_type_does_not_equal = rail.IfOperator(
            task_id='if_payrate_type_does_not_equal',
            test=lambda dag_run: rail.result('get_current_value_for_payrate_type') != dag_run.conf['Pay_Rate_Type'],
            yes_task="get_required_custom_field_uri_for_payrate_type",
            no_task="if_department_uri_does_not_equal",
        )

        get_required_custom_field_uri_for_payrate_type = rail.PythonOperator(
            task_id='get_required_custom_field_uri_for_payrate_type',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_required_user_customfields"), "displayText",
                "PayRate Type", "uri") if bool(dag_run.conf['Pay_Rate_Type']) else null
        )

        if_payrate_type_present = rail.IfOperator(
            task_id='if_payrate_type_present',
            test=lambda: bool(rail.result('get_required_custom_field_uri_for_payrate_type')),
            yes_task="get_enabled_customfield_dropdown_options_53",
            no_task="if_department_uri_does_not_equal",
        )

        get_enabled_customfield_dropdown_options_53 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_53',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_payrate_type') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Pay_Rate_Type'], 'uri', '')
        )

        if_get_enabled_customfield_dropdown_options_53_present = rail.IfOperator(
            task_id='if_get_enabled_customfield_dropdown_options_53_present',
            test=lambda: bool(rail.result('get_enabled_customfield_dropdown_options_53')),
            yes_task="update_dropdown_value_udf_56",
            no_task="if_department_uri_does_not_equal",
        )

        update_dropdown_value_udf_56 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_56',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_payrate_type') }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_customfield_dropdown_options_53') }}"
            }
        )

        mapper_depedant_field_trigger_update_57 = rail.SetVariableOperator(
            task_id='mapper_depedant_field_trigger_update_57',
            name='{{ result("mapper_depedant_field_trigger").name }}',
            value='yes'
        )

        if_department_uri_does_not_equal = rail.IfOperator(
            task_id='if_department_uri_does_not_equal',
            test=lambda dag_run: bool(rail.result('get_user_details')[0]['userDetails']['department'] and \
                rail.result('get_user_details')[0]['userDetails']['department']['uri'] != dag_run.conf['departmenturi']),
            yes_task="update_department_for_user",
            no_task="get_all_permissionsets",
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/departmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ dag_run.conf.departmenturi }}"
            }
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        payrule_name_derived = rail.PythonOperator(
            task_id='payrule_name_derived',
            python_callable=lambda dag_run: python_callable.payrule_name_derived(dag_run, config.imperva_payrule_placeholder)
        )

        payrule_schedule_list = rail.PythonOperator(
            task_id='payrule_schedule_list',
            python_callable=python_callable.payrule_schedule_list
        )

        max_payrule_schedule_from_list = rail.PythonOperator(
            task_id='max_payrule_schedule_from_list',
            python_callable=python_callable.max_payrule_schedule_from_list
        )

        payrule_name = rail.PythonOperator(
            task_id='payrule_name',
            python_callable=python_callable.payrule_name
        )

        if_payrule_names_does_not_equal = rail.IfOperator(
            task_id='if_payrule_names_does_not_equal',
            test=lambda: bool(
                rail.result('payrule_name') != rail.result('payrule_name_derived')),
            yes_task="if_timesheet_displaytext_present",
            no_task="get_current_value_for_country_iso_code",
        )

        if_timesheet_displaytext_present = rail.IfOperator(
            task_id='if_timesheet_displaytext_present',
            test=lambda: bool(
                rail.result('get_user_details')[0]['timesheetTemplate'] and
                rail.result('get_user_details')[0]['timesheetTemplate']['displayText']),
            yes_task="get_timesheet_for_date2_77",
            no_task="get_current_value_for_country_iso_code",
        )

        get_timesheet_for_date2_77 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_77',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                "date": python_callable.get_originalhiredate(dag_run),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        if_timesheet_uri_present = rail.IfOperator(
            task_id='if_timesheet_uri_present',
            test=lambda: bool(
                rail.result('get_timesheet_for_date2_77') and rail.result('get_timesheet_for_date2_77')['timesheet']['uri']),
            yes_task="get_timesheet_details",
            no_task="get_current_value_for_country_iso_code",
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2_77').timesheet.uri }}"
            }
        )

        get_required_payrule_script = rail.RepliconServiceOperator(
            task_id='get_required_payrule_script',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        create_payrule_list = rail.PythonOperator(
            task_id='create_payrule_list',
            python_callable=python_callable.create_payrule_list
        )

        put_payrule_script_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_payrule_script_assignment_schedule',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                "scheduleEntries": rail.result('create_payrule_list')
            }
        )

        get_current_value_for_country_iso_code = rail.PythonOperator(
            task_id='get_current_value_for_country_iso_code',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_details")[0]['userDetails']['customFieldValues'], "customField.displayText",
                "Country ISO Code", "text", '')
        )

        if_country_iso_code_does_not_equal = rail.IfOperator(
            task_id='if_country_iso_code_does_not_equal',
            test=lambda dag_run: bool(
                rail.result('get_current_value_for_country_iso_code') != dag_run.conf['Country_ISO_Code']),
            yes_task="get_required_custom_field_uri_for_country_iso_code",
            no_task="if_user_rehired_or_holiday_calendar_update_present",
        )

        get_required_custom_field_uri_for_country_iso_code = rail.PythonOperator(
            task_id='get_required_custom_field_uri_for_country_iso_code',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_required_user_customfields"), "displayText",
                "Country ISO Code", "uri") if bool(dag_run.conf['Country_ISO_Code']) else null
        )

        if_uri_for_country_iso_code_present = rail.IfOperator(
            task_id='if_uri_for_country_iso_code_present',
            test=lambda: bool(
                rail.result('get_required_custom_field_uri_for_country_iso_code')),
            yes_task="get_enabled_customfield_dropdown_options_95",
            no_task="if_user_rehired_or_holiday_calendar_update_present",
        )

        get_enabled_customfield_dropdown_options_95 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_95',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_country_iso_code') }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Country_ISO_Code'], 'uri', '')
        )

        if_customfield_dropdown_options_95_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_95_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_95')),
            yes_task="update_dropdown_value_udf_96",
            no_task="if_user_rehired_or_holiday_calendar_update_present",
        )

        update_dropdown_value_udf_96 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_96',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_country_iso_code') }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_customfield_dropdown_options_95') }}"
            }
        )

        country_code_updated_99 = rail.PythonOperator(
            task_id='country_code_updated_99',
            python_callable=lambda: 'Country code updated'
        )

        timeoff_trigger_update_100 = rail.SetVariableOperator(
            task_id='timeoff_trigger_update_100',
            name='{{ result("timeoff_trigger").name }}',
            value='update'
        )

        mapper_depedant_field_trigger_update_101 = rail.SetVariableOperator(
            task_id='mapper_depedant_field_trigger_update_101',
            name='{{ result("mapper_depedant_field_trigger").name }}',
            value='yes'
        )

        if_user_rehired_or_holiday_calendar_update_present = rail.IfOperator(
            task_id='if_user_rehired_or_holiday_calendar_update_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_95')),
            yes_task="get_required_custom_field_uri_for_country_name",
            no_task="get_mapper_depedant_field_trigger",
        )

        get_required_custom_field_uri_for_country_name = rail.PythonOperator(
            task_id='get_required_custom_field_uri_for_country_name',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("get_required_user_customfields"), "displayText",
                "Company Name (For Israel Team)", "uri") if bool(dag_run.conf['Country_ISO_Code']) else null
        )

        get_enabled_customfield_dropdown_options_104 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_104',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_country_name') }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', "-", 'uri', '')
        )

        if_customfield_dropdown_options_104_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_104_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_104')),
            yes_task="update_dropdown_value_udf_107",
            no_task="get_all_holiday_calendars",
        )

        update_dropdown_value_udf_107 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_107',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_custom_field_uri_for_country_name') }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_customfield_dropdown_options_104') }}"
            }
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        create_holiday_calendar_list = rail.PythonOperator(
            task_id='create_holiday_calendar_list',
            python_callable=lambda: python_callable.create_holiday_calendar_list(
                rail.result('get_all_holiday_calendars')
            )
        )

        holiday_calendar_uri = rail.PythonOperator(
            task_id='holiday_calendar_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result("create_holiday_calendar_list"), "compare",
                dag_run.conf['Country_ISO_Code'], "uri")
        )

        if_holiday_calendar_present = rail.IfOperator(
            task_id='if_holiday_calendar_present',
            test=lambda: bool(rail.result('holiday_calendar_uri')),
            yes_task="update_holiday_calendar_for_user",
            no_task="get_mapper_depedant_field_trigger"
        )

        update_holiday_calendar_for_user = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('holiday_calendar_uri') }}"
            }
        )

        get_mapper_depedant_field_trigger = rail.GetVariableOperator(
            task_id='get_mapper_depedant_field_trigger',
            name="{{ result('mapper_depedant_field_trigger').name }}"
        )

        if_mapper_depedant_field_variable_is_yes = rail.IfOperator(
            task_id='if_mapper_depedant_field_variable_is_yes',
            test="{{ result('get_mapper_depedant_field_trigger').value == 'yes' }}",
            yes_task="get_all_employee_type_details",
            no_task="if_manager_present_and_equals_to_1"
        )

        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails"
        )

        search_employee_value = rail.PythonOperator(
            task_id='search_employee_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Employee Type')
        )

        get_employee_type_uri = rail.PythonOperator(
            task_id='get_employee_type_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_employee_type_details"), "displayText",
                rail.result("search_employee_value"), "uri")
        )

        if_employee_type_uri_present = rail.IfOperator(
            task_id='if_employee_type_uri_present',
            test=lambda: bool(rail.result('get_employee_type_uri')),
            yes_task="update_employeetype_for_user",
            no_task="search_schedule_value"
        )

        update_employeetype_for_user = rail.RepliconServiceOperator(
            task_id='update_employeetype_for_user',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('get_employee_type_uri') }}"
            }
        )

        search_schedule_value = rail.PythonOperator(
            task_id='search_schedule_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Schedule')
        )

        get_existing_schedule_types = rail.PythonOperator(
            task_id='get_existing_schedule_types',
            python_callable=lambda: rail.result("get_user_details")[0]['schedulePolicies']
        )

        create_schedule_list_to_compare = rail.PythonOperator(
            task_id='create_schedule_list_to_compare',
            python_callable=python_callable.create_schedule_list_to_compare
        )

        get_max_effective_date_from_schedule_list = rail.PythonOperator(
            task_id='get_max_effective_date_from_schedule_list',
            python_callable=python_callable.get_max_effective_date_from_schedule_list
        )

        current_schedule_name = rail.PythonOperator(
            task_id='current_schedule_name',
            python_callable=python_callable.current_schedule_name
        )

        def schedule_value_present_does_not_equal():
            return bool(rail.result('search_schedule_value') and \
            rail.result('search_schedule_value') != rail.result('current_schedule_name'))

        if_schedule_name_present_and_does_not_equal = rail.IfOperator(
            task_id='if_schedule_name_present_and_does_not_equal',
            test=schedule_value_present_does_not_equal,
            yes_task="create_schedule_list_134",
            no_task="search_timesheet_period_type_value"
        )

        create_schedule_list_134 = rail.PythonOperator(
            task_id='create_schedule_list_134',
            python_callable=python_callable.create_schedule_list_134
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_new_office_schedule_uri = rail.PythonOperator(
            task_id='get_new_office_schedule_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_office_schedules"), "displayText",
                rail.result("search_schedule_value"), "uri")
        )

        if_new_office_schedule_uri_present = rail.IfOperator(
            task_id='if_new_office_schedule_uri_present',
            test=lambda: bool(rail.result('get_new_office_schedule_uri')),
            yes_task="add_item_to_schedule_list_144",
            no_task="search_timesheet_period_type_value"
        )

        add_item_to_schedule_list_144 = rail.PythonOperator(
            task_id='add_item_to_schedule_list_144',
            python_callable=python_callable.add_item_to_schedule_list_144
        )

        put_schedule_policy_user = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_user',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                "scheduleEntries": rail.result('add_item_to_schedule_list_144')
            }
        )

        search_timesheet_period_type_value = rail.PythonOperator(
            task_id='search_timesheet_period_type_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Timesheet period')
        )

        timesheetperiod_variable = rail.SetVariableOperator(
            task_id='timesheetperiod_variable',
            name='timesheetperiod',
            value=''
        )

        if_timesheet_period_type_value_present = rail.IfOperator(
            task_id='if_timesheet_period_type_value_present',
            test=lambda: bool(rail.result('search_timesheet_period_type_value')),
            yes_task="put_employeetype_timesheetperiod_uri",
            no_task="search_timesheet_approval_path_value"
        )

        put_employeetype_timesheetperiod_uri = rail.RepliconServiceOperator(
            task_id='put_employeetype_timesheetperiod_uri',
            endpoint='/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'timesheetPeriodTypeUri': rail.result('search_timesheet_period_type_value').split("|")[-1]
            }
        )

        search_timesheet_approval_path_value = rail.PythonOperator(
            task_id='search_timesheet_approval_path_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Timesheet Approval Path')
        )

        if_timesheet_approval_path_value_present = rail.IfOperator(
            task_id='if_timesheet_approval_path_value_present',
            test=lambda: bool(rail.result('search_timesheet_approval_path_value')),
            yes_task="if_correct_approval_path_is_not_assigned",
            no_task="search_timeoff_approval_path_value"
        )

        if_correct_approval_path_is_not_assigned = rail.IfOperator(
            task_id='if_correct_approval_path_is_not_assigned',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_details'), "timesheetApprovalPath.displayText",
                rail.result('search_timesheet_approval_path_value'), "timesheetApprovalPath.uri", ""
            )),
            yes_task="get_timesheet_approval_path_uri",
            no_task="search_timeoff_approval_path_value"
        )

        get_timesheet_approval_path_uri = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_path_uri',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('search_timesheet_approval_path_value'), 'uri', '')
        )

        if_timesheet_approval_path_uri_present = rail.IfOperator(
            task_id='if_timesheet_approval_path_uri_present',
            test=lambda: bool(rail.result('get_timesheet_approval_path_uri')),
            yes_task="update_timesheet_approvalpath_user",
            no_task="search_timeoff_approval_path_value"
        )

        update_timesheet_approvalpath_user = rail.RepliconServiceOperator(
            task_id='update_timesheet_approvalpath_user',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_timesheet_approval_path_uri') }}"
            }
        )

        search_timeoff_approval_path_value = rail.PythonOperator(
            task_id='search_timeoff_approval_path_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Time off Approval Path')
        )

        if_timeoff_approval_path_value_present = rail.IfOperator(
            task_id='if_timeoff_approval_path_value_present',
            test=lambda: bool(rail.result('search_timeoff_approval_path_value')),
            yes_task="if_correct_timeoff_approvalpath_not_assigned",
            no_task="get_all_policy_sets"
        )

        if_correct_timeoff_approvalpath_not_assigned = rail.IfOperator(
            task_id='if_correct_timeoff_approvalpath_not_assigned',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_details'), "timeOffApprovalPath.displayText",
                rail.result('search_timeoff_approval_path_value'), "timeOffApprovalPath.uri", ""
            )),
            yes_task="get_timeoff_approval_path_uri",
            no_task="get_all_policy_sets"
        )

        get_timeoff_approval_path_uri = rail.RepliconServiceOperator(
            task_id='get_timeoff_approval_path_uri',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('search_timeoff_approval_path_value'), 'uri', '')
        )

        if_timeoff_approval_path_uri_present = rail.IfOperator(
            task_id='if_timeoff_approval_path_uri_present',
            test=lambda: bool(rail.result('get_timeoff_approval_path_uri')),
            yes_task="update_timeoff_approvalpath_user",
            no_task="get_all_policy_sets"
        )

        update_timeoff_approvalpath_user = rail.RepliconServiceOperator(
            task_id='update_timeoff_approvalpath_user',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_timeoff_approval_path_uri') }}"
            }
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets'
        )

        search_timesheet_template_value = rail.PythonOperator(
            task_id='search_timesheet_template_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Timesheet Template')
        )

        if_timesheet_template_false_and_uri_true = rail.IfOperator(
            task_id='if_timesheet_template_false_and_uri_true',
            test=lambda: bool(not rail.result('search_timesheet_template_value') and \
                            rail.result('get_user_details')[0]['timesheetTemplate'] and \
                            rail.result('get_user_details')[0]['timesheetTemplate']['uri']),
            yes_task="remove_policysettouser_timesheettemplate",
            no_task="if_correct_timesheettemplate_not_assigned"
        )

        remove_policysettouser_timesheettemplate = rail.RepliconServiceOperator(
            task_id = "remove_policysettouser_timesheettemplate",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_user_details')[0].timesheetTemplate.uri }}" 
                }
        )

        if_correct_timesheettemplate_not_assigned = rail.IfOperator(
            task_id='if_correct_timesheettemplate_not_assigned',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_details'), "timesheetTemplate.displayText",
                rail.result('search_timesheet_template_value'), "timesheetTemplate.uri", ""
            )),
            yes_task="get_timesheet_template_uri",
            no_task="search_timeoff_template_value"
        )

        get_timesheet_template_uri = rail.PythonOperator(
            task_id='get_timesheet_template_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_policy_sets'), "displayText",
                rail.result('search_timesheet_template_value'), "uri", ""
            )
        )

        if_timesheet_template_uri_present = rail.IfOperator(
            task_id='if_timesheet_template_uri_present',
            test=lambda: bool(rail.result('get_timesheet_template_uri')),
            yes_task="assign_timesheet_template",
            no_task="search_timeoff_template_value"
        )

        assign_timesheet_template = rail.RepliconServiceOperator(
            task_id='assign_timesheet_template',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policySetUri": rail.result('get_timesheet_template_uri')
            }
        )

        search_timeoff_template_value = rail.PythonOperator(
            task_id='search_timeoff_template_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Time off Template')
        )

        if_timeoff_template_false_and_uri_true = rail.IfOperator(
            task_id='if_timeoff_template_false_and_uri_true',
            test=lambda: bool(not rail.result('search_timeoff_template_value') and \
                                rail.result('get_user_details')[0]['timeOffTemplate'] and \
                                rail.result('get_user_details')[0]['timeOffTemplate']['uri']),
            yes_task="remove_policysettouser_timeofftemplate",
            no_task="if_correct_timeofftemplate_not_assigned"
        )

        remove_policysettouser_timeofftemplate = rail.RepliconServiceOperator(
            task_id = "remove_policysettouser_timeofftemplate",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_user_details')[0].timeOffTemplate.uri }}" 
                }
        )

        if_correct_timeofftemplate_not_assigned = rail.IfOperator(
            task_id='if_correct_timeofftemplate_not_assigned',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_details'), "timeOffTemplate.displayText",
                rail.result('search_timeoff_template_value'), "timeOffTemplate.uri", ""
            )),
            yes_task="if_timeoff_template_uri_present",
            no_task="search_entry_policy_value"
        )

        if_timeoff_template_uri_present = rail.IfOperator(
            task_id='if_timeoff_template_uri_present',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_policy_sets'), "displayText",
                rail.result('search_timeoff_template_value'), "uri", ""
            )),
            yes_task="assign_timeoff_template",
            no_task="search_entry_policy_value"
        )

        assign_timeoff_template = rail.RepliconServiceOperator(
            task_id='assign_timeoff_template',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policySetUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_policy_sets'), "displayText",
                    rail.result('search_timeoff_template_value'), "uri")
            }
        )

        search_entry_policy_value = rail.PythonOperator(
            task_id='search_entry_policy_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Punch Entry Policy')
        )

        get_assigned_policysets = rail.RepliconServiceOperator(
            task_id='get_assigned_policysets',
            endpoint='/services/PolicySetService1.svc/GetAssignedPolicySetsForUser',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            },
            data_handler=lambda response: {
                "punchentry_policy_uri":rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', "urn:replicon:policy:time-punch", 'policySet', {}).get('uri', ''),
                "punchentry_policy_name":rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', "urn:replicon:policy:time-punch", 'policySet', {}).get('displayText', '')
            }
        )

        if_entry_policy_false_and_uri_true = rail.IfOperator(
            task_id='if_entry_policy_false_and_uri_true',
            test=lambda: bool(not rail.result('search_entry_policy_value') and \
                              rail.result('get_assigned_policysets')['punchentry_policy_uri']),
            yes_task="remove_policysettouser_punchentry_policy",
            no_task="if_punchentry_policy_name_does_not_equal"
        )

        remove_policysettouser_punchentry_policy = rail.RepliconServiceOperator(
            task_id = "remove_policysettouser_punchentry_policy",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_assigned_policysets').punchentry_policy_uri }}" 
                }
        )

        if_punchentry_policy_name_does_not_equal = rail.IfOperator(
            task_id='if_punchentry_policy_name_does_not_equal',
            test=lambda: bool(rail.result('search_entry_policy_value') != \
                              rail.result('get_assigned_policysets')['punchentry_policy_name']),
            yes_task="if_punchentry_policy_present",
            no_task="search_work_week_value"
        )

        if_punchentry_policy_present = rail.IfOperator(
            task_id='if_punchentry_policy_present',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_policy_sets'), "displayText",
                rail.result('search_entry_policy_value'), "uri", ""
            )),
            yes_task="assign_punch_entry_policy",
            no_task="search_work_week_value"
        )

        assign_punch_entry_policy = rail.RepliconServiceOperator(
            task_id='assign_punch_entry_policy',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policySetUri": rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_policy_sets'), "displayText",
                rail.result('search_entry_policy_value'), "uri")
            }
        )

        search_work_week_value = rail.PythonOperator(
            task_id='search_work_week_value',
            python_callable=lambda dag_run: python_callable.search_entries_in_imperva_mapper_table(dag_run, 'Work Week')
        )

        get_workweek_value_uri = rail.PythonOperator(
            task_id='get_workweek_value_uri',
            python_callable=lambda: rail.result(
                "search_work_week_value").split('|')[-1].strip() if rail.result(
                "search_work_week_value") else None
        )

        if_workweek_present_and_does_not_equal = rail.IfOperator(
            task_id='if_workweek_present_and_does_not_equal',
            test=lambda: bool(rail.result('get_workweek_value_uri') and \
                              rail.result('get_user_details')[0]['userDetails']['workWeekStartDay'] and \
                              rail.result('get_workweek_value_uri') != \
                              rail.result('get_user_details')[0]['userDetails']['workWeekStartDay']['uri']),
            yes_task="update_workweek_startday",
            no_task="if_manager_present_and_equals_to_1"
        )

        update_workweek_startday = rail.RepliconServiceOperator(
            task_id='update_workweek_startday',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                "dayOfWeekUri": "{{ result('get_workweek_value_uri') }}"
            }
        )

        if_manager_present_and_equals_to_1 = rail.IfOperator(
            task_id='if_manager_present_and_equals_to_1',
            test=lambda dag_run: bool(dag_run.conf['isManager'] and dag_run.conf['isManager']==1),
            yes_task="get_permission_sets_from_user_details",
            no_task="payrule_name_derived_does_not_equal"
        )

        get_permission_sets_from_user_details = rail.PythonOperator(
            task_id='get_permission_sets_from_user_details',
            python_callable=python_callable.get_permission_sets_from_user_details,
            op_args=[config]
        )

        if_end_user_or_supervisor_does_not_present = rail.IfOperator(
            task_id='if_end_user_or_supervisor_does_not_present',
            test=lambda: bool(not rail.result('get_permission_sets_from_user_details')['end_user_with_report_accesss'] or \
                            not rail.result('get_permission_sets_from_user_details')['imperva_supervisor']),
            yes_task="get_permission_sets_from_permissionsets",
            no_task="payrule_name_derived_does_not_equal"
        )

        get_permission_sets_from_permissionsets = rail.PythonOperator(
            task_id='get_permission_sets_from_permissionsets',
            python_callable=python_callable.get_permission_sets_from_permissionsets,
            op_args=[config]
        )

        add_missing_permissionsets_to_user = rail.RepliconServiceOperator(
            task_id='add_missing_permissionsets_to_user',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'permissionSetUri': "{{ result('get_permission_sets_from_permissionsets').end_user_with_report_accesss }}"
            }
        )

        add_missing_permissionsets_to_user_216 = rail.RepliconServiceOperator(
            task_id='add_missing_permissionsets_to_user_216',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'permissionSetUri': "{{ result('get_permission_sets_from_permissionsets').imperva_supervisor }}"
            }
        )

        payrule_name_derived_does_not_equal = rail.IfOperator(
            task_id='payrule_name_derived_does_not_equal',
            test=lambda: bool(rail.result('payrule_name_derived') != rail.result('payrule_name')),
            yes_task="if_timesheet_template_name_not_present",
            no_task="if_primary_workemail_does_not_equal"
        )

        if_timesheet_template_name_not_present = rail.IfOperator(
            task_id='if_timesheet_template_name_not_present',
            test=lambda: not bool(rail.result('get_user_details')[0]['timesheetTemplate'] and \
                                  rail.result('get_user_details')[0]['timesheetTemplate']['displayText']),
            yes_task="if_timesheet_template_name_uri_present",
            no_task="if_primary_workemail_does_not_equal"
        )

        if_timesheet_template_name_uri_present = rail.IfOperator(
            task_id='if_timesheet_template_name_uri_present',
            test=lambda: bool(rail.result('get_timesheet_template_uri')),
            yes_task="get_timesheet_for_date2_221",
            no_task="get_all_payrule_scripts"
        )

        get_timesheet_for_date2_221 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_221',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                "date": python_callable.get_originalhiredate(dag_run),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        if_timesheet_uri_present_222 = rail.IfOperator(
            task_id='if_timesheet_uri_present_222',
            test=lambda: bool(rail.result('get_timesheet_for_date2_221') and rail.result('get_timesheet_for_date2_221')['timesheet']['uri']),
            yes_task="get_timesheet_details_223",
            no_task="get_all_payrule_scripts"
        )

        get_timesheet_details_223 = rail.RepliconServiceOperator(
            task_id='get_timesheet_details_223',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2_221').timesheet.uri }}"
            }
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id='get_all_payrule_scripts',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        create_payrule_list_225 = rail.PythonOperator(
            task_id='create_payrule_list_225',
            python_callable=python_callable.create_payrule_list_225
        )

        put_payrule_script_assignment_schedule_234 = rail.RepliconServiceOperator(
            task_id='put_payrule_script_assignment_schedule_234',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                "scheduleEntries": rail.result('create_payrule_list_225')
            }
        )

        if_primary_workemail_does_not_equal = rail.IfOperator(
            task_id='if_primary_workemail_does_not_equal',
            test=python_callable.primary_workemail_does_not_equal,
            yes_task="update_email_236",
            no_task="if_firstname_does_not_equal"
        )

        update_email_236 = rail.RepliconServiceOperator(
            task_id='update_email_236',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.primaryWorkEmail }}"
            }
        )

        if_firstname_does_not_equal = rail.IfOperator(
            task_id='if_firstname_does_not_equal',
            test=python_callable.firstname_does_not_equal,
            yes_task="update_firstname_238",
            no_task="if_lastname_does_not_equal"
        )

        update_firstname_238 = rail.RepliconServiceOperator(
            task_id='update_firstname_238',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.Legal_First_Name }}"
            }
        )

        if_lastname_does_not_equal = rail.IfOperator(
            task_id='if_lastname_does_not_equal',
            test=python_callable.lastname_does_not_equal,
            yes_task="update_lastname_240",
            no_task="if_hourly_pay_and_currency_present"
        )

        update_lastname_240 = rail.RepliconServiceOperator(
            task_id='update_lastname_240',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.Legal_Last_Name }}"
            }
        )

        if_hourly_pay_and_currency_present = rail.IfOperator(
            task_id='if_hourly_pay_and_currency_present',
            test="{{dag_run.conf.Hourly_Pay | is_truthy and dag_run.conf.Currency | is_truthy}}",
            yes_task="hourly_rate_schedule_list",
            no_task="if_original_hire_date_present"
        )

        hourly_rate_schedule_list = rail.PythonOperator(
            task_id='hourly_rate_schedule_list',
            python_callable=python_callable.hourly_rate_schedule_list
        )

        get_max_hourly_rate_schedule_list = rail.PythonOperator(
            task_id='get_max_hourly_rate_schedule_list',
            python_callable=python_callable.max_hourly_rate_schedule_list
        )

        get_hourly_rate_amount_currency = rail.PythonOperator(
            task_id='get_hourly_rate_amount_currency',
            python_callable=python_callable.hourly_rate_amount_currency
        )

        if_amount_or_currency_does_not_equal = rail.IfOperator(
            task_id='if_amount_or_currency_does_not_equal',
            test=python_callable.amount_or_currency_does_not_equal,
            yes_task="get_currency_uri",
            no_task="if_original_hire_date_present"
        )

        get_currency_uri = rail.RepliconServiceOperator(
            task_id="get_currency_uri",
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'symbol', dag_run.conf['Currency'], 'uri')
        )

        if_currency_uri_present = rail.IfOperator(
            task_id='if_currency_uri_present',
            test=python_callable.amount_or_currency_does_not_equal,
            yes_task="update_user_payroll",
            no_task="update_variable_warnings"
        )

        update_user_payroll = rail.RepliconServiceOperator(
            task_id='update_user_payroll',
            endpoint='/services/PayrollService1.svc/UpdateUserPayrollRateScheduleOverDateRange',
            data=request_payload.update_user_payroll,
        )

        update_variable_warnings = rail.SetVariableOperator(
            task_id='update_variable_warnings',
            name='{{ result("warnings").name }}',
            value='Pay rate not updated since the currency is not present in Replicon'
        )

        if_original_hire_date_present = rail.IfOperator(
            task_id='if_original_hire_date_present',
            test=lambda dag_run: bool(dag_run.conf['Original_Hire_Date']),
            yes_task="if_original_hire_date_does_not_equal",
            no_task="get_current_value_for_costcenter"
        )

        if_original_hire_date_does_not_equal = rail.IfOperator(
            task_id='if_original_hire_date_does_not_equal',
            test=lambda dag_run: python_callable.get_current_value_from_customfield('Original Hire Date') != dag_run.conf['Original_Hire_Date'],
            yes_task="if_original_hire_date_uri_present",
            no_task="get_current_value_for_costcenter"
        )

        if_original_hire_date_uri_present = rail.IfOperator(
            task_id='if_original_hire_date_uri_present',
            test=lambda: bool(python_callable.original_hire_date_uri()),
            yes_task="update_original_hire_date_udf",
            no_task="get_current_value_for_costcenter"
        )

        update_original_hire_date_udf = rail.RepliconServiceOperator(
            task_id='update_original_hire_date_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.original_hire_date_uri(),
                "value": python_callable.get_originalhiredate(dag_run)
            }
        )

        get_current_value_for_costcenter = rail.PythonOperator(
            task_id='get_current_value_for_costcenter',
            python_callable=python_callable.get_current_value_from_customfield,
            op_args=['Cost Center - ID']
        )

        if_costcenter_doest_equal_and_customfield_uri_present = rail.IfOperator(
            task_id='if_costcenter_doest_equal_and_customfield_uri_present',
            test=lambda dag_run: bool(python_callable.costcenter_doest_equal_and_customfield_uri(dag_run)),
            yes_task="update_costcenter_udf",
            no_task="if_jobcode_present"
        )

        update_costcenter_udf = rail.RepliconServiceOperator(
            task_id='update_costcenter_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.costcenter_doest_equal_and_customfield_uri(dag_run),
                "value": dag_run.conf['Cost_Center_ID']
            }
        )

        if_jobcode_present = rail.IfOperator(
            task_id='if_jobcode_present',
            test=lambda dag_run: bool(python_callable.jobcode_present(dag_run)),
            yes_task="update_jobcode_udf",
            no_task="get_current_value_for_workertype"
        )

        update_jobcode_udf = rail.RepliconServiceOperator(
            task_id='update_jobcode_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.jobcode_present(dag_run),
                "value": dag_run.conf['Job_Code']
            }
        )

        get_current_value_for_workertype = rail.PythonOperator(
            task_id='get_current_value_for_workertype',
            python_callable=python_callable.get_current_value_from_customfield,
            op_args=['Imperva Worker Type']
        )

        if_workertype_does_not_equal_and_customfield_uri_present = rail.IfOperator(
            task_id='if_workertype_does_not_equal_and_customfield_uri_present',
            test=lambda dag_run: bool(python_callable.workertype_does_not_equal_and_customfield_uri_present(dag_run)),
            yes_task="get_enabled_customfield_dropdown_options_282",
            no_task="get_current_value_for_employeetype"
        )

        get_enabled_customfield_dropdown_options_282 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_282',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": python_callable.workertype_does_not_equal_and_customfield_uri_present(dag_run)
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Imperva_Worker_Type'], 'uri', '')
        )

        if_customfield_dropdown_options_284_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_284_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_282')),
            yes_task="update_dropdown_value_udf_285",
            no_task="get_current_value_for_employeetype",
        )

        update_dropdown_value_udf_285 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_285',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.workertype_does_not_equal_and_customfield_uri_present(dag_run),
                "customFieldDropDownOptionUri": rail.result('get_enabled_customfield_dropdown_options_282')
            }
        )

        get_current_value_for_employeetype = rail.PythonOperator(
            task_id='get_current_value_for_employeetype',
            python_callable=python_callable.get_current_value_from_customfield,
            op_args=['Imperva Employee Type']
        )

        if_employeetype_does_not_equal_and_customfield_uri_present = rail.IfOperator(
            task_id='if_employeetype_does_not_equal_and_customfield_uri_present',
            test=lambda dag_run: bool(python_callable.employeetype_does_not_equal_and_customfield_uri_present(dag_run)),
            yes_task="get_enabled_customfield_dropdown_options_290",
            no_task="get_current_value_for_workcountry"
        )

        get_enabled_customfield_dropdown_options_290 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_290',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": python_callable.employeetype_does_not_equal_and_customfield_uri_present(dag_run)
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Imperva_Employee_Type'], 'uri', '')
        )

        if_customfield_dropdown_options_292_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_292_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_290')),
            yes_task="update_dropdown_value_udf_293",
            no_task="get_current_value_for_workcountry",
        )

        update_dropdown_value_udf_293 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_293',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.employeetype_does_not_equal_and_customfield_uri_present(dag_run),
                "customFieldDropDownOptionUri": rail.result('get_enabled_customfield_dropdown_options_290')
            }
        )

        get_current_value_for_workcountry = rail.PythonOperator(
            task_id='get_current_value_for_workcountry',
            python_callable=python_callable.get_current_value_from_customfield,
            op_args=['Work Country']
        )

        if_workcountry_does_not_equal_and_customfield_uri_present = rail.IfOperator(
            task_id='if_workcountry_does_not_equal_and_customfield_uri_present',
            test=lambda dag_run: bool(python_callable.workcountry_does_not_equal_and_customfield_uri_present(dag_run)),
            yes_task="get_enabled_customfield_dropdown_options_298",
            no_task="get_current_value_for_workstate"
        )

        get_enabled_customfield_dropdown_options_298 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_298',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": python_callable.workcountry_does_not_equal_and_customfield_uri_present(dag_run)
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Work_Address_Country'], 'uri', '')
        )

        if_customfield_dropdown_options_300_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_300_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_298')),
            yes_task="update_dropdown_value_udf_301",
            no_task="get_current_value_for_workstate",
        )

        update_dropdown_value_udf_301 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_301',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.workcountry_does_not_equal_and_customfield_uri_present(dag_run),
                "customFieldDropDownOptionUri": rail.result('get_enabled_customfield_dropdown_options_298')
            }
        )

        get_current_value_for_workstate = rail.PythonOperator(
            task_id='get_current_value_for_workstate',
            python_callable=python_callable.get_current_value_from_customfield,
            op_args=['Work State']
        )

        if_workstate_does_not_equal_and_customfield_uri_present = rail.IfOperator(
            task_id='if_workstate_does_not_equal_and_customfield_uri_present',
            test=lambda dag_run: bool(python_callable.workstate_does_not_equal_and_customfield_uri_present(dag_run)),
            yes_task="get_enabled_customfield_dropdown_options_306",
            no_task="get_current_value_for_state_iso_code"
        )

        get_enabled_customfield_dropdown_options_306 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_306',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": python_callable.workstate_does_not_equal_and_customfield_uri_present(dag_run)
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Work_Address_State_Province'], 'uri', '')
        )

        if_customfield_dropdown_options_308_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_308_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_306')),
            yes_task="update_dropdown_value_udf_309",
            no_task="get_current_value_for_state_iso_code",
        )

        update_dropdown_value_udf_309 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_309',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.workstate_does_not_equal_and_customfield_uri_present(dag_run),
                "customFieldDropDownOptionUri": rail.result('get_enabled_customfield_dropdown_options_306')
            }
        )

        if_state_iso_code_equals_usa = rail.IfOperator(
            task_id='if_state_iso_code_equals_usa',
            test=lambda dag_run: dag_run.conf['State_ISO_Code'] == 'USA',
            yes_task="trigger_for_timeoff_change",
            no_task="get_current_value_for_state_iso_code",
        )

        trigger_for_timeoff_change = rail.PythonOperator(
            task_id='trigger_for_timeoff_change',
            python_callable=lambda: 'Time Off trigger change'
        )

        get_current_value_for_state_iso_code = rail.PythonOperator(
            task_id='get_current_value_for_state_iso_code',
            python_callable=python_callable.get_current_value_from_customfield,
            op_args=['State ISO Code']
        )

        if_state_isocode_does_not_equal_and_customfield_uri_present = rail.IfOperator(
            task_id='if_state_isocode_does_not_equal_and_customfield_uri_present',
            test=lambda dag_run: bool(python_callable.state_isocode_does_not_equal_and_customfield_uri_present(dag_run)),
            yes_task="get_enabled_customfield_dropdown_options_316",
            no_task="get_current_value_for_exempt_status"
        )

        get_enabled_customfield_dropdown_options_316 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_316',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": python_callable.state_isocode_does_not_equal_and_customfield_uri_present(dag_run)
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'State_ISO_Code'], 'uri', '')
        )

        if_customfield_dropdown_options_318_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_318_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_316')),
            yes_task="update_dropdown_value_udf_319",
            no_task="get_current_value_for_exempt_status",
        )

        update_dropdown_value_udf_319 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_319',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.state_isocode_does_not_equal_and_customfield_uri_present(dag_run),
                "customFieldDropDownOptionUri": rail.result('get_enabled_customfield_dropdown_options_316')
            }
        )

        get_current_value_for_exempt_status = rail.PythonOperator(
            task_id='get_current_value_for_exempt_status',
            python_callable=python_callable.get_current_value_from_customfield,
            op_args=['Exempt Status']
        )

        if_exemptstatus_does_not_equal_and_customfield_uri_present = rail.IfOperator(
            task_id='if_exemptstatus_does_not_equal_and_customfield_uri_present',
            test=lambda dag_run: bool(python_callable.exemptstatus_does_not_equal_and_customfield_uri_present(dag_run)),
            yes_task="get_enabled_customfield_dropdown_options_324",
            no_task="get_all_timezones"
        )

        get_enabled_customfield_dropdown_options_324 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_324',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": python_callable.exemptstatus_does_not_equal_and_customfield_uri_present(dag_run)
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'Exempt_Status'], 'uri', '')
        )

        if_customfield_dropdown_options_326_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_326_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_324')),
            yes_task="update_dropdown_value_udf_327",
            no_task="update_variable_warnings_329",
        )

        update_dropdown_value_udf_327 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_327',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.exemptstatus_does_not_equal_and_customfield_uri_present(dag_run),
                "customFieldDropDownOptionUri": rail.result('get_enabled_customfield_dropdown_options_324')
            }
        )

        update_variable_warnings_329 = rail.SetVariableOperator(
            task_id='update_variable_warnings_329',
            name='{{ result("warnings").name }}',
            value="Exempt Status not updated since the dropdown option received doesn't exist in Replicon"
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_final_timezone_uri_to_assign = rail.PythonOperator(
            task_id='get_final_timezone_uri_to_assign',
            python_callable=python_callable.final_timezone_uri_to_assign,
        )

        if_timezone_uri_does_not_equal = rail.IfOperator(
            task_id='if_timezone_uri_does_not_equal',
            test=lambda: bool(rail.result('get_user_details')[0]['timeZone'] and \
                            rail.result('get_final_timezone_uri_to_assign') != rail.result('get_user_details')[0]['timeZone']['uri']),
            yes_task="update_timezone_user",
            no_task="get_current_value_for_manager",
        )

        update_timezone_user = rail.RepliconServiceOperator(
            task_id='update_timezone_user',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ result('get_final_timezone_uri_to_assign') }}"
            }
        )

        get_current_value_for_manager = rail.PythonOperator(
            task_id='get_current_value_for_manager',
            python_callable=python_callable.get_current_value_from_customfield,
            op_args=['Is Manager']
        )

        if_manager_does_not_equal_and_custom_uri_present = rail.IfOperator(
            task_id='if_manager_does_not_equal_and_custom_uri_present',
            test=lambda dag_run: bool(python_callable.manager_does_not_equal_and_custom_uri_present(dag_run)),
            yes_task="get_enabled_customfield_dropdown_options_339",
            no_task="if_manager_present"
        )

        get_enabled_customfield_dropdown_options_339 = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_options_339',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": python_callable.manager_does_not_equal_and_custom_uri_present(dag_run)
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText',
               "Yes" if dag_run.conf['isManager'].find("1") >= 0 else "-", 'uri', '')
        )

        if_customfield_dropdown_options_341_present = rail.IfOperator(
            task_id='if_customfield_dropdown_options_341_present',
            test=lambda: bool(
                rail.result('get_enabled_customfield_dropdown_options_339')),
            yes_task="update_dropdown_value_udf_342",
            no_task="update_variable_warnings_344",
        )

        update_dropdown_value_udf_342 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_udf_342',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": python_callable.manager_does_not_equal_and_custom_uri_present(dag_run),
                "customFieldDropDownOptionUri": rail.result('get_enabled_customfield_dropdown_options_339')
            }
        )

        update_variable_warnings_344 = rail.SetVariableOperator(
            task_id='update_variable_warnings_344',
            name='{{ result("warnings").name }}',
            value="Is Manager not updated since the dropdown option received doesn't exist in Replicon"
        )

        if_manager_present = rail.IfOperator(
            task_id='if_manager_present',
            test=lambda dag_run: bool(dag_run.conf['Manager']),
            yes_task="if_manager_username_does_not_equal",
            no_task="if_cost_center_name_present"
        )

        if_manager_username_does_not_equal = rail.IfOperator(
            task_id='if_manager_username_does_not_equal',
            test=lambda dag_run: dag_run.conf['Manager'].lower() != dag_run.conf['Username'].lower(),
            yes_task="create_supervisor_schedule_list",
            no_task="if_cost_center_name_present"
        )

        create_supervisor_schedule_list = rail.PythonOperator(
            task_id='create_supervisor_schedule_list',
            python_callable=python_callable.create_supervisor_schedule_list
        )

        max_effectivedate_and_supervisor_from_list = rail.PythonOperator(
            task_id='max_effectivedate_and_supervisor_from_list',
            python_callable=python_callable.max_effectivedate_and_supervisor_from_list
        )

        if_supervisor_does_not_equal = rail.IfOperator(
            task_id='if_supervisor_does_not_equal',
            test=python_callable.check_current_supervisor,
            yes_task="search_user_in_replicon",
            no_task="if_cost_center_name_present"
        )

        search_user_in_replicon = rail.RepliconServiceOperator(
            task_id="search_user_in_replicon",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_user_payload,
            data_handler=response_filter.get_filtered_user_data
        )

        if_supervisoruri_present_and_status_true = rail.IfOperator(
            task_id='if_supervisoruri_present_and_status_true',
            test=lambda: bool(rail.result('search_user_in_replicon') and rail.result('search_user_in_replicon')[0]['uri'] and rail.result('search_user_in_replicon')[0]['status']),
            yes_task="get_assigned_permissionsets_for_user",
            no_task="add_supervisor_assignment_lookup_table_378"
        )

        get_assigned_permissionsets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_user_in_replicon')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.uri', '')
        )

        if_permissionset_uri_not_present = rail.IfOperator(
            task_id='if_permissionset_uri_not_present',
            test=lambda: not bool(rail.result('get_assigned_permissionsets_for_user')),
            yes_task="add_supervisor_assignment_lookup_table_371",
            no_task="if_current_supervisor_present"
        )

        add_supervisor_assignment_lookup_table_371 = rail.WriteLogOperator(
            task_id='add_supervisor_assignment_lookup_table_371',
            message="NA",
            log="{{ dag_run.conf.supervisor_sync_log }}",
            severity='Success',
            properties=lambda dag_run: {
                'parentjobid': dag_run.conf['parentjobid'],
                'enduseruri': dag_run.conf['useruri'],
                'supervisorid': dag_run.conf['Manager'],
                'status': "update" if rail.result('max_effectivedate_and_supervisor_from_list')['current_supervisor'] else "add",
                'loginname': dag_run.conf['Username']
            }
        )

        if_current_supervisor_present = rail.IfOperator(
            task_id='if_current_supervisor_present',
            test=lambda: bool(rail.result('max_effectivedate_and_supervisor_from_list')['current_supervisor']),
            yes_task="update_supervisor_with_effectivedate",
            no_task="update_initial_supervisor"
        )

        update_supervisor_with_effectivedate = rail.RepliconServiceOperator(
            task_id='update_supervisor_with_effectivedate',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('search_user_in_replicon')[0]['uri'],
                'dateRange': {
                    'startDate': python_callable.get_originalhiredate(dag_run)
                }
            }
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('search_user_in_replicon')[0]['uri']
            }
        )

        add_supervisor_assignment_lookup_table_378 = rail.WriteLogOperator(
            task_id='add_supervisor_assignment_lookup_table_378',
            message="NA",
            log="{{ dag_run.conf.supervisor_sync_log }}",
            severity='Success',
            properties=lambda dag_run: {
                'parentjobid': dag_run.conf['parentjobid'],
                'enduseruri': dag_run.conf['useruri'],
                'supervisorid': dag_run.conf['Manager'],
                'status': "update" if rail.result('max_effectivedate_and_supervisor_from_list')['current_supervisor'] else "add",
                'loginname': dag_run.conf['Username']
            }
        )

        if_cost_center_name_present = rail.IfOperator(
            task_id='if_cost_center_name_present',
            test=lambda dag_run: bool(dag_run.conf['Cost_Center_Name']),
            yes_task="create_cost_center_list",
            no_task="get_all_activities"
        )

        create_cost_center_list = rail.PythonOperator(
            task_id='create_cost_center_list',
            python_callable=python_callable.create_cost_center_list
        )

        uri_and_name_from_cost_center_list = rail.PythonOperator(
            task_id='uri_and_name_from_cost_center_list',
            python_callable=python_callable.uri_and_name_from_cost_center_list
        )

        if_cost_center_name_does_not_equal = rail.IfOperator(
            task_id='if_cost_center_name_does_not_equal',
            test=lambda dag_run: dag_run.conf['Cost_Center_Name'] != rail.result('uri_and_name_from_cost_center_list')['name'],
            yes_task="get_all_costcenters",
            no_task="get_all_activities"
        )

        get_all_costcenters = rail.RepliconServiceOperator(
            task_id="get_all_costcenters",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText',
               dag_run.conf['Cost_Center_Name'], 'uri', '')
        )

        if_costcenter_uri_present = rail.IfOperator(
            task_id='if_costcenter_uri_present',
            test=lambda: bool(rail.result('get_all_costcenters')),
            yes_task="create_cost_center_list_at_398",
            no_task="get_all_activities"
        )

        create_cost_center_list_at_398 = rail.PythonOperator(
            task_id='create_cost_center_list_at_398',
            python_callable=python_callable.create_cost_center_list_at_398
        )

        put_cost_center_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('create_cost_center_list_at_398')
            }
        )

        get_all_activities = rail.RepliconServiceOperator(
            task_id='get_all_activities',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
            data_handler=response_filter.get_activityuris
        )

        if_country_isocode_does_not_equal = rail.IfOperator(
            task_id='if_country_isocode_does_not_equal',
            test=lambda dag_run: bool(dag_run.conf['Country_ISO_Code'] and \
                    (dag_run.conf['Country_ISO_Code'] != rail.result('get_current_value_for_country_iso_code'))),
            yes_task="if_activity_uri_contains_urn",
            no_task="get_timeoff_trigger"
        )

        if_activity_uri_contains_urn = rail.IfOperator(
            task_id='if_activity_uri_contains_urn',
            test=lambda: bool(rail.result('get_all_activities')),
            yes_task="assign_activity",
            no_task="remove_activity"
        )

        assign_activity = rail.RepliconServiceOperator(
            task_id='assign_activity',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "activityUris": rail.result('get_all_activities')
            }
        )

        remove_activity = rail.RepliconServiceOperator(
            task_id='remove_activity',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data= {
                "userUri": "{{dag_run.conf.useruri}}",
                "activityUris": []
            }
        )

        get_timeoff_trigger = rail.GetVariableOperator(
            task_id='get_timeoff_trigger',
            name="{{ result('timeoff_trigger').name }}"
        )

        if_timeoff_trigger_doesnt_equal_to_no = rail.IfOperator(
            task_id='if_timeoff_trigger_doesnt_equal_to_no',
            test="{{ result('get_timeoff_trigger').value != 'no' }}",
            yes_task="trigger_imperva_user_sync_update_timeoff_assignment",
            no_task="get_warnings_value"
        )

        trigger_imperva_user_sync_update_timeoff_assignment = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_user_sync_update_timeoff_assignment',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_user_sync_update_timeoff_assignment,
            conf=request_payload.timeoff_assignment_payload
        )

        wait_for_user_sync_update_timeoff_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_sync_update_timeoff_assignment',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_imperva_user_sync_update_timeoff_assignment") }}'
        )

        get_warnings_value = rail.GetVariableOperator(
            task_id='get_warnings_value',
            name="{{ result('warnings').name }}"
        )

        imperva_user_import_logs_add_entry_419 = rail.WriteLogOperator(
            task_id='imperva_user_import_logs_add_entry_419',
            message="na",
            log= "{{dag_run.conf.user_sync_log}}",
            severity="Success",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": get_dagrun_ecid(dag_run),
                "loginname": dag_run.conf['Username'],
                "employeeid": dag_run.conf['Employee_ID'],
                "status": "Success" if rail.result('get_warnings_value')['value'].find("NA") >= 0 else "Warning",
                "reason": rail.result('get_warnings_value')['value'],
                "action": "Update",
                "country": dag_run.conf['Work_Address_Country']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.user_sync_log}}",
            message="Error | {{ get_error_message() }}",
            severity="Error",
            properties= {
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "loginname": "{{dag_run.conf.Username}}",
                "employeeid": "{{dag_run.conf.Employee_ID}}",
                "status": "Error",
                "reason": "{{get_error_message()}}",
                "action": "Update",
                "country": "{{dag_run.conf.Work_Address_Country}}"
            }
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        mapper_depedant_field_trigger >> timeoff_trigger >> warnings >> get_user_details >> if_status_is_disabled_and_isloginenable_true >> rail.Label(
            "Yes") >> trigger_disable_user_dag >> finish
        if_status_is_disabled_and_isloginenable_true >> rail.Label(
            "No") >> if_status_is_disabled_and_isloginenable_false >> rail.Label("Yes") >> imperva_user_import_logs_add_entry >> finish
        if_status_is_disabled_and_isloginenable_false >> rail.Label("No") >> if_status_is_enable_and_isloginenable_false >> rail.Label(
            "Yes") >> enable_login_re_enable_user >> set_sso_authentication_user >> user_rehired_18 >> update_startdate_end_remove_enddate >> \
        timeoff_trigger_update >> mapper_depedant_field_trigger_update >> if_day_is_present_termination_day_is_not
        if_status_is_enable_and_isloginenable_false >> rail.Label(
            "No") >> if_day_is_present_termination_day_is_not >> rail.Label("Yes") >> update_startdate_end_remove_enddate_29 >> get_required_user_customfields
        if_day_is_present_termination_day_is_not >> rail.Label("No") >> get_required_user_customfields >> current_value_for_imperva_organization >> \
        if_imperva_organization_does_not_equal >> rail.Label("Yes") >> get_required_custom_field_uri_for_imperva_org >> \
        if_imperva_organization_uri_present >> rail.Label("Yes") >> get_enabled_customfield_dropdown_options >> \
        if_customfield_dropdown_options_uri_present >> rail.Label("Yes") >> update_dropdown_value_udf >> \
        mapper_depedant_field_trigger_update_39 >> get_current_value_for_time_type
        if_customfield_dropdown_options_uri_present >> rail.Label("No") >> get_current_value_for_time_type
        if_imperva_organization_uri_present >> rail.Label("No") >> get_current_value_for_time_type
        if_imperva_organization_does_not_equal >> rail.Label("No") >> get_current_value_for_time_type >> if_time_type_does_not_equal >> rail.Label(
            "Yes") >> get_required_custom_field_uri_for_time_type >> if_time_type_uri_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_44 >> if_get_enabled_customfield_dropdown_options_44_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_47 >> mapper_depedant_field_trigger_update_48 >> get_current_value_for_payrate_type
        if_get_enabled_customfield_dropdown_options_44_present >> rail.Label(
            "No") >> get_current_value_for_payrate_type
        if_time_type_uri_present >> rail.Label(
            "No") >> get_current_value_for_payrate_type
        if_time_type_does_not_equal >> rail.Label(
            "No") >> get_current_value_for_payrate_type >> if_payrate_type_does_not_equal >> rail.Label(
            "Yes") >> get_required_custom_field_uri_for_payrate_type >> if_payrate_type_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_53 >> if_get_enabled_customfield_dropdown_options_53_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_56 >> mapper_depedant_field_trigger_update_57 >> if_department_uri_does_not_equal
        if_get_enabled_customfield_dropdown_options_53_present >> rail.Label(
            "No") >> if_department_uri_does_not_equal
        if_payrate_type_present >> rail.Label(
            "No") >> if_department_uri_does_not_equal
        if_payrate_type_does_not_equal >> rail.Label(
            "No") >> if_department_uri_does_not_equal >> rail.Label("Yes") >> update_department_for_user >> get_all_permissionsets
        if_department_uri_does_not_equal >> rail.Label("No") >> get_all_permissionsets >> payrule_name_derived >> \
        payrule_schedule_list >> max_payrule_schedule_from_list >> payrule_name >> if_payrule_names_does_not_equal >> rail.Label(
            "Yes") >> if_timesheet_displaytext_present >> rail.Label("Yes") >> get_timesheet_for_date2_77 >> if_timesheet_uri_present >> rail.Label(
            "Yes") >> get_timesheet_details >> get_required_payrule_script >> create_payrule_list >> put_payrule_script_assignment_schedule >> \
        get_current_value_for_country_iso_code >> if_country_iso_code_does_not_equal
        if_timesheet_uri_present >> rail.Label("No") >> get_current_value_for_country_iso_code
        if_timesheet_displaytext_present >> rail.Label("No") >> get_current_value_for_country_iso_code
        if_payrule_names_does_not_equal >> rail.Label("No") >>get_current_value_for_country_iso_code >> if_country_iso_code_does_not_equal >> rail.Label(
            "Yes") >> get_required_custom_field_uri_for_country_iso_code >> if_uri_for_country_iso_code_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_95 >> if_customfield_dropdown_options_95_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_96 >> country_code_updated_99 >> timeoff_trigger_update_100 >> \
        mapper_depedant_field_trigger_update_101 >> if_user_rehired_or_holiday_calendar_update_present
        if_customfield_dropdown_options_95_present >> rail.Label("NO") >> if_user_rehired_or_holiday_calendar_update_present
        if_uri_for_country_iso_code_present >> rail.Label("No") >> if_user_rehired_or_holiday_calendar_update_present
        if_country_iso_code_does_not_equal >> rail.Label("No") >> if_user_rehired_or_holiday_calendar_update_present >> rail.Label(
            "Yes") >> get_required_custom_field_uri_for_country_name >> get_enabled_customfield_dropdown_options_104 >> \
        if_customfield_dropdown_options_104_present >> rail.Label("Yes") >> update_dropdown_value_udf_107 >> get_all_holiday_calendars
        if_customfield_dropdown_options_104_present >> rail.Label("No") >> get_all_holiday_calendars >> create_holiday_calendar_list >> \
        holiday_calendar_uri >> if_holiday_calendar_present >> rail.Label("Yes") >> update_holiday_calendar_for_user >> get_mapper_depedant_field_trigger
        if_holiday_calendar_present >> rail.Label("No") >> get_mapper_depedant_field_trigger
        if_user_rehired_or_holiday_calendar_update_present >> rail.Label("No") >> get_mapper_depedant_field_trigger >> \
        if_mapper_depedant_field_variable_is_yes >> rail.Label("Yes") >> get_all_employee_type_details >> search_employee_value >> \
        get_employee_type_uri >> if_employee_type_uri_present >> rail.Label("Yes") >> update_employeetype_for_user >> search_schedule_value
        if_employee_type_uri_present >> rail.Label("No") >> search_schedule_value >> get_existing_schedule_types >> create_schedule_list_to_compare >> \
        get_max_effective_date_from_schedule_list >> current_schedule_name >> if_schedule_name_present_and_does_not_equal >> rail.Label(
            "Yes") >> create_schedule_list_134 >> get_all_office_schedules >> get_new_office_schedule_uri >> if_new_office_schedule_uri_present >> rail.Label(
            "Yes") >> add_item_to_schedule_list_144 >> put_schedule_policy_user >> search_timesheet_period_type_value
        if_new_office_schedule_uri_present >> rail.Label(
            "No") >> search_timesheet_period_type_value
        if_schedule_name_present_and_does_not_equal >> rail.Label(
            "No") >> search_timesheet_period_type_value >> timesheetperiod_variable >> if_timesheet_period_type_value_present >> rail.Label(
            "Yes") >> put_employeetype_timesheetperiod_uri >> search_timesheet_approval_path_value
        if_timesheet_period_type_value_present >> rail.Label(
            "No") >> search_timesheet_approval_path_value >> if_timesheet_approval_path_value_present >> rail.Label(
            "Yes") >> if_correct_approval_path_is_not_assigned >> rail.Label(
            "Yes") >> get_timesheet_approval_path_uri >> if_timesheet_approval_path_uri_present >> rail.Label(
            "Yes") >> update_timesheet_approvalpath_user >> search_timeoff_approval_path_value
        if_timesheet_approval_path_uri_present >> rail.Label(
            "No") >> search_timeoff_approval_path_value
        if_correct_approval_path_is_not_assigned >> rail.Label(
            "No") >> search_timeoff_approval_path_value
        if_timesheet_approval_path_value_present >> rail.Label(
            "No") >> search_timeoff_approval_path_value >> if_timeoff_approval_path_value_present >> rail.Label(
            "Yes") >> if_correct_timeoff_approvalpath_not_assigned >> rail.Label("Yes") >> get_timeoff_approval_path_uri >> \
        if_timeoff_approval_path_uri_present >> rail.Label("Yes") >> update_timeoff_approvalpath_user >> get_all_policy_sets
        if_timeoff_approval_path_uri_present >> rail.Label("No") >> get_all_policy_sets
        if_correct_timeoff_approvalpath_not_assigned >> rail.Label("No") >> get_all_policy_sets
        if_timeoff_approval_path_value_present >> rail.Label(
            "No") >> get_all_policy_sets >> search_timesheet_template_value >> if_timesheet_template_false_and_uri_true >> rail.Label(
            "Yes") >> remove_policysettouser_timesheettemplate >> search_timeoff_template_value
        if_timesheet_template_false_and_uri_true >> rail.Label(
            "No") >> if_correct_timesheettemplate_not_assigned >> rail.Label(
            "Yes") >> get_timesheet_template_uri >> if_timesheet_template_uri_present >> rail.Label(
            "Yes") >> assign_timesheet_template >> search_timeoff_template_value
        if_timesheet_template_uri_present >> rail.Label(
            "No") >> search_timeoff_template_value
        if_correct_timesheettemplate_not_assigned >> rail.Label(
            "No") >> search_timeoff_template_value >> if_timeoff_template_false_and_uri_true >> rail.Label(
            "Yes") >> remove_policysettouser_timeofftemplate >> search_entry_policy_value
        if_timeoff_template_false_and_uri_true >> rail.Label(
            "No") >> if_correct_timeofftemplate_not_assigned >> rail.Label(
            "Yes") >> if_timeoff_template_uri_present >> rail.Label(
            "Yes") >> assign_timeoff_template >> search_entry_policy_value
        if_timeoff_template_uri_present >> rail.Label(
            "No") >> search_entry_policy_value
        if_correct_timeofftemplate_not_assigned >> rail.Label(
            "No") >> search_entry_policy_value >> get_assigned_policysets >> if_entry_policy_false_and_uri_true >> rail.Label(
            "Yes") >> remove_policysettouser_punchentry_policy >> search_work_week_value
        if_entry_policy_false_and_uri_true >> rail.Label(
            "No") >> if_punchentry_policy_name_does_not_equal >> rail.Label(
            "Yes") >> if_punchentry_policy_present >> rail.Label(
            "Yes") >> assign_punch_entry_policy >> search_work_week_value
        if_punchentry_policy_present >> rail.Label(
            "No") >> search_work_week_value
        if_punchentry_policy_name_does_not_equal >> rail.Label(
            "No") >> search_work_week_value >> get_workweek_value_uri >> if_workweek_present_and_does_not_equal >> rail.Label(
            "Yes") >> update_workweek_startday >> if_manager_present_and_equals_to_1
        if_workweek_present_and_does_not_equal >> rail.Label(
            "No") >> if_manager_present_and_equals_to_1
        if_mapper_depedant_field_variable_is_yes >> rail.Label("No") >> if_manager_present_and_equals_to_1 >> rail.Label(
            "Yes") >> get_permission_sets_from_user_details >> if_end_user_or_supervisor_does_not_present >> rail.Label(
            "Yes") >> get_permission_sets_from_permissionsets >> add_missing_permissionsets_to_user >> \
        add_missing_permissionsets_to_user_216 >> payrule_name_derived_does_not_equal
        if_end_user_or_supervisor_does_not_present >> rail.Label(
            "No") >> payrule_name_derived_does_not_equal
        if_manager_present_and_equals_to_1 >> rail.Label(
            "No") >> payrule_name_derived_does_not_equal >> rail.Label("Yes") >> if_timesheet_template_name_not_present >> rail.Label(
            "Yes") >> if_timesheet_template_name_uri_present >> rail.Label(
            "Yes") >> get_timesheet_for_date2_221 >> if_timesheet_uri_present_222 >> rail.Label(
            "Yes") >> get_timesheet_details_223 >> get_all_payrule_scripts
        if_timesheet_uri_present_222 >> rail.Label(
            "No") >> get_all_payrule_scripts
        if_timesheet_template_name_uri_present >> rail.Label(
            "No") >> get_all_payrule_scripts >> create_payrule_list_225 >> put_payrule_script_assignment_schedule_234 >> if_primary_workemail_does_not_equal
        if_timesheet_template_name_not_present >> rail.Label(
            "No") >> if_primary_workemail_does_not_equal
        payrule_name_derived_does_not_equal >> rail.Label("No") >> if_primary_workemail_does_not_equal >> rail.Label(
            "Yes") >> update_email_236 >> if_firstname_does_not_equal
        if_primary_workemail_does_not_equal >> rail.Label(
            "No") >> if_firstname_does_not_equal >> rail.Label("Yes") >> update_firstname_238 >> if_lastname_does_not_equal
        if_firstname_does_not_equal >> rail.Label("No") >> if_lastname_does_not_equal >> rail.Label(
            "Yes") >> update_lastname_240 >> if_hourly_pay_and_currency_present
        if_lastname_does_not_equal >> rail.Label(
            "No") >> if_hourly_pay_and_currency_present >> rail.Label(
            "Yes") >> hourly_rate_schedule_list >> get_max_hourly_rate_schedule_list >> get_hourly_rate_amount_currency >> \
        if_amount_or_currency_does_not_equal >> rail.Label(
            "Yes") >> get_currency_uri >> if_currency_uri_present >> rail.Label(
            "Yes") >> update_user_payroll >> if_original_hire_date_present
        if_currency_uri_present >> rail.Label(
            "No") >> update_variable_warnings >> if_original_hire_date_present
        if_amount_or_currency_does_not_equal >> rail.Label("No") >> if_original_hire_date_present
        if_hourly_pay_and_currency_present >> rail.Label(
            "No") >> if_original_hire_date_present >> rail.Label(
            "Yes") >> if_original_hire_date_does_not_equal >> rail.Label(
            "Yes") >> if_original_hire_date_uri_present >> rail.Label(
            "Yes") >> update_original_hire_date_udf >> get_current_value_for_costcenter
        if_original_hire_date_uri_present >> rail.Label(
            "No") >> get_current_value_for_costcenter
        if_original_hire_date_does_not_equal >> rail.Label(
            "NO") >> get_current_value_for_costcenter
        if_original_hire_date_present >> rail.Label(
            "No") >> get_current_value_for_costcenter >> if_costcenter_doest_equal_and_customfield_uri_present >> rail.Label(
            "Yes") >> update_costcenter_udf >> if_jobcode_present
        if_costcenter_doest_equal_and_customfield_uri_present >> rail.Label(
            "No") >> if_jobcode_present >> rail.Label(
            "Yes") >> update_jobcode_udf >> get_current_value_for_workertype
        if_jobcode_present >> rail.Label(
            "No") >> get_current_value_for_workertype >> if_workertype_does_not_equal_and_customfield_uri_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_282 >> if_customfield_dropdown_options_284_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_285 >> get_current_value_for_employeetype
        if_customfield_dropdown_options_284_present >> rail.Label(
            "No") >> get_current_value_for_employeetype
        if_workertype_does_not_equal_and_customfield_uri_present >> rail.Label(
            "No") >> get_current_value_for_employeetype >> if_employeetype_does_not_equal_and_customfield_uri_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_290 >> if_customfield_dropdown_options_292_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_293 >> get_current_value_for_workcountry
        if_customfield_dropdown_options_292_present >> rail.Label(
            "No") >> get_current_value_for_workcountry
        if_employeetype_does_not_equal_and_customfield_uri_present >> rail.Label(
            "No") >> get_current_value_for_workcountry >> if_workcountry_does_not_equal_and_customfield_uri_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_298 >> if_customfield_dropdown_options_300_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_301 >> get_current_value_for_workstate
        if_customfield_dropdown_options_300_present >> rail.Label(
            "No") >> get_current_value_for_workstate
        if_workcountry_does_not_equal_and_customfield_uri_present >> rail.Label(
            "No") >> get_current_value_for_workstate >> if_workstate_does_not_equal_and_customfield_uri_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_306 >> if_customfield_dropdown_options_308_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_309 >> if_state_iso_code_equals_usa >> rail.Label(
            "Yes") >> trigger_for_timeoff_change >> get_current_value_for_state_iso_code
        if_state_iso_code_equals_usa >> rail.Label(
            "No") >> get_current_value_for_state_iso_code
        if_customfield_dropdown_options_308_present >> rail.Label(
            "No") >> get_current_value_for_state_iso_code
        if_workstate_does_not_equal_and_customfield_uri_present >> rail.Label(
            "No") >> get_current_value_for_state_iso_code >> if_state_isocode_does_not_equal_and_customfield_uri_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_316 >> if_customfield_dropdown_options_318_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_319 >> get_current_value_for_exempt_status
        if_customfield_dropdown_options_318_present >> rail.Label(
            "No") >> get_current_value_for_exempt_status
        if_state_isocode_does_not_equal_and_customfield_uri_present >> rail.Label(
            "No") >> get_current_value_for_exempt_status >> if_exemptstatus_does_not_equal_and_customfield_uri_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_324 >> if_customfield_dropdown_options_326_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_327 >> get_all_timezones
        if_customfield_dropdown_options_326_present >> rail.Label(
            "No") >> update_variable_warnings_329 >> get_all_timezones
        if_exemptstatus_does_not_equal_and_customfield_uri_present >> rail.Label(
            "No") >> get_all_timezones >> get_final_timezone_uri_to_assign >> if_timezone_uri_does_not_equal >> rail.Label(
            "Yes") >> update_timezone_user >> get_current_value_for_manager
        if_timezone_uri_does_not_equal >> rail.Label(
            "No") >> get_current_value_for_manager >> if_manager_does_not_equal_and_custom_uri_present >> rail.Label(
            "Yes") >> get_enabled_customfield_dropdown_options_339 >> if_customfield_dropdown_options_341_present >> rail.Label(
            "Yes") >> update_dropdown_value_udf_342 >> if_manager_present
        if_customfield_dropdown_options_341_present >> rail.Label(
            "No") >> update_variable_warnings_344 >> if_manager_present
        if_manager_does_not_equal_and_custom_uri_present >> rail.Label(
            "No") >> if_manager_present >> rail.Label(
            "Yes") >> if_manager_username_does_not_equal >> rail.Label(
            "Yes") >> create_supervisor_schedule_list >> max_effectivedate_and_supervisor_from_list >> if_supervisor_does_not_equal >> rail.Label(
            "Yes") >> search_user_in_replicon >> if_supervisoruri_present_and_status_true >> rail.Label(
            "Yes") >> get_assigned_permissionsets_for_user >> if_permissionset_uri_not_present >> rail.Label(
            "Yes") >> add_supervisor_assignment_lookup_table_371 >> if_cost_center_name_present
        if_permissionset_uri_not_present >> rail.Label(
            "No") >> if_current_supervisor_present >> rail.Label(
            "Yes") >> update_supervisor_with_effectivedate >> if_cost_center_name_present
        if_current_supervisor_present >> rail.Label(
            "No") >> update_initial_supervisor >> if_cost_center_name_present
        if_supervisoruri_present_and_status_true >> rail.Label(
            "No") >> add_supervisor_assignment_lookup_table_378 >> if_cost_center_name_present
        if_supervisor_does_not_equal >> rail.Label(
            "No") >> if_cost_center_name_present
        if_manager_username_does_not_equal >> rail.Label(
            "No") >> if_cost_center_name_present
        if_manager_present >> rail.Label(
            "No") >> if_cost_center_name_present >> rail.Label(
            "Yes") >> create_cost_center_list >> uri_and_name_from_cost_center_list >> if_cost_center_name_does_not_equal >> rail.Label(
            "Yes") >> get_all_costcenters >> if_costcenter_uri_present >> rail.Label(
            "Yes") >> create_cost_center_list_at_398 >> put_cost_center_schedule_for_user >> get_all_activities
        if_costcenter_uri_present >> rail.Label(
            "No") >> get_all_activities
        if_cost_center_name_does_not_equal >> rail.Label(
            "No") >> get_all_activities
        if_cost_center_name_present >> rail.Label(
            "No") >> get_all_activities >> if_country_isocode_does_not_equal >> rail.Label(
            "Yes") >> if_activity_uri_contains_urn >> rail.Label("Yes") >> assign_activity >> get_timeoff_trigger
        if_activity_uri_contains_urn >> rail.Label("No") >> remove_activity >> get_timeoff_trigger
        if_country_isocode_does_not_equal >> rail.Label(
            "No") >> get_timeoff_trigger >> if_timeoff_trigger_doesnt_equal_to_no >> rail.Label(
            "Yes") >> trigger_imperva_user_sync_update_timeoff_assignment >> wait_for_user_sync_update_timeoff_assignment >> get_warnings_value
        if_timeoff_trigger_doesnt_equal_to_no >> rail.Label(
            "No") >> get_warnings_value >> imperva_user_import_logs_add_entry_419 >> finish
        finish >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
