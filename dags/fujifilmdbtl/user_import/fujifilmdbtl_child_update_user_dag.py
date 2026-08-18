from datetime import timedelta, datetime
from pendulum import now
import json
from airflow.models import Variable
import rail
from fujifilmdbtl.user_import.utils import request_payload, python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_child_update_user_{config.instance}',
        description=f'FDT_Child Workflow to update user {config.instance}',
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_variable_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable_2',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_variable_2 = rail.SetVariableOperator(
            task_id='declare_variable_2',
            append=False,
            name='trigger_rehire',
            value="No"
        )

        declare_variable_3 = rail.SetVariableOperator(
            task_id='declare_variable_3',
            append=False,
            name='timeoff_transition_trigger',
            value="No TimeOff Change"
        )

        if_request_servicedate_not_contains_5 = rail.IfOperator(
            task_id='if_request_servicedate_not_contains_5',
            test=lambda dag_run: '/' not in dag_run.conf['servicedate'] or not dag_run.conf['servicedate'],
            yes_task="fdt_user_import_logs_add_entry_6",
            no_task="log_service_datein_time_8",
        )

        fdt_user_import_logs_add_entry_6 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_6',
            log=lambda dag_run: dag_run.conf['userimportlogtable'],
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "username": rail.render_template("{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"),
                "loginname": dag_run.conf['loginname'],
                "emplid": dag_run.conf['emplid'],
                "action": "Update",
                "status": "Exception",
                "details": ("Incorrect Log" if ("/" in dag_run.conf['servicedate']) else "Service date is not in the correct format") if dag_run.conf['servicedate'] else "Service Date is not present in the feed file",
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        log_service_datein_time_8 = rail.PythonOperator(
            task_id='log_service_datein_time_8',
            python_callable=lambda dag_run:  (datetime.strptime(
                dag_run.conf['servicedate'], "%m/%d/%Y")).isoformat()
        )

        invoke_custom_ruby_code_service_date_9 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_service_date_9',
            python_callable=lambda dag_run: python_callable.get_split_date(
                dag_run.conf['servicedate'], "%m/%d/%Y")
        )

        bulk_get_users3_10 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_10',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": null
            }
        )

        if_request_servicedate_present_11 = rail.IfOperator(
            task_id='if_request_servicedate_present_11',
            test='''{{ dag_run.conf.servicedate | is_truthy() }}''',
            yes_task="log_start_dateasperfeedfile_12",
            no_task="if_userdetails_isenabled_is_not_true_17",
        )

        log_start_dateasperfeedfile_12 = rail.PythonOperator(
            task_id='log_start_dateasperfeedfile_12',
            python_callable=lambda: str(rail.render_template(
                "{{ result('invoke_custom_ruby_code_service_date_9').day }}/{{ result('invoke_custom_ruby_code_service_date_9').month }}/{{ result('invoke_custom_ruby_code_service_date_9').year }}"))
        )

        log_start_dateasper_repliconprofile_13 = rail.PythonOperator(
            task_id='log_start_dateasper_repliconprofile_13',
            python_callable=lambda: str(rail.result('bulk_get_users3_10')[0]['userDetails']['employmentDateRange']['startDate']['day'])+"/"+str(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['employmentDateRange']['startDate']['month'])+"/"+str(rail.result(
                    'bulk_get_users3_10')[0]['userDetails']['employmentDateRange']['startDate']['year'])
        )

        if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_13messageto_date_14 = rail.IfOperator(
            task_id='if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_13messageto_date_14',
            test=lambda: datetime.strptime(rail.result('log_start_dateasperfeedfile_12'), "%d/%m/%Y") != datetime.strptime(
                rail.result('log_start_dateasper_repliconprofile_13'), "%d/%m/%Y"),
            yes_task="fdt_user_import_logs_add_entry_15",
            no_task="if_userdetails_isenabled_is_not_true_17",
        )

        fdt_user_import_logs_add_entry_15 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_15',
            log="{{ dag_run.conf.userimportlogtable}}",
            message="na",
            severity="Exception",
            properties={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "action": "Update",
                "status": "Exception",
                "details": "Service date in the feed file and start date in user profile are not same",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_userdetails_isenabled_is_not_true_17 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_17',
            test='''{{ result('bulk_get_users3_10')[0].userDetails.isEnabled | is_falsy  and dag_run.conf.eestatus == 'A' }}''',
            yes_task="if_request_rehiredate_not_contains_18",
            no_task="if_request_firstname_present_24",
        )

        if_request_rehiredate_not_contains_18 = rail.IfOperator(
            task_id='if_request_rehiredate_not_contains_18',
            test=lambda dag_run: not dag_run.conf.get(
                'rehiredate') or '/' not in dag_run.conf.get('rehiredate', ''),
            yes_task="fdt_user_import_logs_add_entry_19",
            no_task="enable_login_21",
        )

        fdt_user_import_logs_add_entry_19 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_19',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "parentjobid": rail.render_template("{{dag_run.conf.parentjobid}}"),
                "username": rail.render_template("{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"),
                "loginname": rail.render_template("{{ dag_run.conf.loginname }}"),
                "emplid": rail.render_template("{{ dag_run.conf.emplid }}"),
                "action": "Update",
                "status": "Exception",
                "details": rail.render_template("User not enabled or rehire logic not appiled as" + (
                    ("Incorrect Log" if ("/" in dag_run.conf['servicedate']) else "Rehire date is not in the correct format") if dag_run.conf['servicedate'] else "Rehire Date is not present in the feed file")),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        enable_login_21 = rail.RepliconServiceOperator(
            task_id='enable_login_21',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        update_employment_date_rangetoremoveenddate_22 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangetoremoveenddate_22',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('invoke_custom_ruby_code_service_date_9').year }}",
                        "month": "{{ result('invoke_custom_ruby_code_service_date_9').month }}",
                        "day": "{{ result('invoke_custom_ruby_code_service_date_9').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        update_variable_23 = rail.SetVariableOperator(
            task_id='update_variable_23',
            append=False,
            name='{{ result("declare_variable_2").name }}',
            value="Yes"
        )

        if_request_firstname_present_24 = rail.IfOperator(
            task_id='if_request_firstname_present_24',
            test='''{{ dag_run.conf.firstname | is_truthy and result('bulk_get_users3_10')[0].userDetails.firstName != dag_run.conf.firstname }}''',
            yes_task="update_first_name_25",
            no_task="if_request_regulartemporary_equals_to_t_26",
        )

        update_first_name_25 = rail.RepliconServiceOperator(
            task_id='update_first_name_25',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_request_regulartemporary_equals_to_t_26 = rail.IfOperator(
            task_id='if_request_regulartemporary_equals_to_t_26',
            test=lambda dag_run: dag_run.conf['regulartemporary'] == 't' and bool(rail.result(
                'bulk_get_users3_10')[0]['holidayCalendar']),
            yes_task="remove_holiday_calendarassignmentsforusers_27",
            no_task="if_request_regulartemporary_not_equals_to_t_28",
        )

        remove_holiday_calendarassignmentsforusers_27 = rail.RepliconServiceOperator(
            task_id='remove_holiday_calendarassignmentsforusers_27',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": null
            }
        )

        if_request_regulartemporary_not_equals_to_t_28 = rail.IfOperator(
            task_id='if_request_regulartemporary_not_equals_to_t_28',
            test=lambda dag_run: dag_run.conf['regulartemporary'] != 't' and not (bool(rail.result(
                'bulk_get_users3_10')[0]['holidayCalendar'])),
            yes_task="get_holiday_calendar_for_new_users_29",
            no_task="if_request_loginname_present_32",
        )

        get_holiday_calendar_for_new_users_29 = rail.RepliconServiceOperator(
            task_id='get_holiday_calendar_for_new_users_29',
            endpoint="/services/HolidayCalendarService1.svc/GetHolidayCalendarForNewUsers",
            data=None
        )

        if_d_displaytext_present_30 = rail.IfOperator(
            task_id='if_d_displaytext_present_30',
            test='''{{ result('get_holiday_calendar_for_new_users_29') | is_truthy }}''',
            yes_task="add_default_holiday_calendarforuser_31",
            no_task="if_request_loginname_present_32",
        )

        add_default_holiday_calendarforuser_31 = rail.RepliconServiceOperator(
            task_id='add_default_holiday_calendarforuser_31',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('get_holiday_calendar_for_new_users_29').uri }}"
            }
        )

        if_request_loginname_present_32 = rail.IfOperator(
            task_id='if_request_loginname_present_32',
            test='''{{ dag_run.conf.loginname | is_truthy  and result('bulk_get_users3_10')[0].securityConfiguration.loginName != dag_run.conf.loginname }}''',
            yes_task="apply_user_modifications_updateloginname_33",
            no_task="if_request_email_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34",
        )

        apply_user_modifications_updateloginname_33 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_updateloginname_33',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "securitySettingsToApply": {
                        "loginEnabled": "true",
                        "loginName": "{{ dag_run.conf.loginname }}",
                        "ssoName": "{{ dag_run.conf.loginname }}",
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ]
                    }
                }
            }
        )

        if_request_email_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34 = rail.IfOperator(
            task_id='if_request_email_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34',
            test=lambda dag_run: dag_run.conf['email'] and dag_run.conf['email'] != ((rail.result(
                'bulk_get_users3_10')[0]['userDetails']['emailAddress']).lower() if rail.result(
                    'bulk_get_users3_10')[0]['userDetails']['emailAddress'] else ""),
            yes_task="update_email_35",
            no_task="if_request_lastname_present_36",
        )

        update_email_35 = rail.RepliconServiceOperator(
            task_id='update_email_35',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.email }}"
            }
        )

        if_request_lastname_present_36 = rail.IfOperator(
            task_id='if_request_lastname_present_36',
            test='''{{ dag_run.conf.lastname | is_truthy  and dag_run.conf.lastname != result('bulk_get_users3_10')[0].userDetails.lastName }}''',
            yes_task="update_last_name_37",
            no_task="if_request_emplid_present_38",
        )

        update_last_name_37 = rail.RepliconServiceOperator(
            task_id='update_last_name_37',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_request_emplid_present_38 = rail.IfOperator(
            task_id='if_request_emplid_present_38',
            test='''{{ dag_run.conf.emplid | is_truthy and result('bulk_get_users3_10')[0].userDetails.employeeId != dag_run.conf.emplid }}''',
            yes_task="update_employee_id_39",
            no_task="date_split_todaysdate_40",
        )

        update_employee_id_39 = rail.RepliconServiceOperator(
            task_id='update_employee_id_39',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeId": "{{ dag_run.conf.emplid }}"
            }
        )

        date_split_todaysdate_40 = rail.PythonOperator(
            task_id='date_split_todaysdate_40',
            python_callable=lambda: python_callable.get_split_date(
                now())
        )

        if_request_eetype_present_fulltimehourly_41 = rail.IfOperator(
            task_id='if_request_eetype_present_fulltimehourly_41',
            test='''{{ dag_run.conf.eetype | is_truthy and result('bulk_get_users3_10')[0].employeeType.name.lower() != dag_run.conf.eetype }}''',
            yes_task="declare_variable_42",
            no_task="if_request_department_present_92",
        )

        declare_variable_42 = rail.SetVariableOperator(
            task_id='declare_variable_42',
            append=False,
            name='employee_type',
            value=None
        )

        if_request_eetype_equals_to_h_43 = rail.IfOperator(
            task_id='if_request_eetype_equals_to_h_43',
            test='''{{ dag_run.conf.eetype == 'h' }}''',
            yes_task="update_variable_44",
            no_task="if_request_eetype_equals_to_s_45",
        )

        update_variable_44 = rail.SetVariableOperator(
            task_id='update_variable_44',
            append=False,
            name='{{ result("declare_variable_42").name }}',
            value="Hourly"
        )

        if_request_eetype_equals_to_s_45 = rail.IfOperator(
            task_id='if_request_eetype_equals_to_s_45',
            test='''{{ dag_run.conf.eetype == 's' }}''',
            yes_task="update_variable_46",
            no_task="_adhoc_http_action_47",
        )

        update_variable_46 = rail.SetVariableOperator(
            task_id='update_variable_46',
            append=False,
            name='{{ result("declare_variable_42").name }}',
            value="Salaried"
        )

        _adhoc_http_action_47 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_47',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.get_dag_run_var('employee_type'), 'uri', '')
        )

        if_log_employee_type_uri_48_present_49 = rail.IfOperator(
            task_id='if_log_employee_type_uri_48_present_49',
            test='''{{ result('_adhoc_http_action_47') | is_truthy }}''',
            yes_task="update_employee_type_for_user_50",
            no_task="_adhoc_http_action_51",
        )

        update_employee_type_for_user_50 = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user_50',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('_adhoc_http_action_47') }}"
            }
        )

        _adhoc_http_action_51 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_51',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        if_request_eetype_equals_to_s_52 = rail.IfOperator(
            task_id='if_request_eetype_equals_to_s_52',
            test='''{{ dag_run.conf.eetype == 's' and result('bulk_get_users3_10')[0].employeeType.name.lower() != 'salaried' }}''',
            yes_task="log_pluckif_pay_ruleispresent_62",
            no_task="if_request_eetype_equals_to_h_72",
        )

        log_pluckif_pay_ruleispresent_62 = rail.PythonOperator(
            task_id='log_pluckif_pay_ruleispresent_62',
            python_callable=lambda dag_run:  "Salaried-Dummy" if "s" in (
                dag_run.conf['eetype']).lower() else "No Payrule"
        )

        log_get_pay_rule_script_uri_63 = rail.PythonOperator(
            task_id='log_get_pay_rule_script_uri_63',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result("_adhoc_http_action_51"), 'displayText', rail.result(
                'log_pluckif_pay_ruleispresent_62'), 'uri', '') if rail.result("_adhoc_http_action_51") else null
        )

        get_payrulescript_schedule_list_to_apply_hourly = rail.PythonOperator(
            task_id='get_payrulescript_schedule_list_to_apply_hourly',
            python_callable=lambda: python_callable.get_modified_payrule_list(
                rail.result('bulk_get_users3_10')[0]['payRuleScriptSchedule'], rail.result('log_get_pay_rule_script_uri_63'))
        )

        if_payrulescript_schedule_list_to_apply_hourly = rail.IfOperator(
            task_id='if_payrulescript_schedule_list_to_apply_hourly',
            test=lambda: rail.result(
                'get_payrulescript_schedule_list_to_apply_hourly'),
            yes_task='put_payroll_assignment_hourly',
            no_task='if_request_eetype_equals_to_h_72'
        )

        put_payroll_assignment_hourly = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment_hourly',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_payrulescript_schedule_list_to_apply_hourly')
            }
        )

        if_request_eetype_equals_to_h_72 = rail.IfOperator(
            task_id='if_request_eetype_equals_to_h_72',
            test='''{{ dag_run.conf.eetype == 'h' and result('bulk_get_users3_10')[0].employeeType.name.lower()!='hourly' }}''',
            yes_task="log_pluckif_pay_ruleispresent_82",
            no_task="if_request_department_present_92",
        )

        log_pluckif_pay_ruleispresent_82 = rail.PythonOperator(
            task_id='log_pluckif_pay_ruleispresent_82',
            python_callable=lambda dag_run:  (
                "FDBT Weekly FT Overtime" if "f" in dag_run.conf['fullparttime'] else "FDBT Weekly PT Overtime") if "h" in dag_run.conf['eetype'].lower() else "Salaried-Dummy"
        )

        log_get_pay_rule_script_uri_83 = rail.PythonOperator(
            task_id='log_get_pay_rule_script_uri_83',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result("_adhoc_http_action_51"), 'displayText', rail.result(
                'log_pluckif_pay_ruleispresent_82'), 'uri', '') if rail.result("_adhoc_http_action_51") else null
        )

        get_payrulescript_schedule_list_to_apply_salaried = rail.PythonOperator(
            task_id='get_payrulescript_schedule_list_to_apply_salaried',
            python_callable=lambda: python_callable.get_modified_payrule_list(
                rail.result('bulk_get_users3_10')[0]['payRuleScriptSchedule'], rail.result('log_get_pay_rule_script_uri_83'))
        )

        if_payrulescript_schedule_list_to_apply_salaried = rail.IfOperator(
            task_id='if_payrulescript_schedule_list_to_apply_salaried',
            test=lambda: rail.result(
                'get_payrulescript_schedule_list_to_apply_salaried'),
            yes_task='put_payroll_assignment_salaried',
            no_task='if_request_department_present_92'
        )

        put_payroll_assignment_salaried = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment_salaried',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_payrulescript_schedule_list_to_apply_salaried')
            }
        )

        if_request_department_present_92 = rail.IfOperator(
            task_id='if_request_department_present_92',
            test=lambda dag_run: dag_run.conf['department'] and rail.result(
                'bulk_get_users3_10')[0]['userDetails']['department'] and dag_run.conf['department'].lower() != rail.result(
                    'bulk_get_users3_10')[0]['userDetails']['department']['name'].lower(),
            yes_task="if_request_deptid_present_93",
            no_task="if_request_managerid_present_103",
        )

        if_request_deptid_present_93 = rail.IfOperator(
            task_id='if_request_deptid_present_93',
            test='''{{ dag_run.conf.deptid | is_truthy }}''',
            yes_task="get_datafordepartmentbasedonnameandcode_94",
            no_task="log_error_logfordepartmentidblank_102",
        )

        get_datafordepartmentbasedonnameandcode_94 = rail.RepliconServiceOperator(
            task_id='get_datafordepartmentbasedonnameandcode_94',
            endpoint="/services/DepartmentListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_data_for_department_based_on_name_and_code(
                dag_run.conf['department'], dag_run.conf['deptid']),
            data_handler=lambda response: response['rows']
        )

        get_department_search_result_list = rail.PythonOperator(
            task_id='get_department_search_result_list',
            python_callable=lambda: python_callable.get_department_list_output(
                rail.result('get_datafordepartmentbasedonnameandcode_94'))
        )

        log_departmenturi_96 = rail.PythonOperator(
            task_id='log_departmenturi_96',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_department_search_result_list'), 'enabled', "True", 'uri') if rail.result('get_department_search_result_list') else ""
        )

        if_log_departmenturi_96_present_97 = rail.IfOperator(
            task_id='if_log_departmenturi_96_present_97',
            test='''{{ result('log_departmenturi_96') | is_truthy }}''',
            yes_task="update_department_for_user_98",
            no_task="log_error_logfordepartmentnotpresent_100",
        )

        update_department_for_user_98 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_98',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ result('log_departmenturi_96') }}"
            }
        )

        log_error_logfordepartmentnotpresent_100 = rail.PythonOperator(
            task_id='log_error_logfordepartmentnotpresent_100',
            python_callable=lambda dag_run:  "Department not updated for User " + dag_run.conf['firstname'] +
            dag_run.conf['lastname'] + " . " + dag_run.conf['department'] +
            "is not available in Replicon."
        )

        log_error_logfordepartmentidblank_102 = rail.PythonOperator(
            task_id='log_error_logfordepartmentidblank_102',
            python_callable=lambda dag_run:  "Department not updated for User " + dag_run.conf['firstname'] +
            dag_run.conf['lastname'] + ". " + dag_run.conf['deptid'] +
            "not available in the feed file."
        )

        if_request_managerid_present_103 = rail.IfOperator(
            task_id='if_request_managerid_present_103',
            test='''{{ dag_run.conf.managerid | is_truthy }}''',
            yes_task="search_users_104",
            no_task="log_get_assignedshiftcurrent_value_137",
        )

        search_users_104 = rail.RepliconServiceOperator(
            task_id='search_users_104',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_search_user_payload_for_supervisor(
                dag_run.conf['managerid']),
            data_handler=lambda response: response['rows']
        )

        log_getthenumberofuserswithsameemployeeid_105 = rail.PythonOperator(
            task_id='log_getthenumberofuserswithsameemployeeid_105',
            python_callable=lambda: len(rail.result('search_users_104')) if rail.result(
                'search_users_104') else ""
        )

        if_log_getthenumberofuserswithsameemployeeid_105_present_106 = rail.IfOperator(
            task_id='if_log_getthenumberofuserswithsameemployeeid_105_present_106',
            test='''{{ result('log_getthenumberofuserswithsameemployeeid_105') | is_truthy and result('log_getthenumberofuserswithsameemployeeid_105') > 1 }}''',
            yes_task="log_errorforsupervisorassignment_107",
            no_task="get_supervisor_details",
        )

        log_errorforsupervisorassignment_107 = rail.PythonOperator(
            task_id='log_errorforsupervisorassignment_107',
            python_callable=lambda dag_run:  "Supervisor assignment skipped as" +
            rail.result('log_getthenumberofuserswithsameemployeeid_105') +
            "users have same employee id as " + dag_run.conf['managerid']
        )

        get_supervisor_details = rail.PythonOperator(
            task_id='get_supervisor_details',
            python_callable=lambda: python_callable.get_search_user_details(
                rail.result('search_users_104'))
        )

        log_supervisor_loginname_109 = rail.PythonOperator(
            task_id='log_supervisor_loginname_109',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_supervisor_details'), 'employeeid', dag_run.conf['managerid'], 'loginname', "") if rail.result('get_supervisor_details') else ''
        )

        if_request_loginname_equals_to_dataloggerlog_supervisor_loginname_109message_110 = rail.IfOperator(
            task_id='if_request_loginname_equals_to_dataloggerlog_supervisor_loginname_109message_110',
            test='''{{ dag_run.conf.loginname == result('log_supervisor_loginname_109') }}''',
            yes_task="log_error_messagefor_supervisoranduserloginnamesame_111",
            no_task="if_request_loginname_not_equals_to_dataloggerlog_supervisor_loginname_109message_112",
        )

        log_error_messagefor_supervisoranduserloginnamesame_111 = rail.PythonOperator(
            task_id='log_error_messagefor_supervisoranduserloginnamesame_111',
            python_callable=lambda:  "Supervsior not updated for" + rail.render_template(
                " {{ dag_run.conf.loginname }} ") + "as user's and supervsior's login name are same"
        )

        if_request_loginname_not_equals_to_dataloggerlog_supervisor_loginname_109message_112 = rail.IfOperator(
            task_id='if_request_loginname_not_equals_to_dataloggerlog_supervisor_loginname_109message_112',
            test='''{{ dag_run.conf.loginname != result('log_supervisor_loginname_109') }}''',
            yes_task="_adhoc_http_action_113",
            no_task="log_get_assignedshiftcurrent_value_137",
        )

        _adhoc_http_action_113 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_113',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        log_getsupervisor_uri_114 = rail.PythonOperator(
            task_id='log_getsupervisor_uri_114',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_supervisor_details'), 'employeeid', dag_run.conf['managerid'], 'uri', "") if rail.result('get_supervisor_details') else null
        )

        if_log_getsupervisor_uri_114_blank_115 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_114_blank_115',
            test='''{{ result('log_getsupervisor_uri_114') | is_falsy }}''',
            yes_task="fdt_supervisor_assignment_table_add_entry_116",
            no_task="if_log_getsupervisor_uri_114_present_117",
        )

        fdt_supervisor_assignment_table_add_entry_116 = rail.WriteLogOperator(
            task_id='fdt_supervisor_assignment_table_add_entry_116',
            log='''{{dag_run.conf.supervisorassignmentlookuptable}}''',
            message="na",
            severity="na",
            properties={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "user_uri": "{{ dag_run.conf.useruri }}",
                "user_name": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ result('log_supervisor_loginname_109') }}",
                "supervisor_id": "{{ dag_run.conf.managerid }}",
                "action": "Update",
                "emplid": "{{ dag_run.conf.emplid }}",
                "childjobid": "{{dag_run_ecid()}}",
            }
        )

        if_log_getsupervisor_uri_114_present_117 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_114_present_117',
            test='''{{ result('log_getsupervisor_uri_114') | is_truthy }}''',
            yes_task="log_getsupervisor_status_118",
            no_task="log_get_assignedshiftcurrent_value_137",
        )

        log_getsupervisor_status_118 = rail.PythonOperator(
            task_id='log_getsupervisor_status_118',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_supervisor_details'), 'employeeid', dag_run.conf['managerid'], 'enabled', "") if rail.result('get_supervisor_details') else null
        )

        _adhoc_http_action_119 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_119',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_getsupervisor_uri_114') }}"
            }
        )

        log_checkifsupervisorhassupervisorpermission_120 = rail.PythonOperator(
            task_id='log_checkifsupervisorhassupervisorpermission_120',
            python_callable=lambda: (rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_119'), 'policyUri', "urn:replicon:policy:supervision", 'permissionSet.uri', "")) if rail.result(
                    '_adhoc_http_action_119') else null
        )

        if_log_checkifsupervisorhassupervisorpermission_120_blank_121 = rail.IfOperator(
            task_id='if_log_checkifsupervisorhassupervisorpermission_120_blank_121',
            test='''{{ result('log_checkifsupervisorhassupervisorpermission_120') | is_falsy }}''',
            yes_task="log_get_supervisor_permission_122",
            no_task="get_supervisor_assignment_details_124",
        )

        log_get_supervisor_permission_122 = rail.PythonOperator(
            task_id='log_get_supervisor_permission_122',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('_adhoc_http_action_113'), 'displayText', "Supervisor", 'uri')
        )

        assign_supervsior_permission_set_to_user_123 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_123',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('log_getsupervisor_uri_114') }}",
                "permissionSetUri": "{{ result('log_get_supervisor_permission_122') }}"
            }
        )

        get_supervisor_assignment_details_124 = rail.RepliconServiceOperator(
            task_id='get_supervisor_assignment_details_124',
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "asOfDate": {
                    "year": "{{result('date_split_todaysdate_40').year}}",
                    "month": "{{result('date_split_todaysdate_40').month}}",
                    "day": "{{result('date_split_todaysdate_40').day}}"
                }
            }
        )

        if_supervisor_displaytext_blank_urnrepliconlisttypenull_125 = rail.IfOperator(
            task_id='if_supervisor_displaytext_blank_urnrepliconlisttypenull_125',
            test='''{{ result('get_supervisor_assignment_details_124') | is_falsy }}''',
            yes_task="if_log_getsupervisor_status_118_equals_to_true_126",
            no_task="if_supervisor_displaytext_present_urnrepliconlisttypenull_130",
        )

        if_log_getsupervisor_status_118_equals_to_true_126 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_118_equals_to_true_126',
            test='''{{ result('log_getsupervisor_status_118') | is_truthy}}''',
            yes_task="update_initial_supervisor_127",
            no_task="fdt_supervisor_assignment_table_add_entry_129",
        )

        update_initial_supervisor_127 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_127',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('log_getsupervisor_uri_114') }}",
                "scheduleEntries": []
            }
        )

        fdt_supervisor_assignment_table_add_entry_129 = rail.WriteLogOperator(
            task_id='fdt_supervisor_assignment_table_add_entry_129',
            log=lambda dag_run: dag_run.conf['supervisorassignmentlookuptable'],
            message="na",
            severity="na",
            properties={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "user_uri": "{{ dag_run.conf.useruri }}",
                "user_name": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ result('log_supervisor_loginname_109') }}",
                "supervisor_id": "{{ dag_run.conf.managerid }}",
                "action": "Update",
                "emplid": "{{ dag_run.conf.emplid }}",
                "childjobid": "{{dag_run_ecid()}}",
            }
        )

        if_supervisor_displaytext_present_urnrepliconlisttypenull_130 = rail.IfOperator(
            task_id='if_supervisor_displaytext_present_urnrepliconlisttypenull_130',
            test=lambda: rail.result('get_supervisor_assignment_details_124'),
            yes_task="log_thesupervisorloginname_131",
            no_task="log_get_assignedshiftcurrent_value_137",
        )

        log_thesupervisorloginname_131 = rail.PythonOperator(
            task_id='log_thesupervisorloginname_131',
            python_callable=lambda: rail.render_template(
                "{{ result('get_supervisor_assignment_details_124').supervisor.user.loginName }}")
        )

        if_log_supervisor_loginname_109_present_132 = rail.IfOperator(
            task_id='if_log_supervisor_loginname_109_present_132',
            test='''{{ result('log_supervisor_loginname_109') | is_truthy and result('log_supervisor_loginname_109') != result('log_thesupervisorloginname_131') }}''',
            yes_task="if_log_getsupervisor_status_118_equals_to_true_133",
            no_task="log_get_assignedshiftcurrent_value_137",
        )

        if_log_getsupervisor_status_118_equals_to_true_133 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_118_equals_to_true_133',
            test='''{{ result('log_getsupervisor_status_118') | is_truthy}}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_134",
            no_task="fdt_supervisor_assignment_table_add_entry_136",
        )

        update_supervisor_assignment_schedule_over_date_range_134 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_134',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('log_getsupervisor_uri_114') }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{result('date_split_todaysdate_40').year}}",
                        "month": "{{result('date_split_todaysdate_40').month}}",
                        "day": "{{result('date_split_todaysdate_40').day}}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        fdt_supervisor_assignment_table_add_entry_136 = rail.WriteLogOperator(
            task_id='fdt_supervisor_assignment_table_add_entry_136',
            log=lambda dag_run: dag_run.conf['supervisorassignmentlookuptable'],
            message="na",
            severity="na",
            properties={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "user_uri": "{{ dag_run.conf.useruri }}",
                "user_name": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ result('log_supervisor_loginname_109') }}",
                "supervisor_id": "{{ dag_run.conf.managerid }}",
                "action": "Update",
                "emplid": "{{ dag_run.conf.emplid }}",
                "childjobid": "{{dag_run.ecid()}}",
            }
        )

        log_get_assignedshiftcurrent_value_137 = rail.PythonOperator(
            task_id='log_get_assignedshiftcurrent_value_137',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], 'displayText', "Assigned Shift", 'text', "")
        )

        if_request_assignedshift_present_138 = rail.IfOperator(
            task_id='if_request_assignedshift_present_138',
            test='''{{ dag_run.conf.assignedshift | is_truthy  and result('log_get_assignedshiftcurrent_value_137') != dag_run.conf.assignedshift }}''',
            yes_task="log_get_assigned_shifturi_139",
            no_task="if_request_hourlyratejobdata_present_146",
        )

        log_get_assigned_shifturi_139 = rail.PythonOperator(
            task_id='log_get_assigned_shifturi_139',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], 'customField.displayText', "Assigned Shift", 'customField.uri', '')
        )

        get_all_custom_field_drop_down_optionsfor_assigned_shift_140 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsfor_assigned_shift_140',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_get_assigned_shifturi_139') }}"
            }
        )

        log_get_assigned_shiftvalueuri_141 = rail.PythonOperator(
            task_id='log_get_assigned_shiftvalueuri_141',
            python_callable=lambda dag_run:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_field_drop_down_optionsfor_assigned_shift_140'), 'displayText', dag_run.conf['assignedshift'], 'uri', '')
        )

        if_log_get_assigned_shiftvalueuri_141_present_datalogger4e8bc76fmessage_142 = rail.IfOperator(
            task_id='if_log_get_assigned_shiftvalueuri_141_present_datalogger4e8bc76fmessage_142',
            test='''{{ result('log_get_assigned_shiftvalueuri_141') | is_truthy }}''',
            yes_task="update_assigned_shift_u_d_f_143",
            no_task="if_log_get_assigned_shiftvalueuri_141_blank_144",
        )

        update_assigned_shift_u_d_f_143 = rail.RepliconServiceOperator(
            task_id='update_assigned_shift_u_d_f_143',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_assigned_shifturi_139') }}",
                "customFieldDropDownOptionUri": "{{ result('log_get_assigned_shiftvalueuri_141') }}"
            }
        )

        if_log_get_assigned_shiftvalueuri_141_blank_144 = rail.IfOperator(
            task_id='if_log_get_assigned_shiftvalueuri_141_blank_144',
            test='''{{ result('log_get_assigned_shiftvalueuri_141') | is_falsy }}''',
            yes_task="log_assignedshiftnotupdated_145",
            no_task="if_request_hourlyratejobdata_present_146",
        )

        log_assignedshiftnotupdated_145 = rail.PythonOperator(
            task_id='log_assignedshiftnotupdated_145',
            python_callable=lambda:  "Assigned shift is not updated as" +
            rail.render_template(
                "{{ dag_run.conf.assignedshift }}") + "not available in Replicon"
        )

        if_request_hourlyratejobdata_present_146 = rail.IfOperator(
            task_id='if_request_hourlyratejobdata_present_146',
            test='''{{ dag_run.conf.hourlyratejobdata | is_truthy  or dag_run.conf.hourlyrate2 | is_truthy }}''',
            yes_task="_adhoc_http_action_148",
            no_task="if_request_regulartemporary_present_174",
        )

        _adhoc_http_action_148 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_148',
            endpoint="/services/CurrencyService2.svc/GetBaseCurrency"
        )

        log_get_current_pay_rate_amount_164 = rail.PythonOperator(
            task_id='log_get_current_pay_rate_amount_164',
            python_callable=lambda: python_callable.get_current_value_from_schedule_list_for_user(
                rail.result('bulk_get_users3_10')[0]['payrollRateSchedule'], 'hourlyRate', 'amount')
        )

        if_hourlyrate_not_equals_to_current_pay_rate_amount_164_165 = rail.IfOperator(
            task_id='if_hourlyrate_not_equals_to_current_pay_rate_amount_164_165',
            test=lambda dag_run: not (rail.result('log_get_current_pay_rate_amount_164')) or (
                float(dag_run.conf['hourlyratejobdata']) if dag_run.conf['assignedshift'] == "1" else float(
                    dag_run.conf['hourlyrate2'])) != float(rail.result('log_get_current_pay_rate_amount_164')),
            yes_task="put_user_payroll_rate_schedule_168",
            no_task="if_request_regulartemporary_present_174",
        )

        put_user_payroll_rate_schedule_168 = rail.RepliconServiceOperator(
            task_id='put_user_payroll_rate_schedule_168',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.get_input_payload_for_user_pay_rate_modification
        )

        if_request_regulartemporary_present_174 = rail.IfOperator(
            task_id='if_request_regulartemporary_present_174',
            test='''{{ dag_run.conf.regulartemporary | is_truthy }}''',
            yes_task="log_get_regular_temporaryuri_175",
            no_task="if_request_jobtitle_present_180",
        )

        log_get_regular_temporaryuri_175 = rail.PythonOperator(
            task_id='log_get_regular_temporaryuri_175',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Regular/Temporary", 'customField.uri')
        )

        log_get_regular_temporarycurrent_value_176 = rail.PythonOperator(
            task_id='log_get_regular_temporarycurrent_value_176',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Regular/Temporary", 'text')
        )

        if_request_regulartemporary_not_equals_to_dataloggerlog_get_regular_temporarycurrent_value_176message_177 = rail.IfOperator(
            task_id='if_request_regulartemporary_not_equals_to_dataloggerlog_get_regular_temporarycurrent_value_176message_177',
            test='''{{ dag_run.conf.regulartemporary != result('log_get_regular_temporarycurrent_value_176') }}''',
            yes_task="update_regular_temporary_u_d_f_178",
            no_task="if_request_jobtitle_present_180",
        )

        update_regular_temporary_u_d_f_178 = rail.RepliconServiceOperator(
            task_id='update_regular_temporary_u_d_f_178',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_regular_temporaryuri_175') }}",
                "value": "{{ dag_run.conf.regulartemporary }}"
            }
        )

        update_variable_179 = rail.SetVariableOperator(
            task_id='update_variable_179',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Trigger Timeoff Change"
        )

        if_request_jobtitle_present_180 = rail.IfOperator(
            task_id='if_request_jobtitle_present_180',
            test='''{{ dag_run.conf.jobtitle | is_truthy }}''',
            yes_task="log_get_job_titleuri_181",
            no_task="if_request_fullparttime_present_185",
        )

        log_get_job_titleuri_181 = rail.PythonOperator(
            task_id='log_get_job_titleuri_181',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Job Title", 'customField.uri')
        )

        log_get_job_titlecurrent_value_182 = rail.PythonOperator(
            task_id='log_get_job_titlecurrent_value_182',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Job Title", 'text')
        )

        if_request_jobtitle_not_equals_to_dataloggerlog_get_job_titlecurrent_value_182message_183 = rail.IfOperator(
            task_id='if_request_jobtitle_not_equals_to_dataloggerlog_get_job_titlecurrent_value_182message_183',
            test='''{{ dag_run.conf.jobtitle != result('log_get_job_titlecurrent_value_182') }}''',
            yes_task="update_job_title_u_d_f_184",
            no_task="if_request_fullparttime_present_185",
        )

        update_job_title_u_d_f_184 = rail.RepliconServiceOperator(
            task_id='update_job_title_u_d_f_184',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_job_titleuri_181') }}",
                "value": "{{ dag_run.conf.jobtitle }}"
            }
        )

        if_request_fullparttime_present_185 = rail.IfOperator(
            task_id='if_request_fullparttime_present_185',
            test='''{{ dag_run.conf.fullparttime | is_truthy }}''',
            yes_task="log_get_full_part_timeuri_186",
            no_task="if_declare_variable_3_value_equals_to_triggertimeoffchange_212",
        )

        log_get_full_part_timeuri_186 = rail.PythonOperator(
            task_id='log_get_full_part_timeuri_186',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_10')[
                0]['userDetails']['customFieldValues'], "customField.displayText", "Full/Part Time", 'customField.uri')
        )

        log_get_full_part_timecurrent_value_187 = rail.PythonOperator(
            task_id='log_get_full_part_timecurrent_value_187',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Full/Part Time", 'text')
        )

        if_request_fullparttime_not_equals_to_dataloggerlog_get_full_part_timecurrent_value_187message_188 = rail.IfOperator(
            task_id='if_request_fullparttime_not_equals_to_dataloggerlog_get_full_part_timecurrent_value_187message_188',
            test='''{{ dag_run.conf.fullparttime != result('log_get_full_part_timecurrent_value_187') }}''',
            yes_task="update_full_part_time_u_d_f_189",
            no_task="if_declare_variable_3_value_equals_to_triggertimeoffchange_212",
        )

        update_full_part_time_u_d_f_189 = rail.RepliconServiceOperator(
            task_id='update_full_part_time_u_d_f_189',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_full_part_timeuri_186') }}",
                "value": "{{ dag_run.conf.fullparttime }}"
            }
        )

        update_variable_190 = rail.SetVariableOperator(
            task_id='update_variable_190',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Trigger Timeoff Change"
        )

        get_all_office_schedules_191 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_191',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        declare_variable_192 = rail.SetVariableOperator(
            task_id='declare_variable_192',
            append=False,
            name='schedule',
            value="8 Hours/Day; Mon-Fri"
        )

        if_request_fullparttime_not_equals_to_f_193 = rail.IfOperator(
            task_id='if_request_fullparttime_not_equals_to_f_193',
            test='''{{ dag_run.conf.fullparttime != 'f' }}''',
            yes_task="update_variable_194",
            no_task="log_officeschedule_uri_195",
        )

        update_variable_194 = rail.SetVariableOperator(
            task_id='update_variable_194',
            append=False,
            name='{{ result("declare_variable_192").name }}',
            value="4 Hours/Day; Mon-Fri"
        )

        log_officeschedule_uri_195 = rail.PythonOperator(
            task_id='log_officeschedule_uri_195',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_office_schedules_191'), 'displayText', rail.get_dag_run_var('schedule'), 'uri')
        )

        if_log_officeschedule_uri_195_present_196 = rail.IfOperator(
            task_id='if_log_officeschedule_uri_195_present_196',
            test='''{{ result('log_officeschedule_uri_195') | is_truthy }}''',
            yes_task="declare_list_197",
            no_task="if_declare_variable_3_value_equals_to_triggertimeoffchange_212",
        )

        declare_list_197 = rail.SetVariableOperator(
            task_id='declare_list_197',
            append=False,
            name='schedule_entries',
            value=[]
        )

        if_existing_schedule_policies_present = rail.IfOperator(
            task_id='if_existing_schedule_policies_present',
            test=lambda: rail.result(
                'bulk_get_users3_10')[0]['schedulePolicies'],
            yes_task='foreach_schedule_policy_assigned_to_user',
            no_task='if_declare_list_197_list_items_less_than_1_206'
        )

        foreach_schedule_policy_assigned_to_user = rail.ForEachOperator(
            task_id='foreach_schedule_policy_assigned_to_user',
            items=lambda: rail.result(
                'bulk_get_users3_10')[0]['schedulePolicies'],
            start_task='if_effectivedate_day_blank_200',
            end_task='foreach_schedule_policy_assigned_to_user_end'
        )

        if_effectivedate_day_blank_200 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_200',
            test=lambda: rail.result('foreach_schedule_policy_assigned_to_user')[
                'effectiveDate'],
            yes_task="log_effective_date_203",
            no_task="insert_to_list_201",
        )

        insert_to_list_201 = rail.SetVariableOperator(
            task_id='insert_to_list_201',
            append=True,
            name='{{ result("declare_list_197").name }}',
            value={
                "schedulePolicy": {
                    "officeScheduleUri": "{{ result('foreach_schedule_policy_assigned_to_user').officeSchedule.uri }}",
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "{{ result('foreach_schedule_policy_assigned_to_user').scheduleTypeUri }}"
                },
                "effectiveDate": null
            }
        )

        log_effective_date_203 = rail.PythonOperator(
            task_id='log_effective_date_203',
            python_callable=lambda:  rail.render_template(
                "{{ result('foreach_schedule_policy_assigned_to_user').effectiveDate.day }}/{{ result('foreach_schedule_policy_assigned_to_user').effectiveDate.month }}/{{ result('foreach_schedule_policy_assigned_to_user').effectiveDate.year }}")
        )

        if_to_date_to_time_not_equals_to_todayto_time_204 = rail.IfOperator(
            task_id='if_to_date_to_time_not_equals_to_todayto_time_204',
            test=lambda: bool(datetime.strptime(rail.result(
                'log_effective_date_203'), "%d/%m/%Y").date() != now().date()),
            yes_task="insert_to_list_205",
            no_task="foreach_schedule_policy_assigned_to_user_end",
        )

        insert_to_list_205 = rail.SetVariableOperator(
            task_id='insert_to_list_205',
            append=True,
            name='{{ result("declare_list_197").name }}',
            value={
                "schedulePolicy": {
                    "officeScheduleUri": "{{ result('foreach_schedule_policy_assigned_to_user').officeSchedule.uri }}",
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "{{ result('foreach_schedule_policy_assigned_to_user').scheduleTypeUri }}"
                },
                "effectiveDate": {
                    "year": "{{ result('foreach_schedule_policy_assigned_to_user').effectiveDate.year }}",
                    "month": "{{ result('foreach_schedule_policy_assigned_to_user').effectiveDate.month }}",
                    "day": "{{ result('foreach_schedule_policy_assigned_to_user').effectiveDate.day }}"
                }
            }
        )

        foreach_schedule_policy_assigned_to_user_end = rail.EmptyOperator(
            task_id='foreach_schedule_policy_assigned_to_user_end',
        )

        if_declare_list_197_list_items_less_than_1_206 = rail.IfOperator(
            task_id='if_declare_list_197_list_items_less_than_1_206',
            test=lambda: bool(
                len(rail.get_dag_run_var("schedule_entries")) < 1),
            yes_task="put_schedule_policy_schedule_for_user_207",
            no_task="if_declare_list_197_list_items_greater_than_0_208",
        )

        put_schedule_policy_schedule_for_user_207 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_207',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": "{{ result('log_officeschedule_uri_195') }}",
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_declare_list_197_list_items_greater_than_0_208 = rail.IfOperator(
            task_id='if_declare_list_197_list_items_greater_than_0_208',
            test=lambda: bool(
                len(rail.get_dag_run_var("schedule_entries")) > 0),
            yes_task="insert_to_list_209",
            no_task="if_declare_variable_3_value_equals_to_triggertimeoffchange_212",
        )

        insert_to_list_209 = rail.SetVariableOperator(
            task_id='insert_to_list_209',
            append=True,
            name='{{ result("declare_list_197").name }}',
            value={
                "schedulePolicy": {
                    "officeScheduleUri": "{{ result('log_officeschedule_uri_195') }}",
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                },
                "effectiveDate": {
                    "year": int(now().strftime("%Y")),
                    "month": int(now().strftime("%m")),
                    "day": int(now().strftime("%d"))
                }
            }
        )

        log_getoffice_scheduleentriestobeassigned_210 = rail.PythonOperator(
            task_id='log_getoffice_scheduleentriestobeassigned_210',
            python_callable=lambda: rail.get_dag_run_var("schedule_entries")
        )

        put_schedule_policy_schedule_for_user_211 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_211',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": "{{ result('log_getoffice_scheduleentriestobeassigned_210') }}"
            }
        )

        if_declare_variable_3_value_equals_to_triggertimeoffchange_212 = rail.IfOperator(
            task_id='if_declare_variable_3_value_equals_to_triggertimeoffchange_212',
            test=lambda: bool(rail.get_dag_run_var(
                'timeoff_transition_trigger') == 'Trigger Timeoff Change' and rail.get_dag_run_var('trigger_rehire') != 'Yes'),
            yes_task="log_get_adjusted_start_datefortransitionuri_213",
            no_task="if_request_paygroup_present_216",
        )

        log_get_adjusted_start_datefortransitionuri_213 = rail.PythonOperator(
            task_id='log_get_adjusted_start_datefortransitionuri_213',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Adjusted Start Date for transition", 'customField.uri')
        )

        update_adjusted_start_datefortransition_u_d_f_214 = rail.RepliconServiceOperator(
            task_id='update_adjusted_start_datefortransition_u_d_f_214',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_adjusted_start_datefortransitionuri_213') }}",
                "value": {
                    "year": "{{result('date_split_todaysdate_40').year}}",
                    "month":  "{{result('date_split_todaysdate_40').month}}",
                    "day":  "{{result('date_split_todaysdate_40').day }}"
                }
            }
        )

        trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_existing_user_ftpt_or_rt_change_v3_0215 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_existing_user_ftpt_or_rt_change_v3_0215',
            retries=0,
            trigger_dag_id=f'fujifilmdbtl_child_add_remove_timeoff_type_for_existing_user_ftpt_or_rt_change_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "ftpt": dag_run.conf['fullparttime'],
                "regulartemp": dag_run.conf['regulartemporary'],
                "startdatemonth": now().strftime("%b"),
                "startdate": rail.render_template("{{result('date_split_todaysdate_40').day}}/{{result('date_split_todaysdate_40').month}}/{{result('date_split_todaysdate_40').year}}")
            }
        )

        wait_for_completion_trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_existing_user_ftpt_or_rt_change_v3_0215 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_existing_user_ftpt_or_rt_change_v3_0215',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_existing_user_ftpt_or_rt_change_v3_0215") }}'
        )

        if_request_paygroup_present_216 = rail.IfOperator(
            task_id='if_request_paygroup_present_216',
            test='''{{ dag_run.conf.paygroup | is_truthy }}''',
            yes_task="log_get_pay_groupuri_217",
            no_task="if_request_eestatus_present_221",
        )

        log_get_pay_groupuri_217 = rail.PythonOperator(
            task_id='log_get_pay_groupuri_217',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Pay Group", 'customField.uri')
        )

        log_get_pay_groupcurrent_value_218 = rail.PythonOperator(
            task_id='log_get_pay_groupcurrent_value_218',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Pay Group", 'text')
        )

        if_request_paygroup_not_equals_to_dataloggerlog_get_pay_groupcurrent_value_218message_219 = rail.IfOperator(
            task_id='if_request_paygroup_not_equals_to_dataloggerlog_get_pay_groupcurrent_value_218message_219',
            test='''{{ dag_run.conf.paygroup != result('log_get_pay_groupcurrent_value_218') }}''',
            yes_task="update_pay_group_u_d_f_220",
            no_task="if_request_eestatus_present_221",
        )

        update_pay_group_u_d_f_220 = rail.RepliconServiceOperator(
            task_id='update_pay_group_u_d_f_220',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_pay_groupuri_217') }}",
                "value": "{{ dag_run.conf.paygroup }}"
            }
        )

        if_request_eestatus_present_221 = rail.IfOperator(
            task_id='if_request_eestatus_present_221',
            test='''{{ dag_run.conf.eestatus | is_truthy }}''',
            yes_task="log_get_e_e_statusuri_222",
            no_task="if_request_company_present_226",
        )

        log_get_e_e_statusuri_222 = rail.PythonOperator(
            task_id='log_get_e_e_statusuri_222',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "EE Status", 'customField.uri')
        )

        log_get_e_e_statuscurrent_value_223 = rail.PythonOperator(
            task_id='log_get_e_e_statuscurrent_value_223',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "EE Status", 'text')
        )

        if_request_eestatus_not_equals_to_dataloggerlog_get_e_e_statuscurrent_value_223message_224 = rail.IfOperator(
            task_id='if_request_eestatus_not_equals_to_dataloggerlog_get_e_e_statuscurrent_value_223message_224',
            test='''{{ dag_run.conf.eestatus != result('log_get_e_e_statuscurrent_value_223') }}''',
            yes_task="update_e_e_status_u_d_f_225",
            no_task="if_request_company_present_226",
        )

        update_e_e_status_u_d_f_225 = rail.RepliconServiceOperator(
            task_id='update_e_e_status_u_d_f_225',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_e_e_statusuri_222') }}",
                "value": "{{ dag_run.conf.eestatus }}"
            }
        )

        if_request_company_present_226 = rail.IfOperator(
            task_id='if_request_company_present_226',
            test='''{{ dag_run.conf.company | is_truthy }}''',
            yes_task="log_get_companyuri_227",
            no_task="if_request_managerid_present_231",
        )

        log_get_companyuri_227 = rail.PythonOperator(
            task_id='log_get_companyuri_227',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Company", 'customField.uri')
        )

        log_get_companycurrent_value_228 = rail.PythonOperator(
            task_id='log_get_companycurrent_value_228',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Company", 'text')
        )

        if_request_company_not_equals_to_dataloggerlog_get_companycurrent_value_228message_229 = rail.IfOperator(
            task_id='if_request_company_not_equals_to_dataloggerlog_get_companycurrent_value_228message_229',
            test='''{{ dag_run.conf.company != result('log_get_companycurrent_value_228') }}''',
            yes_task="update_company_u_d_f_230",
            no_task="if_request_managerid_present_231",
        )

        update_company_u_d_f_230 = rail.RepliconServiceOperator(
            task_id='update_company_u_d_f_230',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_companyuri_227') }}",
                "value": "{{ dag_run.conf.company }}"
            }
        )

        if_request_managerid_present_231 = rail.IfOperator(
            task_id='if_request_managerid_present_231',
            test='''{{ dag_run.conf.managerid | is_truthy }}''',
            yes_task="log_get_manager_i_duri_232",
            no_task="if_request_autolinkratetype_present_236",
        )

        log_get_manager_i_duri_232 = rail.PythonOperator(
            task_id='log_get_manager_i_duri_232',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Manager ID", 'customField.uri')
        )

        log_get_manager_i_dcurrent_value_233 = rail.PythonOperator(
            task_id='log_get_manager_i_dcurrent_value_233',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Manager ID", 'text')
        )

        if_request_managerid_not_equals_to_dataloggerlog_get_manager_i_dcurrent_value_233message_234 = rail.IfOperator(
            task_id='if_request_managerid_not_equals_to_dataloggerlog_get_manager_i_dcurrent_value_233message_234',
            test='''{{ dag_run.conf.managerid != result('log_get_manager_i_dcurrent_value_233') }}''',
            yes_task="update_manager_i_d_u_d_f_235",
            no_task="if_request_autolinkratetype_present_236",
        )

        update_manager_i_d_u_d_f_235 = rail.RepliconServiceOperator(
            task_id='update_manager_i_d_u_d_f_235',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_manager_i_duri_232') }}",
                "value": "{{ dag_run.conf.managerid }}"
            }
        )

        if_request_autolinkratetype_present_236 = rail.IfOperator(
            task_id='if_request_autolinkratetype_present_236',
            test='''{{ dag_run.conf.autolinkratetype | is_truthy }}''',
            yes_task="log_get_autolink_rate_typeuri_237",
            no_task="if_request_annualsalary_present_241",
        )

        log_get_autolink_rate_typeuri_237 = rail.PythonOperator(
            task_id='log_get_autolink_rate_typeuri_237',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Autolink Rate Type", 'customField.uri')
        )

        log_get_autolink_rate_typecurrent_value_238 = rail.PythonOperator(
            task_id='log_get_autolink_rate_typecurrent_value_238',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Autolink Rate Type", 'text')
        )

        if_request_autolinkratetype_not_equals_to_dataloggerlog_get_autolink_rate_typecurrent_value_238message_239 = rail.IfOperator(
            task_id='if_request_autolinkratetype_not_equals_to_dataloggerlog_get_autolink_rate_typecurrent_value_238message_239',
            test='''{{ dag_run.conf.autolinkratetype != result('log_get_autolink_rate_typecurrent_value_238') }}''',
            yes_task="update_autolink_rate_type_u_d_f_240",
            no_task="if_request_annualsalary_present_241",
        )

        update_autolink_rate_type_u_d_f_240 = rail.RepliconServiceOperator(
            task_id='update_autolink_rate_type_u_d_f_240',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_autolink_rate_typeuri_237') }}",
                "value": "{{ dag_run.conf.autolinkratetype }}"
            }
        )

        if_request_annualsalary_present_241 = rail.IfOperator(
            task_id='if_request_annualsalary_present_241',
            test='''{{ dag_run.conf.annualsalary | is_truthy }}''',
            yes_task="log_get_gross_annual_salaryuri_242",
            no_task="if_request_rehiredate_present_249",
        )

        log_get_gross_annual_salaryuri_242 = rail.PythonOperator(
            task_id='log_get_gross_annual_salaryuri_242',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Gross Annual Salary", 'customField.uri')
        )

        log_get_gross_annual_salarycurrent_value_243 = rail.PythonOperator(
            task_id='log_get_gross_annual_salarycurrent_value_243',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Gross Annual Salary", 'text')
        )

        if_request_annualsalary_not_equals_to_dataloggerlog_get_gross_annual_salarycurrent_value_243message_244 = rail.IfOperator(
            task_id='if_request_annualsalary_not_equals_to_dataloggerlog_get_gross_annual_salarycurrent_value_243message_244',
            test='''{{ dag_run.conf.annualsalary != result('log_get_gross_annual_salarycurrent_value_243') }}''',
            yes_task="update_gross_annual_salary_u_d_f_245",
            no_task="if_request_rehiredate_present_249",
        )

        update_gross_annual_salary_u_d_f_245 = rail.RepliconServiceOperator(
            task_id='update_gross_annual_salary_u_d_f_245',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_gross_annual_salaryuri_242') }}",
                "value": "{{ dag_run.conf.annualsalary }}"
            }
        )

        log_get_biweekly_gross_wagesuri_246 = rail.PythonOperator(
            task_id='log_get_biweekly_gross_wagesuri_246',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_10')[
                0]['userDetails']['customFieldValues'], "customField.displayText", "Biweekly Gross Wages", 'customField.uri')
        )

        log_biweekly_gross_wages_247 = rail.PythonOperator(
            task_id='log_biweekly_gross_wages_247',
            python_callable=lambda dag_run: round(
                (float(dag_run.conf['annualsalary'])) / 26) if dag_run.conf['annualsalary'] else 0
        )

        update_biweekly_gross_wages_u_d_f_248 = rail.RepliconServiceOperator(
            task_id='update_biweekly_gross_wages_u_d_f_248',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_biweekly_gross_wagesuri_246') }}",
                "value": "{{ result('log_biweekly_gross_wages_247') }}"
            }
        )

        if_request_rehiredate_present_249 = rail.IfOperator(
            task_id='if_request_rehiredate_present_249',
            test='''{{ dag_run.conf.rehiredate | is_truthy }}''',
            yes_task="if_request_rehiredate_not_contains_250",
            no_task="fdt_user_import_logs_add_entry_283",
        )

        if_request_rehiredate_not_contains_250 = rail.IfOperator(
            task_id='if_request_rehiredate_not_contains_250',
            test=lambda dag_run: bool("/" not in dag_run.conf['rehiredate']),
            yes_task="log_logforincorrectrehiredateformat_251",
            no_task="log_rehire_datein_time_253",
        )

        log_logforincorrectrehiredateformat_251 = rail.PythonOperator(
            task_id='log_logforincorrectrehiredateformat_251',
            python_callable=lambda:  "Rehire date is not in the correct format"
        )

        log_rehire_datein_time_253 = rail.PythonOperator(
            task_id='log_rehire_datein_time_253',
            python_callable=lambda dag_run:  datetime.strptime(
                dag_run.conf['rehiredate'], "%m/%d/%Y").isoformat()
        )

        log_get_rehire_start_dateuri_254 = rail.PythonOperator(
            task_id='log_get_rehire_start_dateuri_254',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_10')[
                0]['userDetails']['customFieldValues'], "customField.displayText", "Rehire Start Date", 'customField.uri')
        )

        log_get_rehire_start_datecurrent_value_255 = rail.PythonOperator(
            task_id='log_get_rehire_start_datecurrent_value_255',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_10')[
                0]['userDetails']['customFieldValues'], "customField.displayText", "Rehire Start Date", 'text')
        )

        if_rehire_date_not_equals_to_current_rehire_date_256 = rail.IfOperator(
            task_id='if_rehire_date_not_equals_to_current_rehire_date_256',
            test=lambda dag_run: not (rail.result('log_get_rehire_start_datecurrent_value_255')) or (
                python_callable.to_datetime(dag_run.conf['rehiredate'], "%m/%d/%Y") != python_callable.to_datetime(rail.result(
                    'log_get_rehire_start_datecurrent_value_255'), "%B %d, %Y")),
            yes_task="invoke_custom_ruby_code_rehire_date_257",
            no_task="fdt_user_import_logs_add_entry_283",
        )

        invoke_custom_ruby_code_rehire_date_257 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_rehire_date_257',
            python_callable=lambda dag_run: python_callable.get_split_date(
                dag_run.conf['rehiredate'], "%m/%d/%Y")
        )

        update_rehire_start_date_u_d_f_258 = rail.RepliconServiceOperator(
            task_id='update_rehire_start_date_u_d_f_258',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_rehire_start_dateuri_254') }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_rehire_date_257').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_rehire_date_257').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_rehire_date_257').day }}"
                }
            }
        )

        if_declare_variable_2_value_equals_to_yes_259 = rail.IfOperator(
            task_id='if_declare_variable_2_value_equals_to_yes_259',
            test=lambda: rail.get_dag_run_var('trigger_rehire') == 'Yes',
            yes_task="if_enddate_day_present_260",
            no_task="fdt_user_import_logs_add_entry_283",
        )

        if_enddate_day_present_260 = rail.IfOperator(
            task_id='if_enddate_day_present_260',
            test='''{{ result('bulk_get_users3_10')[0].userDetails.employmentDateRange.endDate | is_truthy }}''',
            yes_task="log_end_datein_d_d_m_m_y_y_y_y_261",
            no_task="log_rehire_error_282",
        )

        log_end_datein_d_d_m_m_y_y_y_y_261 = rail.PythonOperator(
            task_id='log_end_datein_d_d_m_m_y_y_y_y_261',
            python_callable=lambda:  rail.render_template(
                "{{ result('bulk_get_users3_10')[0].userDetails.employmentDateRange.endDate.day }}/{{ result('bulk_get_users3_10')[0].userDetails.employmentDateRange.endDate.month }}/{{ result('bulk_get_users3_10')[0].userDetails.employmentDateRange.endDate.year }}")
        )

        log_total_termination_tenure_262 = rail.PythonOperator(
            task_id='log_total_termination_tenure_262',
            python_callable=lambda dag_run:  float((python_callable.to_datetime(dag_run.conf['rehiredate'], "%m/%d/%Y") - python_callable.to_datetime(
                rail.result('log_end_datein_d_d_m_m_y_y_y_y_261'), "%d/%m/%Y")).days / 365)
        )

        log_total_service_tenure_263 = rail.PythonOperator(
            task_id='log_total_service_tenure_263',
            python_callable=lambda dag_run:  float((python_callable.to_datetime(rail.result(
                'log_end_datein_d_d_m_m_y_y_y_y_261'), "%d/%m/%Y") - python_callable.to_datetime(dag_run.conf['servicedate'], "%m/%d/%Y")).days / 365)
        )

        log_get_adjusted_service_dateuri_264 = rail.PythonOperator(
            task_id='log_get_adjusted_service_dateuri_264',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_10')[
                0]['userDetails']['customFieldValues'], "customField.displayText", "Adjusted Service Date", 'customField.uri')
        )

        if_log_total_termination_tenure_262_greater_than_total_service_tenure_263_treat_the_rehire_as_new_user_265 = rail.IfOperator(
            task_id='if_log_total_termination_tenure_262_greater_than_total_service_tenure_263_treat_the_rehire_as_new_user_265',
            test='''{{ result('log_total_termination_tenure_262') > result('log_total_service_tenure_263') }}''',
            yes_task="trigger_dag_child_add_remove_to_type_for_rehire_new_user_0266",
            no_task="if_to_f_less_than_1_treattherehirebasedontheactualservicetenure_269",
        )

        trigger_dag_child_add_remove_to_type_for_rehire_new_user_0266 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_child_add_remove_to_type_for_rehire_new_user_0266',
            retries=0,
            trigger_dag_id=f'fujifilmdbtl_child_add_remove_timeoff_type_for_rehire_new_user_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "ftpt": dag_run.conf['fullparttime'],
                "regulartemp": dag_run.conf['regulartemporary'],
                "startdatemonth": datetime.strftime(rail.result('log_service_datein_time_8'), "%b"),
                "rehiredate": rail.render_template(
                    "{{ result('invoke_custom_ruby_code_rehire_date_257').month }}/{{ result('invoke_custom_ruby_code_rehire_date_257').day }}/{{ result('invoke_custom_ruby_code_rehire_date_257').year }}")
            }
        )

        wait_for_completion_trigger_fdt_child_add_remove_to_type_for_rehire_new_user_0266 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_fdt_child_add_remove_to_type_for_rehire_new_user_0266',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_child_add_remove_to_type_for_rehire_new_user_0266") }}'
        )

        update_adjusted_service_date_u_d_f_267 = rail.RepliconServiceOperator(
            task_id='update_adjusted_service_date_u_d_f_267',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_adjusted_service_dateuri_264') }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_rehire_date_257').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_rehire_date_257').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_rehire_date_257').day }}"
                }
            }
        )

        if_to_f_less_than_1_treattherehirebasedontheactualservicetenure_269 = rail.IfOperator(
            task_id='if_to_f_less_than_1_treattherehirebasedontheactualservicetenure_269',
            test=lambda: bool(
                float(rail.result('log_total_termination_tenure_262')) < 1),
            yes_task="trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_termination_1_v3_0270",
            no_task="trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_new_user_v3_0273",
        )

        trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_termination_1_v3_0270 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_termination_1_v3_0270',
            retries=0,
            trigger_dag_id=f'fujifilmdbtl_child_add_remove_timeoff_type_for_rehire_termination_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "ftpt": dag_run.conf['fullparttime'],
                "regulartemp": dag_run.conf['regulartemporary'],
                "startdatemonth": python_callable.to_datetime(dag_run.conf['rehiredate']).strftime("%b"),
                "rehiredate": rail.render_template(
                    "{{ result('invoke_custom_ruby_code_rehire_date_257').day }}/{{ result('invoke_custom_ruby_code_rehire_date_257').month }}/{{ result('invoke_custom_ruby_code_rehire_date_257').year }}"),
                "startdate":  rail.render_template(
                    "{{ result('invoke_custom_ruby_code_service_date_9').day }}/{{ result('invoke_custom_ruby_code_service_date_9').month }}/{{ result('invoke_custom_ruby_code_service_date_9').year }}"),
                "enddate":  rail.render_template(
                    "{{result('date_split_todaysdate_40').day}}/{{result('date_split_todaysdate_40').month}}/{{result('date_split_todaysdate_40').year}}")
            }
        )

        wait_for_completion_trigger_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_termination_1_v3_0270 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_termination_1_v3_0270',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_termination_1_v3_0270") }}'
        )

        update_adjusted_service_date_u_d_f_271 = rail.RepliconServiceOperator(
            task_id='update_adjusted_service_date_u_d_f_271',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_adjusted_service_dateuri_264') }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_service_date_9').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_service_date_9').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_service_date_9').day }}"
                }
            }
        )

        trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_new_user_v3_0273 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_new_user_v3_0273',
            retries=0,
            trigger_dag_id=f'fujifilmdbtl_child_add_remove_timeoff_type_for_rehire_new_user_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "ftpt": dag_run.conf['fullparttime'],
                "regulartemp": dag_run.conf['regulartemporary'],
                "startdatemonth": python_callable.to_datetime(dag_run.conf['rehiredate']).strftime("%b"),
                "rehiredate": rail.render_template("{{ result('invoke_custom_ruby_code_rehire_date_257').month }}/{{ result('invoke_custom_ruby_code_rehire_date_257').date }}/{{ result('invoke_custom_ruby_code_rehire_date_257').year }}")
            }
        )

        wait_for_completion_trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_new_user_v3_0273 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_new_user_v3_0273',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_new_user_v3_0273") }}'
        )

        log_derived_adjusted_start_date_basedon_formula_start_date_rehire_date_end_date_274 = rail.PythonOperator(
            task_id='log_derived_adjusted_start_date_basedon_formula_start_date_rehire_date_end_date_274',
            python_callable=lambda:  datetime.strptime(rail.result('log_service_datein_time_8'), "%m/%d/%Y") + timedelta(
                days=int(rail.result('log_total_termination_tenure_262'))*365)
        )

        invoke_custom_ruby_code_adjusted_start_date_275 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_adjusted_start_date_275',
            python_callable=lambda: rail.result(
                'log_derived_adjusted_start_date_basedon_formula_start_date_rehire_date_end_date_274')
        )

        update_adjusted_service_date_u_d_f_276 = rail.RepliconServiceOperator(
            task_id='update_adjusted_service_date_u_d_f_276',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_adjusted_service_dateuri_264') }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_adjusted_start_date_275').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_adjusted_start_date_275').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_adjusted_start_date_275').day }}"
                }
            }
        )

        log_get_dateforrehirecalculationuri_277 = rail.PythonOperator(
            task_id='log_get_dateforrehirecalculationuri_277',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_10')[0]['userDetails']['customFieldValues'], "customField.displayText", "Date for rehire calculation", 'customField.uri')
        )

        log_derived_dateforrehirecalculation_basedon_formula_rehire_date1_year_278 = rail.PythonOperator(
            task_id='log_derived_dateforrehirecalculation_basedon_formula_rehire_date1_year_278',
            python_callable=lambda dag_run: (python_callable.to_datetime(
                dag_run.conf['rehiredate']) + timedelta(days=365)).strftime("%m/%d/%Y")
        )

        invoke_custom_ruby_code_dateforrehirecalculation_279 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_dateforrehirecalculation_279',
            python_callable=lambda: python_callable.get_split_date(rail.result(
                'log_derived_dateforrehirecalculation_basedon_formula_rehire_date1_year_278'))
        )

        update_dateforrehirecalculation_u_d_f_280 = rail.RepliconServiceOperator(
            task_id='update_dateforrehirecalculation_u_d_f_280',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_get_dateforrehirecalculationuri_277') }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_dateforrehirecalculation_279').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_dateforrehirecalculation_279').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_dateforrehirecalculation_279').day }}"
                }
            }
        )

        log_rehire_error_282 = rail.PythonOperator(
            task_id='log_rehire_error_282',
            python_callable=lambda:  "End date not present in the user profile. Rehire calucations for timeoff update can not be processed"
        )

        fdt_user_import_logs_add_entry_283 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_283',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="na",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "username": rail.render_template("{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"),
                "loginname": dag_run.conf['loginname'],
                "emplid": dag_run.conf['emplid'],
                "action": "Update",
                "status": str("Exception" if rail.result('log_error_logfordepartmentidblank_102') else (
                    "Exception" if rail.result('log_error_logfordepartmentnotpresent_100') else (
                        "Exception" if rail.result('log_errorforsupervisorassignment_107') else (
                            "Exception" if rail.result('log_error_messagefor_supervisoranduserloginnamesame_111') else (
                                "Exception" if rail.result('log_rehire_error_282') else (
                                    "Exception" if rail.result('log_logforincorrectrehiredateformat_251') else (
                                        "Exception" if rail.result('log_assignedshiftnotupdated_145') else "Success"))))))),
                "details": rail.smartjoin_by_delim(str("Updated Successfully" + "," + str(
                    rail.result('log_error_logfordepartmentidblank_102') or "") + "," + str(
                        rail.result('log_error_logfordepartmentnotpresent_100') or "") + "," + str(
                            rail.result('log_errorforsupervisorassignment_107') or "") + "," + str(
                                rail.result('log_error_messagefor_supervisoranduserloginnamesame_111') or "") + "," + str(
                                    rail.result('log_assignedshiftnotupdated_145') or "") + "," + str(rail.result('log_rehire_error_282') or "") + "," + str(
                                        rail.result('log_logforincorrectrehiredateformat_251') or "")).split(","), ";"),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "action": "Update",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "childjobid": "{{dag_run_ecid()}}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> declare_variable_2
        declare_variable_2 >> declare_variable_3 >> if_request_servicedate_not_contains_5
        if_request_servicedate_not_contains_5 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entry_6 >> catch_and_log_errors
        if_request_servicedate_not_contains_5 >> rail.Label(
            'No') >> log_service_datein_time_8 >> invoke_custom_ruby_code_service_date_9 >> bulk_get_users3_10 >> if_request_servicedate_present_11
        if_request_servicedate_present_11 >> rail.Label(
            'Yes') >> log_start_dateasperfeedfile_12 >> log_start_dateasper_repliconprofile_13 >> if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_13messageto_date_14
        if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_13messageto_date_14 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entry_15 >> catch_and_log_errors
        if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_13messageto_date_14 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_17
        if_request_servicedate_present_11 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_17
        if_userdetails_isenabled_is_not_true_17 >> rail.Label(
            'Yes') >> if_request_rehiredate_not_contains_18
        if_request_rehiredate_not_contains_18 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entry_19 >> catch_and_log_errors
        if_request_rehiredate_not_contains_18 >> rail.Label(
            'No') >> enable_login_21 >> update_employment_date_rangetoremoveenddate_22 >> update_variable_23 >> if_request_firstname_present_24
        if_userdetails_isenabled_is_not_true_17 >> rail.Label(
            'No') >> if_request_firstname_present_24
        if_request_firstname_present_24 >> rail.Label(
            'Yes') >> update_first_name_25 >> if_request_regulartemporary_equals_to_t_26
        if_request_firstname_present_24 >> rail.Label(
            'No') >> if_request_regulartemporary_equals_to_t_26
        if_request_regulartemporary_equals_to_t_26 >> rail.Label(
            'Yes') >> remove_holiday_calendarassignmentsforusers_27 >> if_request_regulartemporary_not_equals_to_t_28
        if_request_regulartemporary_equals_to_t_26 >> rail.Label(
            'No') >> if_request_regulartemporary_not_equals_to_t_28
        if_request_regulartemporary_not_equals_to_t_28 >> rail.Label(
            'Yes') >> get_holiday_calendar_for_new_users_29 >> if_d_displaytext_present_30
        if_d_displaytext_present_30 >> rail.Label(
            'Yes') >> add_default_holiday_calendarforuser_31 >> if_request_loginname_present_32
        if_d_displaytext_present_30 >> rail.Label(
            'No') >> if_request_loginname_present_32
        if_request_regulartemporary_not_equals_to_t_28 >> rail.Label(
            'No') >> if_request_loginname_present_32
        if_request_loginname_present_32 >> rail.Label(
            'Yes') >> apply_user_modifications_updateloginname_33 >> if_request_email_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34
        if_request_loginname_present_32 >> rail.Label(
            'No') >> if_request_email_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34
        if_request_email_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34 >> rail.Label(
            'Yes') >> update_email_35 >> if_request_lastname_present_36
        if_request_email_present_dataworkato_servicereceive_requestrequestlastnamedowncase_34 >> rail.Label(
            'No') >> if_request_lastname_present_36
        if_request_lastname_present_36 >> rail.Label(
            'Yes') >> update_last_name_37 >> if_request_emplid_present_38
        if_request_lastname_present_36 >> rail.Label(
            'No') >> if_request_emplid_present_38
        if_request_emplid_present_38 >> rail.Label(
            'Yes') >> update_employee_id_39 >> date_split_todaysdate_40

        if_request_emplid_present_38 >> rail.Label(
            'No') >> date_split_todaysdate_40 >> if_request_eetype_present_fulltimehourly_41
        if_request_eetype_present_fulltimehourly_41 >> rail.Label(
            'Yes') >> declare_variable_42 >> if_request_eetype_equals_to_h_43
        if_request_eetype_equals_to_h_43 >> rail.Label(
            'Yes') >> update_variable_44 >> if_request_eetype_equals_to_s_45
        if_request_eetype_equals_to_h_43 >> rail.Label(
            'No') >> if_request_eetype_equals_to_s_45
        if_request_eetype_equals_to_s_45 >> rail.Label(
            'Yes') >> update_variable_46 >> _adhoc_http_action_47
        if_request_eetype_equals_to_s_45 >> rail.Label(
            'No') >> _adhoc_http_action_47 >> if_log_employee_type_uri_48_present_49
        if_log_employee_type_uri_48_present_49 >> rail.Label(
            'Yes') >> update_employee_type_for_user_50 >> _adhoc_http_action_51
        if_log_employee_type_uri_48_present_49 >> rail.Label(
            'No') >> _adhoc_http_action_51 >> if_request_eetype_equals_to_s_52
        if_request_eetype_equals_to_s_52 >> rail.Label(
            'Yes') >> log_pluckif_pay_ruleispresent_62

        log_pluckif_pay_ruleispresent_62 >> log_get_pay_rule_script_uri_63 \
            >> get_payrulescript_schedule_list_to_apply_hourly >> if_payrulescript_schedule_list_to_apply_hourly

        if_payrulescript_schedule_list_to_apply_hourly >> rail.Label(
            'Yes') >> put_payroll_assignment_hourly >> if_request_eetype_equals_to_h_72
        if_payrulescript_schedule_list_to_apply_hourly >> rail.Label(
            'No') >> if_request_eetype_equals_to_h_72

        if_request_eetype_equals_to_s_52 >> rail.Label(
            'No') >> if_request_eetype_equals_to_h_72

        if_request_eetype_equals_to_h_72 >> rail.Label(
            'Yes') >> log_pluckif_pay_ruleispresent_82 >> log_get_pay_rule_script_uri_83 >> get_payrulescript_schedule_list_to_apply_salaried

        get_payrulescript_schedule_list_to_apply_salaried >> if_payrulescript_schedule_list_to_apply_salaried

        if_payrulescript_schedule_list_to_apply_salaried >> rail.Label(
            'Yes') >> put_payroll_assignment_salaried >> if_request_department_present_92

        if_payrulescript_schedule_list_to_apply_salaried >> rail.Label(
            'No') >> if_request_department_present_92

        if_request_eetype_equals_to_h_72 >> rail.Label(
            'No') >> if_request_department_present_92
        if_request_eetype_present_fulltimehourly_41 >> rail.Label(
            'No') >> if_request_department_present_92
        if_request_department_present_92 >> rail.Label(
            'Yes') >> if_request_deptid_present_93
        if_request_deptid_present_93 >> rail.Label(
            'Yes') >> get_datafordepartmentbasedonnameandcode_94 >> get_department_search_result_list \
            >> log_departmenturi_96 >> if_log_departmenturi_96_present_97
        if_log_departmenturi_96_present_97 >> rail.Label(
            'Yes') >> update_department_for_user_98 >> log_error_logfordepartmentidblank_102
        if_log_departmenturi_96_present_97 >> rail.Label(
            'No') >> log_error_logfordepartmentnotpresent_100 >> if_request_managerid_present_103
        if_request_deptid_present_93 >> rail.Label(
            'No') >> log_error_logfordepartmentidblank_102 >> if_request_managerid_present_103

        if_request_department_present_92 >> rail.Label(
            'No') >> if_request_managerid_present_103
        if_request_managerid_present_103 >> rail.Label(
            'Yes') >> search_users_104 >> log_getthenumberofuserswithsameemployeeid_105 >> if_log_getthenumberofuserswithsameemployeeid_105_present_106
        if_log_getthenumberofuserswithsameemployeeid_105_present_106 >> rail.Label(
            'Yes') >> log_errorforsupervisorassignment_107 >> log_get_assignedshiftcurrent_value_137
        if_log_getthenumberofuserswithsameemployeeid_105_present_106 >> rail.Label(
            'No') >> get_supervisor_details >> log_supervisor_loginname_109 >> if_request_loginname_equals_to_dataloggerlog_supervisor_loginname_109message_110
        if_request_loginname_equals_to_dataloggerlog_supervisor_loginname_109message_110 >> rail.Label(
            'Yes') >> log_error_messagefor_supervisoranduserloginnamesame_111 \
            >> if_request_loginname_not_equals_to_dataloggerlog_supervisor_loginname_109message_112
        if_request_loginname_equals_to_dataloggerlog_supervisor_loginname_109message_110 >> rail.Label(
            'No') >> if_request_loginname_not_equals_to_dataloggerlog_supervisor_loginname_109message_112
        if_request_loginname_not_equals_to_dataloggerlog_supervisor_loginname_109message_112 >> rail.Label(
            'Yes') >> _adhoc_http_action_113 >> log_getsupervisor_uri_114 >> if_log_getsupervisor_uri_114_blank_115
        if_log_getsupervisor_uri_114_blank_115 >> rail.Label(
            'Yes') >> fdt_supervisor_assignment_table_add_entry_116 >> if_log_getsupervisor_uri_114_present_117
        if_log_getsupervisor_uri_114_blank_115 >> rail.Label(
            'No') >> if_log_getsupervisor_uri_114_present_117
        if_log_getsupervisor_uri_114_present_117 >> rail.Label(
            'Yes') >> log_getsupervisor_status_118 >> _adhoc_http_action_119 >> log_checkifsupervisorhassupervisorpermission_120 \
            >> if_log_checkifsupervisorhassupervisorpermission_120_blank_121
        if_log_checkifsupervisorhassupervisorpermission_120_blank_121 >> rail.Label(
            'Yes') >> log_get_supervisor_permission_122 >> assign_supervsior_permission_set_to_user_123 >> get_supervisor_assignment_details_124
        if_log_checkifsupervisorhassupervisorpermission_120_blank_121 >> rail.Label(
            'No') >> get_supervisor_assignment_details_124 >> if_supervisor_displaytext_blank_urnrepliconlisttypenull_125
        if_supervisor_displaytext_blank_urnrepliconlisttypenull_125 >> rail.Label(
            'Yes') >> if_log_getsupervisor_status_118_equals_to_true_126
        if_log_getsupervisor_status_118_equals_to_true_126 >> rail.Label(
            'Yes') >> update_initial_supervisor_127 >> if_supervisor_displaytext_present_urnrepliconlisttypenull_130
        if_log_getsupervisor_status_118_equals_to_true_126 >> rail.Label(
            'No') >> fdt_supervisor_assignment_table_add_entry_129 >> if_supervisor_displaytext_present_urnrepliconlisttypenull_130
        if_supervisor_displaytext_blank_urnrepliconlisttypenull_125 >> rail.Label(
            'No') >> if_supervisor_displaytext_present_urnrepliconlisttypenull_130
        if_supervisor_displaytext_present_urnrepliconlisttypenull_130 >> rail.Label(
            'Yes') >> log_thesupervisorloginname_131 >> if_log_supervisor_loginname_109_present_132
        if_log_supervisor_loginname_109_present_132 >> rail.Label(
            'Yes') >> if_log_getsupervisor_status_118_equals_to_true_133
        if_log_getsupervisor_status_118_equals_to_true_133 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_134 >> log_get_assignedshiftcurrent_value_137
        if_log_getsupervisor_status_118_equals_to_true_133 >> rail.Label(
            'No') >> fdt_supervisor_assignment_table_add_entry_136 >> log_get_assignedshiftcurrent_value_137
        if_log_supervisor_loginname_109_present_132 >> rail.Label(
            'No') >> log_get_assignedshiftcurrent_value_137
        if_supervisor_displaytext_present_urnrepliconlisttypenull_130 >> rail.Label(
            'No') >> log_get_assignedshiftcurrent_value_137
        if_log_getsupervisor_uri_114_present_117 >> rail.Label(
            'No') >> log_get_assignedshiftcurrent_value_137
        if_request_loginname_not_equals_to_dataloggerlog_supervisor_loginname_109message_112 >> rail.Label(
            'No') >> log_get_assignedshiftcurrent_value_137
        if_request_managerid_present_103 >> rail.Label(
            'No') >> log_get_assignedshiftcurrent_value_137 >> if_request_assignedshift_present_138
        if_request_assignedshift_present_138 >> rail.Label(
            'Yes') >> log_get_assigned_shifturi_139 >> get_all_custom_field_drop_down_optionsfor_assigned_shift_140 \
            >> log_get_assigned_shiftvalueuri_141 >> if_log_get_assigned_shiftvalueuri_141_present_datalogger4e8bc76fmessage_142
        if_log_get_assigned_shiftvalueuri_141_present_datalogger4e8bc76fmessage_142 >> rail.Label(
            'Yes') >> update_assigned_shift_u_d_f_143 >> if_log_get_assigned_shiftvalueuri_141_blank_144
        if_log_get_assigned_shiftvalueuri_141_present_datalogger4e8bc76fmessage_142 >> rail.Label(
            'No') >> if_log_get_assigned_shiftvalueuri_141_blank_144
        if_log_get_assigned_shiftvalueuri_141_blank_144 >> rail.Label(
            'Yes') >> log_assignedshiftnotupdated_145 >> if_request_hourlyratejobdata_present_146
        if_log_get_assigned_shiftvalueuri_141_blank_144 >> rail.Label(
            'No') >> if_request_hourlyratejobdata_present_146
        if_request_assignedshift_present_138 >> rail.Label(
            'No') >> if_request_hourlyratejobdata_present_146
        if_request_hourlyratejobdata_present_146 >> rail.Label(
            'Yes') >> _adhoc_http_action_148 >> log_get_current_pay_rate_amount_164 >> if_hourlyrate_not_equals_to_current_pay_rate_amount_164_165
        if_request_hourlyratejobdata_present_146 >> rail.Label(
            'No') >> if_request_regulartemporary_present_174

        if_hourlyrate_not_equals_to_current_pay_rate_amount_164_165 >> rail.Label(
            'No') >> if_request_regulartemporary_present_174
        if_hourlyrate_not_equals_to_current_pay_rate_amount_164_165 >> rail.Label(
            'Yes') >> put_user_payroll_rate_schedule_168 >> if_request_regulartemporary_present_174

        if_request_regulartemporary_present_174 >> rail.Label(
            'Yes') >> log_get_regular_temporaryuri_175 >> log_get_regular_temporarycurrent_value_176 \
            >> if_request_regulartemporary_not_equals_to_dataloggerlog_get_regular_temporarycurrent_value_176message_177
        if_request_regulartemporary_not_equals_to_dataloggerlog_get_regular_temporarycurrent_value_176message_177 >> rail.Label(
            'Yes') >> update_regular_temporary_u_d_f_178 >> update_variable_179 >> if_request_jobtitle_present_180
        if_request_regulartemporary_not_equals_to_dataloggerlog_get_regular_temporarycurrent_value_176message_177 >> rail.Label(
            'No') >> if_request_jobtitle_present_180
        if_request_regulartemporary_present_174 >> rail.Label(
            'No') >> if_request_jobtitle_present_180
        if_request_jobtitle_present_180 >> rail.Label(
            'Yes') >> log_get_job_titleuri_181 >> log_get_job_titlecurrent_value_182 \
            >> if_request_jobtitle_not_equals_to_dataloggerlog_get_job_titlecurrent_value_182message_183
        if_request_jobtitle_not_equals_to_dataloggerlog_get_job_titlecurrent_value_182message_183 >> rail.Label(
            'Yes') >> update_job_title_u_d_f_184 >> if_request_fullparttime_present_185
        if_request_jobtitle_not_equals_to_dataloggerlog_get_job_titlecurrent_value_182message_183 >> rail.Label(
            'No') >> if_request_fullparttime_present_185
        if_request_jobtitle_present_180 >> rail.Label(
            'No') >> if_request_fullparttime_present_185
        if_request_fullparttime_present_185 >> rail.Label(
            'Yes') >> log_get_full_part_timeuri_186 >> log_get_full_part_timecurrent_value_187 \
            >> if_request_fullparttime_not_equals_to_dataloggerlog_get_full_part_timecurrent_value_187message_188
        if_request_fullparttime_not_equals_to_dataloggerlog_get_full_part_timecurrent_value_187message_188 >> rail.Label(
            'Yes') >> update_full_part_time_u_d_f_189 >> update_variable_190 >> get_all_office_schedules_191 \
            >> declare_variable_192 >> if_request_fullparttime_not_equals_to_f_193
        if_request_fullparttime_not_equals_to_f_193 >> rail.Label(
            'Yes') >> update_variable_194 >> log_officeschedule_uri_195
        if_request_fullparttime_not_equals_to_f_193 >> rail.Label(
            'No') >> log_officeschedule_uri_195 >> if_log_officeschedule_uri_195_present_196
        if_log_officeschedule_uri_195_present_196 >> rail.Label(
            'Yes') >> declare_list_197 >> if_existing_schedule_policies_present

        if_existing_schedule_policies_present >> rail.Label(
            'Yes') >> foreach_schedule_policy_assigned_to_user
        if_existing_schedule_policies_present >> rail.Label(
            'No') >> if_declare_list_197_list_items_less_than_1_206

        if_effectivedate_day_blank_200 >> rail.Label(
            'No') >> insert_to_list_201

        insert_to_list_201 >> foreach_schedule_policy_assigned_to_user_end

        foreach_schedule_policy_assigned_to_user >> if_effectivedate_day_blank_200
        if_effectivedate_day_blank_200 >> rail.Label(
            'Yes') >> log_effective_date_203

        log_effective_date_203 >> if_to_date_to_time_not_equals_to_todayto_time_204

        if_to_date_to_time_not_equals_to_todayto_time_204 >> rail.Label(
            'Yes') >> insert_to_list_205 >> foreach_schedule_policy_assigned_to_user_end
        if_to_date_to_time_not_equals_to_todayto_time_204 >> rail.Label(
            'No') >> foreach_schedule_policy_assigned_to_user_end
        foreach_schedule_policy_assigned_to_user >> foreach_schedule_policy_assigned_to_user_end >> if_declare_list_197_list_items_less_than_1_206
        if_declare_list_197_list_items_less_than_1_206 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_207 >> if_declare_list_197_list_items_greater_than_0_208
        if_declare_list_197_list_items_less_than_1_206 >> rail.Label(
            'No') >> if_declare_list_197_list_items_greater_than_0_208
        if_declare_list_197_list_items_greater_than_0_208 >> rail.Label(
            'Yes') >> insert_to_list_209 >> log_getoffice_scheduleentriestobeassigned_210 >> put_schedule_policy_schedule_for_user_211 \
            >> if_declare_variable_3_value_equals_to_triggertimeoffchange_212

        if_declare_list_197_list_items_greater_than_0_208 >> rail.Label(
            'No') >> if_declare_variable_3_value_equals_to_triggertimeoffchange_212
        if_log_officeschedule_uri_195_present_196 >> rail.Label(
            'No') >> if_declare_variable_3_value_equals_to_triggertimeoffchange_212
        if_request_fullparttime_not_equals_to_dataloggerlog_get_full_part_timecurrent_value_187message_188 >> rail.Label(
            'No') >> if_declare_variable_3_value_equals_to_triggertimeoffchange_212
        if_request_fullparttime_present_185 >> rail.Label(
            'No') >> if_declare_variable_3_value_equals_to_triggertimeoffchange_212
        if_declare_variable_3_value_equals_to_triggertimeoffchange_212 >> rail.Label(
            'Yes') >> log_get_adjusted_start_datefortransitionuri_213 >> update_adjusted_start_datefortransition_u_d_f_214 \
            >> trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_existing_user_ftpt_or_rt_change_v3_0215 \
            >> wait_for_completion_trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_existing_user_ftpt_or_rt_change_v3_0215 \
            >> if_request_paygroup_present_216
        if_declare_variable_3_value_equals_to_triggertimeoffchange_212 >> rail.Label(
            'No') >> if_request_paygroup_present_216
        if_request_paygroup_present_216 >> rail.Label(
            'Yes') >> log_get_pay_groupuri_217 >> log_get_pay_groupcurrent_value_218 \
            >> if_request_paygroup_not_equals_to_dataloggerlog_get_pay_groupcurrent_value_218message_219
        if_request_paygroup_not_equals_to_dataloggerlog_get_pay_groupcurrent_value_218message_219 >> rail.Label(
            'Yes') >> update_pay_group_u_d_f_220 >> if_request_eestatus_present_221
        if_request_paygroup_not_equals_to_dataloggerlog_get_pay_groupcurrent_value_218message_219 >> rail.Label(
            'No') >> if_request_eestatus_present_221
        if_request_paygroup_present_216 >> rail.Label(
            'No') >> if_request_eestatus_present_221
        if_request_eestatus_present_221 >> rail.Label(
            'Yes') >> log_get_e_e_statusuri_222 >> log_get_e_e_statuscurrent_value_223 \
            >> if_request_eestatus_not_equals_to_dataloggerlog_get_e_e_statuscurrent_value_223message_224
        if_request_eestatus_not_equals_to_dataloggerlog_get_e_e_statuscurrent_value_223message_224 >> rail.Label(
            'Yes') >> update_e_e_status_u_d_f_225 >> if_request_company_present_226
        if_request_eestatus_not_equals_to_dataloggerlog_get_e_e_statuscurrent_value_223message_224 >> rail.Label(
            'No') >> if_request_company_present_226
        if_request_eestatus_present_221 >> rail.Label(
            'No') >> if_request_company_present_226
        if_request_company_present_226 >> rail.Label(
            'Yes') >> log_get_companyuri_227 >> log_get_companycurrent_value_228 \
            >> if_request_company_not_equals_to_dataloggerlog_get_companycurrent_value_228message_229
        if_request_company_not_equals_to_dataloggerlog_get_companycurrent_value_228message_229 >> rail.Label(
            'Yes') >> update_company_u_d_f_230 >> if_request_managerid_present_231
        if_request_company_not_equals_to_dataloggerlog_get_companycurrent_value_228message_229 >> rail.Label(
            'No') >> if_request_managerid_present_231
        if_request_company_present_226 >> rail.Label(
            'No') >> if_request_managerid_present_231
        if_request_managerid_present_231 >> rail.Label(
            'Yes') >> log_get_manager_i_duri_232 >> log_get_manager_i_dcurrent_value_233 \
            >> if_request_managerid_not_equals_to_dataloggerlog_get_manager_i_dcurrent_value_233message_234
        if_request_managerid_not_equals_to_dataloggerlog_get_manager_i_dcurrent_value_233message_234 >> rail.Label(
            'Yes') >> update_manager_i_d_u_d_f_235 >> if_request_autolinkratetype_present_236
        if_request_managerid_not_equals_to_dataloggerlog_get_manager_i_dcurrent_value_233message_234 >> rail.Label(
            'No') >> if_request_autolinkratetype_present_236
        if_request_managerid_present_231 >> rail.Label(
            'No') >> if_request_autolinkratetype_present_236

        if_request_autolinkratetype_present_236 >> rail.Label(
            'Yes') >> log_get_autolink_rate_typeuri_237 >> log_get_autolink_rate_typecurrent_value_238 \
            >> if_request_autolinkratetype_not_equals_to_dataloggerlog_get_autolink_rate_typecurrent_value_238message_239
        if_request_autolinkratetype_not_equals_to_dataloggerlog_get_autolink_rate_typecurrent_value_238message_239 >> rail.Label(
            'Yes') >> update_autolink_rate_type_u_d_f_240 >> if_request_annualsalary_present_241
        if_request_autolinkratetype_not_equals_to_dataloggerlog_get_autolink_rate_typecurrent_value_238message_239 >> rail.Label(
            'No') >> if_request_annualsalary_present_241
        if_request_autolinkratetype_present_236 >> rail.Label(
            'No') >> if_request_annualsalary_present_241
        if_request_annualsalary_present_241 >> rail.Label(
            'Yes') >> log_get_gross_annual_salaryuri_242 >> log_get_gross_annual_salarycurrent_value_243 \
            >> if_request_annualsalary_not_equals_to_dataloggerlog_get_gross_annual_salarycurrent_value_243message_244
        if_request_annualsalary_not_equals_to_dataloggerlog_get_gross_annual_salarycurrent_value_243message_244 >> rail.Label(
            'Yes') >> update_gross_annual_salary_u_d_f_245 \
            >> log_get_biweekly_gross_wagesuri_246 >> log_biweekly_gross_wages_247 >> update_biweekly_gross_wages_u_d_f_248 \
            >> if_request_rehiredate_present_249
        if_request_annualsalary_not_equals_to_dataloggerlog_get_gross_annual_salarycurrent_value_243message_244 >> rail.Label(
            'No') >> if_request_rehiredate_present_249
        if_request_annualsalary_present_241 >> rail.Label(
            'No') >> if_request_rehiredate_present_249
        if_request_rehiredate_present_249 >> rail.Label(
            'Yes') >> if_request_rehiredate_not_contains_250
        if_request_rehiredate_not_contains_250 >> rail.Label(
            'Yes') >> log_logforincorrectrehiredateformat_251 >> fdt_user_import_logs_add_entry_283
        if_request_rehiredate_not_contains_250 >> rail.Label('No') >> log_rehire_datein_time_253 >> log_get_rehire_start_dateuri_254 \
            >> log_get_rehire_start_datecurrent_value_255 >> if_rehire_date_not_equals_to_current_rehire_date_256
        if_rehire_date_not_equals_to_current_rehire_date_256 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_rehire_date_257 >> update_rehire_start_date_u_d_f_258 >> if_declare_variable_2_value_equals_to_yes_259
        if_declare_variable_2_value_equals_to_yes_259 >> rail.Label(
            'Yes') >> if_enddate_day_present_260
        if_enddate_day_present_260 >> rail.Label('Yes') >> log_end_datein_d_d_m_m_y_y_y_y_261 >> log_total_termination_tenure_262 \
            >> log_total_service_tenure_263 >> log_get_adjusted_service_dateuri_264 \
            >> if_log_total_termination_tenure_262_greater_than_total_service_tenure_263_treat_the_rehire_as_new_user_265
        if_log_total_termination_tenure_262_greater_than_total_service_tenure_263_treat_the_rehire_as_new_user_265 >> rail.Label(
            'Yes') >> trigger_dag_child_add_remove_to_type_for_rehire_new_user_0266 \
            >> wait_for_completion_trigger_fdt_child_add_remove_to_type_for_rehire_new_user_0266 >> update_adjusted_service_date_u_d_f_267 \
            >> log_rehire_error_282
        if_log_total_termination_tenure_262_greater_than_total_service_tenure_263_treat_the_rehire_as_new_user_265 >> rail.Label(
            'No') >> if_to_f_less_than_1_treattherehirebasedontheactualservicetenure_269
        if_to_f_less_than_1_treattherehirebasedontheactualservicetenure_269 >> rail.Label(
            'Yes') >> trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_termination_1_v3_0270 \
            >> wait_for_completion_trigger_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_termination_1_v3_0270 \
            >> update_adjusted_service_date_u_d_f_271 >> fdt_user_import_logs_add_entry_283
        if_to_f_less_than_1_treattherehirebasedontheactualservicetenure_269 >> rail.Label(
            'No') >> trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_new_user_v3_0273 \
            >> wait_for_completion_trigger_dag_run_live_fdt_child_workflow_to_add_remove_timeoff_type_for_rehire_new_user_v3_0273 \
            >> log_derived_adjusted_start_date_basedon_formula_start_date_rehire_date_end_date_274 \
            >> invoke_custom_ruby_code_adjusted_start_date_275 >> update_adjusted_service_date_u_d_f_276 >> log_get_dateforrehirecalculationuri_277 \
            >> log_derived_dateforrehirecalculation_basedon_formula_rehire_date1_year_278 \
            >> invoke_custom_ruby_code_dateforrehirecalculation_279 >> update_dateforrehirecalculation_u_d_f_280 >> fdt_user_import_logs_add_entry_283
        if_enddate_day_present_260 >> rail.Label(
            'No') >> log_rehire_error_282 >> fdt_user_import_logs_add_entry_283
        if_declare_variable_2_value_equals_to_yes_259 >> rail.Label(
            'No') >> fdt_user_import_logs_add_entry_283
        if_rehire_date_not_equals_to_current_rehire_date_256 >> rail.Label(
            'No') >> fdt_user_import_logs_add_entry_283
        if_request_rehiredate_present_249 >> rail.Label(
            'No') >> fdt_user_import_logs_add_entry_283 >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
