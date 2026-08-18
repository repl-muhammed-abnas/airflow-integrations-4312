
from datetime import timedelta, datetime
import itertools
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_user_import_dtna_child_add_user_dtna_prod_{config.instance}',
        description=f'Live|Child_Add_User_DTNA_Prod {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        max_active_runs=config.child_dag_max_active_runs,
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
            no_task='log_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_3 = rail.PythonOperator(
            task_id='log_3',
            python_callable=lambda dag_run:  dag_run.conf['AddUserPermission1'] or dag_run.conf['AddUserPermission2'] or dag_run.conf['AddUserPermission3'] or dag_run.conf[
                'AddUserPermission4'] or dag_run.conf['AddUserPermission5'] or dag_run.conf['AddUserPermission6'] or dag_run.conf['AddUserPermission7']
        )

        create_user_4 = rail.RepliconServiceOperator(
            task_id='create_user_4',
            endpoint="/services/importService1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['LoginName'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['FirstName'],
                    "lastname": dag_run.conf['LastName'],
                    "emailAddress": dag_run.conf['Email'],
                    "employeeId": dag_run.conf['EmployeeID'],
                    "department": {
                        "uri": null,
                        "name": dag_run.conf['DepartmentName'],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {"year": datetime.strptime(dag_run.conf['StartDate'], '%m/%d/%Y').year, "month": datetime.strptime(dag_run.conf['StartDate'], '%m/%d/%Y').month, "day": datetime.strptime(dag_run.conf['StartDate'], '%m/%d/%Y').day} if dag_run.conf['StartDate'] else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['LoginName'],
                        "password": "D@!mLerTru(k$100"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [{
                        "uri": null,
                        "name": rail.result('log_3')
                    }],
                    "policySets": [
                        {
                            "uri": null,
                            "name": dag_run.conf['TimeSheetTemplate']
                        }
                    ],
                    "employeeType": {
                        "uri": null,
                        "name": dag_run.conf['EmployeeType']
                    },
                    "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system",
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {"name": dag_run.conf['TimesheetApprovalPath']} if dag_run.conf['TimesheetApprovalPath'] else null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": [],
                    "assignedActivities": [],
                    "timeZone": null,
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

        if_request_departmentname_not_equals_to_dtna_6 = rail.IfOperator(
            task_id='if_request_departmentname_not_equals_to_dtna_6',
            test='''{{ dag_run.conf.DepartmentName | lower != 'dtna' }}''',
            yes_task="_adhoc_http_action_7",
            no_task="update_custom_field_w_r_k_r_i_d_10",
        )

        _adhoc_http_action_7 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_7',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data=None
        )

        log_get_required_department_uri_8 = rail.PythonOperator(
            task_id='log_get_required_department_uri_8',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_7'), 'displayText', dag_run.conf['DepartmentName'], 'uri')
        )

        update_department_for_user_9 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_9',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "departmentUri": "{{ result('log_get_required_department_uri_8') }}"
            }
        )

        update_custom_field_w_r_k_r_i_d_10 = rail.RepliconServiceOperator(
            task_id='update_custom_field_w_r_k_r_i_d_10',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_4').uri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:dbea7d87-c1d5-479e-a630-2a56ed952760",
                "value": "{{ dag_run.conf.CustomFieldWRKR_ID }}"
            }
        )

        update_custom_field_c_l_n_t_w_r_k_r_i_d_11 = rail.RepliconServiceOperator(
            task_id='update_custom_field_c_l_n_t_w_r_k_r_i_d_11',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_4').uri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:66ccc605-17a9-460f-a5ba-7817adb4f2be",
                "value": "{{ dag_run.conf.CustomFieldCLNT_WRKR_ID }}"
            }
        )

        update_custom_field_j_o_b_c_o_d_e_12 = rail.RepliconServiceOperator(
            task_id='update_custom_field_j_o_b_c_o_d_e_12',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_4').uri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:f92c4898-6309-4075-adf7-8d7202ebcc83",
                "value": "{{ dag_run.conf.CustomFieldJOB_CODE }}"
            }
        )

        update_custom_field_h_i_r_i_n_g_m_a_n_a_g_e_r_i_d_13 = rail.RepliconServiceOperator(
            task_id='update_custom_field_h_i_r_i_n_g_m_a_n_a_g_e_r_i_d_13',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_4').uri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:022de43a-eeb5-4c81-9add-7c7b1886ac4c",
                "value": "{{ dag_run.conf.CustomFieldHIRING_MANAGER_ID }}"
            }
        )

        update_custom_field_a_p_p_r_i_d_14 = rail.RepliconServiceOperator(
            task_id='update_custom_field_a_p_p_r_i_d_14',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_4').uri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:1028f0d8-8d93-4a1a-829b-4a08e372c0c1",
                "value": "{{ dag_run.conf.CustomFieldAPPR_ID }}"
            }
        )

        update_custom_field_initials_e_n_g_15 = rail.RepliconServiceOperator(
            task_id='update_custom_field_initials_e_n_g_15',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_4').uri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:0cc396f4-4c70-4903-b806-d118267fda6d",
                "value": "{{ dag_run.conf.CustomFieldInitialsENG }}"
            }
        )

        if_request_supervisorloginname_present_16 = rail.IfOperator(
            task_id='if_request_supervisorloginname_present_16',
            test='''{{ dag_run.conf.SupervisorLoginName | is_truthy }}''',
            yes_task="get_supervisor_details_18",
            no_task="if_request_holidaycalendar_present_28",
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        get_supervisor_details_18 = rail.RepliconServicePageOperator(
            task_id="get_supervisor_details_18",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['SupervisorLoginName'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['SupervisorLoginName'])
        )

        if_request_supervisorstartdate_blank_19 = rail.IfOperator(
            task_id='if_request_supervisorstartdate_blank_19',
            test='''{{ dag_run.conf.SupervisorStartDate | is_falsy }}''',
            yes_task="update_initial_supervisor_20",
            no_task="if_request_supervisorstartdate_present_21",
        )

        update_initial_supervisor_20 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_20',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "supervisorUri": "{{ result('get_supervisor_details_18').useruri }}",
                "dateRange": null
            }
        )

        if_request_supervisorstartdate_present_21 = rail.IfOperator(
            task_id='if_request_supervisorstartdate_present_21',
            test='''{{ dag_run.conf.SupervisorStartDate | is_truthy }}''',
            yes_task="update_initial_supervisor_updatesupervisoralongwiththerequiredeffectivedate_25",
            no_task="if_request_holidaycalendar_present_28",
        )

        update_initial_supervisor_updatesupervisoralongwiththerequiredeffectivedate_25 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_updatesupervisoralongwiththerequiredeffectivedate_25',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_4')['uri'],
                "supervisorUri": rail.result('get_supervisor_details_18')['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['SupervisorStartDate'], '%m/%d/%Y').year,
                        "month": datetime.strptime(dag_run.conf['SupervisorStartDate'], '%m/%d/%Y').month,
                        "day": datetime.strptime(dag_run.conf['SupervisorStartDate'], '%m/%d/%Y').day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_holidaycalendar_present_28 = rail.IfOperator(
            task_id='if_request_holidaycalendar_present_28',
            test='''{{ dag_run.conf.HolidayCalendar | is_truthy }}''',
            yes_task="_adhoc_http_action_29",
            no_task="if_request_authenticationtype_equals_to_sso_32",
        )

        _adhoc_http_action_29 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_29',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=None
        )

        log_get_required_holiday_calendar_uri_30 = rail.PythonOperator(
            task_id='log_get_required_holiday_calendar_uri_30',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_29'), 'displayText', dag_run.conf['HolidayCalendar'], 'uri')
        )

        update_holiday_calendar_31 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_31',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "holidayCalendarUri": "{{ result('log_get_required_holiday_calendar_uri_30') }}"
            }
        )

        if_request_authenticationtype_equals_to_sso_32 = rail.IfOperator(
            task_id='if_request_authenticationtype_equals_to_sso_32',
            test='''{{ dag_run.conf.AuthenticationType | lower == 'sso'}}''',
            yes_task="set_s_s_o_authentication_for_user_33",
            no_task="if_request_workweek_present_sso_34",
        )

        set_s_s_o_authentication_for_user_33 = rail.RepliconServiceOperator(
            task_id='set_s_s_o_authentication_for_user_33',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "loginName": "{{ dag_run.conf.LoginName }}"
            }
        )

        if_request_workweek_present_sso_34 = rail.IfOperator(
            task_id='if_request_workweek_present_sso_34',
            test='''{{ dag_run.conf.WorkWeek | is_truthy }}''',
            yes_task="_adhoc_http_action_35",
            no_task="if_request_timezone_present_sso_39",
        )

        _adhoc_http_action_35 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_35',
            endpoint="/services/InternationalizationService1.svc/GetAllDaysOfWeek",
            data=None
        )

        log_getthestartdayofthe_workweek_36 = rail.PythonOperator(
            task_id='log_getthestartdayofthe_workweek_36',
            python_callable=lambda dag_run:  dag_run.conf['WorkWeek'].split(" ")[
                0]
        )

        log_get_required_workweek_uri_37 = rail.PythonOperator(
            task_id='log_get_required_workweek_uri_37',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_35'), 'name', rail.result('log_getthestartdayofthe_workweek_36'), 'uri')
        )

        update_work_week_for_user_38 = rail.RepliconServiceOperator(
            task_id='update_work_week_for_user_38',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "dayOfWeekUri": "{{ result('log_get_required_workweek_uri_37') }}"
            }
        )

        if_request_timezone_present_sso_39 = rail.IfOperator(
            task_id='if_request_timezone_present_sso_39',
            test='''{{ dag_run.conf.TimeZone | is_truthy }}''',
            yes_task="_adhoc_http_action_40",
            no_task="if_request_licenseseats_present_sso_43",
        )

        _adhoc_http_action_40 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_40',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            data=None
        )

        log_get_required_timezone_uri_41 = rail.PythonOperator(
            task_id='log_get_required_timezone_uri_41',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_40'), 'displayText', dag_run.conf['TimeZone'], 'uri')
        )

        update_time_zone_for_user_42 = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_42',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "timeZoneUri": "{{ result('log_get_required_timezone_uri_41') }}"
            }
        )

        if_request_licenseseats_present_sso_43 = rail.IfOperator(
            task_id='if_request_licenseseats_present_sso_43',
            test='''{{ dag_run.conf.LicenseSeats | is_truthy }}''',
            yes_task="update_license_seats_for_user_44",
            no_task="if_request_customfieldsupplier_id_present_45",
        )

        update_license_seats_for_user_44 = rail.RepliconServiceOperator(
            task_id='update_license_seats_for_user_44',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "productUris": [
                    "urn:replicon-saas:product:time-bill-plus"
                ]
            }
        )

        if_request_customfieldsupplier_id_present_45 = rail.IfOperator(
            task_id='if_request_customfieldsupplier_id_present_45',
            test='''{{ dag_run.conf.CustomFieldSUPPLIER_ID | is_truthy }}''',
            yes_task="_adhoc_http_action_46",
            no_task="if_request_scheduletype_present_sso_60",
        )

        _adhoc_http_action_46 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_46',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4"
            }
        )

        log_get_required_supplier_i_ddropdown_uri_47 = rail.PythonOperator(
            task_id='log_get_required_supplier_i_ddropdown_uri_47',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_46'), 'displayText', dag_run.conf['CustomFieldSUPPLIER_ID'], 'uri')
        )

        if_log_16_present_48 = rail.IfOperator(
            task_id='if_log_16_present_48',
            test='''{{ result('log_get_required_supplier_i_ddropdown_uri_47') | is_truthy }}''',
            yes_task="update_dropdown_valuefor_supplier_i_d_49",
            no_task="put_drop_down_optionsfor_supplier_i_d_55",
        )

        update_dropdown_valuefor_supplier_i_d_49 = rail.RepliconServiceOperator(
            task_id='update_dropdown_valuefor_supplier_i_d_49',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_4').uri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4",
                "customFieldDropDownOptionUri": "{{ result('log_get_required_supplier_i_ddropdown_uri_47') }}"
            }
        )

        def get_supplier_id(dag_run):
            supplier_id_options = []
            supplier_options = rail.result('_adhoc_http_action_46')
            for supplier_id in supplier_options:
                supplier_id_options.append({
                    "target": {
                        "name": supplier_id['displayText']
                    },
                    "name": supplier_id['displayText'],
                    "isEnabled": supplier_id['isEnabled']
                })

            supplier_id_options.append({
                "name": dag_run.conf['CustomFieldSUPPLIER_ID'],
                "isEnabled": "true"
            })
            return supplier_id_options

        put_drop_down_optionsfor_supplier_i_d_55 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_supplier_i_d_55',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4",
                "customFieldDropDownOptionUris": get_supplier_id(dag_run)
            }
        )

        _adhoc_http_action_56 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_56',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4"
            }
        )

        log_get_required_supplier_i_ddropdown_uri_57 = rail.PythonOperator(
            task_id='log_get_required_supplier_i_ddropdown_uri_57',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_56'), 'displayText', dag_run.conf['CustomFieldSUPPLIER_ID'], 'uri')
        )

        if_log_29_present_58 = rail.IfOperator(
            task_id='if_log_29_present_58',
            test='''{{ result('log_get_required_supplier_i_ddropdown_uri_57') | is_truthy }}''',
            yes_task="update_dropdown_valuefor_supplier_i_d_59",
            no_task="if_request_scheduletype_present_sso_60",
        )

        update_dropdown_valuefor_supplier_i_d_59 = rail.RepliconServiceOperator(
            task_id='update_dropdown_valuefor_supplier_i_d_59',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_4').uri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4",
                "customFieldDropDownOptionUri": "{{ result('log_get_required_supplier_i_ddropdown_uri_57') }}"
            }
        )

        if_request_scheduletype_present_sso_60 = rail.IfOperator(
            task_id='if_request_scheduletype_present_sso_60',
            test='''{{ dag_run.conf.ScheduleType | is_truthy }}''',
            yes_task="put_schedule_policy_schedule_for_user_61",
            no_task="if_request_adduserpermission2_present_62",
        )

        put_schedule_policy_schedule_for_user_61 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_61',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": "{{ dag_run.conf.ScheduleType }}",
                            "officeSchedule": null,
                            "scheduleTypeUri": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_adduserpermission2_present_62 = rail.IfOperator(
            task_id='if_request_adduserpermission2_present_62',
            test='''{{ dag_run.conf.AddUserPermission2 | is_truthy  or dag_run.conf.AddUserPermission3 | is_truthy  or dag_run.conf.AddUserPermission4 | is_truthy  or dag_run.conf.AddUserPermission5 | is_truthy  or dag_run.conf.AddUserPermission6 | is_truthy  or dag_run.conf.AddUserPermission7 | is_truthy }}''',
            yes_task="_adhoc_http_action_63",
            no_task="if_request_groupcost_center_name_present_75",
        )

        _adhoc_http_action_63 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_63',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        def get_permissions_set_uris(dag_run):
            existing_permissions = rail.result('_adhoc_http_action_63')
            permission_uri_list = []
            permission_names = [dag_run.conf['AddUserPermission1'], dag_run.conf['AddUserPermission2'],
                                dag_run.conf['AddUserPermission3'], dag_run.conf['AddUserPermission4'],
                                dag_run.conf['AddUserPermission5'], dag_run.conf['AddUserPermission6'],
                                dag_run.conf['AddUserPermission7']]
            for permission_name in permission_names:
                permission_uri = rail.find_first_by_attr_and_get_attr(
                    existing_permissions, 'displayText', permission_name, 'uri')
                if permission_uri:
                    permission_uri_list.append(permission_uri)
            return permission_uri_list

        put_permission_set_assignments_for_user_74 = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_74',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_4')['uri'],
                "permissionSetUris": get_permissions_set_uris(dag_run)
            }
        )

        if_request_groupcost_center_name_present_75 = rail.IfOperator(
            task_id='if_request_groupcost_center_name_present_75',
            test='''{{ dag_run.conf.GroupCOST_CENTER_NAME | is_truthy }}''',
            yes_task="_adhoc_http_action_76",
            no_task="if_request_groupmanagereng_present_89",
        )

        _adhoc_http_action_76 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_76',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data=None
        )

        def get_cost_center_uri(dag_run, cost_center_task):
            existing_cost_center = rail.result(cost_center_task)
            cost_center_info = list(filter(lambda x: x['displayText'] and x['displayText'].lower(
            ) == dag_run.conf['GroupCOST_CENTER_NAME'].lower(), existing_cost_center))
            return cost_center_info[0]['uri'] if cost_center_info else None

        log_get_required_cost_center_uri_80 = rail.PythonOperator(
            task_id='log_get_required_cost_center_uri_80',
            python_callable=lambda dag_run: get_cost_center_uri(
                dag_run, '_adhoc_http_action_76')
        )

        if_log_21_present_81 = rail.IfOperator(
            task_id='if_log_21_present_81',
            test='''{{ result('log_get_required_cost_center_uri_80') | is_truthy }}''',
            yes_task="if_request_groupcost_center_name_effectivedate_blank_sso_82",
            no_task="if_request_groupmanagereng_present_89",
        )

        if_request_groupcost_center_name_effectivedate_blank_sso_82 = rail.IfOperator(
            task_id='if_request_groupcost_center_name_effectivedate_blank_sso_82',
            test='''{{ dag_run.conf.GroupCOST_CENTER_NAME_EffectiveDate | is_falsy }}''',
            yes_task="put_cost_center_schedule_for_user_83",
            no_task="if_request_groupcost_center_name_effectivedate_present_84",
        )

        put_cost_center_schedule_for_user_83 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_83',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "scheduleEntries": [
                    {
                        "costCenter": {
                            "uri": "{{ result('log_get_required_cost_center_uri_80') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_groupcost_center_name_effectivedate_present_84 = rail.IfOperator(
            task_id='if_request_groupcost_center_name_effectivedate_present_84',
            test='''{{ dag_run.conf.GroupCOST_CENTER_NAME_EffectiveDate | is_truthy }}''',
            yes_task="put_cost_center_schedule_for_user_88",
            no_task="if_request_groupmanagereng_present_89",
        )

        put_cost_center_schedule_for_user_88 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_88',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_4')['uri'],
                    "scheduleEntries": [
                        {
                            "costCenter": {
                                "uri": rail.result('log_get_required_cost_center_uri_80'),
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": {
                                "year": datetime.strptime(dag_run.conf['GroupCOST_CENTER_NAME_EffectiveDate'], '%m/%d/%Y').year,
                                "month": datetime.strptime(dag_run.conf['GroupCOST_CENTER_NAME_EffectiveDate'], '%m/%d/%Y').month,
                                "day": datetime.strptime(dag_run.conf['GroupCOST_CENTER_NAME_EffectiveDate'], '%m/%d/%Y').day
                            }
                        }
                ]
            }
        )

        if_request_groupmanagereng_present_89 = rail.IfOperator(
            task_id='if_request_groupmanagereng_present_89',
            test='''{{ dag_run.conf.GroupManagerENG | is_truthy }}''',
            yes_task="_adhoc_http_action_90",
            no_task="dtna_user_import_add_entry_103",
        )

        _adhoc_http_action_90 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_90',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
            data=None
        )

        def get_division_uri(dag_run, division_task):
            existing_division = rail.result(division_task)
            division_info = list(filter(lambda x: x['displayText'] and x['displayText'].lower(
            ) == dag_run.conf['GroupManagerENG'].lower(), existing_division))
            return division_info[0]['uri'] if division_info else None

        log_get_required_cost_center_uri_94 = rail.PythonOperator(
            task_id='log_get_required_cost_center_uri_94',
            python_callable=lambda dag_run: get_division_uri(
                dag_run, '_adhoc_http_action_90')
        )

        if_log_24_present_95 = rail.IfOperator(
            task_id='if_log_24_present_95',
            test='''{{ result('log_get_required_cost_center_uri_94') | is_truthy }}''',
            yes_task="if_request_groupmanagerengeffectivedate_blank_sso_96",
            no_task="dtna_user_import_add_entry_103",
        )

        if_request_groupmanagerengeffectivedate_blank_sso_96 = rail.IfOperator(
            task_id='if_request_groupmanagerengeffectivedate_blank_sso_96',
            test='''{{ dag_run.conf.GroupManagerENGEffectiveDate | is_falsy }}''',
            yes_task="put_division_schedule_for_user_97",
            no_task="if_request_groupmanagerengeffectivedate_present_98",
        )

        put_division_schedule_for_user_97 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_97',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ result('create_user_4').uri }}",
                "scheduleEntries": [
                    {
                        "division": {
                            "uri": "{{ result('log_get_required_cost_center_uri_94') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_groupmanagerengeffectivedate_present_98 = rail.IfOperator(
            task_id='if_request_groupmanagerengeffectivedate_present_98',
            test='''{{ dag_run.conf.GroupManagerENGEffectiveDate | is_truthy }}''',
            yes_task="put_division_schedule_for_user_102",
            no_task="dtna_user_import_add_entry_103",
        )

        put_division_schedule_for_user_102 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_102',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_4')['uri'],
                "scheduleEntries": [
                    {
                        "division": {
                            "uri": rail.result('log_get_required_cost_center_uri_94'),
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": datetime.strptime(dag_run.conf['GroupManagerENGEffectiveDate'], '%m/%d/%Y').year,
                            "month": datetime.strptime(dag_run.conf['GroupManagerENGEffectiveDate'], '%m/%d/%Y').month,
                            "day": datetime.strptime(dag_run.conf['GroupManagerENGEffectiveDate'], '%m/%d/%Y').day
                        }
                    }
                ]
            }
        )

        dtna_user_import_add_entry_103 = rail.WriteLogOperator(
            task_id='dtna_user_import_add_entry_103',
            message="na",
            severity="Success",
            properties={
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "status": "Success",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        dtna_user_import_add_entry_105 = rail.WriteLogOperator(
            task_id='dtna_user_import_add_entry_105',
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "status": "Error",
                "failure/reason": "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> log_3 >> create_user_4 >> if_request_departmentname_not_equals_to_dtna_6
        if_request_departmentname_not_equals_to_dtna_6 >> rail.Label(
            'No') >> update_custom_field_w_r_k_r_i_d_10
        if_request_departmentname_not_equals_to_dtna_6 >> rail.Label('Yes') >> _adhoc_http_action_7 >> \
            log_get_required_department_uri_8 >> update_department_for_user_9 >> \
            update_custom_field_w_r_k_r_i_d_10 >> update_custom_field_c_l_n_t_w_r_k_r_i_d_11 >> \
            update_custom_field_j_o_b_c_o_d_e_12 >> update_custom_field_h_i_r_i_n_g_m_a_n_a_g_e_r_i_d_13 >> update_custom_field_a_p_p_r_i_d_14 >> \
            update_custom_field_initials_e_n_g_15 >> if_request_supervisorloginname_present_16
        if_request_supervisorloginname_present_16 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_28
        if_request_supervisorloginname_present_16 >> rail.Label(
            'Yes') >> get_supervisor_details_18 >> if_request_supervisorstartdate_blank_19
        if_request_supervisorstartdate_blank_19 >> rail.Label(
            'No') >> if_request_supervisorstartdate_present_21
        if_request_supervisorstartdate_blank_19 >> rail.Label(
            'Yes') >> update_initial_supervisor_20 >> if_request_supervisorstartdate_present_21
        if_request_supervisorstartdate_present_21 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_28
        if_request_supervisorstartdate_present_21 >> rail.Label(
            'Yes') >> update_initial_supervisor_updatesupervisoralongwiththerequiredeffectivedate_25 >> if_request_holidaycalendar_present_28
        if_request_holidaycalendar_present_28 >> rail.Label(
            'No') >> if_request_authenticationtype_equals_to_sso_32
        if_request_holidaycalendar_present_28 >> rail.Label('Yes') >> _adhoc_http_action_29 >> \
            log_get_required_holiday_calendar_uri_30 >> update_holiday_calendar_31 >> \
            if_request_authenticationtype_equals_to_sso_32
        if_request_authenticationtype_equals_to_sso_32 >> rail.Label(
            'No') >> if_request_workweek_present_sso_34
        if_request_authenticationtype_equals_to_sso_32 >> rail.Label(
            'Yes') >> set_s_s_o_authentication_for_user_33 >> if_request_workweek_present_sso_34
        if_request_workweek_present_sso_34 >> rail.Label(
            'No') >> if_request_timezone_present_sso_39
        if_request_workweek_present_sso_34 >> rail.Label('Yes') >> _adhoc_http_action_35 >> \
            log_getthestartdayofthe_workweek_36 >> log_get_required_workweek_uri_37 >> \
            update_work_week_for_user_38 >> if_request_timezone_present_sso_39
        if_request_timezone_present_sso_39 >> rail.Label(
            'No') >> if_request_licenseseats_present_sso_43
        if_request_timezone_present_sso_39 >> rail.Label(
            'Yes') >> _adhoc_http_action_40 >> log_get_required_timezone_uri_41 >> \
            update_time_zone_for_user_42 >> if_request_licenseseats_present_sso_43
        if_request_licenseseats_present_sso_43 >> rail.Label(
            'No') >> if_request_customfieldsupplier_id_present_45
        if_request_licenseseats_present_sso_43 >> rail.Label(
            'Yes') >> update_license_seats_for_user_44 >> if_request_customfieldsupplier_id_present_45
        if_request_customfieldsupplier_id_present_45 >> rail.Label(
            'No') >> if_request_scheduletype_present_sso_60
        if_request_customfieldsupplier_id_present_45 >> rail.Label('No') >> _adhoc_http_action_46 >> log_get_required_supplier_i_ddropdown_uri_47 >> \
            if_log_16_present_48
        if_log_16_present_48 >> rail.Label(
            'Yes') >> update_dropdown_valuefor_supplier_i_d_49 >> if_request_scheduletype_present_sso_60
        if_log_16_present_48 >> rail.Label('No') >> put_drop_down_optionsfor_supplier_i_d_55 >> _adhoc_http_action_56 >> \
            log_get_required_supplier_i_ddropdown_uri_57 >> if_log_29_present_58
        if_log_29_present_58 >> rail.Label(
            'No') >> if_request_scheduletype_present_sso_60
        if_log_29_present_58 >> rail.Label(
            'Yes') >> update_dropdown_valuefor_supplier_i_d_59 >> if_request_scheduletype_present_sso_60
        if_request_scheduletype_present_sso_60 >> rail.Label(
            'No') >> if_request_adduserpermission2_present_62
        if_request_scheduletype_present_sso_60 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_61 >> if_request_adduserpermission2_present_62
        if_request_adduserpermission2_present_62 >> rail.Label(
            'No') >> if_request_groupcost_center_name_present_75
        if_request_adduserpermission2_present_62 >> rail.Label(
            'Yes') >> _adhoc_http_action_63 >> put_permission_set_assignments_for_user_74 >> if_request_groupcost_center_name_present_75
        if_request_groupcost_center_name_present_75 >> rail.Label(
            'No') >> if_request_groupmanagereng_present_89
        if_request_groupcost_center_name_present_75 >> rail.Label(
            'Yes') >> _adhoc_http_action_76 >> log_get_required_cost_center_uri_80 >> if_log_21_present_81
        if_log_21_present_81 >> rail.Label(
            'No') >> if_request_groupmanagereng_present_89
        if_log_21_present_81 >> rail.Label(
            'Yes') >> if_request_groupcost_center_name_effectivedate_blank_sso_82
        if_request_groupcost_center_name_effectivedate_blank_sso_82 >> rail.Label(
            'No') >> if_request_groupcost_center_name_effectivedate_present_84
        if_request_groupcost_center_name_effectivedate_blank_sso_82 >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_83 >> if_request_groupcost_center_name_effectivedate_present_84
        if_request_groupcost_center_name_effectivedate_present_84 >> rail.Label(
            'No') >> if_request_groupmanagereng_present_89
        if_request_groupcost_center_name_effectivedate_present_84 >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_88 >> if_request_groupmanagereng_present_89
        if_request_groupmanagereng_present_89 >> rail.Label(
            'No') >> dtna_user_import_add_entry_103
        if_request_groupmanagereng_present_89 >> rail.Label(
            'Yes') >> _adhoc_http_action_90 >> log_get_required_cost_center_uri_94 >> if_log_24_present_95
        if_log_24_present_95 >> rail.Label(
            'No') >> dtna_user_import_add_entry_103
        if_log_24_present_95 >> rail.Label(
            'Yes') >> if_request_groupmanagerengeffectivedate_blank_sso_96
        if_request_groupmanagerengeffectivedate_blank_sso_96 >> rail.Label(
            'No') >> if_request_groupmanagerengeffectivedate_present_98
        if_request_groupmanagerengeffectivedate_blank_sso_96 >> rail.Label(
            'Yes') >> put_division_schedule_for_user_97 >> if_request_groupmanagerengeffectivedate_present_98
        if_request_groupmanagerengeffectivedate_present_98 >> rail.Label(
            'No') >> dtna_user_import_add_entry_103
        if_request_groupmanagerengeffectivedate_present_98 >> rail.Label(
            'Yes') >> put_division_schedule_for_user_102 >> dtna_user_import_add_entry_103 >> dtna_user_import_add_entry_105 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
