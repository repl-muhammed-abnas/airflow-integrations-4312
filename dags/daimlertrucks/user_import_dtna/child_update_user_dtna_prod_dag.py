
from datetime import timedelta, datetime
import itertools
import pendulum
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_user_import_dtna_child_update_user_dtna_prod_{config.instance}',
        description=f'Live|Child_Update_User_DTNA_Prod {config.instance}',
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
            no_task='get_user_details_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def get_date(startdate):
            if startdate:
                year = startdate['year']
                month = startdate['month']
                day = startdate['day']
                return f"{month}/{day}/{year}"
            return ""

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri'],
                'startdate': get_date(row['cells'][4]['dateValue']) if 'dateValue' in row['cells'][4] else None
            }, flaten_rows)))
            return users_info[0] if users_info else None

        get_user_details_3 = rail.RepliconServicePageOperator(
            task_id="get_user_details_3",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled',
                    'urn:replicon:user-list-column:start-date'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['LoginName'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['LoginName'])
        )

        if_request_isloginenabled_equals_to_true_4 = rail.IfOperator(
            task_id='if_request_isloginenabled_equals_to_true_4',
            test='''{{ dag_run.conf.IsLoginEnabled == 'true' }}''',
            yes_task="_adhoc_http_action_enable_login_statusforthe_user_5",
            no_task="if_request_employeeid_present_sso_6",
        )

        _adhoc_http_action_enable_login_statusforthe_user_5 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_enable_login_statusforthe_user_5',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}"
            }
        )

        if_request_employeeid_present_sso_6 = rail.IfOperator(
            task_id='if_request_employeeid_present_sso_6',
            test='''{{ dag_run.conf.EmployeeID | is_truthy }}''',
            yes_task="update_employee_id_7",
            no_task="if_request_firstname_present_sso_8",
        )

        update_employee_id_7 = rail.RepliconServiceOperator(
            task_id='update_employee_id_7',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "employeeId": "{{ dag_run.conf.EmployeeID }}"
            }
        )

        if_request_firstname_present_sso_8 = rail.IfOperator(
            task_id='if_request_firstname_present_sso_8',
            test='''{{ dag_run.conf.FirstName | is_truthy }}''',
            yes_task="update_first_name_9",
            no_task="if_request_lastname_present_sso_10",
        )

        update_first_name_9 = rail.RepliconServiceOperator(
            task_id='update_first_name_9',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "firstname": "{{ dag_run.conf.FirstName }}"
            }
        )

        if_request_lastname_present_sso_10 = rail.IfOperator(
            task_id='if_request_lastname_present_sso_10',
            test='''{{ dag_run.conf.LastName | is_truthy }}''',
            yes_task="update_last_name_11",
            no_task="if_request_email_present_sso_12",
        )

        update_last_name_11 = rail.RepliconServiceOperator(
            task_id='update_last_name_11',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "lastname": "{{ dag_run.conf.LastName }}"
            }
        )

        if_request_email_present_sso_12 = rail.IfOperator(
            task_id='if_request_email_present_sso_12',
            test='''{{ dag_run.conf.Email | is_truthy }}''',
            yes_task="update_email_13",
            no_task="if_request_employeetype_present_14",
        )

        update_email_13 = rail.RepliconServiceOperator(
            task_id='update_email_13',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "email": "{{ dag_run.conf.Email }}"
            }
        )

        if_request_employeetype_present_14 = rail.IfOperator(
            task_id='if_request_employeetype_present_14',
            test='''{{ dag_run.conf.EmployeeType | is_truthy }}''',
            yes_task="_adhoc_http_action_15",
            no_task="if_request_departmentname_present_21",
        )

        _adhoc_http_action_15 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_15',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data=None
        )

        def get_employeetype_uri(dag_run, employeetype_task):
            existing_employeetype = rail.result(employeetype_task)
            employeetype_info = list(filter(lambda x: x['displayText'] and x['displayText'].lower(
            ) == dag_run.conf['EmployeeType'].lower(), existing_employeetype))
            return employeetype_info[0]['uri'] if employeetype_info else None

        log_get_required_employee_type_uri_19 = rail.PythonOperator(
            task_id='log_get_required_employee_type_uri_19',
            python_callable=lambda dag_run: get_employeetype_uri(
                dag_run, '_adhoc_http_action_15')
        )

        update_employee_type_for_user_20 = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user_20',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "employeeTypeUri": "{{ result('log_get_required_employee_type_uri_19') }}"
            }
        )

        if_request_departmentname_present_21 = rail.IfOperator(
            task_id='if_request_departmentname_present_21',
            test='''{{ dag_run.conf.DepartmentName | is_truthy }}''',
            yes_task="_adhoc_http_action_22",
            no_task="if_request_timesheetapprovalpath_present_25",
        )

        _adhoc_http_action_22 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_22',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data=None
        )

        log_get_required_department_uri_23 = rail.PythonOperator(
            task_id='log_get_required_department_uri_23',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_22'), 'displayText', dag_run.conf['DepartmentName'], 'uri')
        )

        update_department_for_user_24 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_24',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "departmentUri": "{{ result('log_get_required_department_uri_23') }}"
            }
        )

        if_request_timesheetapprovalpath_present_25 = rail.IfOperator(
            task_id='if_request_timesheetapprovalpath_present_25',
            test='''{{ dag_run.conf.TimesheetApprovalPath | is_truthy }}''',
            yes_task="_adhoc_http_action_26",
            no_task="if_request_timesheettemplate_present_29",
        )

        _adhoc_http_action_26 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_26',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data=None
        )

        log_get_required_timesheet_approval_path_uri_27 = rail.PythonOperator(
            task_id='log_get_required_timesheet_approval_path_uri_27',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_26'), 'displayText', dag_run.conf['TimesheetApprovalPath'], 'uri')
        )

        update_approval_path_for_user_28 = rail.RepliconServiceOperator(
            task_id='update_approval_path_for_user_28',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "approvalPathUri": "{{ result('log_get_required_timesheet_approval_path_uri_27') }}"
            }
        )

        if_request_timesheettemplate_present_29 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_29',
            test='''{{ dag_run.conf.TimeSheetTemplate | is_truthy }}''',
            yes_task="_adhoc_http_action_30",
            no_task="if_request_timesheetperiodtype_present_33",
        )

        _adhoc_http_action_30 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_30',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data=None
        )

        log_get_required_timesheet_template_uri_31 = rail.PythonOperator(
            task_id='log_get_required_timesheet_template_uri_31',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_30'), 'displayText', dag_run.conf['TimeSheetTemplate'], 'uri')
        )

        put_policy_set_assignments_for_user_32 = rail.RepliconServiceOperator(
            task_id='put_policy_set_assignments_for_user_32',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "policySetUris": [
                    "{{ result('log_get_required_timesheet_template_uri_31') }}"
                ]
            }
        )

        if_request_timesheetperiodtype_present_33 = rail.IfOperator(
            task_id='if_request_timesheetperiodtype_present_33',
            test='''{{ dag_run.conf.TimesheetPeriodType | is_truthy }}''',
            yes_task="if_request_timesheetperiodtype_equals_to_system_34",
            no_task="if_request_customfieldwrkr_id_present_sso_40",
        )

        if_request_timesheetperiodtype_equals_to_system_34 = rail.IfOperator(
            task_id='if_request_timesheetperiodtype_equals_to_system_34',
            test='''{{ dag_run.conf.TimesheetPeriodType | lower == 'system' }}''',
            yes_task="update_timesheetperiod_35",
            no_task="if_request_timesheetperiodtype_equals_to_employeetype_36",
        )

        update_timesheetperiod_35 = rail.RepliconServiceOperator(
            task_id='update_timesheetperiod_35',
            endpoint="/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system"
            }
        )

        if_request_timesheetperiodtype_equals_to_employeetype_36 = rail.IfOperator(
            task_id='if_request_timesheetperiodtype_equals_to_employeetype_36',
            test='''{{ dag_run.conf.TimesheetPeriodType | lower == 'employee type' }}''',
            yes_task="update_timesheetperiod_37",
            no_task="if_request_timesheetperiodtype_equals_to_department_38",
        )

        update_timesheetperiod_37 = rail.RepliconServiceOperator(
            task_id='update_timesheetperiod_37',
            endpoint="/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:based-on-employee-type-assignment"
            }
        )

        if_request_timesheetperiodtype_equals_to_department_38 = rail.IfOperator(
            task_id='if_request_timesheetperiodtype_equals_to_department_38',
            test='''{{ dag_run.conf.TimesheetPeriodType == 'department' }}''',
            yes_task="update_timesheetperiod_39",
            no_task="if_request_customfieldwrkr_id_present_sso_40",
        )

        update_timesheetperiod_39 = rail.RepliconServiceOperator(
            task_id='update_timesheetperiod_39',
            endpoint="/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:based-on-department-assignment"
            }
        )

        if_request_customfieldwrkr_id_present_sso_40 = rail.IfOperator(
            task_id='if_request_customfieldwrkr_id_present_sso_40',
            test='''{{ dag_run.conf.CustomFieldWRKR_ID | is_truthy }}''',
            yes_task="update_custom_field_w_r_k_r_i_d_41",
            no_task="if_request_customfieldclnt_wrkr_id_present_sso_42",
        )

        update_custom_field_w_r_k_r_i_d_41 = rail.RepliconServiceOperator(
            task_id='update_custom_field_w_r_k_r_i_d_41',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('get_user_details_3').useruri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:dbea7d87-c1d5-479e-a630-2a56ed952760",
                "value": "{{ dag_run.conf.CustomFieldWRKR_ID }}"
            }
        )

        if_request_customfieldclnt_wrkr_id_present_sso_42 = rail.IfOperator(
            task_id='if_request_customfieldclnt_wrkr_id_present_sso_42',
            test='''{{ dag_run.conf.CustomFieldCLNT_WRKR_ID | is_truthy }}''',
            yes_task="update_custom_field_c_l_n_t_w_r_k_r_i_d_43",
            no_task="if_request_customfieldjob_code_present_sso_44",
        )

        update_custom_field_c_l_n_t_w_r_k_r_i_d_43 = rail.RepliconServiceOperator(
            task_id='update_custom_field_c_l_n_t_w_r_k_r_i_d_43',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('get_user_details_3').useruri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:66ccc605-17a9-460f-a5ba-7817adb4f2be",
                "value": "{{ dag_run.conf.CustomFieldCLNT_WRKR_ID }}"
            }
        )

        if_request_customfieldjob_code_present_sso_44 = rail.IfOperator(
            task_id='if_request_customfieldjob_code_present_sso_44',
            test='''{{ dag_run.conf.CustomFieldJOB_CODE | is_truthy }}''',
            yes_task="update_custom_field_j_o_b_c_o_d_e_45",
            no_task="if_request_customfieldhiring_manager_id_present_sso_46",
        )

        update_custom_field_j_o_b_c_o_d_e_45 = rail.RepliconServiceOperator(
            task_id='update_custom_field_j_o_b_c_o_d_e_45',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('get_user_details_3').useruri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:f92c4898-6309-4075-adf7-8d7202ebcc83",
                "value": "{{ dag_run.conf.CustomFieldJOB_CODE }}"
            }
        )

        if_request_customfieldhiring_manager_id_present_sso_46 = rail.IfOperator(
            task_id='if_request_customfieldhiring_manager_id_present_sso_46',
            test='''{{ dag_run.conf.CustomFieldHIRING_MANAGER_ID | is_truthy }}''',
            yes_task="update_custom_field_h_i_r_i_n_g_m_a_n_a_g_e_r_i_d_47",
            no_task="if_request_customfieldappr_id_present_sso_48",
        )

        update_custom_field_h_i_r_i_n_g_m_a_n_a_g_e_r_i_d_47 = rail.RepliconServiceOperator(
            task_id='update_custom_field_h_i_r_i_n_g_m_a_n_a_g_e_r_i_d_47',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('get_user_details_3').useruri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:022de43a-eeb5-4c81-9add-7c7b1886ac4c",
                "value": "{{ dag_run.conf.CustomFieldHIRING_MANAGER_ID }}"
            }
        )

        if_request_customfieldappr_id_present_sso_48 = rail.IfOperator(
            task_id='if_request_customfieldappr_id_present_sso_48',
            test='''{{ dag_run.conf.CustomFieldAPPR_ID | is_truthy }}''',
            yes_task="update_custom_field_a_p_p_r_i_d_49",
            no_task="if_request_customfieldinitialseng_present_sso_50",
        )

        update_custom_field_a_p_p_r_i_d_49 = rail.RepliconServiceOperator(
            task_id='update_custom_field_a_p_p_r_i_d_49',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('get_user_details_3').useruri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:1028f0d8-8d93-4a1a-829b-4a08e372c0c1",
                "value": "{{ dag_run.conf.CustomFieldAPPR_ID }}"
            }
        )

        if_request_customfieldinitialseng_present_sso_50 = rail.IfOperator(
            task_id='if_request_customfieldinitialseng_present_sso_50',
            test='''{{ dag_run.conf.CustomFieldInitialsENG | is_truthy }}''',
            yes_task="update_custom_field_initials_e_n_g_51",
            no_task="log_today_date_52",
        )

        update_custom_field_initials_e_n_g_51 = rail.RepliconServiceOperator(
            task_id='update_custom_field_initials_e_n_g_51',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('get_user_details_3').useruri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:0cc396f4-4c70-4903-b806-d118267fda6d",
                "value": "{{ dag_run.conf.CustomFieldInitialsENG }}"
            }
        )

        log_today_date_52 = rail.PythonOperator(
            task_id='log_today_date_52',
            python_callable=lambda:  pendulum.now(
                config.pacific_timezone).strftime('%m/%d/%Y')
        )

        if_request_supervisorloginname_present_53 = rail.IfOperator(
            task_id='if_request_supervisorloginname_present_53',
            test='''{{ dag_run.conf.SupervisorLoginName | is_truthy }}''',
            yes_task="get_get_supervisordetails_55",
            no_task="if_request_startdate_present_73",
        )

        get_get_supervisordetails_55 = rail.RepliconServicePageOperator(
            task_id="get_get_supervisordetails_55",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled',
                    'urn:replicon:user-list-column:start-date'
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

        get_user_details_56 = rail.RepliconServiceOperator(
            task_id='get_user_details_56',
            endpoint='/services/UserService1.svc/GetUserDetails',
            data={
                "userUri": '''{{ result('get_user_details_3').useruri }}'''
            }
        )

        if_supervisor_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorloginname_56 = rail.IfOperator(
            task_id='if_supervisor_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorloginname_56',
            test='''{{ result('get_user_details_56').supervisor | is_truthy and result('get_user_details_56').supervisor.user.loginName != dag_run.conf.SupervisorLoginName }}''',
            yes_task="if_request_supervisorstartdate_blank_57",
            no_task="if_request_holidaycalendar_present_67",
        )

        if_request_supervisorstartdate_blank_57 = rail.IfOperator(
            task_id='if_request_supervisorstartdate_blank_57',
            test='''{{ dag_run.conf.SupervisorStartDate | is_falsy }}''',
            yes_task="if_request_supervisorstartdate_present_62",
            no_task="update_supervisor_assignment_schedule_over_date_range_updatedsupervisorwith_effective_date_61",
        )

        update_supervisor_assignment_schedule_over_date_range_updatedsupervisorwith_effective_date_61 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_updatedsupervisorwith_effective_date_61',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda: {
                "userUri": rail.result('get_user_details_3')['useruri'],
                "supervisorUri": rail.result('get_get_supervisordetails_55')['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": pendulum.now(config.pacific_timezone).year,
                        "month": pendulum.now(config.pacific_timezone).month,
                        "day": pendulum.now(config.pacific_timezone).day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_supervisorstartdate_present_62 = rail.IfOperator(
            task_id='if_request_supervisorstartdate_present_62',
            test='''{{ dag_run.conf.SupervisorStartDate | is_truthy }}''',
            yes_task="if_request_holidaycalendar_present_67",
            no_task="update_supervisor_assignment_schedule_over_date_range_updatedsupervisorwith_effective_date_66",
        )

        update_supervisor_assignment_schedule_over_date_range_updatedsupervisorwith_effective_date_66 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_updatedsupervisorwith_effective_date_66',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": rail.result('get_user_details_3')['useruri'],
                "supervisorUri": rail.result('get_user_details_3')['useruri'],
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

        if_request_holidaycalendar_present_67 = rail.IfOperator(
            task_id='if_request_holidaycalendar_present_67',
            test='''{{ dag_run.conf.HolidayCalendar | is_truthy }}''',
            yes_task="_adhoc_http_action_68",
            no_task="if_request_startdate_present_73",
        )

        _adhoc_http_action_68 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_68',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=None
        )

        log_get_required_holiday_calendar_uri_69 = rail.PythonOperator(
            task_id='log_get_required_holiday_calendar_uri_69',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_68'), 'displayText', dag_run.conf['HolidayCalendar'], 'uri')
        )

        update_holiday_calendar_70 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_70',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "holidayCalendarUri": "{{ result('log_get_required_holiday_calendar_uri_69') }}"
            }
        )

        if_request_startdate_present_73 = rail.IfOperator(
            task_id='if_request_startdate_present_73',
            test='''{{ dag_run.conf.StartDate | is_truthy  or dag_run.conf.EndDate | is_truthy }}''',
            yes_task="if_request_startdate_present_74",
            no_task="if_request_authenticationtype_equals_to_sso_96",
        )

        if_request_startdate_present_74 = rail.IfOperator(
            task_id='if_request_startdate_present_74',
            test='''{{ dag_run.conf.StartDate | is_truthy  and dag_run.conf.EndDate | is_falsy }}''',
            yes_task="update_employment_date_range_78",
            no_task="if_request_startdate_present_79",
        )

        update_employment_date_range_78 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_78',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": rail.result('get_user_details_3')['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['StartDate'], '%m/%d/%Y').year,
                        "month": datetime.strptime(dag_run.conf['StartDate'], '%m/%d/%Y').month,
                        "day": datetime.strptime(dag_run.conf['StartDate'], '%m/%d/%Y').day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_startdate_present_79 = rail.IfOperator(
            task_id='if_request_startdate_present_79',
            test='''{{ dag_run.conf.StartDate | is_truthy  and dag_run.conf.EndDate | is_truthy }}''',
            yes_task="update_employment_date_range_86",
            no_task="if_request_startdate_blank_87",
        )

        update_employment_date_range_86 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_86',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": rail.result('get_user_details_3')['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['StartDate'], '%m/%d/%Y').year,
                        "month": datetime.strptime(dag_run.conf['StartDate'], '%m/%d/%Y').month,
                        "day": datetime.strptime(dag_run.conf['StartDate'], '%m/%d/%Y').day
                    },
                    "endDate": {
                        "year": datetime.strptime(dag_run.conf['EndDate'], '%m/%d/%Y').year,
                        "month": datetime.strptime(dag_run.conf['EndDate'], '%m/%d/%Y').month,
                        "day": datetime.strptime(dag_run.conf['EndDate'], '%m/%d/%Y').day
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_startdate_blank_87 = rail.IfOperator(
            task_id='if_request_startdate_blank_87',
            test='''{{ dag_run.conf.StartDate | is_falsy  and dag_run.conf.EndDate | is_truthy }}''',
            yes_task="update_employment_date_range_95",
            no_task="if_request_authenticationtype_equals_to_sso_96",
        )

        update_employment_date_range_95 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_95',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": rail.result('get_user_details_3')['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(rail.result('get_user_details_3')['startdate'], '%m/%d/%Y').year,
                        "month": datetime.strptime(rail.result('get_user_details_3')['startdate'], '%m/%d/%Y').month,
                        "day": datetime.strptime(rail.result('get_user_details_3')['startdate'], '%m/%d/%Y').day
                    },
                    "endDate": {
                        "year": datetime.strptime(dag_run.conf['EndDate'], '%m/%d/%Y').year,
                        "month": datetime.strptime(dag_run.conf['EndDate'], '%m/%d/%Y').month,
                        "day": datetime.strptime(dag_run.conf['EndDate'], '%m/%d/%Y').day
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_authenticationtype_equals_to_sso_96 = rail.IfOperator(
            task_id='if_request_authenticationtype_equals_to_sso_96',
            test='''{{ dag_run.conf.AuthenticationType == 'sso' }}''',
            yes_task="set_s_s_o_authentication_for_user_97",
            no_task="if_request_workweek_present_sso_98",
        )

        set_s_s_o_authentication_for_user_97 = rail.RepliconServiceOperator(
            task_id='set_s_s_o_authentication_for_user_97',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "loginName": "{{ dag_run.conf.LoginName }}"
            }
        )

        if_request_workweek_present_sso_98 = rail.IfOperator(
            task_id='if_request_workweek_present_sso_98',
            test='''{{ dag_run.conf.WorkWeek | is_truthy }}''',
            yes_task="_adhoc_http_action_99",
            no_task="if_request_timezone_present_sso_103",
        )

        _adhoc_http_action_99 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_99',
            endpoint="/services/InternationalizationService1.svc/GetAllDaysOfWeek",
            data=None
        )

        log_getthestartdayofthe_workweek_100 = rail.PythonOperator(
            task_id='log_getthestartdayofthe_workweek_100',
            python_callable=lambda dag_run:  dag_run.conf['WorkWeek'].split(" ")[
                0]
        )

        log_get_required_workweek_uri_101 = rail.PythonOperator(
            task_id='log_get_required_workweek_uri_101',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_99'), 'name', rail.result('log_getthestartdayofthe_workweek_100'), 'uri')
        )

        update_work_week_for_user_102 = rail.RepliconServiceOperator(
            task_id='update_work_week_for_user_102',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "dayOfWeekUri": "{{ result('log_get_required_workweek_uri_101') }}"
            }
        )

        if_request_timezone_present_sso_103 = rail.IfOperator(
            task_id='if_request_timezone_present_sso_103',
            test='''{{ dag_run.conf.TimeZone | is_truthy }}''',
            yes_task="_adhoc_http_action_104",
            no_task="if_request_licenseseats_present_sso_107",
        )

        _adhoc_http_action_104 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_104',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            data=None
        )

        log_get_required_timezone_uri_105 = rail.PythonOperator(
            task_id='log_get_required_timezone_uri_105',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_104'), 'displayText', dag_run.conf['TimeZone'], 'uri')
        )

        update_time_zone_for_user_106 = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_106',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "timeZoneUri": "{{ result('log_get_required_timezone_uri_105') }}"
            }
        )

        if_request_licenseseats_present_sso_107 = rail.IfOperator(
            task_id='if_request_licenseseats_present_sso_107',
            test='''{{ dag_run.conf.LicenseSeats | is_truthy }}''',
            yes_task="update_license_seats_for_user_108",
            no_task="if_request_customfieldsupplier_id_present_109",
        )

        update_license_seats_for_user_108 = rail.RepliconServiceOperator(
            task_id='update_license_seats_for_user_108',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "productUris": [
                    "urn:replicon-saas:product:time-bill-plus"
                ]
            }
        )

        if_request_customfieldsupplier_id_present_109 = rail.IfOperator(
            task_id='if_request_customfieldsupplier_id_present_109',
            test='''{{ dag_run.conf.CustomFieldSUPPLIER_ID | is_truthy }}''',
            yes_task="_adhoc_http_action_110",
            no_task="if_request_scheduletype_present_sso_124",
        )

        _adhoc_http_action_110 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_110',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4"
            }
        )

        log_get_required_supplier_i_ddropdownoption_uri_111 = rail.PythonOperator(
            task_id='log_get_required_supplier_i_ddropdownoption_uri_111',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_110'), 'displayText', dag_run.conf['CustomFieldSUPPLIER_ID'], 'uri')
        )

        if_log_16_present_112 = rail.IfOperator(
            task_id='if_log_16_present_112',
            test='''{{ result('log_get_required_supplier_i_ddropdownoption_uri_111') | is_truthy }}''',
            yes_task="update_dropdown_value_113",
            no_task="if_log_16_blank_114",
        )

        update_dropdown_value_113 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_113',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('get_user_details_3').useruri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4",
                "customFieldDropDownOptionUri": "{{ result('log_get_required_supplier_i_ddropdownoption_uri_111') }}"
            }
        )

        if_log_16_blank_114 = rail.IfOperator(
            task_id='if_log_16_blank_114',
            test='''{{ result('log_get_required_supplier_i_ddropdownoption_uri_111') | is_falsy }}''',
            yes_task="put_drop_down_optionsfor_supplier_i_d_119",
            no_task="if_request_scheduletype_present_sso_124",
        )

        def get_supplier_id(dag_run):
            supplier_id_options = []
            supplier_options = rail.result('_adhoc_http_action_110')
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

        put_drop_down_optionsfor_supplier_i_d_119 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_supplier_i_d_119',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4",
                "customFieldDropDownOptionUris": get_supplier_id(dag_run)
            }
        )

        _adhoc_http_action_120 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_120',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4"
            }
        )

        log_get_required_supplier_ddropdownoption_uri_121 = rail.PythonOperator(
            task_id='log_get_required_supplier_ddropdownoption_uri_121',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_120'), 'displayText', dag_run.conf['CustomFieldSUPPLIER_ID'], 'uri')
        )

        if_log_177_present_122 = rail.IfOperator(
            task_id='if_log_177_present_122',
            test='''{{ result('log_get_required_supplier_ddropdownoption_uri_121') | is_truthy }}''',
            yes_task="update_dropdown_value_123",
            no_task="if_request_scheduletype_present_sso_124",
        )

        update_dropdown_value_123 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_123',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('get_user_details_3').useruri }}",
                "customFieldUri": "urn:replicon-tenant:{{get_tenant_slug()}}:user-defined-field:ce45a999-6a60-4407-bbf6-c844b5c5d8a4",
                "customFieldDropDownOptionUri": "{{ result('log_get_required_supplier_ddropdownoption_uri_121') }}"
            }
        )

        if_request_scheduletype_present_sso_124 = rail.IfOperator(
            task_id='if_request_scheduletype_present_sso_124',
            test='''{{ dag_run.conf.ScheduleType | is_truthy }}''',
            yes_task="log_125",
            no_task="if_request_adduserpermission2_present_158",
        )

        log_125 = rail.PythonOperator(
            task_id='log_125',
            python_callable=lambda dag_run:  dag_run.conf['ScheduleType']
        )

        def get_datetime_obj(effectiveDate):
            year = effectiveDate['year']
            month = effectiveDate['month']
            day = effectiveDate['day']
            return datetime.strptime(f"{year}/{month}/{day}", '%Y/%m/%d')

        def is_current_effective_date(effective_date):
            if effective_date:
                replicon_effective_date = get_datetime_obj(
                    effective_date)
                return bool(replicon_effective_date.date() <= pendulum.now(config.pacific_timezone).date())
            return True

        def get_schedulepolicyschedule_assignments(response):
            current_policy = list(filter(lambda x: x['endDate'] is None and is_current_effective_date(
                x['effectiveDate']), response))
            print("current_policy", current_policy)
            if current_policy and len(current_policy) != 0:
                schedule_name = current_policy[0]['officeSchedule']['displayText']
                schedule_date = current_policy[0]['effectiveDate']
                return {
                    "current_shcedule_name": schedule_name,
                    "effective_date": schedule_date}
            return {"current_shcedule_name": None, "effective_date": None}

        _adhoc_http_action_126 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_126',
            endpoint="/services/SchedulingService2.svc/GetSchedulePolicyScheduleForUser",
            data={
                "userUri": '''{{ result('get_user_details_3').useruri }}'''
            },
            data_handler=get_schedulepolicyschedule_assignments
        )

        if_officeschedule_displaytext_present_dataloggerlog_19message_127 = rail.IfOperator(
            task_id='if_officeschedule_displaytext_present_dataloggerlog_19message_127',
            test='''{{ result('_adhoc_http_action_126').current_shcedule_name | is_truthy and result('_adhoc_http_action_126').current_shcedule_name | lower != dag_run.conf.ScheduleType | lower }}''',
            yes_task="log_128",
            no_task="if_officeschedule_Change_with_initial_157",
        )

        log_128 = rail.PythonOperator(
            task_id='log_128',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_126'), 'displayText', dag_run.conf['ScheduleType'], 'uri')
        )

        def new_schedule_effective_date(schedule_policy_task):
            today_date = pendulum.now(
                config.pacific_timezone).strftime('%m/%d/%Y')
            current_effive_policy = rail.result(schedule_policy_task)
            if current_effive_policy['effective_date']:
                year = current_effive_policy['effective_date']['year']
                month = current_effive_policy['effective_date']['month']
                day = current_effive_policy['effective_date']['day']
                current_effective_date = f"{year}/{month}/{day}"
                if current_effective_date == today_date:
                    new_effiective_date = datetime.strptime(
                        f"{year}/{month}/{day}", '%Y/%m/%d').timedelta(days=1)
                    return {
                        "year": new_effiective_date.year,
                        "month": new_effiective_date.month,
                        "day": new_effiective_date.day,
                    }
            return {
                "year": pendulum.now(config.pacific_timezone).year,
                "month": pendulum.now(config.pacific_timezone).month,
                "day": pendulum.now(config.pacific_timezone).day,
            }

        _adhoc_http_action_129 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_129',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": rail.result('get_user_details_3')['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "schedulePolicyToApply": {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementSchedule": [],
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['ScheduleType'],
                                        "officeSchedule": {
                                            "officeScheduleUri": null,
                                            "name": dag_run.conf['ScheduleType']
                                        },
                                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                                    },
                                    "effectiveDate": new_schedule_effective_date('_adhoc_http_action_126')
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_officeschedule_Change_with_initial_157 = rail.IfOperator(
            task_id='if_officeschedule_Change_with_initial_157',
            test='''{{ result('_adhoc_http_action_126').current_shcedule_name | is_falsy or (result('_adhoc_http_action_126').current_shcedule_name | is_truthy and result('_adhoc_http_action_126').current_shcedule_name | lower != dag_run.conf.ScheduleType | lower) }}''',
            yes_task="put_schedule_policy_schedule_for_user_157",
            no_task="if_request_adduserpermission2_present_158",
        )

        put_schedule_policy_schedule_for_user_157 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_157',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
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

        if_request_adduserpermission2_present_158 = rail.IfOperator(
            task_id='if_request_adduserpermission2_present_158',
            test='''{{ dag_run.conf.AddUserPermission2 | is_truthy  or dag_run.conf.AddUserPermission3 | is_truthy  or dag_run.conf.AddUserPermission4 | is_truthy  or dag_run.conf.AddUserPermission5 | is_truthy  or dag_run.conf.AddUserPermission6 | is_truthy  or dag_run.conf.AddUserPermission7 | is_truthy }}''',
            yes_task="_adhoc_http_action_159",
            no_task="if_request_groupcost_center_name_present_171",
        )

        _adhoc_http_action_159 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_159',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        def get_permissions_set_uris(dag_run):
            existing_permissions = rail.result('_adhoc_http_action_159')
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

        put_permission_set_assignments_for_user_170 = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_170',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": rail.result('get_user_details_3')['useruri'],
                "permissionSetUris": get_permissions_set_uris(dag_run)
            }
        )

        if_request_groupcost_center_name_present_171 = rail.IfOperator(
            task_id='if_request_groupcost_center_name_present_171',
            test='''{{ dag_run.conf.GroupCOST_CENTER_NAME | is_truthy }}''',
            yes_task="get_effective_user_group_membership_174",
            no_task="if_request_groupmanagereng_present_239",
        )

        get_effective_user_group_membership_174 = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership_174',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "dateRange": null
            }
        )

        _adhoc_http_action_175 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_175',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data=None
        )

        def is_costcenter_available(dag_run):
            existing_costcenter = rail.result('_adhoc_http_action_175')
            input_costcenter = rail.find_first_by_attr_and_get_attr(
                existing_costcenter, 'displayText', dag_run.conf['GroupCOST_CENTER_NAME'], 'uri')
            return bool(input_costcenter)

        if_pluckuri_firstnil_present_176 = rail.IfOperator(
            task_id='if_pluckuri_firstnil_present_176',
            test=is_costcenter_available,
            yes_task="if_displaytext_downcasenil_not_equals_to_groupcost_center_name_177",
            no_task="if_request_groupmanagereng_present_239",
        )

        if_displaytext_downcasenil_not_equals_to_groupcost_center_name_177 = rail.IfOperator(
            task_id='if_displaytext_downcasenil_not_equals_to_groupcost_center_name_177',
            test='''{{ (result('get_effective_user_group_membership_174').costCenters | length == 0 and dag_run.conf.GroupCOST_CENTER_NAME | is_truthy) or (result('get_effective_user_group_membership_174').costCenters | length > 0 and result('get_effective_user_group_membership_174').costCenters[0].costCenter.costCenter.displayText | is_truthy and result('get_effective_user_group_membership_174').costCenters[0].costCenter.costCenter.displayText | lower != dag_run.conf.GroupCOST_CENTER_NAME | lower) }}''',
            yes_task="if_request_groupcost_center_name_effectivedate_present_178",
            no_task="if_request_groupmanagereng_present_239",
        )

        if_request_groupcost_center_name_effectivedate_present_178 = rail.IfOperator(
            task_id='if_request_groupcost_center_name_effectivedate_present_178',
            test='''{{ dag_run.conf.GroupCOST_CENTER_NAME_EffectiveDate | is_truthy }}''',
            yes_task="_adhoc_http_action_179",
            no_task="_adhoc_http_action_181",
        )

        _adhoc_http_action_179 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_179',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "user": {
                    "uri": "{{ result('get_user_details_3').useruri }}"
                },
                "modifications": {
                    "costCenterScheduleToApply": {
                        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "updateCostCenterScheduleOverDateRange": {
                            "replacementCostCenterScheduleEntries": {
                                "costCenter": {
                                    "name": "{{ dag_run.conf.GroupCOST_CENTER_NAME }}"
                                },
                                "effectiveDate": {
                                    "year": '''{{ dag_run.conf.GroupCOST_CENTER_NAME_EffectiveDate.split("/")[2]}}''',
                                    "month": '''{{ dag_run.conf.GroupCOST_CENTER_NAME_EffectiveDate.split("/")[0] }}''',
                                    "day": '''{{ dag_run.conf.GroupCOST_CENTER_NAME_EffectiveDate.split("/")[1] }}''',
                                }
                            }
                        }
                    }
                }
            }
        )

        _adhoc_http_action_181 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_181',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "user": {
                    "uri": "{{ result('get_user_details_3').useruri }}"
                },
                "modifications": {
                    "costCenterScheduleToApply": {
                        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "updateCostCenterScheduleOverDateRange": {
                            "replacementCostCenterScheduleEntries": {
                                "costCenter": {
                                    "name": "{{ dag_run.conf.GroupCOST_CENTER_NAME }}"
                                },
                                "effectiveDate": {
                                    "year": '''{{ result('log_today_date_52')').split("/")[2] }}''',
                                    "month": '''{{ result('log_today_date_52')').split("/")[0] }}''',
                                    "day": '''{{ result('log_today_date_52')').split("/")[1] }}''',
                                }
                            }
                        }
                    }
                }
            }
        )

        if_request_groupmanagereng_present_239 = rail.IfOperator(
            task_id='if_request_groupmanagereng_present_239',
            test='''{{ dag_run.conf.GroupManagerENG | is_truthy }}''',
            yes_task="get_effective_user_group_membership_240",
            no_task="dtna_user_import_add_entry_307",
        )

        get_effective_user_group_membership_240 = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership_240',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ result('get_user_details_3').useruri }}",
                "dateRange": null
            }
        )

        _adhoc_http_action_241 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_241',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
            data=None
        )

        def is_division_available(dag_run):
            existing_division = rail.result('_adhoc_http_action_241')
            input_division = rail.find_first_by_attr_and_get_attr(
                existing_division, 'displayText', dag_run.conf['GroupManagerENG'], 'uri')
            return bool(input_division)

        if_pluckuri_firstnil_present_242 = rail.IfOperator(
            task_id='if_pluckuri_firstnil_present_242',
            test=is_division_available,
            yes_task="if_displaytext_downcasenil_not_equals_to_dataworkato_servicereceive_requestrequestgroupmanagerengdowncase_243",
            no_task="dtna_user_import_add_entry_307",
        )

        if_displaytext_downcasenil_not_equals_to_dataworkato_servicereceive_requestrequestgroupmanagerengdowncase_243 = rail.IfOperator(
            task_id='if_displaytext_downcasenil_not_equals_to_dataworkato_servicereceive_requestrequestgroupmanagerengdowncase_243',
            test='''{{ (result('get_effective_user_group_membership_240').divisions | length == 0 and dag_run.conf.GroupManagerENG | is_truthy) or (result('get_effective_user_group_membership_240').divisions | length > 0 and result('get_effective_user_group_membership_240').divisions[0].division.division.displayText | is_truthy and result('get_effective_user_group_membership_240').divisions[0].division.division.displayText | lower != dag_run.conf.GroupManagerENG| lower) }}''',
            yes_task="if_request_groupmanagerengeffectivedate_present_244",
            no_task="dtna_user_import_add_entry_307",
        )

        if_request_groupmanagerengeffectivedate_present_244 = rail.IfOperator(
            task_id='if_request_groupmanagerengeffectivedate_present_244',
            test='''{{ dag_run.conf.GroupManagerENGEffectiveDate | is_truthy }}''',
            yes_task="_adhoc_http_action_245",
            no_task="_adhoc_http_action_247",
        )

        _adhoc_http_action_245 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_245',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('get_user_details_3').useruri }}"
                },
                "modifications": {
                    "divisionScheduleToApply": {
                        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "updateDivisionScheduleOverDateRange": {
                            "replacementDivisionScheduleEntries": {
                                "division": {
                                    "name": "{{ dag_run.conf.GroupManagerENG }}"
                                },
                                "effectiveDate": {
                                    "year": '''{{ dag_run.conf.GroupManagerENGEffectiveDate.split("/")[2] }}''',
                                    "month": '''{{ dag_run.conf.GroupManagerENGEffectiveDate.split("/")[0] }}''',
                                    "day": '''{{ dag_run.conf.GroupManagerENGEffectiveDate.split("/")[1] }}''',
                                }
                            }
                        }
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
            }
        )

        _adhoc_http_action_247 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_247',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('get_user_details_3').useruri }}"
                },
                "modifications": {
                    "divisionScheduleToApply": {
                        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "updateDivisionScheduleOverDateRange": {
                            "replacementDivisionScheduleEntries": {
                                "division": {
                                    "name": "{{ dag_run.conf.GroupManagerENG }}"
                                },
                                "effectiveDate": {
                                    "year": '''{{ result('log_today_date_52').split("/")[2] }}''',
                                    "month": '''{{ result('log_today_date_52').split("/")[0] }}''',
                                    "day": '''{{ result('log_today_date_52').split("/")[1] }}'''
                                }
                            }
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        dtna_user_import_add_entry_307 = rail.WriteLogOperator(
            task_id='dtna_user_import_add_entry_307',
            message="na",
            severity="Success",
            properties={
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "status": "Success",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        dtna_user_import_add_entry_309 = rail.WriteLogOperator(
            task_id='dtna_user_import_add_entry_309',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
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
            'No') >> get_user_details_3 >> if_request_isloginenabled_equals_to_true_4
        if_request_isloginenabled_equals_to_true_4 >> rail.Label(
            'No') >> if_request_employeeid_present_sso_6
        if_request_isloginenabled_equals_to_true_4 >> rail.Label(
            'Yes') >> _adhoc_http_action_enable_login_statusforthe_user_5 >> if_request_employeeid_present_sso_6
        if_request_employeeid_present_sso_6 >> rail.Label(
            'No') >> if_request_firstname_present_sso_8
        if_request_employeeid_present_sso_6 >> rail.Label(
            'Yes') >> update_employee_id_7 >> if_request_firstname_present_sso_8
        if_request_firstname_present_sso_8 >> rail.Label(
            'No') >> if_request_lastname_present_sso_10
        if_request_firstname_present_sso_8 >> rail.Label(
            'Yes') >> update_first_name_9 >> if_request_lastname_present_sso_10
        if_request_lastname_present_sso_10 >> rail.Label(
            'No') >> if_request_email_present_sso_12
        if_request_lastname_present_sso_10 >> rail.Label(
            'Yes') >> update_last_name_11 >> if_request_email_present_sso_12
        if_request_email_present_sso_12 >> rail.Label(
            'No') >> if_request_employeetype_present_14
        if_request_email_present_sso_12 >> rail.Label(
            'Yes') >> update_email_13 >> if_request_employeetype_present_14
        if_request_employeetype_present_14 >> rail.Label(
            'No') >> if_request_departmentname_present_21
        if_request_employeetype_present_14 >> rail.Label('Yes') >> _adhoc_http_action_15 >> log_get_required_employee_type_uri_19 >>\
            update_employee_type_for_user_20 >> if_request_departmentname_present_21
        if_request_departmentname_present_21 >> rail.Label(
            'No') >> if_request_timesheetapprovalpath_present_25
        if_request_departmentname_present_21 >> rail.Label('Yes') >> _adhoc_http_action_22 >> log_get_required_department_uri_23 >> \
            update_department_for_user_24 >> if_request_timesheetapprovalpath_present_25
        if_request_timesheetapprovalpath_present_25 >> rail.Label(
            'No') >> if_request_timesheettemplate_present_29
        if_request_timesheetapprovalpath_present_25 >> rail.Label('Yes') >> _adhoc_http_action_26 >> log_get_required_timesheet_approval_path_uri_27 >> \
            update_approval_path_for_user_28 >> if_request_timesheettemplate_present_29
        if_request_timesheettemplate_present_29 >> rail.Label(
            'No') >> if_request_timesheetperiodtype_present_33
        if_request_timesheettemplate_present_29 >> rail.Label('Yes') >> _adhoc_http_action_30 >> log_get_required_timesheet_template_uri_31 >> \
            put_policy_set_assignments_for_user_32 >> if_request_timesheetperiodtype_present_33
        if_request_timesheetperiodtype_present_33 >> rail.Label(
            'No') >> if_request_customfieldwrkr_id_present_sso_40
        if_request_timesheetperiodtype_present_33 >> rail.Label(
            'Yes') >> if_request_timesheetperiodtype_equals_to_system_34
        if_request_timesheetperiodtype_equals_to_system_34 >> rail.Label(
            'No') >> if_request_timesheetperiodtype_equals_to_employeetype_36
        if_request_timesheetperiodtype_equals_to_system_34 >> rail.Label(
            'Yes') >> update_timesheetperiod_35 >> if_request_timesheetperiodtype_equals_to_employeetype_36
        if_request_timesheetperiodtype_equals_to_employeetype_36 >> rail.Label(
            'No') >> if_request_timesheetperiodtype_equals_to_department_38
        if_request_timesheetperiodtype_equals_to_employeetype_36 >> rail.Label(
            'Yes') >> update_timesheetperiod_37 >> if_request_timesheetperiodtype_equals_to_department_38
        if_request_timesheetperiodtype_equals_to_department_38 >> rail.Label(
            'No') >> if_request_customfieldwrkr_id_present_sso_40
        if_request_timesheetperiodtype_equals_to_department_38 >> rail.Label(
            'Yes') >> update_timesheetperiod_39 >> if_request_customfieldwrkr_id_present_sso_40
        if_request_customfieldwrkr_id_present_sso_40 >> rail.Label(
            'No') >> if_request_customfieldclnt_wrkr_id_present_sso_42
        if_request_customfieldwrkr_id_present_sso_40 >> rail.Label(
            'Yes') >> update_custom_field_w_r_k_r_i_d_41 >> if_request_customfieldclnt_wrkr_id_present_sso_42
        if_request_customfieldclnt_wrkr_id_present_sso_42 >> rail.Label(
            'No') >> if_request_customfieldjob_code_present_sso_44
        if_request_customfieldclnt_wrkr_id_present_sso_42 >> rail.Label(
            'Yes') >> update_custom_field_c_l_n_t_w_r_k_r_i_d_43 >> if_request_customfieldjob_code_present_sso_44
        if_request_customfieldjob_code_present_sso_44 >> rail.Label(
            'No') >> if_request_customfieldhiring_manager_id_present_sso_46
        if_request_customfieldjob_code_present_sso_44 >> rail.Label(
            'Yes') >> update_custom_field_j_o_b_c_o_d_e_45 >> if_request_customfieldhiring_manager_id_present_sso_46
        if_request_customfieldhiring_manager_id_present_sso_46 >> rail.Label(
            'No') >> if_request_customfieldappr_id_present_sso_48
        if_request_customfieldhiring_manager_id_present_sso_46 >> rail.Label(
            'Yes') >> update_custom_field_h_i_r_i_n_g_m_a_n_a_g_e_r_i_d_47 >> if_request_customfieldappr_id_present_sso_48
        if_request_customfieldappr_id_present_sso_48 >> rail.Label(
            'No') >> if_request_customfieldinitialseng_present_sso_50
        if_request_customfieldappr_id_present_sso_48 >> rail.Label(
            'Yes') >> update_custom_field_a_p_p_r_i_d_49 >> if_request_customfieldinitialseng_present_sso_50
        if_request_customfieldinitialseng_present_sso_50 >> rail.Label(
            'No') >> log_today_date_52
        if_request_customfieldinitialseng_present_sso_50 >> rail.Label('Yes') >> update_custom_field_initials_e_n_g_51 >> log_today_date_52 >>\
            if_request_supervisorloginname_present_53
        if_request_supervisorloginname_present_53 >> rail.Label(
            'No') >> if_request_startdate_present_73
        if_request_supervisorloginname_present_53 >> rail.Label(
            'Yes') >> get_get_supervisordetails_55 >> get_user_details_56 >> if_supervisor_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorloginname_56
        if_supervisor_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorloginname_56 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_67
        if_supervisor_loginname_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorloginname_56 >> rail.Label(
            'Yes') >> if_request_supervisorstartdate_blank_57
        if_request_supervisorstartdate_blank_57 >> rail.Label(
            'No') >> if_request_supervisorstartdate_present_62
        if_request_supervisorstartdate_blank_57 >> rail.Label('Yes') >> \
            update_supervisor_assignment_schedule_over_date_range_updatedsupervisorwith_effective_date_61 >> \
            if_request_supervisorstartdate_present_62
        if_request_supervisorstartdate_present_62 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_67
        if_request_supervisorstartdate_present_62 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_updatedsupervisorwith_effective_date_66 >> if_request_holidaycalendar_present_67
        if_request_holidaycalendar_present_67 >> rail.Label(
            'No') >> if_request_startdate_present_73
        if_request_holidaycalendar_present_67 >> rail.Label('Yes') >> _adhoc_http_action_68 >> \
            log_get_required_holiday_calendar_uri_69 >> update_holiday_calendar_70 >> \
            if_request_startdate_present_73
        if_request_startdate_present_73 >> rail.Label(
            'No') >> if_request_authenticationtype_equals_to_sso_96
        if_request_startdate_present_73 >> rail.Label(
            'Yes') >> if_request_startdate_present_74
        if_request_startdate_present_74 >> rail.Label(
            'No') >> if_request_startdate_present_79
        if_request_startdate_present_74 >> rail.Label(
            'Yes') >> update_employment_date_range_78 >> if_request_startdate_present_79
        if_request_startdate_present_79 >> rail.Label(
            'No') >> if_request_startdate_blank_87
        if_request_startdate_present_79 >> rail.Label(
            'Yes') >> update_employment_date_range_86 >> if_request_startdate_blank_87
        if_request_startdate_blank_87 >> rail.Label(
            'No') >> if_request_authenticationtype_equals_to_sso_96
        if_request_startdate_blank_87 >> rail.Label(
            'Yes') >> update_employment_date_range_95 >> if_request_authenticationtype_equals_to_sso_96
        if_request_authenticationtype_equals_to_sso_96 >> rail.Label(
            'No') >> if_request_workweek_present_sso_98
        if_request_authenticationtype_equals_to_sso_96 >> rail.Label(
            'Yes') >> set_s_s_o_authentication_for_user_97 >> if_request_workweek_present_sso_98
        if_request_workweek_present_sso_98 >> rail.Label(
            'No') >> if_request_timezone_present_sso_103
        if_request_workweek_present_sso_98 >> rail.Label('Yes') >> _adhoc_http_action_99 >> log_getthestartdayofthe_workweek_100 >> log_get_required_workweek_uri_101 >> update_work_week_for_user_102 >> \
            if_request_timezone_present_sso_103
        if_request_timezone_present_sso_103 >> rail.Label(
            'No') >> if_request_licenseseats_present_sso_107
        if_request_timezone_present_sso_103 >> rail.Label('Yes') >> _adhoc_http_action_104 >> log_get_required_timezone_uri_105 >> update_time_zone_for_user_106 >> \
            if_request_licenseseats_present_sso_107
        if_request_licenseseats_present_sso_107 >> rail.Label(
            'No') >> if_request_customfieldsupplier_id_present_109
        if_request_licenseseats_present_sso_107 >> rail.Label(
            'Yes') >> update_license_seats_for_user_108 >> if_request_customfieldsupplier_id_present_109
        if_request_customfieldsupplier_id_present_109 >> rail.Label(
            'No') >> if_request_scheduletype_present_sso_124
        if_request_customfieldsupplier_id_present_109 >> rail.Label('Yes') >> _adhoc_http_action_110 >> log_get_required_supplier_i_ddropdownoption_uri_111 >> \
            if_log_16_present_112
        if_log_16_present_112 >> rail.Label('No') >> if_log_16_blank_114
        if_log_16_present_112 >> rail.Label(
            'Yes') >> update_dropdown_value_113 >> if_log_16_blank_114
        if_log_16_blank_114 >> rail.Label(
            'No') >> if_request_scheduletype_present_sso_124
        if_log_16_blank_114 >> rail.Label('Yes') >> put_drop_down_optionsfor_supplier_i_d_119 >> _adhoc_http_action_120 >> \
            log_get_required_supplier_ddropdownoption_uri_121 >> \
            if_log_177_present_122
        if_log_177_present_122 >> rail.Label(
            'No') >> if_request_scheduletype_present_sso_124
        if_log_177_present_122 >> rail.Label(
            'Yes') >> update_dropdown_value_123 >> if_request_scheduletype_present_sso_124
        if_request_scheduletype_present_sso_124 >> rail.Label(
            'No') >> if_request_adduserpermission2_present_158
        if_request_scheduletype_present_sso_124 >> rail.Label(
            'Yes') >> log_125 >> _adhoc_http_action_126 >> if_officeschedule_displaytext_present_dataloggerlog_19message_127
        if_officeschedule_displaytext_present_dataloggerlog_19message_127 >> rail.Label(
            'No') >> if_officeschedule_Change_with_initial_157
        if_officeschedule_Change_with_initial_157 >> rail.Label(
            'No') >> if_request_adduserpermission2_present_158
        if_officeschedule_Change_with_initial_157 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_157 >> if_request_adduserpermission2_present_158
        if_officeschedule_displaytext_present_dataloggerlog_19message_127 >> rail.Label(
            'Yes') >> log_128 >> _adhoc_http_action_129 >> if_request_adduserpermission2_present_158
        if_request_adduserpermission2_present_158 >> rail.Label(
            'No') >> if_request_groupcost_center_name_present_171
        if_request_adduserpermission2_present_158 >> rail.Label(
            'Yes') >> _adhoc_http_action_159 >> put_permission_set_assignments_for_user_170 >> if_request_groupcost_center_name_present_171
        if_request_groupcost_center_name_present_171 >> rail.Label(
            'No') >> if_request_groupmanagereng_present_239
        if_request_groupcost_center_name_present_171 >> rail.Label(
            'Yes') >> get_effective_user_group_membership_174 >> _adhoc_http_action_175 >> if_pluckuri_firstnil_present_176
        if_pluckuri_firstnil_present_176 >> rail.Label(
            'No') >> if_request_groupmanagereng_present_239
        if_pluckuri_firstnil_present_176 >> rail.Label(
            'Yes') >> if_displaytext_downcasenil_not_equals_to_groupcost_center_name_177
        if_displaytext_downcasenil_not_equals_to_groupcost_center_name_177 >> rail.Label(
            'No') >> if_request_groupmanagereng_present_239
        if_displaytext_downcasenil_not_equals_to_groupcost_center_name_177 >> rail.Label(
            'Yes') >> if_request_groupcost_center_name_effectivedate_present_178
        if_request_groupcost_center_name_effectivedate_present_178 >> rail.Label(
            'No') >> _adhoc_http_action_179 >> if_request_groupmanagereng_present_239
        if_request_groupcost_center_name_effectivedate_present_178 >> rail.Label(
            'No') >> _adhoc_http_action_181 >> if_request_groupmanagereng_present_239
        if_request_groupmanagereng_present_239 >> rail.Label(
            'No') >> dtna_user_import_add_entry_307
        if_request_groupmanagereng_present_239 >> rail.Label('Yes') >> get_effective_user_group_membership_240 >> _adhoc_http_action_241 >> \
            if_pluckuri_firstnil_present_242
        if_pluckuri_firstnil_present_242 >> rail.Label(
            'No') >> dtna_user_import_add_entry_307
        if_pluckuri_firstnil_present_242 >> rail.Label(
            'Yes') >> if_displaytext_downcasenil_not_equals_to_dataworkato_servicereceive_requestrequestgroupmanagerengdowncase_243
        if_displaytext_downcasenil_not_equals_to_dataworkato_servicereceive_requestrequestgroupmanagerengdowncase_243 >> rail.Label(
            'No') >> dtna_user_import_add_entry_307
        if_displaytext_downcasenil_not_equals_to_dataworkato_servicereceive_requestrequestgroupmanagerengdowncase_243 >> rail.Label(
            'Yes') >> if_request_groupmanagerengeffectivedate_present_244
        if_request_groupmanagerengeffectivedate_present_244 >> rail.Label(
            'No') >> _adhoc_http_action_245 >> dtna_user_import_add_entry_307
        if_request_groupmanagerengeffectivedate_present_244 >> rail.Label(
            'Yes') >> _adhoc_http_action_247 >> dtna_user_import_add_entry_307
        
        dtna_user_import_add_entry_307 >> dtna_user_import_add_entry_309 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
