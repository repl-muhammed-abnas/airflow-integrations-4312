
from datetime import datetime, timedelta, timezone
import json
import itertools
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from nrdc.user_import_v1.utils.custom_method import c3_c4_supervisors_loginname
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.nrdc_basicaddupdate,
        description=f'Live|NRDC_Basic Add/Update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
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
            no_task='c3_c4_supervisors'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='c3_c4_supervisors',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        c3_c4_supervisors = rail.PythonOperator(
            task_id="c3_c4_supervisors",
            python_callable=c3_c4_supervisors_loginname,
            op_args=[config.c3_c4_profile_supervisors_variable]
        )

        log_startdate_4 = rail.PythonOperator(
            task_id='log_startdate_4',
            python_callable=lambda dag_run: datetime.strptime(
                dag_run.conf['whencreated'], '%Y-%m-%d %H:%M:%S').day
        )

        if_request_firstname_blank_5 = rail.IfOperator(
            task_id='if_request_firstname_blank_5',
            test="{{ dag_run.conf.firstname | is_falsy  or dag_run.conf.lastname | is_falsy }}",
            yes_task="nrdc_user_import_logs_add_entry_6",
            no_task="if_request_emailaddress_blank_8",
        )

        nrdc_user_import_logs_add_entry_6 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_6',
            message="fixme get message from prop ",
            severity="fixme get severity from prop ",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "{{ dag_run_ecid() }} - Add User - First Name and Last Name must be provided",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_7 = rail.EmptyOperator(
            task_id='stop_7',
        )

        if_request_emailaddress_blank_8 = rail.IfOperator(
            task_id='if_request_emailaddress_blank_8',
            test='''{{ dag_run.conf.emailaddress | is_falsy }}''',
            yes_task="nrdc_user_import_logs_add_entry_9",
            no_task="if_request_department_blank_11",
        )

        nrdc_user_import_logs_add_entry_9 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_9',
            message="fixme get message from prop ",
            severity="fixme get severity from prop ",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "{{ dag_run_ecid() }} - Add User - Email address must be provided",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_10 = rail.EmptyOperator(
            task_id='stop_10',
        )

        if_request_department_blank_11 = rail.IfOperator(
            task_id='if_request_department_blank_11',
            test='''{{ dag_run.conf.department | is_falsy }}''',
            yes_task="nrdc_user_import_logs_add_entry_12",
            no_task="if_request_loginname_blank_14",
        )

        nrdc_user_import_logs_add_entry_12 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_12',
            message="fixme get message from prop ",
            severity="fixme get severity from prop ",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "{{ dag_run_ecid() }} - Add User - Department must be provided",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_13 = rail.EmptyOperator(
            task_id='stop_13',

        )

        if_request_loginname_blank_14 = rail.IfOperator(
            task_id='if_request_loginname_blank_14',
            test='''{{ dag_run.conf.loginname | is_falsy }}''',
            yes_task="nrdc_user_import_logs_add_entry_15",
            no_task="search_users_17",
        )

        nrdc_user_import_logs_add_entry_15 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_15',
            message="fixme get message from prop ",
            severity="fixme get severity from prop ",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "{{ dag_run_ecid() }} - Add User - Logon name must be provided",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_16 = rail.EmptyOperator(
            task_id='stop_16',

        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            return list(map(lambda row: {
                'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'loginname': row['cells'][1]['textValue'],
                'useruri': row['cells'][1]['uri']

            }, flaten_rows))

        search_users_17 = rail.RepliconServicePageOperator(
            task_id="search_users_17",
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
                            'text': dag_run.conf['loginname']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler
        )

        def get_user_uri_17(profile_name, users_list_task):
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result(users_list_task)))
            return user_info[0]['useruri'] if user_info else None

        def get_user_status_91(profile_name, users_list_task):
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result(users_list_task)))
            return user_info[0]['status'] if user_info else 'False'

        log_presenceofexistingloginname_18 = rail.PythonOperator(
            task_id='log_presenceofexistingloginname_18',
            # pylint: disable=line-too-long
            python_callable=lambda dag_run:  get_user_uri_17(
                dag_run.conf['loginname'], 'search_users_17')
        )

        if_log_presenceofexistingloginname_18_present_19 = rail.IfOperator(
            task_id='if_log_presenceofexistingloginname_18_present_19',
            test='''{{ result('log_presenceofexistingloginname_18') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_20",
            no_task="log_startdate_22",
        )

        nrdc_user_import_logs_add_entry_20 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_20',
            message="fixme get message from prop ",
            severity="fixme get severity from prop ",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Exception",
                "details": "{{ dag_run_ecid() }} Login name already exists.",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_21 = rail.EmptyOperator(
            task_id='stop_21',

        )

        log_startdate_22 = rail.PythonOperator(
            task_id='log_startdate_22',
            python_callable=lambda dag_run: datetime.strptime(
                dag_run.conf['whencreated'], '%Y-%m-%d %H:%M:%S').month
        )

        log_startdate_23 = rail.PythonOperator(
            task_id='log_startdate_23',
            python_callable=lambda dag_run: datetime.strptime(
                dag_run.conf['whencreated'], '%Y-%m-%d %H:%M:%S').year
        )

        log_today_24 = rail.PythonOperator(
            task_id='log_today_24',
            python_callable=lambda:  '''=today.to_date'''
        )

        log_todayday_25 = rail.PythonOperator(
            task_id='log_todayday_25',
            python_callable=lambda: datetime.now(timezone.utc).day
        )

        log_todaymonth_26 = rail.PythonOperator(
            task_id='log_todaymonth_26',
            python_callable=lambda:  datetime.now(timezone.utc).month
        )

        log_todayyear_27 = rail.PythonOperator(
            task_id='log_todayyear_27',
            python_callable=lambda:  datetime.now(timezone.utc).year
        )

        get_enabled_departments_28 = rail.RepliconServiceOperator(
            task_id='get_enabled_departments_28',
            endpoint="/services/departmentService1.svc/GetEnabledDepartments",
        )

        def get_existing_department_uri(task_name):
            dag_run_department = rail.get_dag_run_conf()['department']
            existing_department = rail.result(task_name)
            input_department_info = list(filter(
                lambda item: item['displayText'] == dag_run_department, existing_department))
            return input_department_info[0]['uri'] if input_department_info else None

        log_department_uri_29 = rail.PythonOperator(
            task_id='log_department_uri_29',
            python_callable=lambda: get_existing_department_uri(
                'get_enabled_departments_28')
        )

        get_all_custom_fields_30 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_30',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data=lambda: {
                "objectUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user:1"
            }
        )

        def get_customoef_uri(custom_field_info):
            existing_customoefs = rail.result('get_all_custom_fields_30')
            input_department_info = list(filter(
                lambda item: item['displayText'] == custom_field_info, existing_customoefs))
            return input_department_info[0]['uri'] if input_department_info else None

        log_u_d_f_uri_office_31 = rail.PythonOperator(
            task_id='log_u_d_f_uri_office_31',
            python_callable=lambda: get_customoef_uri("Office")
        )

        log_u_d_f_uri_type_32 = rail.PythonOperator(
            task_id='log_u_d_f_uri_type_32',
            python_callable=lambda:  get_customoef_uri("Type")
        )

        log_u_d_f_uri_title_33 = rail.PythonOperator(
            task_id='log_u_d_f_uri_title_33',
            python_callable=lambda:  get_customoef_uri("Title")
        )

        log_u_d_f_email_notification_34 = rail.PythonOperator(
            task_id='log_u_d_f_email_notification_34',
            python_callable=lambda:  get_customoef_uri("Email Notification")
        )

        log_u_d_f_urii_c_i_m_s_number_35 = rail.PythonOperator(
            task_id='log_u_d_f_urii_c_i_m_s_number_35',
            python_callable=lambda:  get_customoef_uri("iCIMS Number")
        )

        log_u_d_f_uri_employee_number_n_o_t_i_n_u_s_e_currently_36 = rail.PythonOperator(
            task_id='log_u_d_f_uri_employee_number_n_o_t_i_n_u_s_e_currently_36',
            python_callable=lambda:  get_customoef_uri("Employee Number")
        )

        log_u_d_f_uri_user_name_n_o_t_i_n_u_s_e_currently_37 = rail.PythonOperator(
            task_id='log_u_d_f_uri_user_name_n_o_t_i_n_u_s_e_currently_37',
            python_callable=lambda:   get_customoef_uri("User-Name")
        )

        declare_list_38 = rail.SetVariableOperator(
            task_id='declare_list_38',
            append=False,
            name='customFieldValues',
            value=[]
        )

        if_request_office_present_39 = rail.IfOperator(
            task_id='if_request_office_present_39',
            test='''{{ dag_run.conf.office | is_truthy }}''',
            yes_task="insert_to_list_office_40",
            no_task="if_request_empnumber_present_41",
        )

        insert_to_list_office_40 = rail.SetVariableOperator(
            task_id='insert_to_list_office_40',
            append=True,
            name='{{ result("declare_list_38").name }}',
            value={
                "customField": {
                    "uri": "{{ result('log_u_d_f_uri_office_31') }}"
                },
                "dropDownOption": {
                    "name": "{{ dag_run.conf.office }}"
                }
            }
        )

        if_request_empnumber_present_41 = rail.IfOperator(
            task_id='if_request_empnumber_present_41',
            test='''{{ dag_run.conf.empnumber | is_truthy }}''',
            yes_task="insert_to_list_i_c_i_m_s_i_d_42",
            no_task="if_request_userfullname_present_44",
        )

        insert_to_list_i_c_i_m_s_i_d_42 = rail.SetVariableOperator(
            task_id='insert_to_list_i_c_i_m_s_i_d_42',
            append=True,
            name='{{ result("declare_list_38").name }}',
            value={
                "customField": {
                    "uri": "{{ result('log_u_d_f_urii_c_i_m_s_number_35') }}"
                },
                "text": "{{ dag_run.conf.empnumber }}"
            }
        )

        insert_to_list_employeenumber_43 = rail.SetVariableOperator(
            task_id='insert_to_list_employeenumber_43',
            append=True,
            name='{{ result("declare_list_38").name }}',
            value={
                "customField": {
                    "uri": "{{ result('log_u_d_f_uri_employee_number_n_o_t_i_n_u_s_e_currently_36') }}"
                },
                "text": "{{ dag_run.conf.empnumber }}"
            }
        )

        if_request_userfullname_present_44 = rail.IfOperator(
            task_id='if_request_userfullname_present_44',
            test='''{{ dag_run.conf.userfullname | is_truthy }}''',
            yes_task="insert_to_list_username_45",
            no_task="if_request_title_present_46",
        )

        insert_to_list_username_45 = rail.SetVariableOperator(
            task_id='insert_to_list_username_45',
            append=True,
            name='{{ result("declare_list_38").name }}',
            value=lambda dag_run: {
                "customField": {
                    "uri": rail.result('log_u_d_f_uri_user_name_n_o_t_i_n_u_s_e_currently_37')
                },
                "text": dag_run.conf['userfullname']
            }
        )

        if_request_title_present_46 = rail.IfOperator(
            task_id='if_request_title_present_46',
            test='''{{ dag_run.conf.title | is_truthy }}''',
            yes_task="insert_to_list_title_47",
            no_task="if_request_emailaddress_present_48",
        )

        insert_to_list_title_47 = rail.SetVariableOperator(
            task_id='insert_to_list_title_47',
            append=True,
            name='{{ result("declare_list_38").name }}',
            value={
                "customField": {
                    "uri": "{{ result('log_u_d_f_uri_title_33') }}"
                },
                "text": "{{ dag_run.conf.title }}"
            }
        )

        if_request_emailaddress_present_48 = rail.IfOperator(
            task_id='if_request_emailaddress_present_48',
            test='''{{ dag_run.conf.emailaddress | is_truthy }}''',
            yes_task="insert_to_list_title_49",
            no_task="declare_list_50",
        )

        insert_to_list_title_49 = rail.SetVariableOperator(
            task_id='insert_to_list_title_49',
            append=True,
            name='{{ result("declare_list_38").name }}',
            value={
                "customField": {
                    "uri": "{{ result('log_u_d_f_email_notification_34') }}"
                },
                "text": "{{ dag_run.conf.emailaddress }}"
            }
        )

        declare_list_50 = rail.SetVariableOperator(
            task_id='declare_list_50',
            append=False,
            name='permissionsets',
            value=[]
        )

        insert_to_list_51 = rail.SetVariableOperator(
            task_id='insert_to_list_51',
            append=True,
            name='{{ result("declare_list_50").name }}',
            value={
                "name": "End user"
            }
        )

        if_request_locationuri_present_52 = rail.IfOperator(
            task_id='if_request_locationuri_present_52',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="insert_to_list_53",
            no_task="if_request_type_equals_to_delegate_57",
        )

        insert_to_list_53 = rail.SetVariableOperator(
            task_id='insert_to_list_53',
            append=True,
            name='{{ result("declare_list_50").name }}',
            value={
                "name": "All Timesheets"
            }
        )

        log_policydataaccessscopeforthepermission_54 = rail.PythonOperator(
            task_id='log_policydataaccessscopeforthepermission_54',
            python_callable=lambda dag_run: [
                {
                    "policyUri": "urn:replicon:policy:payroll-management",
                    "location": {
                        "uri": dag_run.conf['locationuri'],
                        "parentUri": null,
                        "name": null
                    },
                    "division": null,
                    "serviceCenter": null,
                    "costCenter": null,
                    "departmentGroup": null,
                    "employeeTypeGroup": null
                }
            ]
        )

        else_55 = rail.EmptyOperator(
            task_id='else_55',
        )

        log_policydataaccessscopeforthepermissionwithoutlocation_56 = rail.PythonOperator(
            task_id='log_policydataaccessscopeforthepermissionwithoutlocation_56',
            python_callable=lambda:  '''[]'''
        )

        if_request_type_equals_to_delegate_57 = rail.IfOperator(
            task_id='if_request_type_equals_to_delegate_57',
            test='''{{ dag_run.conf.type == 'Delegate' }}''',
            yes_task="insert_to_list_58",
            no_task="log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_59",
        )

        insert_to_list_58 = rail.SetVariableOperator(
            task_id='insert_to_list_58',
            append=True,
            name='{{ result("declare_list_50").name }}',
            value=lambda: {
                "name": "Delegates"
            }
        )

        log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_59 = rail.PythonOperator(
            task_id='log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_59',
            # pylint: disable=line-too-long
            python_callable=lambda:  rail.result('log_policydataaccessscopeforthepermission_54') or rail.result(
                'log_policydataaccessscopeforthepermissionwithoutlocation_56')
        )

        log_f_i_n_a_lpermissiontopass_60 = rail.PythonOperator(
            task_id='log_f_i_n_a_lpermissiontopass_60',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_list_50')['name'])
        )

        insert_to_list_type_61 = rail.SetVariableOperator(
            task_id='insert_to_list_type_61',
            append=True,
            name='{{ result("declare_list_38").name }}',
            value={
                "customField": {
                    "uri": "{{ result('log_u_d_f_uri_type_32') }}"
                },
                "dropDownOption": {
                    "name": "{{ dag_run.conf.type }}"
                }
            }
        )

        log_customfieldbody_62 = rail.PythonOperator(
            task_id='log_customfieldbody_62',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_list_38')['name'])
        )

        log_requireduserstatus_63 = rail.PythonOperator(
            task_id='log_requireduserstatus_63',
            python_callable=lambda dag_run: "true" if dag_run.conf['status'].lower(
            ) == "enabled" else "false"
        )

        if_authtype_downcase_equals_to_sso_64 = rail.IfOperator(
            task_id='if_authtype_downcase_equals_to_sso_64',
            test="{{ dag_run.conf.authtype =='sso' }}",
            yes_task="if_request_timesheettype_present_65",
            no_task="if_authtype_downcase_equals_to_replicon_69",
        )

        if_request_timesheettype_present_65 = rail.IfOperator(
            task_id='if_request_timesheettype_present_65',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.timesheettype | is_truthy  and dag_run.conf.timesheettype | matches('NA') | is_falsy  and dag_run.conf.timesheettype | matches('No Timesheet') | is_falsy  and dag_run.conf.timesheettype | matches('na') | is_falsy }}''',
            yes_task="put_user2_66",
            no_task="put_user2_68",
        )

        put_user2_66 = rail.RepliconServiceOperator(
            task_id='put_user2_66',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['loginname'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_29'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_startdate_23'),
                            "month": rail.result('log_startdate_22'),
                            "day": rail.result('log_startdate_4')
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": rail.result('log_requireduserstatus_63'),
                        "loginName": dag_run.conf['loginname'],
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_60'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": dag_run.conf['timesheettype']
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    } if config.instance == "production" else null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_62'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "policyDataAccessScopes": rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_59'),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        put_user2_68 = rail.RepliconServiceOperator(
            task_id='put_user2_68',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['loginname'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_29'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_startdate_23'),
                            "month": rail.result('log_startdate_22'),
                            "day": rail.result('log_startdate_4')
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": rail.result('log_requireduserstatus_63'),
                        "loginName": dag_run.conf['loginname'],
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_60'))),
                    "policySets": [],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    } if config.instance == "production" else null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_62'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "policyDataAccessScopes": rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_59'),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        if_authtype_downcase_equals_to_replicon_69 = rail.IfOperator(
            task_id='if_authtype_downcase_equals_to_replicon_69',
            test="{{dag_run.conf.authtype | lower =='replicon' }}",
            yes_task="put_user2_70",
            no_task="log_useruri_71",
        )

        put_user2_70 = rail.RepliconServiceOperator(
            task_id='put_user2_70',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['loginname'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": null,
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_29'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_startdate_23'),
                            "month": rail.result('log_startdate_22'),
                            "day": rail.result('log_startdate_4')
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:replicon"
                        ],
                        "isLoginEnabled": rail.result('log_requireduserstatus_63'),
                        "loginName": dag_run.conf['loginname'],
                        "password": "Replicon@12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_60'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": dag_run.conf['timesheettype']
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    } if config.instance == "production" else null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_62'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "policyDataAccessScopes": rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_59'),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        def get_created_user_uri(upstream_task):
            user_70 = rail.result(upstream_task)[
                'uri'] if rail.result(upstream_task) else None
            user_66 = rail.result('put_user2_66')[
                'uri'] if rail.result('put_user2_66') else None
            user_68 = rail.result('put_user2_68')[
                'uri'] if rail.result('put_user2_68') else None
            return user_70 or user_66 or user_68

        log_useruri_71 = rail.PythonOperator(
            task_id='log_useruri_71',
            python_callable=lambda:  get_created_user_uri('put_user2_70')
        )

        log_forlookuplogs_72 = rail.PythonOperator(
            task_id='log_forlookuplogs_72',
            python_callable=lambda:  '''User Created successfully'''
        )

        get_enabled_custom_field_drop_down_options_73 = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_options_73',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_u_d_f_uri_type_32') }}"
            }
        )

        def get_cust_dropdown_uri(dropdown_info):
            enabled_dropdown_options = rail.result(
                'get_enabled_custom_field_drop_down_options_73')
            dropddown_info = list(filter(
                lambda item: item['displayText'] == dropdown_info, enabled_dropdown_options))
            return dropddown_info[0]['uri'] if dropddown_info else None

        log_t_y_p_edropdownoption_uri_74 = rail.PythonOperator(
            task_id='log_t_y_p_edropdownoption_uri_74',
            python_callable=lambda dag_run:  get_cust_dropdown_uri(
                dag_run.conf['type'])
        )

        if_log_t_y_p_edropdownoption_uri_74_present_75 = rail.IfOperator(
            task_id='if_log_t_y_p_edropdownoption_uri_74_present_75',
            test='''{{ result('log_t_y_p_edropdownoption_uri_74') | is_truthy and result('log_useruri_71') | is_truthy }}''',
            yes_task="update_dropdown_value_76",
            no_task="if_request_locationuri_present_78",
        )

        update_dropdown_value_76 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_76',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('log_useruri_71') }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_32') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_edropdownoption_uri_74') }}"
            }
        )

        log_forlookuplogs_77 = rail.PythonOperator(
            task_id='log_forlookuplogs_77',
            python_callable=lambda dag_run:  "Type updated to" +
            dag_run.conf['type']
        )

        if_request_locationuri_present_78 = rail.IfOperator(
            task_id='if_request_locationuri_present_78',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') and result('log_useruri_71') | is_truthy }}''',
            yes_task="put_location_schedule_for_user_79",
            no_task="if_request_primaryuseruri_present_81",
        )

        put_location_schedule_for_user_79 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_79',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('log_useruri_71') }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ dag_run.conf.locationuri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_forlookuplogs_80 = rail.PythonOperator(
            task_id='log_forlookuplogs_80',
            python_callable=lambda:  '''Location updated'''
        )

        if_request_primaryuseruri_present_81 = rail.IfOperator(
            task_id='if_request_primaryuseruri_present_81',
            test='''{{ dag_run.conf.primaryuseruri | is_truthy  and dag_run.conf.primaryuseruri | starts_with('urn') }}''',
            yes_task="impersonate_and_create_interactive_session_82",
            no_task="if_request_manager_present_87",
        )

        impersonate_and_create_interactive_session_82 = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session_82',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{ result('log_useruri_71') }}"
            }
        )

        log_a_u_t_h_t_o_k_e_n_83 = rail.PythonOperator(
            task_id='log_a_u_t_h_t_o_k_e_n_83',
            # pylint: disable=line-too-long
            python_callable=lambda:  list(filter(
                lambda item: item['name'] == "AUTHTOKEN", rail.result('impersonate_and_create_interactive_session_82')['sessionCookies']))[0]["value"]
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv284 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv284',
            retries=0,
            items=[1],
            trigger_dag_id=config.nrdc_assignsubstituteusersv2,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "authtoken": rail.result('log_a_u_t_h_t_o_k_e_n_83'),
                "suburi": dag_run.conf['primaryuseruri'],
                "actualuri": rail.result('log_useruri_71'),
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv284 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv284',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_assign_substitute_usersv284") }}'
        )

        else_85 = rail.EmptyOperator(
            task_id='else_85',
        )

        nrdc_user_import_logs_add_entry_86 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_86',
            message="fixme get message from prop ",
            severity="fixme get severity from prop ",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Exception",
                "details": "Substitute user not assigned as primary user profile not found | {{ dag_run_ecid() }}",
                "action": "Update | SUbstitute user assignment",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        if_request_manager_present_87 = rail.IfOperator(
            task_id='if_request_manager_present_87',
            test='''{{ dag_run.conf.manager | is_truthy }}''',
            yes_task="if_request_type_equals_to_c4_88",
            no_task="log_forlookuplogs_100",
        )

        if_request_type_equals_to_c4_88 = rail.IfOperator(
            task_id='if_request_type_equals_to_c4_88',
            test='''{{ dag_run.conf.type == 'C4' }}''',
            yes_task="search_users_89",
            no_task="if_request_type_equals_to_c3_c_r11_assigning_zach_shankassupervisorfor_c3_94",
        )

        search_users_89 = rail.RepliconServicePageOperator(
            task_id="search_users_89",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
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
                            'text': rail.result('c3_c4_supervisors')['c4_supervisor']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler
        )

        log_supervisoruritoassign_90 = rail.PythonOperator(
            task_id='log_supervisoruritoassign_90',
            python_callable=lambda:  get_user_uri_17(
                rail.result('c3_c4_supervisors')['c4_supervisor'], 'search_users_89')
        )

        log_supervisoruritoassignstatus_91 = rail.PythonOperator(
            task_id='log_supervisoruritoassignstatus_91',
            python_callable=lambda:  get_user_status_91(
                rail.result('c3_c4_supervisors')['c4_supervisor'], 'search_users_89')
        )

        if_log_supervisoruritoassign_90_present_92 = rail.IfOperator(
            task_id='if_log_supervisoruritoassign_90_present_92',
            test='''{{ result('log_supervisoruritoassign_90') | is_truthy  and result('log_supervisoruritoassignstatus_91') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_93",
            no_task="if_request_type_equals_to_c3_c_r11_assigning_zach_shankassupervisorfor_c3_94",
        )

        update_supervisor_assignment_schedule_over_date_range_93 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_93',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('log_useruri_71') }}",
                "supervisorUri": "{{ result('log_supervisoruritoassign_90') }}",
                "dateRange": null
            }
        )

        if_request_type_equals_to_c3_c_r11_assigning_zach_shankassupervisorfor_c3_94 = rail.IfOperator(
            task_id='if_request_type_equals_to_c3_c_r11_assigning_zach_shankassupervisorfor_c3_94',
            test='''{{ dag_run.conf.type == 'C3' }}''',
            yes_task="search_users_95",
            no_task="log_forlookuplogs_100",
        )

        search_users_95 = rail.RepliconServicePageOperator(
            task_id="search_users_95",
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
                            'text': dag_run.conf['loginname']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler
        )

        log_supervisoruritoassign_96 = rail.PythonOperator(
            task_id='log_supervisoruritoassign_96',
            python_callable=lambda:  get_user_uri_17(
                rail.result('c3_c4_supervisors')['c3_supervisor'], 'search_users_95')
        )

        log_supervisoruritoassignstatus_97 = rail.PythonOperator(
            task_id='log_supervisoruritoassignstatus_97',
            python_callable=lambda:  get_user_status_91(
                rail.result('c3_c4_supervisors')['c3_supervisor'], 'search_users_95')
        )

        if_log_supervisoruritoassign_96_present_98 = rail.IfOperator(
            task_id='if_log_supervisoruritoassign_96_present_98',
            test='''{{ result('log_supervisoruritoassign_96') | is_truthy  and result('log_supervisoruritoassignstatus_97') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_99",
            no_task="log_forlookuplogs_100",
        )

        update_supervisor_assignment_schedule_over_date_range_99 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_99',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('log_useruri_71') }}",
                "supervisorUri": "{{ result('log_supervisoruritoassign_96') }}",
                "dateRange": null
            }
        )

        def get_log_meesage():
            message_logs = []
            message_logs.append(rail.result('log_forlookuplogs_72'))
            message_logs.append(rail.result('log_forlookuplogs_77'))
            message_logs.append(rail.result('log_forlookuplogs_80'))
            return rail.smartjoin_by_delim(message_logs, '|')

        log_forlookuplogs_100 = rail.PythonOperator(
            task_id='log_forlookuplogs_100',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda:  get_log_meesage()
        )

        if_log_finallogs_101_present_102 = rail.IfOperator(
            task_id='if_log_finallogs_101_present_102',
            test='''{{ result('log_forlookuplogs_100') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_103",
            no_task="nrdc_user_import_logs_add_entry_103",
        )

        nrdc_user_import_logs_add_entry_103 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_103',
            message="fixme get message from prop ",
            severity="fixme get severity from prop ",
            properties={
                "user": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} | {{ dag_run.conf.emailaddress }}",
                "action": "Update ",
                "status": "Sucess",
                "details": "{{ result('log_forlookuplogs_100') }}|{{ dag_run_ecid() }}",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )
 
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "action": "NA",
                "jobId": "{{ dag_run_ecid() }}"
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> c3_c4_supervisors
        c3_c4_supervisors >> log_startdate_4 >> if_request_firstname_blank_5
        if_request_firstname_blank_5 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_6 >> stop_7 >> log_to_sumo
        if_request_firstname_blank_5 >> rail.Label(
            'No') >> if_request_emailaddress_blank_8
        if_request_emailaddress_blank_8 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_9 >> stop_10 >> log_to_sumo
        if_request_emailaddress_blank_8 >> rail.Label(
            'No') >> if_request_department_blank_11
        if_request_department_blank_11 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_12 >> stop_13 >> log_to_sumo
        if_request_department_blank_11 >> rail.Label(
            'No') >> if_request_loginname_blank_14
        if_request_loginname_blank_14 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_15 >> stop_16 >> log_to_sumo
        if_request_loginname_blank_14 >> rail.Label(
            'No') >> search_users_17 >> log_presenceofexistingloginname_18 >> \
            if_log_presenceofexistingloginname_18_present_19
        if_log_presenceofexistingloginname_18_present_19 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_20 >> stop_21 >> log_to_sumo
        if_log_presenceofexistingloginname_18_present_19 >> rail.Label(
            'No') >> log_startdate_22 >> log_startdate_23 >> log_today_24 >> log_todayday_25 >> log_todaymonth_26 >> \
            log_todayyear_27 >> get_enabled_departments_28 >> log_department_uri_29 >> \
            get_all_custom_fields_30 >> log_u_d_f_uri_office_31 >> log_u_d_f_uri_type_32 >> \
            log_u_d_f_uri_title_33 >> log_u_d_f_email_notification_34 >> log_u_d_f_urii_c_i_m_s_number_35 >> \
            log_u_d_f_uri_employee_number_n_o_t_i_n_u_s_e_currently_36 >> \
            log_u_d_f_uri_user_name_n_o_t_i_n_u_s_e_currently_37 >> declare_list_38 >> if_request_office_present_39
        if_request_office_present_39 >> rail.Label(
            'Yes') >> insert_to_list_office_40 >> if_request_empnumber_present_41
        if_request_office_present_39 >> rail.Label(
            'No') >> if_request_empnumber_present_41
        if_request_empnumber_present_41 >> rail.Label(
            'Yes') >> insert_to_list_i_c_i_m_s_i_d_42 >> insert_to_list_employeenumber_43 >> \
            if_request_userfullname_present_44
        if_request_empnumber_present_41 >> rail.Label(
            'No') >> if_request_userfullname_present_44
        if_request_userfullname_present_44 >> rail.Label(
            'Yes') >> insert_to_list_username_45 >> if_request_title_present_46
        if_request_userfullname_present_44 >> rail.Label(
            'No') >> if_request_title_present_46
        if_request_title_present_46 >> rail.Label(
            'Yes') >> insert_to_list_title_47 >> if_request_emailaddress_present_48
        if_request_title_present_46 >> rail.Label(
            'No') >> if_request_emailaddress_present_48
        if_request_emailaddress_present_48 >> rail.Label(
            'Yes') >> insert_to_list_title_49 >> declare_list_50
        if_request_emailaddress_present_48 >> rail.Label(
            'No') >> declare_list_50 >> insert_to_list_51 >> if_request_locationuri_present_52
        if_request_locationuri_present_52 >> rail.Label(
            'Yes') >> insert_to_list_53 >> log_policydataaccessscopeforthepermission_54 >> \
            else_55 >> log_policydataaccessscopeforthepermissionwithoutlocation_56 >> \
            if_request_type_equals_to_delegate_57
        if_request_locationuri_present_52 >> rail.Label(
            'No') >> if_request_type_equals_to_delegate_57
        if_request_type_equals_to_delegate_57 >> rail.Label(
            'Yes') >> insert_to_list_58 >> log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_59
        if_request_type_equals_to_delegate_57 >> rail.Label(
            'No') >> log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_59 >> log_f_i_n_a_lpermissiontopass_60 >> \
            insert_to_list_type_61 >> log_customfieldbody_62 >> \
            log_requireduserstatus_63 >> if_authtype_downcase_equals_to_sso_64
        if_authtype_downcase_equals_to_sso_64 >> rail.Label(
            'Yes') >> if_request_timesheettype_present_65
        if_request_timesheettype_present_65 >> rail.Label(
            'Yes') >> put_user2_66 >> if_authtype_downcase_equals_to_replicon_69
        if_request_timesheettype_present_65 >> rail.Label(
            'No') >> put_user2_68 >> if_authtype_downcase_equals_to_replicon_69
        if_authtype_downcase_equals_to_sso_64 >> rail.Label(
            'No') >> if_authtype_downcase_equals_to_replicon_69
        if_authtype_downcase_equals_to_replicon_69 >> rail.Label(
            'Yes') >> put_user2_70 >> log_useruri_71
        if_authtype_downcase_equals_to_replicon_69 >> rail.Label(
            'No') >> log_useruri_71 >> log_forlookuplogs_72 >> get_enabled_custom_field_drop_down_options_73 >> \
            log_t_y_p_edropdownoption_uri_74 >> if_log_t_y_p_edropdownoption_uri_74_present_75
        if_log_t_y_p_edropdownoption_uri_74_present_75 >> rail.Label(
            'Yes') >> update_dropdown_value_76 >> log_forlookuplogs_77 >> if_request_locationuri_present_78
        if_log_t_y_p_edropdownoption_uri_74_present_75 >> rail.Label(
            'No') >> if_request_locationuri_present_78
        if_request_locationuri_present_78 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_79 >> log_forlookuplogs_80 >> \
            if_request_primaryuseruri_present_81
        if_request_locationuri_present_78 >> rail.Label(
            'No') >> if_request_primaryuseruri_present_81
        if_request_primaryuseruri_present_81 >> rail.Label(
            'Yes') >> impersonate_and_create_interactive_session_82 >> log_a_u_t_h_t_o_k_e_n_83 >> \
            trigger_dag_run_live_nrdc_assign_substitute_usersv284 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv284 >> else_85 >> \
            nrdc_user_import_logs_add_entry_86 >> if_request_manager_present_87
        if_request_primaryuseruri_present_81 >> rail.Label(
            'No') >> if_request_manager_present_87
        if_request_manager_present_87 >> rail.Label(
            'Yes') >> if_request_type_equals_to_c4_88
        if_request_type_equals_to_c4_88 >> rail.Label(
            'Yes') >> search_users_89 >> log_supervisoruritoassign_90 >> log_supervisoruritoassignstatus_91 >> \
            if_log_supervisoruritoassign_90_present_92
        if_log_supervisoruritoassign_90_present_92 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_93 >> \
            if_request_type_equals_to_c3_c_r11_assigning_zach_shankassupervisorfor_c3_94
        if_log_supervisoruritoassign_90_present_92 >> rail.Label(
            'No') >> if_request_type_equals_to_c3_c_r11_assigning_zach_shankassupervisorfor_c3_94
        if_request_type_equals_to_c4_88 >> rail.Label(
            'No') >> if_request_type_equals_to_c3_c_r11_assigning_zach_shankassupervisorfor_c3_94
        if_request_type_equals_to_c3_c_r11_assigning_zach_shankassupervisorfor_c3_94 >> rail.Label(
            'Yes') >> search_users_95 >> log_supervisoruritoassign_96 >> log_supervisoruritoassignstatus_97 >> \
            if_log_supervisoruritoassign_96_present_98
        if_log_supervisoruritoassign_96_present_98 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_99 >> log_forlookuplogs_100
        if_log_supervisoruritoassign_96_present_98 >> rail.Label(
            'No') >> log_forlookuplogs_100
        if_request_type_equals_to_c3_c_r11_assigning_zach_shankassupervisorfor_c3_94 >> rail.Label(
            'No') >> log_forlookuplogs_100
        if_request_manager_present_87 >> rail.Label(
            'No') >> log_forlookuplogs_100 >> if_log_finallogs_101_present_102
        if_log_finallogs_101_present_102 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_103
        if_log_finallogs_101_present_102 >> rail.Label(
            'No') >> nrdc_user_import_logs_add_entry_103 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
