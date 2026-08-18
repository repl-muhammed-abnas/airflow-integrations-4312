
from datetime import datetime, timedelta
import json
import itertools
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from nrdc.user_import_v3.utils.custom_method import c3_c4_supervisors_loginname
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.nrdc_add_user_v2,
        description=f'NRDC_Add_User_v2 {config.instance}',
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
            end_task='nrdc_user_import_logs_add_entry_339',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        c3_c4_supervisors = rail.PythonOperator(
            task_id="c3_c4_supervisors",
            python_callable=c3_c4_supervisors_loginname,
            op_args=[config.c3_c4_profile_supervisors_variable]
        )

        if_request_firstname_blank_3 = rail.IfOperator(
            task_id='if_request_firstname_blank_3',
            test="{{ dag_run.conf.firstname | is_falsy  or dag_run.conf.lastname | is_falsy }}",
            yes_task="nrdc_user_import_logs_add_entry_4",
            no_task="if_request_emailaddress_blank_6",
        )

        nrdc_user_import_logs_add_entry_4 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_4',
            message="User creation failed: First Name and Last Name are required",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "First Name and Last Name must be provided",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_5 = rail.EmptyOperator(
            task_id='stop_5',

        )

        if_request_emailaddress_blank_6 = rail.IfOperator(
            task_id='if_request_emailaddress_blank_6',
            test='''{{ dag_run.conf.emailaddress | is_falsy }}''',
            yes_task="nrdc_user_import_logs_add_entry_7",
            no_task="if_request_department_blank_9",
        )

        nrdc_user_import_logs_add_entry_7 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_7',
            message="User creation failed: Email address is required",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "Email address must be provided",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_8 = rail.EmptyOperator(
            task_id='stop_8',

        )

        if_request_department_blank_9 = rail.IfOperator(
            task_id='if_request_department_blank_9',
            test='''{{ dag_run.conf.department | is_falsy }}''',
            yes_task="nrdc_user_import_logs_add_entry_10",
            no_task="if_request_logonname_blank_12",
        )

        nrdc_user_import_logs_add_entry_10 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_10',
            message="User creation failed: Department is required",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "Department must be provided",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_11 = rail.EmptyOperator(
            task_id='stop_11',

        )

        if_request_logonname_blank_12 = rail.IfOperator(
            task_id='if_request_logonname_blank_12',
            test='''{{ dag_run.conf.logonname | is_falsy }}''',
            yes_task="nrdc_user_import_logs_add_entry_13",
            no_task="declare_list_15",
        )

        nrdc_user_import_logs_add_entry_13 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_13',
            message="User creation failed: Login name is required",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "Logon name must be provided",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_14 = rail.EmptyOperator(
            task_id='stop_14',

        )

        declare_list_15 = rail.SetVariableOperator(
            task_id='declare_list_15',
            append=False,
            name='permissionsets',
            value=[]
        )

        insert_to_list_16 = rail.SetVariableOperator(
            task_id='insert_to_list_16',
            append=True,
            name='{{ result("declare_list_15").name }}',
            value=lambda: {
                "name": "End User"
            }
        )

        if_request_locationuri_present_17 = rail.IfOperator(
            task_id='if_request_locationuri_present_17',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="log_policydataaccessscopeforthepermission_18",
            no_task="log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22",
        )

        log_policydataaccessscopeforthepermission_18 = rail.PythonOperator(
            task_id='log_policydataaccessscopeforthepermission_18',
            python_callable=lambda dag_run:  [
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

        insert_to_list_19 = rail.SetVariableOperator(
            task_id='insert_to_list_19',
            append=True,
            name='{{ result("declare_list_15").name }}',
            value=lambda: {
                "name": "All Timesheets"
            }
        )

        else_20 = rail.EmptyOperator(
            task_id='else_20',
        )

        log_policydataaccessscopeforthepermissionwithoutlocation_21 = rail.PythonOperator(
            task_id='log_policydataaccessscopeforthepermissionwithoutlocation_21',
            python_callable=lambda:  '''[]'''
        )

        log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22 = rail.PythonOperator(
            task_id='log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22',
            python_callable=lambda: rail.result(
                'log_policydataaccessscopeforthepermission_18')
            if rail.result('log_policydataaccessscopeforthepermission_18') else [
            ]
        )

        log_f_i_n_a_lpermissiontopass_23 = rail.PythonOperator(
            task_id='log_f_i_n_a_lpermissiontopass_23',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_list_15')['name'])
        )

        if_request_whencreated_not_contains_24 = rail.IfOperator(
            task_id='if_request_whencreated_not_contains_24',
            test='''{{ dag_run.conf.whencreated | matches('-') }}''',
            yes_task="log_startdate_27",
            no_task="nrdc_user_import_logs_add_entry_25",
        )

        nrdc_user_import_logs_add_entry_25 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_25',
            message="Date format is incoorect",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "Date format is incoorect",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_26 = rail.EmptyOperator(
            task_id='stop_26',

        )

        log_startdate_27 = rail.PythonOperator(
            task_id='log_startdate_27',
            python_callable=lambda dag_run: dag_run.conf['whencreated']
        )

        log_startday_28 = rail.PythonOperator(
            task_id='log_startday_28',
            python_callable=lambda dag_run:  datetime.strptime(
                dag_run.conf['whencreated'], '%Y-%m-%d %H:%M:%S').day
        )

        log_startmonth_29 = rail.PythonOperator(
            task_id='log_startmonth_29',
            python_callable=lambda dag_run:  datetime.strptime(
                dag_run.conf['whencreated'], '%Y-%m-%d %H:%M:%S').month
        )

        log_start_year_30 = rail.PythonOperator(
            task_id='log_start_year_30',
            python_callable=lambda dag_run:  datetime.strptime(
                dag_run.conf['whencreated'], '%Y-%m-%d %H:%M:%S').year
        )

        log_loginname_31 = rail.PythonOperator(
            task_id='log_loginname_31',
            python_callable=lambda dag_run:  dag_run.conf['logonname'].split(
                "@")[0] if dag_run.conf['logonname'] else None
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
            return users_info[0] if users_info else {}

        def get_data_req(dag_run):
            return {
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
                            'text': dag_run.conf['logonname'].split('@')[0]
                        }
                    }
                }
            }

        search_users_32 = rail.RepliconServicePageOperator(
            task_id="search_users_32",
            endpoint="/services/UserListService1.svc/GetData",
            data=get_data_req,
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['logonname'].split('@')[0])
        )

        def get_existing_user_uri():
            return rail.result('search_users_32')['useruri'] if rail.result('search_users_32') else None

        log_presenceofexistingloginname_33 = rail.PythonOperator(
            task_id='log_presenceofexistingloginname_33',
            python_callable=get_existing_user_uri
        )

        if_log_presenceofexistingloginname_33_present_34 = rail.IfOperator(
            task_id='if_log_presenceofexistingloginname_33_present_34',
            test="{{ result('log_presenceofexistingloginname_33') | is_truthy }}",
            yes_task="nrdc_user_import_logs_add_entry_35",
            no_task="get_enabled_departments_37",
        )

        nrdc_user_import_logs_add_entry_35 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_35',
            message="Login name already exists",
            severity="Info",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "Login name already exists - '{{ result('log_loginname_31') }}'",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_36 = rail.EmptyOperator(
            task_id='stop_36',

        )

        get_enabled_departments_37 = rail.RepliconServiceOperator(
            task_id='get_enabled_departments_37',
            endpoint="/services/departmentService1.svc/GetEnabledDepartments",

        )

        def get_existing_department_uri(dag_run):
            existing_department = rail.result('get_enabled_departments_37')
            input_department_info = list(filter(
                lambda item: item['displayText'] == dag_run.conf['department'], existing_department))
            return input_department_info[0]['uri'] if input_department_info else None

        log_department_uri_38 = rail.PythonOperator(
            task_id='log_department_uri_38',
            python_callable=get_existing_department_uri
        )

        if_log_department_uri_38_blank_39 = rail.IfOperator(
            task_id='if_log_department_uri_38_blank_39',
            test='''{{ result('log_department_uri_38') | is_truthy }}''',
            yes_task="get_all_custom_fields_42",
            no_task="nrdc_user_import_logs_add_entry_40",
        )

        nrdc_user_import_logs_add_entry_40 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_40',
            message="Department must be provided",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "Department must be provided / Invalid department",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_41 = rail.EmptyOperator(
            task_id='stop_41',

        )

        def get_department_req():
            return {
                "objectUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user:1"
            }

        get_all_custom_fields_42 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_42',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data=get_department_req
        )

        def get_customoef_uri(custom_field_info):
            existing_customoefs = rail.result('get_all_custom_fields_42')
            input_department_info = list(filter(
                lambda item: item['displayText'] == custom_field_info, existing_customoefs))
            return input_department_info[0]['uri'] if input_department_info else None

        log_u_d_f_uri_office_43 = rail.PythonOperator(
            task_id='log_u_d_f_uri_office_43',
            python_callable=lambda: get_customoef_uri("Office")
        )

        log_u_d_f_uri_email_notification_44 = rail.PythonOperator(
            task_id='log_u_d_f_uri_email_notification_44',
            python_callable=lambda: get_customoef_uri("Email Notification")
        )

        log_u_d_f_uri_type_45 = rail.PythonOperator(
            task_id='log_u_d_f_uri_type_45',
            python_callable=lambda: get_customoef_uri("Type")
        )

        get_enabled_custom_field_drop_down_options_type_46 = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_options_type_46',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}"
            }
        )

        def get_cust_dropdown_uri(dropdown_info):
            enabled_dropdown_options = rail.result(
                'get_enabled_custom_field_drop_down_options_type_46')
            dropddown_info = list(filter(
                lambda item: item['displayText'] == dropdown_info, enabled_dropdown_options))
            return dropddown_info[0]['uri'] if dropddown_info else None

        log_u_d_f_uri_title_47 = rail.PythonOperator(
            task_id='log_u_d_f_uri_title_47',
            python_callable=lambda: get_customoef_uri("Title")
        )

        log_u_d_f_urii_c_i_m_s_number_48 = rail.PythonOperator(
            task_id='log_u_d_f_urii_c_i_m_s_number_48',
            python_callable=lambda: get_customoef_uri("iCIMS Number")
        )

        log_u_d_f_uri_employee_number_n_o_t_i_n_u_s_e_currently_49 = rail.PythonOperator(
            task_id='log_u_d_f_uri_employee_number_n_o_t_i_n_u_s_e_currently_49',
            python_callable=lambda: get_customoef_uri("Employee Number")
        )

        log_u_d_f_uri_user_name_n_o_t_i_n_u_s_e_currently_50 = rail.PythonOperator(
            task_id='log_u_d_f_uri_user_name_n_o_t_i_n_u_s_e_currently_50',
            python_callable=lambda: get_customoef_uri("User-Name")
        )

        declare_list_51 = rail.SetVariableOperator(
            task_id='declare_list_51',
            append=False,
            name='Users for substitute user assignment',
            value=[]
        )

        declare_list_52 = rail.SetVariableOperator(
            task_id='declare_list_52',
            append=False,
            name='customFieldValues',
            value=[]
        )

        if_request_office_present_53 = rail.IfOperator(
            task_id='if_request_office_present_53',
            test='''{{ dag_run.conf.office | is_truthy }}''',
            yes_task="insert_to_list_office_54",
            no_task="if_request_emailaddress_present_55",
        )

        insert_to_list_office_54 = rail.SetVariableOperator(
            task_id='insert_to_list_office_54',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value=lambda dag_run: {
                "customField": {
                    "uri": rail.result('log_u_d_f_uri_office_43')
                },
                "dropDownOption": {
                    "name": dag_run.conf['office']
                }
            }
        )

        if_request_emailaddress_present_55 = rail.IfOperator(
            task_id='if_request_emailaddress_present_55',
            test='''{{ dag_run.conf.emailaddress | is_truthy }}''',
            yes_task="insert_to_list_email_notification_56",
            no_task="if_request_empnumber_present_57",
        )

        insert_to_list_email_notification_56 = rail.SetVariableOperator(
            task_id='insert_to_list_email_notification_56',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value=lambda dag_run: {
                "customField": {
                    "uri": rail.result('log_u_d_f_uri_email_notification_44')
                },
                "text": dag_run.conf['emailaddress']
            }
        )

        if_request_empnumber_present_57 = rail.IfOperator(
            task_id='if_request_empnumber_present_57',
            test='''{{ dag_run.conf.empnumber | is_truthy }}''',
            yes_task="insert_to_list_i_c_i_m_s_i_d_58",
            no_task="if_request_displayname_present_60",
        )

        insert_to_list_i_c_i_m_s_i_d_58 = rail.SetVariableOperator(
            task_id='insert_to_list_i_c_i_m_s_i_d_58',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value=lambda dag_run: {
                "customField": {
                    "uri": rail.result('log_u_d_f_urii_c_i_m_s_number_48')
                },
                "text": dag_run.conf['empnumber']
            }
        )

        insert_to_list_e_m_pnumber_59 = rail.SetVariableOperator(
            task_id='insert_to_list_e_m_pnumber_59',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value=lambda dag_run: {
                "customField": {
                    "uri": rail.result('log_u_d_f_uri_employee_number_n_o_t_i_n_u_s_e_currently_49')
                },
                "text": dag_run.conf['empnumber']
            }
        )

        if_request_displayname_present_60 = rail.IfOperator(
            task_id='if_request_displayname_present_60',
            test='''{{ dag_run.conf.displayname | is_truthy }}''',
            yes_task="insert_to_list_username_61",
            no_task="if_request_title_present_62",
        )

        insert_to_list_username_61 = rail.SetVariableOperator(
            task_id='insert_to_list_username_61',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value=lambda dag_run: {
                "customField": {
                    "uri": rail.result('log_u_d_f_uri_user_name_n_o_t_i_n_u_s_e_currently_50')
                },
                "text": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        if_request_title_present_62 = rail.IfOperator(
            task_id='if_request_title_present_62',
            test='''{{ dag_run.conf.title | is_truthy }}''',
            yes_task="insert_to_list_title_63",
            no_task="insert_to_list_type_64",
        )

        insert_to_list_title_63 = rail.SetVariableOperator(
            task_id='insert_to_list_title_63',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value=lambda dag_run: {
                "customField": {
                    "uri": rail.result('log_u_d_f_uri_title_47')
                },
                "text": dag_run.conf['title']
            }
        )

        insert_to_list_type_64 = rail.SetVariableOperator(
            task_id='insert_to_list_type_64',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value=lambda: {
                "customField": {
                    "uri": rail.result('log_u_d_f_uri_type_45')
                },
                "dropDownOption": {
                    "name": "C4"
                }
            }
        )

        log_customfieldbody_65 = rail.PythonOperator(
            task_id='log_customfieldbody_65',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_list_52')['name'])
        )

        def all_result_data_handler(result, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'loginname': row['cells'][1]['textValue'],
                'useruri': row['cells'][0]['uri'],
                'enabled': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
            }, flaten_rows)))

            return users_info[0] if users_info else {}

        search_users_67 = rail.RepliconServicePageOperator(
            task_id="search_users_67",
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
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('c3_c4_supervisors')['c4_supervisor'])
        )

        def get_existing_user_uri1():
            return rail.result('search_users_67')['useruri'] if rail.result('search_users_67') else None

        def get_existing_user_status():
            return rail.result('search_users_67')['enabled'] if rail.result('search_users_67') else None

        log_checkif_zara_aktheruserexisits_c4supervisot_68 = rail.PythonOperator(
            task_id='log_checkif_zara_aktheruserexisits_c4supervisot_68',
            python_callable=get_existing_user_uri1
        )

        log_checkif_zara_aktheruserenabled_c4supervisot_69 = rail.PythonOperator(
            task_id='log_checkif_zara_aktheruserenabled_c4supervisot_69',
            python_callable=get_existing_user_status
        )

        search_users_lookforzshankafsupervisor_70 = rail.RepliconServicePageOperator(
            task_id="search_users_lookforzshankafsupervisor_70",
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
                            'text': rail.result('c3_c4_supervisors')['c3_supervisor']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('c3_c4_supervisors')['c3_supervisor'])
        )

        log_checkif_zach_shankuserexists_c_r11_71 = rail.PythonOperator(
            task_id='log_checkif_zach_shankuserexists_c_r11_71',
            python_callable=lambda:  rail.result('search_users_lookforzshankafsupervisor_70')[
                'useruri'] if rail.result('search_users_lookforzshankafsupervisor_70') else None
        )

        log_checkif_zach_shankifitsenabled_72 = rail.PythonOperator(
            task_id='log_checkif_zach_shankifitsenabled_72',
            python_callable=lambda:  rail.result('search_users_lookforzshankafsupervisor_70')[
                'enabled'] if rail.result('search_users_lookforzshankafsupervisor_70') else None
        )

        if_request_c4orc3present_contains_c4_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_73 = rail.IfOperator(
            task_id='if_request_c4orc3present_contains_c4_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_73',
            test='''{{ dag_run.conf.c4orc3_present | matches('C4') }}''',
            yes_task="put_user2_c4_user_74",
            no_task="if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82",
        )

        put_user2_c4_user_74 = rail.RepliconServiceOperator(
            task_id='put_user2_c4_user_74',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31'),
                        "parameterCorrelationId": null
                    },
                    "firstname": "Action Fund",
                    "lastname": dag_run.conf['displayname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_38'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_start_year_30'),
                            "month": rail.result('log_startmonth_29'),
                            "day": rail.result('log_startday_28')
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
                        "loginName": rail.result('log_loginname_31'),
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C4 Timesheet"
                        }
                    ],
                    "employeeType": null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_65'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                            "uri": null,
                            "parent": null,
                            "name": "Full-time Salaried",
                            "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        updateing_s_s_o_i_d_75 = rail.RepliconServiceOperator(
            task_id='updateing_s_s_o_i_d_75',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('put_user2_c4_user_74').uri }}",
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
                    "securitySettingsToApply": {
                        "loginEnabled": null,
                        "forcePasswordChange": null,
                        "loginName": null,
                        "ssoName": "{{ result('put_user2_c4_user_74').loginName }}",
                        "password": null,
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "emailMFAResendVerificationEmail": "false",
                        "emailMFATryAddMethodFromUsersEmail": "false",
                        "isMFAMethodRequired": "false",
                        "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name",
                        "clearIsLockedOut": "false"
                    },
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        if_request_locationuri_present_76 = rail.IfOperator(
            task_id='if_request_locationuri_present_76',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_77",
            no_task="nrdc_user_import_logs_add_entry_78",
        )

        put_location_schedule_for_user_77 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_77',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_c4_user_74').uri }}",
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

        nrdc_user_import_logs_add_entry_78 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_78',
            message=lambda: f"C4 user profile created successfully for {rail.result('put_user2_c4_user_74')['loginName']}",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ result('put_user2_c4_user_74').loginName }}",
                "status": "Success",
                "details": "C4 user profile created successfully",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        log_forlookuplogs_79 = rail.PythonOperator(
            task_id='log_forlookuplogs_79',
            python_callable=lambda:  '''C4 user profile created successfully'''
        )

        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_80 = rail.IfOperator(
            task_id='if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_80',
            # pylint: disable=line-too-long
            test='''{{ result('log_checkif_zara_aktheruserexisits_c4supervisot_68') | is_truthy  and result('log_checkif_zara_aktheruserenabled_c4supervisot_69') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_81",
            no_task="if_loa_present_c4",
        )

        update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_81 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_81',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_c4_user_74').uri }}",
                "supervisorUri": "{{ result('log_checkif_zara_aktheruserexisits_c4supervisot_68') }}",
                "dateRange": null
            }
        )

        if_loa_present_c4 = rail.IfOperator(
            task_id='if_loa_present_c4',
            test='''{{ dag_run.conf.leaveofabsence | matches('LOA') }}''',
            yes_task="get_user_details",
            no_task="if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82",
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('put_user2_c4_user_74').uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_custom_fieldsforuser_c4 = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_c4',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('put_user2_c4_user_74').uri }}"
            }
        )

        log_loa_u_d_f_c4 = rail.PythonOperator(
            task_id='log_loa_u_d_f_c4',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f_c4 = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_c4',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_c4_user_74').uri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f_c4') }}",
                "value": "Yes"
            }
        )

        update_user_timesheet_c4 = rail.RepliconServiceOperator(
            task_id='update_user_timesheet_c4',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda:{
                "user": {
                    "uri": rail.result("put_user2_c4_user_74")['uri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        disable_userprofile_c4 = rail.RepliconServiceOperator(
            task_id='disable_userprofile_c4',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('put_user2_c4_user_74').uri }}"
            }
        )

        if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82 = rail.IfOperator(
            task_id='if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82',
            test='''{{ dag_run.conf.c4orc3_present | matches('Delegate') }}''',
            yes_task="put_user2_delegate_user_83",
            no_task="if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91",
        )

        put_user2_delegate_user_83 = rail.RepliconServiceOperator(
            task_id='put_user2_delegate_user_83',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31'),
                        "parameterCorrelationId": null
                    },
                    "firstname": "Delegate",
                    "lastname": dag_run.conf['displayname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_38'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_start_year_30'),
                            "month": rail.result('log_startmonth_29'),
                            "day": rail.result('log_startday_28')
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
                        "loginName": rail.result('log_loginname_31'),
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [{"name": "End User"}, {"name": "Delegates"}, {"name": "All Timesheets"}],
                    "policySets": [],
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_65'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                            "uri": null,
                            "parent": null,
                            "name": "Full-time Salaried",
                            "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        updateing_s_s_o_i_d_84 = rail.RepliconServiceOperator(
            task_id='updateing_s_s_o_i_d_84',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('put_user2_delegate_user_83').uri }}",
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
                    "securitySettingsToApply": {
                        "loginEnabled": null,
                        "forcePasswordChange": null,
                        "loginName": null,
                        "ssoName": "{{ result('put_user2_delegate_user_83').loginName }}",
                        "password": null,
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "emailMFAResendVerificationEmail": "false",
                        "emailMFATryAddMethodFromUsersEmail": "false",
                        "isMFAMethodRequired": "false",
                        "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name",
                        "clearIsLockedOut": "false"
                    },
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        if_request_locationuri_present_85 = rail.IfOperator(
            task_id='if_request_locationuri_present_85',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_86",
            no_task="log_type_delegate_uri_87",
        )

        put_location_schedule_for_user_86 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_86',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_delegate_user_83').uri }}",
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

        log_type_delegate_uri_87 = rail.PythonOperator(
            task_id='log_type_delegate_uri_87',
            python_callable=lambda:  get_cust_dropdown_uri("Delegate")
        )

        update_dropdown_value_88 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_88',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_delegate_user_83').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_type_delegate_uri_87') }}"
            }
        )

        nrdc_user_import_logs_add_entry_89 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_89',
            message="Delegate user profile created successfully",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "Delegate user profile successfully created",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        log_forlookuplogs_90 = rail.PythonOperator(
            task_id='log_forlookuplogs_90',
            python_callable=lambda:  '''C4 user profile created successfully'''
        )

        if_loa_present_delegate = rail.IfOperator(
            task_id='if_loa_present_delegate',
            test='''{{ dag_run.conf.leaveofabsence | matches('LOA') }}''',
            yes_task="get_user_details_delegate",
            no_task="if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91",
        )

        get_user_details_delegate = rail.RepliconServiceOperator(
            task_id='get_user_details_delegate',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('put_user2_delegate_user_83').uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_custom_fieldsforuser_delegate = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_delegate',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        log_loa_u_d_f_delegate = rail.PythonOperator(
            task_id='log_loa_u_d_f_delegate',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f_delegate = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_delegate',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_delegate_user_83').uri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f_delegate') }}",
                "value": "Yes"
            }
        )

        update_user_timesheet_delegate = rail.RepliconServiceOperator(
            task_id='update_user_timesheet_delegate',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda:{
                "user": {
                    "uri": rail.result("put_user2_delegate_user_83")['uri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        disable_userprofile_delegate = rail.RepliconServiceOperator(
            task_id='disable_userprofile_delegate',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91 = rail.IfOperator(
            task_id='if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91',
            test='''{{ dag_run.conf.c4orc3_present == 'Delegate and 1' }}''',
            yes_task="put_user2_c4_userescondary_92",
            no_task="if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101",
        )

        put_user2_c4_userescondary_92 = rail.RepliconServiceOperator(
            task_id='put_user2_c4_userescondary_92',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "af",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Action Fund",
                    "lastname": dag_run.conf['displayname'],
                    "emailAddress": null,
                    "employeeId":  dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_38'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_start_year_30'),
                            "month": rail.result('log_startmonth_29'),
                            "day": rail.result('log_startday_28')
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
                        "loginName": rail.result('log_loginname_31') + "af",
                        "password": "Replicon@12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C4 Timesheet"
                        }
                    ],
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_65'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                            "uri": null,
                            "parent": null,
                            "name": "Full-time Salaried",
                            "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        updateing_s_s_o_i_d_93 = rail.RepliconServiceOperator(
            task_id='updateing_s_s_o_i_d_93',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('put_user2_c4_userescondary_92').uri }}",
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
                    "securitySettingsToApply": {
                        "loginEnabled": null,
                        "forcePasswordChange": null,
                        "loginName": null,
                        "ssoName": "{{ result('put_user2_c4_userescondary_92').loginName }}",
                        "password": null,
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "emailMFAResendVerificationEmail": "false",
                        "emailMFATryAddMethodFromUsersEmail": "false",
                        "isMFAMethodRequired": "false",
                        "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name",
                        "clearIsLockedOut": "false"
                    },
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        if_request_locationuri_present_94 = rail.IfOperator(
            task_id='if_request_locationuri_present_94',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_95",
            no_task="insert_to_list_96",
        )

        put_location_schedule_for_user_95 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_95',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_c4_userescondary_92').uri }}",
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

        insert_to_list_96 = rail.SetVariableOperator(
            task_id='insert_to_list_96',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_c4_userescondary_92').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_97 = rail.IfOperator(
            task_id='if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_97',
            # pylint: disable=line-too-long
            test='''{{ result('log_checkif_zara_aktheruserexisits_c4supervisot_68') | is_truthy  and result('log_checkif_zara_aktheruserenabled_c4supervisot_69') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_98",
            no_task="log_forlookuplogs_99",
        )

        update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_98 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_98',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_c4_userescondary_92').uri }}",
                "supervisorUri": "{{ result('log_checkif_zara_aktheruserexisits_c4supervisot_68') }}",
                "dateRange": null
            }
        )

        log_forlookuplogs_99 = rail.PythonOperator(
            task_id='log_forlookuplogs_99',
            python_callable=lambda:  '''C4 user profile created successfully'''
        )

        nrdc_user_import_logs_add_entry_100 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_100',
            message="C4 user profile created successfully",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "C4 user profile successfully created",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        if_loa_present_delegate_and_1 = rail.IfOperator(
            task_id='if_loa_present_delegate_and_1',
            test='''{{ dag_run.conf.leaveofabsence | matches('LOA') }}''',
            yes_task="get_user_details_delegate_and_1",
            no_task="if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101",
        )

        get_user_details_delegate_and_1 = rail.RepliconServiceOperator(
            task_id='get_user_details_delegate_and_1',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('put_user2_c4_userescondary_92').uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_custom_fieldsforuser_delegate_and_1 = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_delegate_and_1',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('put_user2_c4_userescondary_92').uri }}"
            }
        )

        log_loa_u_d_f_delegate_and_1 = rail.PythonOperator(
            task_id='log_loa_u_d_f_delegate_and_1',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f_delegate_and_1 = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_delegate_and_1',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_c4_userescondary_92').uri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f_delegate_and_1') }}",
                "value": "Yes"
            }
        )

        update_user_timesheet_delegate_and_1 = rail.RepliconServiceOperator(
            task_id='update_user_timesheet_delegate_and_1',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda:{
                "user": {
                    "uri": rail.result("put_user2_c4_userescondary_92")['uri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        disable_userprofile_delegate_and_1 = rail.RepliconServiceOperator(
            task_id='disable_userprofile_delegate_and_1',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('put_user2_c4_userescondary_92').uri }}"
            }
        )

        if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101 = rail.IfOperator(
            task_id='if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101',
            test='''{{ dag_run.conf.c4orc3_present == 'C3 Only' }}''',
            yes_task="put_user2_lobby_timesheet_102",
            no_task="c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156",
        )

        put_user2_lobby_timesheet_102 = rail.RepliconServiceOperator(
            task_id='put_user2_lobby_timesheet_102',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31'),
                        "parameterCorrelationId": null
                    },
                    "firstname": "C3 Lobbying Timesheet",
                    "lastname": dag_run.conf['displayname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_38'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_start_year_30'),
                            "month": rail.result('log_startmonth_29'),
                            "day": rail.result('log_startday_28')
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
                        "loginName": rail.result('log_loginname_31'),
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 Lobbying Timesheet"
                        }
                    ],
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_65'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                            "uri": null,
                            "parent": null,
                            "name": "Full-time Salaried",
                            "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        updateing_s_s_o_i_d_103 = rail.RepliconServiceOperator(
            task_id='updateing_s_s_o_i_d_103',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('put_user2_lobby_timesheet_102').uri }}",
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
                    "securitySettingsToApply": {
                        "loginEnabled": null,
                        "forcePasswordChange": null,
                        "loginName": null,
                        "ssoName": "{{ result('put_user2_lobby_timesheet_102').loginName }}",
                        "password": null,
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "emailMFAResendVerificationEmail": "false",
                        "emailMFATryAddMethodFromUsersEmail": "false",
                        "isMFAMethodRequired": "false",
                        "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name",
                        "clearIsLockedOut": "false"
                    },
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        if_request_locationuri_present_104 = rail.IfOperator(
            task_id='if_request_locationuri_present_104',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_105",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_106",
        )

        put_location_schedule_for_user_105 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_105',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_lobby_timesheet_102').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_106 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_106',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_107",
            no_task="if_d_uri_present_108",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_107 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_107',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_lobby_timesheet_102').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_108 = rail.IfOperator(
            task_id='if_d_uri_present_108',
            test='''{{ result('put_user2_lobby_timesheet_102').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_109",
            no_task="log_type_lobby_timesheet_uri_110",
        )

        log_forlookuplogs_109 = rail.PythonOperator(
            task_id='log_forlookuplogs_109',
            python_callable=lambda:  '''C3 user profile created successfully'''
        )

        log_type_lobby_timesheet_uri_110 = rail.PythonOperator(
            task_id='log_type_lobby_timesheet_uri_110',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri(
                "Lobbying Timesheet")
        )

        update_dropdown_value_111 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_111',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_lobby_timesheet_102').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_type_lobby_timesheet_uri_110') }}"
            }
        )

        def get_log_lookup_152(delimeter):
            log_forlookuplogs = []
            log_forlookuplogs_109 = rail.result('log_forlookuplogs_109')
            if log_forlookuplogs_109:
                log_forlookuplogs.append(log_forlookuplogs_109)
            return rail.smartjoin_by_delim(log_forlookuplogs, delimeter, delimeter)

        log_log_153 = rail.PythonOperator(
            task_id='log_log_153',
            python_callable=lambda:  get_log_lookup_152('|')
        )

        if_log_log_153_present_154 = rail.IfOperator(
            task_id='if_log_log_153_present_154',
            test='''{{ result('log_log_153') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_155",
            no_task="if_loa_present_lobby",
        )

        nrdc_user_import_logs_add_entry_155 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_155',
            message="User Add",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "{{ result('log_log_153') }}",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        if_loa_present_lobby = rail.IfOperator(
            task_id='if_loa_present_lobby',
            test='''{{ dag_run.conf.leaveofabsence | matches('LOA') }}''',
            yes_task="get_user_details_lobby",
            no_task="c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156",
        )

        get_user_details_lobby = rail.RepliconServiceOperator(
            task_id='get_user_details_lobby',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('put_user2_lobby_timesheet_102').uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_custom_fieldsforuser_lobby = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_lobby',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('put_user2_lobby_timesheet_102').uri }}"
            }
        )

        log_loa_u_d_f_lobby  = rail.PythonOperator(
            task_id='log_loa_u_d_f_lobby',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f_lobby = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_lobby',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_lobby_timesheet_102').uri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f_lobby') }}",
                "value": "Yes"
            }
        )

        update_user_timesheet_lobby = rail.RepliconServiceOperator(
            task_id='update_user_timesheet_lobby',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda:{
                "user": {
                    "uri": rail.result("put_user2_lobby_timesheet_102")['uri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        disable_userprofile_lobby  = rail.RepliconServiceOperator(
            task_id='disable_userprofile_lobby',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('put_user2_lobby_timesheet_102').uri }}"
            }
        )

        c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156 = rail.IfOperator(
            task_id='c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156',
            test='''{{ dag_run.conf.c4orc3_present == 'C4 and C3' }}''',
            yes_task="put_user2_lobby_timsheet_157",
            no_task="c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211",
        )

        put_user2_lobby_timsheet_157 = rail.RepliconServiceOperator(
            task_id='put_user2_lobby_timsheet_157',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "lt",
                        "parameterCorrelationId": null
                    },
                    "firstname": "C3 Lobbying Timesheet",
                    "lastname": dag_run.conf['displayname'],
                    "emailAddress": null,
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_38'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_start_year_30'),
                            "month": rail.result('log_startmonth_29'),
                            "day": rail.result('log_startday_28')
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
                        "loginName": rail.result('log_loginname_31') + "lt",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 Lobbying Timesheet"
                        }
                    ],
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_65'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                            "uri": null,
                            "parent": null,
                            "name": "Full-time Salaried",
                            "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_158 = rail.SetVariableOperator(
            task_id='insert_to_list_158',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_lobby_timsheet_157').uri }}",
                "subuseruri": "{{ result('put_user2_c4_user_74').uri }}"
            }
        )

        if_request_locationuri_present_159 = rail.IfOperator(
            task_id='if_request_locationuri_present_159',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_160",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_161",
        )

        put_location_schedule_for_user_160 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_160',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_lobby_timsheet_157').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_161 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_161',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_162",
            no_task="if_d_uri_present_163",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_162 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_162',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_lobby_timsheet_157').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_163 = rail.IfOperator(
            task_id='if_d_uri_present_163',
            test='''{{ result('put_user2_lobby_timsheet_157').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_164",
            no_task="log_t_y_p_e_lobby_timsheet_uri_165",
        )

        log_forlookuplogs_164 = rail.PythonOperator(
            task_id='log_forlookuplogs_164',
            python_callable=lambda:  '''C3 user profile created successfully'''
        )

        log_t_y_p_e_lobby_timsheet_uri_165 = rail.PythonOperator(
            task_id='log_t_y_p_e_lobby_timsheet_uri_165',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri(
                "Lobbying Timesheet")
        )

        update_dropdown_value_166 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_166',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_lobby_timsheet_157').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_lobby_timsheet_uri_165') }}"
            }
        )

        def get_log_lookup_207(delimeter):
            log_forlookuplogs = []
            log_forlookuplogs_164 = rail.result('log_forlookuplogs_164')
            if log_forlookuplogs_164:
                log_forlookuplogs.append(log_forlookuplogs_164)
            return rail.smartjoin_by_delim(log_forlookuplogs, delimeter, delimeter)

        log_splitandjoinedtoremoveextraspace_208 = rail.PythonOperator(
            task_id='log_splitandjoinedtoremoveextraspace_208',
            python_callable=lambda:  get_log_lookup_207('|')
        )

        if_log_splitandjoinedtoremoveextraspace_208_present_209 = rail.IfOperator(
            task_id='if_log_splitandjoinedtoremoveextraspace_208_present_209',
            test='''{{ result('log_splitandjoinedtoremoveextraspace_208') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_210",
            no_task="if_loa_present_c3_and_c4",
        )

        nrdc_user_import_logs_add_entry_210 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_210',
            message="User Add",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "{{ result('log_splitandjoinedtoremoveextraspace_208') }}",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        if_loa_present_c3_and_c4 = rail.IfOperator(
            task_id='if_loa_present_c3_and_c4',
            test='''{{ dag_run.conf.leaveofabsence | matches('LOA') }}''',
            yes_task="get_user_details_c3_and_c4",
            no_task="c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211",
        )

        get_user_details_c3_and_c4 = rail.RepliconServiceOperator(
            task_id='get_user_details_c3_and_c4',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('put_user2_lobby_timsheet_157').uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_custom_fieldsforuser_c3_and_c4 = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_c3_and_c4',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('put_user2_lobby_timsheet_157').uri }}"
            }
        )

        log_loa_u_d_f_c3_and_c4  = rail.PythonOperator(
            task_id='log_loa_u_d_f_c3_and_c4',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f_c3_and_c4 = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_c3_and_c4',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_lobby_timsheet_157').uri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f_c3_and_c4') }}",
                "value": "Yes"
            }
        )

        update_user_timesheet_c3_and_c4 = rail.RepliconServiceOperator(
            task_id='update_user_timesheet_c3_and_c4',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda:{
                "user": {
                    "uri": rail.result("put_user2_lobby_timsheet_157")['uri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        disable_userprofile_c3_and_c4  = rail.RepliconServiceOperator(
            task_id='disable_userprofile_c3_and_c4',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('put_user2_lobby_timsheet_157').uri }}"
            }
        )

        c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211 = rail.IfOperator(
            task_id='c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211',
            test='''{{ dag_run.conf.c4orc3_present == 'C3 and Delegate' }}''',
            yes_task="put_user2_lobby_timesheet_212",
            no_task="c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266",
        )

        put_user2_lobby_timesheet_212 = rail.RepliconServiceOperator(
            task_id='put_user2_lobby_timesheet_212',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "lt",
                        "parameterCorrelationId": null
                    },
                    "firstname": "C3 Lobbying Timesheet",
                    "lastname": dag_run.conf['displayname'],
                    "emailAddress": null,
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_38'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_start_year_30'),
                            "month": rail.result('log_startmonth_29'),
                            "day": rail.result('log_startday_28')
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
                        "loginName": rail.result('log_loginname_31') + "lt",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 Lobbying Timesheet"
                        }
                    ],
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_65'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                            "uri": null,
                            "parent": null,
                            "name": "Full-time Salaried",
                            "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_213 = rail.SetVariableOperator(
            task_id='insert_to_list_213',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_lobby_timesheet_212').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_214 = rail.IfOperator(
            task_id='if_request_locationuri_present_214',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_215",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_216",
        )

        put_location_schedule_for_user_215 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_215',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_lobby_timesheet_212').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_216 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_216',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_217",
            no_task="if_d_uri_present_218",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_217 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_217',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_lobby_timesheet_212').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_218 = rail.IfOperator(
            task_id='if_d_uri_present_218',
            test='''{{ result('put_user2_lobby_timesheet_212').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_219",
            no_task="log_t_y_p_e_lobby_timesheet_uri_220",
        )

        log_forlookuplogs_219 = rail.PythonOperator(
            task_id='log_forlookuplogs_219',
            python_callable=lambda:  '''C3 user profile created successfully'''
        )

        log_t_y_p_e_lobby_timesheet_uri_220 = rail.PythonOperator(
            task_id='log_t_y_p_e_lobby_timesheet_uri_220',
            python_callable=lambda:  get_cust_dropdown_uri(
                "Lobbying Timesheet")
        )

        update_dropdown_value_221 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_221',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_lobby_timesheet_212').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_lobby_timesheet_uri_220') }}"
            }
        )

        def get_log_lookup_263(delimeter):
            log_forlookuplogs = []
            log_forlookuplogs_219 = rail.result('log_forlookuplogs_219')
            if log_forlookuplogs_219:
                log_forlookuplogs.append(log_forlookuplogs_219)
            return rail.smartjoin_by_delim(log_forlookuplogs, delimeter, delimeter)

        log_splitandjoinedtoremoveextraspace_263 = rail.PythonOperator(
            task_id='log_splitandjoinedtoremoveextraspace_263',
            python_callable=lambda:  get_log_lookup_263('|')
        )

        if_log_splitandjoinedtoremoveextraspace_263_present_264 = rail.IfOperator(
            task_id='if_log_splitandjoinedtoremoveextraspace_263_present_264',
            test='''{{ result('log_splitandjoinedtoremoveextraspace_263') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_265",
            no_task="if_loa_present_delegate_all",
        )

        nrdc_user_import_logs_add_entry_265 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_265',
            message="User Add",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "{{ result('log_splitandjoinedtoremoveextraspace_263') }}",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        if_loa_present_delegate_all = rail.IfOperator(
            task_id='if_loa_present_delegate_all',
            test='''{{ dag_run.conf.leaveofabsence | matches('LOA') }}''',
            yes_task="get_user_details_delegate_all",
            no_task="c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266",
        )

        get_user_details_delegate_all = rail.RepliconServiceOperator(
            task_id='get_user_details_delegate_all',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('put_user2_lobby_timesheet_212').uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_custom_fieldsforuser_delegate_all = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_delegate_all',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('put_user2_lobby_timesheet_212').uri }}"
            }
        )

        log_loa_u_d_f_delegate_all  = rail.PythonOperator(
            task_id='log_loa_u_d_f_delegate_all',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f_delegate_all = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_delegate_all',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_lobby_timesheet_212').uri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f_delegate_all') }}",
                "value": "Yes"
            }
        )

        update_user_timesheet_delegate_all = rail.RepliconServiceOperator(
            task_id='update_user_timesheet_delegate_all',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda:{
                "user": {
                    "uri": rail.result("put_user2_lobby_timesheet_212")['uri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        disable_userprofile_delegate_all  = rail.RepliconServiceOperator(
            task_id='disable_userprofile_delegate_all',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('put_user2_lobby_timesheet_212').uri }}"
            }
        )

        c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266 = rail.IfOperator(
            task_id='c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266',
            test='''{{ dag_run.conf.c4orc3_present == 'Delegate and all' }}''',
            yes_task="put_user2_c4_userescondary_267",
            no_task="if_declare_list_51_list_items_greater_than_0_330",
        )

        put_user2_c4_userescondary_267 = rail.RepliconServiceOperator(
            task_id='put_user2_c4_userescondary_267',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "af",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Action Fund",
                    "lastname": dag_run.conf['displayname'],
                    "emailAddress": null,
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_38'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_start_year_30'),
                            "month": rail.result('log_startmonth_29'),
                            "day": rail.result('log_startday_28')
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
                        "loginName": rail.result('log_loginname_31') + "af",
                        "password": "Replicon@12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C4 Timesheet"
                        }
                    ],
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_65'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                            "uri": null,
                            "parent": null,
                            "name": "Full-time Salaried",
                            "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_268 = rail.SetVariableOperator(
            task_id='insert_to_list_268',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_c4_userescondary_267').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_269 = rail.IfOperator(
            task_id='if_request_locationuri_present_269',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_270",
            no_task="if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_271",
        )

        put_location_schedule_for_user_270 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_270',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_c4_userescondary_267').uri }}",
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

        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_271 = rail.IfOperator(
            task_id='if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_271',
            # pylint: disable=line-too-long
            test='''{{ result('log_checkif_zara_aktheruserexisits_c4supervisot_68') | is_truthy  and result('log_checkif_zara_aktheruserenabled_c4supervisot_69') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_272",
            no_task="log_forlookuplogs_273",
        )

        update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_272 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_272',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_c4_userescondary_267').uri }}",
                "supervisorUri": "{{ result('log_checkif_zara_aktheruserexisits_c4supervisot_68') }}",
                "dateRange": null
            }
        )

        log_forlookuplogs_273 = rail.PythonOperator(
            task_id='log_forlookuplogs_273',
            python_callable=lambda:  '''C4 user profile created successfully'''
        )

        if_loa_present_delegate_all2 = rail.IfOperator(
            task_id='if_loa_present_delegate_all2',
            test='''{{ dag_run.conf.leaveofabsence | matches('LOA') }}''',
            yes_task="get_user_details_delegate_all2",
            no_task="put_user2_lobby_timesheet_274",
        )

        get_user_details_delegate_all2 = rail.RepliconServiceOperator(
            task_id='get_user_details_delegate_all2',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('put_user2_c4_userescondary_267').uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_custom_fieldsforuser_delegate_all2 = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_delegate_all2',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('put_user2_c4_userescondary_267').uri }}"
            }
        )

        log_loa_u_d_f_delegate_all2  = rail.PythonOperator(
            task_id='log_loa_u_d_f_delegate_all2',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f_delegate_all2 = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_delegate_all2',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_c4_userescondary_267').uri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f_delegate_all2') }}",
                "value": "Yes"
            }
        )

        update_user_timesheet_delegate_all2 = rail.RepliconServiceOperator(
            task_id='update_user_timesheet_delegate_all2',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda:{
                "user": {
                    "uri": rail.result("put_user2_c4_userescondary_267")['uri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        disable_userprofile_delegate_all2  = rail.RepliconServiceOperator(
            task_id='disable_userprofile_delegate_all2',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('put_user2_c4_userescondary_267').uri }}"
            }
        )

        put_user2_lobby_timesheet_274 = rail.RepliconServiceOperator(
            task_id='put_user2_lobby_timesheet_274',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "lt",
                        "parameterCorrelationId": null
                    },
                    "firstname": "C3 Lobbying Timesheet",
                    "lastname": dag_run.conf['displayname'],
                    "emailAddress": null,
                    "employeeId": dag_run.conf['empid'],
                    "department": {
                        "uri": rail.result('log_department_uri_38'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_start_year_30'),
                            "month": rail.result('log_startmonth_29'),
                            "day": rail.result('log_startday_28')
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
                        "loginName": rail.result('log_loginname_31') + "lt",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 Lobbying Timesheet"
                        }
                    ],
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": json.loads(json.dumps(rail.result('log_customfieldbody_65'))),
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                            "uri": null,
                            "parent": null,
                            "name": "Full-time Salaried",
                            "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_275 = rail.SetVariableOperator(
            task_id='insert_to_list_275',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_lobby_timesheet_274').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_276 = rail.IfOperator(
            task_id='if_request_locationuri_present_276',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_277",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_278",
        )

        put_location_schedule_for_user_277 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_277',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_lobby_timesheet_274').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_278 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_278',
            # pylint: disable=line-too-long
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_279",
            no_task="if_d_uri_present_280",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_279 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_279',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_lobby_timesheet_274').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_280 = rail.IfOperator(
            task_id='if_d_uri_present_280',
            test='''{{ result('put_user2_lobby_timesheet_274').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_281",
            no_task="log_t_y_p_e_lobby_timesheet_uri_282",
        )

        log_forlookuplogs_281 = rail.PythonOperator(
            task_id='log_forlookuplogs_281',
            python_callable=lambda:  '''C3 user profile created successfully'''
        )

        nrdc_user_import_logs_add_entry_280 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_280',
            message=lambda: f"C3 user profile created successfully.",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "C3 user profile created successfully",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )


        log_t_y_p_e_lobby_timesheet_uri_282 = rail.PythonOperator(
            task_id='log_t_y_p_e_lobby_timesheet_uri_282',
            python_callable=lambda:  get_cust_dropdown_uri(
                "Lobbying Timesheet")
        )

        update_dropdown_value_283 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_283',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_lobby_timesheet_274').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_lobby_timesheet_uri_282') }}"
            }
        )

        def get_log_lookup_324(delimeter):
            log_forlookuplogs = []
            log_forlookuplogs_273 = rail.result('log_forlookuplogs_273')
            if log_forlookuplogs_273:
                log_forlookuplogs.append(log_forlookuplogs_273)
            return rail.smartjoin_by_delim(log_forlookuplogs, delimeter, delimeter)
        
        if_loa_present_lobby2 = rail.IfOperator(
            task_id='if_loa_present_lobby2',
            test='''{{ dag_run.conf.leaveofabsence | matches('LOA') }}''',
            yes_task="get_user_details_lobby2",
            no_task="log_splitandjoinedtoremoveextraspace_325",
        )

        get_user_details_lobby2 = rail.RepliconServiceOperator(
            task_id='get_user_details_lobby2',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('put_user2_lobby_timesheet_274').uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_custom_fieldsforuser_lobby2 = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_lobby2',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('put_user2_lobby_timesheet_274').uri }}"
            }
        )

        log_loa_u_d_f_lobby2  = rail.PythonOperator(
            task_id='log_loa_u_d_f_lobby2',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f_lobby2 = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_lobby2',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_lobby_timesheet_274').uri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f_lobby2') }}",
                "value": "Yes"
            }
        )

        update_user_timesheet_lobby2 = rail.RepliconServiceOperator(
            task_id='update_user_timesheet_lobby2',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda:{
                "user": {
                    "uri": rail.result("put_user2_lobby_timesheet_274")['uri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        disable_userprofile_lobby2  = rail.RepliconServiceOperator(
            task_id='disable_userprofile_lobby2',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('put_user2_lobby_timesheet_274').uri }}"
            }
        )

        log_splitandjoinedtoremoveextraspace_325 = rail.PythonOperator(
            task_id='log_splitandjoinedtoremoveextraspace_325',
            python_callable=lambda:  get_log_lookup_324('|')
        )

        if_log_splitandjoinedtoremoveextraspace_325_present_326 = rail.IfOperator(
            task_id='if_log_splitandjoinedtoremoveextraspace_325_present_326',
            test='''{{ result('log_splitandjoinedtoremoveextraspace_325') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_327",
            no_task="if_declare_list_51_list_items_greater_than_0_330",
        )

        nrdc_user_import_logs_add_entry_327 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_327',
            message="Added",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "{{ result('log_splitandjoinedtoremoveextraspace_325') }}",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        def has_subuser():
            subuser_info = rail.get_dag_run_var(
                rail.result('declare_list_51')['name'])
            return bool(subuser_info)

        if_declare_list_51_list_items_greater_than_0_330 = rail.IfOperator(
            task_id='if_declare_list_51_list_items_greater_than_0_330',
            test=has_subuser,
            yes_task="get_51_list_331",
            no_task="stop_338",
        )

        get_51_list_331 = rail.PythonOperator(
            task_id='get_51_list_331',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_51')['name'])
        )

        foreach_declare_list_51_331 = rail.ForEachOperator(
            task_id='foreach_declare_list_51_331',
            items="{{ result('get_51_list_331') | to_json }}",
            start_task='if_foreach_a5502989_331_useruri_present_332',
            end_task='foreach_declare_list_51_331_end'
        )

        if_foreach_a5502989_331_useruri_present_332 = rail.IfOperator(
            task_id='if_foreach_a5502989_331_useruri_present_332',
            test='''{{ result('foreach_declare_list_51_331').useruri | is_truthy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2335",
            no_task="if_foreach_a5502989_331_useruri_blank_336",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2335 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2335',
            retries=0,
            items=[-1],
            trigger_dag_id=config.nrdc_assignsubstituteusersv2,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('foreach_declare_list_51_331')['subuseruri'],
                "actualuri": rail.result('foreach_declare_list_51_331')['useruri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2335 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2335',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_assign_substitute_usersv2335") }}'
        )

        if_foreach_a5502989_331_useruri_blank_336 = rail.IfOperator(
            task_id='if_foreach_a5502989_331_useruri_blank_336',
            test='''{{ result('foreach_declare_list_51_331').useruri | is_falsy }}''',
            yes_task="nrdc_user_import_logs_add_entry_337",
            no_task="foreach_declare_list_51_331_end",
        )

        nrdc_user_import_logs_add_entry_337 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_337',
            message="Substitute user assignment",
            severity="Exception",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Exception",
                "details": "Substitute user not assigned as the user profile not present",
                "action": "Add | Substitute user assignment",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        foreach_declare_list_51_331_end = rail.EmptyOperator(
            task_id='foreach_declare_list_51_331_end',
        )

        stop_338 = rail.EmptyOperator(
            task_id='stop_338'
        )

        nrdc_user_import_logs_add_entry_339 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_339',
            trigger_rule='one_failed',
            message="User creation failed",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> nrdc_user_import_logs_add_entry_339
        can_run_batch_task >> rail.Label('No') >> c3_c4_supervisors
        c3_c4_supervisors >> if_request_firstname_blank_3 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_4 >> stop_5 >> nrdc_user_import_logs_add_entry_339
        if_request_firstname_blank_3 >> rail.Label(
            'No') >> if_request_emailaddress_blank_6
        if_request_emailaddress_blank_6 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_7 >> stop_8 >> nrdc_user_import_logs_add_entry_339
        if_request_emailaddress_blank_6 >> rail.Label(
            'No') >> if_request_department_blank_9
        if_request_department_blank_9 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_10 >> stop_11 >> nrdc_user_import_logs_add_entry_339
        if_request_department_blank_9 >> rail.Label(
            'No') >> if_request_logonname_blank_12
        if_request_logonname_blank_12 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_13 >> stop_14 >> nrdc_user_import_logs_add_entry_339
        if_request_logonname_blank_12 >> rail.Label(
            'No') >> declare_list_15 >> insert_to_list_16 >> if_request_locationuri_present_17
        if_request_locationuri_present_17 >> rail.Label(
            'Yes') >> log_policydataaccessscopeforthepermission_18 >> insert_to_list_19 >> else_20 >> \
            log_policydataaccessscopeforthepermissionwithoutlocation_21 >> log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22
        if_request_locationuri_present_17 >> rail.Label(
            'No') >> log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22 >> log_f_i_n_a_lpermissiontopass_23 >> if_request_whencreated_not_contains_24
        if_request_whencreated_not_contains_24 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_25 >> stop_26 >> nrdc_user_import_logs_add_entry_339
        if_request_whencreated_not_contains_24 >> rail.Label(
            'No') >> log_startdate_27 >> log_startday_28 >> log_startmonth_29 >> log_start_year_30 >> log_loginname_31 >> search_users_32 >> \
            log_presenceofexistingloginname_33 >> if_log_presenceofexistingloginname_33_present_34
        if_log_presenceofexistingloginname_33_present_34 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_35 >> stop_36 >> nrdc_user_import_logs_add_entry_339
        if_log_presenceofexistingloginname_33_present_34 >> rail.Label(
            'No') >> get_enabled_departments_37 >> log_department_uri_38 >> if_log_department_uri_38_blank_39
        if_log_department_uri_38_blank_39 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_40 >> stop_41 >> nrdc_user_import_logs_add_entry_339
        if_log_department_uri_38_blank_39 >> rail.Label(
            'No') >> get_all_custom_fields_42 >> log_u_d_f_uri_office_43 >> log_u_d_f_uri_email_notification_44 >> \
            log_u_d_f_uri_type_45 >> get_enabled_custom_field_drop_down_options_type_46 >> \
            log_u_d_f_uri_title_47 >> log_u_d_f_urii_c_i_m_s_number_48 >> log_u_d_f_uri_employee_number_n_o_t_i_n_u_s_e_currently_49 >> \
            log_u_d_f_uri_user_name_n_o_t_i_n_u_s_e_currently_50 >> declare_list_51 >> declare_list_52 >> \
            if_request_office_present_53
        if_request_office_present_53 >> rail.Label(
            'Yes') >> insert_to_list_office_54 >> if_request_emailaddress_present_55
        if_request_office_present_53 >> rail.Label(
            'No') >> if_request_emailaddress_present_55
        if_request_emailaddress_present_55 >> rail.Label(
            'Yes') >> insert_to_list_email_notification_56 >> if_request_empnumber_present_57
        if_request_emailaddress_present_55 >> rail.Label(
            'No') >> if_request_empnumber_present_57
        if_request_empnumber_present_57 >> rail.Label(
            'Yes') >> insert_to_list_i_c_i_m_s_i_d_58 >> insert_to_list_e_m_pnumber_59 >> if_request_displayname_present_60
        if_request_empnumber_present_57 >> rail.Label(
            'No') >> if_request_displayname_present_60
        if_request_displayname_present_60 >> rail.Label(
            'Yes') >> insert_to_list_username_61 >> if_request_title_present_62
        if_request_displayname_present_60 >> rail.Label(
            'No') >> if_request_title_present_62
        if_request_title_present_62 >> rail.Label(
            'Yes') >> insert_to_list_title_63 >> insert_to_list_type_64
        if_request_title_present_62 >> rail.Label(
            'No') >> insert_to_list_type_64 >> log_customfieldbody_65 >> search_users_67 >> log_checkif_zara_aktheruserexisits_c4supervisot_68 >> \
            log_checkif_zara_aktheruserenabled_c4supervisot_69 >> search_users_lookforzshankafsupervisor_70 >> log_checkif_zach_shankuserexists_c_r11_71 >> \
            log_checkif_zach_shankifitsenabled_72 >> if_request_c4orc3present_contains_c4_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_73
        if_request_c4orc3present_contains_c4_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_73 >> rail.Label(
            'Yes') >> put_user2_c4_user_74 >> updateing_s_s_o_i_d_75 >> if_request_locationuri_present_76
        if_request_locationuri_present_76 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_77 >> nrdc_user_import_logs_add_entry_78
        if_request_locationuri_present_76 >> rail.Label(
            'No') >> nrdc_user_import_logs_add_entry_78 >> log_forlookuplogs_79 >> if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_80
        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_80 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_81 >> \
            if_loa_present_c4 >> rail.Label("Yes") >> get_user_details >> get_custom_fieldsforuser_c4 >> log_loa_u_d_f_c4 >> updateemployee_number_u_d_f_c4 >> update_user_timesheet_c4 >> disable_userprofile_c4 >>if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82
        if_loa_present_c4 >> rail.Label("No") >> if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82
        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_80 >> rail.Label(
            'No') >> if_loa_present_c4 >> if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82
        if_request_c4orc3present_contains_c4_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_73 >> rail.Label(
            'No') >> if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82
        if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82 >> rail.Label(
            'Yes') >> put_user2_delegate_user_83 >> updateing_s_s_o_i_d_84 >> if_request_locationuri_present_85
        if_request_locationuri_present_85 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_86 >> log_type_delegate_uri_87
        if_request_locationuri_present_85 >> rail.Label(
            'No') >> log_type_delegate_uri_87 >> update_dropdown_value_88 >> nrdc_user_import_logs_add_entry_89 >> \
            log_forlookuplogs_90 >> if_loa_present_delegate >> rail.Label("Yes") >> get_user_details_delegate >> get_custom_fieldsforuser_delegate >> log_loa_u_d_f_delegate >> updateemployee_number_u_d_f_delegate >> update_user_timesheet_delegate >> disable_userprofile_delegate >> if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91
        if_loa_present_delegate >> rail.Label("No") >> if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91
        if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82 >> rail.Label(
            'No') >> if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91
        if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91 >> rail.Label(
            'Yes') >> put_user2_c4_userescondary_92 >> updateing_s_s_o_i_d_93 >> if_request_locationuri_present_94
        if_request_locationuri_present_94 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_95 >> insert_to_list_96
        if_request_locationuri_present_94 >> rail.Label(
            'No') >> insert_to_list_96 >> if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_97
        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_97 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_98 >> \
            log_forlookuplogs_99
        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_97 >> rail.Label(
            'No') >> log_forlookuplogs_99 >> nrdc_user_import_logs_add_entry_100 >> \
            if_loa_present_delegate_and_1 >> rail.Label("Yes") >> get_user_details_delegate_and_1 >> get_custom_fieldsforuser_delegate_and_1  >> log_loa_u_d_f_delegate_and_1 >> updateemployee_number_u_d_f_delegate_and_1 >> update_user_timesheet_delegate_and_1 >> disable_userprofile_delegate_and_1 >> if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101
        if_loa_present_delegate_and_1 >> rail.Label("No") >> if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101
        if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91 >> rail.Label(
            'No') >> if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101
        if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101 >> rail.Label(
            'Yes') >> put_user2_lobby_timesheet_102 >> updateing_s_s_o_i_d_103 >> if_request_locationuri_present_104
        if_request_locationuri_present_104 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_105 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_106
        if_request_locationuri_present_104 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_106
        if_log_checkif_zach_shankuserexists_c_r11_71_present_106 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_107 >> \
            if_d_uri_present_108
        if_log_checkif_zach_shankuserexists_c_r11_71_present_106 >> rail.Label(
            'No') >> if_d_uri_present_108
        if_d_uri_present_108 >> rail.Label(
            'Yes') >> log_forlookuplogs_109 >> log_type_lobby_timesheet_uri_110
        if_d_uri_present_108 >> rail.Label(
            'No') >> log_type_lobby_timesheet_uri_110 >> update_dropdown_value_111 >> log_log_153 >> if_log_log_153_present_154
        if_log_log_153_present_154 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_155 >> if_loa_present_lobby >> rail.Label("Yes") >> get_user_details_lobby >> get_custom_fieldsforuser_lobby >> log_loa_u_d_f_lobby >> updateemployee_number_u_d_f_lobby >> update_user_timesheet_lobby >> disable_userprofile_lobby >> c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156
        if_loa_present_lobby >> rail.Label("No") >> c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156
        if_log_log_153_present_154 >> rail.Label(
            'No')  >> if_loa_present_lobby >> c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156
        if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101 >> rail.Label(
            'No') >> c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156
        c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156 >> rail.Label(
            'Yes') >> put_user2_lobby_timsheet_157 >> insert_to_list_158 >> if_request_locationuri_present_159
        if_request_locationuri_present_159 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_160 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_161
        if_request_locationuri_present_159 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_161
        if_log_checkif_zach_shankuserexists_c_r11_71_present_161 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_162 >> \
            if_d_uri_present_163
        if_log_checkif_zach_shankuserexists_c_r11_71_present_161 >> rail.Label(
            'No') >> if_d_uri_present_163
        if_d_uri_present_163 >> rail.Label(
            'Yes') >> log_forlookuplogs_164 >> log_t_y_p_e_lobby_timsheet_uri_165
        if_d_uri_present_163 >> rail.Label(
            'No') >> log_t_y_p_e_lobby_timsheet_uri_165 >> update_dropdown_value_166 \
            >> log_splitandjoinedtoremoveextraspace_208 >> if_log_splitandjoinedtoremoveextraspace_208_present_209
        if_log_splitandjoinedtoremoveextraspace_208_present_209 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_210 >> if_loa_present_c3_and_c4 >> rail.Label("Yes") >> get_user_details_c3_and_c4 >> log_loa_u_d_f_c3_and_c4 >> updateemployee_number_u_d_f_c3_and_c4 >> get_custom_fieldsforuser_c3_and_c4 >> update_user_timesheet_c3_and_c4 >> disable_userprofile_c3_and_c4 >> c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211
        if_loa_present_c3_and_c4 >> rail.Label("No") >> c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211
        if_log_splitandjoinedtoremoveextraspace_208_present_209 >> rail.Label(
            'No') >> if_loa_present_c3_and_c4 >> c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211
        c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156 >> rail.Label(
            'No') >> c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211
        c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211 >> rail.Label(
            'Yes') >> put_user2_lobby_timesheet_212 >> insert_to_list_213 >> if_request_locationuri_present_214
        if_request_locationuri_present_214 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_215 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_216
        if_request_locationuri_present_214 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_216
        if_log_checkif_zach_shankuserexists_c_r11_71_present_216 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_217 >> \
            if_d_uri_present_218
        if_log_checkif_zach_shankuserexists_c_r11_71_present_216 >> rail.Label(
            'No') >> if_d_uri_present_218
        if_d_uri_present_218 >> rail.Label(
            'Yes') >> log_forlookuplogs_219 >> log_t_y_p_e_lobby_timesheet_uri_220
        if_d_uri_present_218 >> rail.Label(
            'No') >> log_t_y_p_e_lobby_timesheet_uri_220 >> update_dropdown_value_221 >> \
            log_splitandjoinedtoremoveextraspace_263 >> \
            if_log_splitandjoinedtoremoveextraspace_263_present_264
        if_log_splitandjoinedtoremoveextraspace_263_present_264 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_265 >> if_loa_present_delegate_all >> rail.Label("Yes") >> get_user_details_delegate_all >> get_custom_fieldsforuser_delegate_all >> log_loa_u_d_f_delegate_all >> updateemployee_number_u_d_f_delegate_all >> update_user_timesheet_delegate_all >> disable_userprofile_delegate_all  >> \
            c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266
        if_loa_present_delegate_all >> rail.Label("No") >> c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266
        if_log_splitandjoinedtoremoveextraspace_263_present_264 >> rail.Label(
            'No') >> if_loa_present_delegate_all >> c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266
        c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211 >> rail.Label(
            'No') >> c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266
        c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266 >> rail.Label(
            'Yes') >> put_user2_c4_userescondary_267 >> insert_to_list_268 >> if_request_locationuri_present_269
        if_request_locationuri_present_269 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_270 >> if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_271
        if_request_locationuri_present_269 >> rail.Label(
            'No') >> if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_271
        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_271 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_272 >> log_forlookuplogs_273
        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_271 >> rail.Label(
            'No') >> log_forlookuplogs_273  >> if_loa_present_delegate_all2 >> rail.Label("Yes") >> get_user_details_delegate_all2 >> get_custom_fieldsforuser_delegate_all2 >> log_loa_u_d_f_delegate_all2 >> updateemployee_number_u_d_f_delegate_all2 >> update_user_timesheet_delegate_all2 >> disable_userprofile_delegate_all2 >> put_user2_lobby_timesheet_274 >> insert_to_list_275 >> if_request_locationuri_present_276
        if_loa_present_delegate_all2 >> rail.Label("No") >> put_user2_lobby_timesheet_274
        if_request_locationuri_present_276 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_277 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_278
        if_request_locationuri_present_276 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_278
        if_log_checkif_zach_shankuserexists_c_r11_71_present_278 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_279 >> if_d_uri_present_280
        if_log_checkif_zach_shankuserexists_c_r11_71_present_278 >> rail.Label(
            'No') >> if_d_uri_present_280
        if_d_uri_present_280 >> rail.Label(
            'Yes') >> log_forlookuplogs_281 >> nrdc_user_import_logs_add_entry_280 >> log_t_y_p_e_lobby_timesheet_uri_282
        if_d_uri_present_280 >> rail.Label(
            'No') >> log_t_y_p_e_lobby_timesheet_uri_282 >> update_dropdown_value_283 >> if_loa_present_lobby2 >> rail.Label("Yes") >> get_user_details_lobby2 >> get_custom_fieldsforuser_lobby2 >> log_loa_u_d_f_lobby2 >> updateemployee_number_u_d_f_lobby2 >> update_user_timesheet_lobby2 >> disable_userprofile_lobby2 >> \
            log_splitandjoinedtoremoveextraspace_325 >> if_log_splitandjoinedtoremoveextraspace_325_present_326
        if_loa_present_lobby2 >> rail.Label("No") >> log_splitandjoinedtoremoveextraspace_325
        if_log_splitandjoinedtoremoveextraspace_325_present_326 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_327 >> if_declare_list_51_list_items_greater_than_0_330
        if_log_splitandjoinedtoremoveextraspace_325_present_326 >> rail.Label(
            'No') >> if_declare_list_51_list_items_greater_than_0_330
        c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266 >> rail.Label(
            'No') >> if_declare_list_51_list_items_greater_than_0_330
        if_declare_list_51_list_items_greater_than_0_330 >> rail.Label(
            'Yes') >> get_51_list_331 >> foreach_declare_list_51_331 >> if_foreach_a5502989_331_useruri_present_332
        if_foreach_a5502989_331_useruri_present_332 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2335 >> \
            if_foreach_a5502989_331_useruri_blank_336
        if_foreach_a5502989_331_useruri_present_332 >> rail.Label(
            'No') >> if_foreach_a5502989_331_useruri_blank_336
        if_foreach_a5502989_331_useruri_blank_336 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_337 >> foreach_declare_list_51_331_end
        if_foreach_a5502989_331_useruri_blank_336 >> rail.Label(
            'No') >> foreach_declare_list_51_331_end
        foreach_declare_list_51_331 >> foreach_declare_list_51_331_end >> wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2335 >> \
            stop_338 >> nrdc_user_import_logs_add_entry_339 >> log_to_sumo
        if_declare_list_51_list_items_greater_than_0_330 >> rail.Label(
            'No') >> stop_338

    return dag


rail.for_each_instance(create_dag)
