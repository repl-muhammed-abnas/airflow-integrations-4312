
from datetime import datetime, timedelta
import json
import itertools
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nrdc_add_user_v2_{config.instance}',
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
            no_task='if_request_firstname_blank_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_firstname_blank_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_firstname_blank_3 = rail.IfOperator(
            task_id='if_request_firstname_blank_3',
            test="{{ dag_run.conf.firstname | is_falsy  or dag_run.conf.lastname | is_falsy }}",
            yes_task="nrdc_user_import_logs_add_entry_4",
            no_task="if_request_emailaddress_blank_6",
        )

        nrdc_user_import_logs_add_entry_4 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_4',
            message="User Add",
            severity="Failed",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "{{ dag_run_ecid() }} - First Name and Last Name must be provided",
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
            message="User Add",
            severity="Failed",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "{{ dag_run_ecid() }} Email address must be provided",
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
            message="User Add",
            severity="Failed",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "{{ dag_run_ecid() }} - Department must be provided",
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
            message="User Add",
            severity="Failed",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Failed",
                "details": "{{ dag_run_ecid() }} - Logon name must be provided",
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
                "details": "{{ dag_run_ecid() }} - Date format is incoorect",
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
                "details": "{{ dag_run_ecid() }} - Login name already exists - '{{ result('log_loginname_31') }}'",
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
                "details": "{{ dag_run_ecid() }} -Department must be provided/ Invaid department",
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

        # def get_office_customoef_uri(custom_field_info):
        #     existing_customoefs = rail.result('get_all_custom_fields_42')
        #     input_department_info = list(filter(
        #         lambda item: item['displayText'] == "Office", existing_customoefs))
        #     return input_department_info[0]['uri'] if input_department_info else None

        # def get_email_notify_customoef_uri(dag_run):
        #     existing_customoefs = rail.result('get_all_custom_fields_42')
        #     input_department_info = list(filter(
        #         lambda item: item['displayText'] == "Email Notification", existing_customoefs))
        #     return input_department_info[0]['uri'] if input_department_info else None

        # def get_Type_customoef_uri(dag_run):
        #     existing_customoefs = rail.result('get_all_custom_fields_42')
        #     input_department_info = list(filter(
        #         lambda item: item['displayText'] == "Type", existing_customoefs))
        #     return input_department_info[0]['uri'] if input_department_info else None

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
            data={
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
                            'text': "ZAkhter"
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, "ZAkhter")
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
            data={
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
                            'text': "zshankaf"
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, "zshankaf")
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
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
            message="User Add",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ result('put_user2_c4_user_74').loginName }}",
                "status": "Success",
                "details": "C4 primary user profile created successfully|{{ dag_run_ecid() }} ",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        log_forlookuplogs_79 = rail.PythonOperator(
            task_id='log_forlookuplogs_79',
            python_callable=lambda:  '''C4 primary user profile created successfully'''
        )

        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_80 = rail.IfOperator(
            task_id='if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_80',
            # pylint: disable=line-too-long
            test='''{{ result('log_checkif_zara_aktheruserexisits_c4supervisot_68') | is_truthy  and result('log_checkif_zara_aktheruserenabled_c4supervisot_69') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zaraassignedasthesupervisor_81",
            no_task="if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82",
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
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
            message="User Add",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "Delegate user profile succesffully created|{{ dag_run_ecid() }}",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        log_forlookuplogs_90 = rail.PythonOperator(
            task_id='log_forlookuplogs_90',
            python_callable=lambda:  '''C4 primary user profile created successfully'''
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
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
            python_callable=lambda:  '''C4 secondary  user profile created successfully'''
        )

        nrdc_user_import_logs_add_entry_100 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_100',
            message="User Add",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "C4 secondary user profile succesffully created|{{ dag_run_ecid() }}",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101 = rail.IfOperator(
            task_id='if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101',
            test='''{{ dag_run.conf.c4orc3_present == 'C3 Only' }}''',
            yes_task="put_user2_federal_legislative_102",
            no_task="c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156",
        )

        put_user2_federal_legislative_102 = rail.RepliconServiceOperator(
            task_id='put_user2_federal_legislative_102',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31'),
                        "parameterCorrelationId": null
                    },
                    "firstname": "Federal Legislative",
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
                            "name": "C3 - Federal Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "uri": "{{ result('put_user2_federal_legislative_102').uri }}",
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
                        "ssoName": "{{ result('put_user2_federal_legislative_102').loginName }}",
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
                "userUri": "{{ result('put_user2_federal_legislative_102').uri }}",
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
                "userUri": "{{ result('put_user2_federal_legislative_102').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_108 = rail.IfOperator(
            task_id='if_d_uri_present_108',
            test='''{{ result('put_user2_federal_legislative_102').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_109",
            no_task="log_type_federal_legislative_uri_110",
        )

        log_forlookuplogs_109 = rail.PythonOperator(
            task_id='log_forlookuplogs_109',
            python_callable=lambda:  '''FL primary user profile created successfully'''
        )

        log_type_federal_legislative_uri_110 = rail.PythonOperator(
            task_id='log_type_federal_legislative_uri_110',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri(
                "Federal Legislative")
        )

        update_dropdown_value_111 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_111',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_federal_legislative_102').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_type_federal_legislative_uri_110') }}"
            }
        )

        put_user2_local_administrative_112 = rail.RepliconServiceOperator(
            task_id='put_user2_local_administrative_112',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "la",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Local administrative",
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
                        "loginName": rail.result('log_loginname_31') + "la",
                        "password": "Replicon@12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Local Administrative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_113 = rail.SetVariableOperator(
            task_id='insert_to_list_113',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_local_administrative_112').uri }}",
                "subuseruri": "{{ result('put_user2_federal_legislative_102').uri }}"
            }
        )

        if_request_locationuri_present_114 = rail.IfOperator(
            task_id='if_request_locationuri_present_114',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_115",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_116",
        )

        put_location_schedule_for_user_115 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_115',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_local_administrative_112').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_116 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_116',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_117",
            no_task="if_d_uri_present_118",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_117 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_117',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_local_administrative_112').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_118 = rail.IfOperator(
            task_id='if_d_uri_present_118',
            test='''{{ result('put_user2_local_administrative_112').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_119",
            no_task="log_t_y_p_e_localadministrative_uri_120",
        )

        log_forlookuplogs_119 = rail.PythonOperator(
            task_id='log_forlookuplogs_119',
            python_callable=lambda:  '''LA user profile created successfully'''
        )

        log_t_y_p_e_localadministrative_uri_120 = rail.PythonOperator(
            task_id='log_t_y_p_e_localadministrative_uri_120',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri(
                "Local administrative")
        )

        update_dropdown_value_121 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_121',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_local_administrative_112').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_localadministrative_uri_120') }}"
            }
        )

        put_user2_local_legislative_122 = rail.RepliconServiceOperator(
            task_id='put_user2_local_legislative_122',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "ll",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Local legislative",
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
                        "loginName": rail.result('log_loginname_31') + "ll",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Local Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_123 = rail.SetVariableOperator(
            task_id='insert_to_list_123',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_local_legislative_122').uri }}",
                "subuseruri": "{{ result('put_user2_federal_legislative_102').uri }}"
            }
        )

        if_request_locationuri_present_124 = rail.IfOperator(
            task_id='if_request_locationuri_present_124',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_125",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_126",
        )

        put_location_schedule_for_user_125 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_125',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_local_legislative_122').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_126 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_126',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_127",
            no_task="if_d_uri_present_128",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_127 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_127',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_local_legislative_122').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_128 = rail.IfOperator(
            task_id='if_d_uri_present_128',
            test='''{{ result('put_user2_local_legislative_122').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_129",
            no_task="log_t_y_p_e_locallegislative_uri_130",
        )

        log_forlookuplogs_129 = rail.PythonOperator(
            task_id='log_forlookuplogs_129',
            python_callable=lambda:  '''LL user profile created successfully'''
        )

        log_t_y_p_e_locallegislative_uri_130 = rail.PythonOperator(
            task_id='log_t_y_p_e_locallegislative_uri_130',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri("Local legislative")
        )

        update_dropdown_value_131 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_131',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_local_legislative_122').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_locallegislative_uri_130') }}"
            }
        )

        put_user2_state_administrative_132 = rail.RepliconServiceOperator(
            task_id='put_user2_state_administrative_132',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "sa",
                        "parameterCorrelationId": null
                    },
                    "firstname": "State administrative",
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
                        "loginName": rail.result('log_loginname_31') + "sa",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - State Administrative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_133 = rail.SetVariableOperator(
            task_id='insert_to_list_133',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_state_administrative_132').uri }}",
                "subuseruri": "{{ result('put_user2_federal_legislative_102').uri }}"
            }
        )

        if_request_locationuri_present_134 = rail.IfOperator(
            task_id='if_request_locationuri_present_134',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_135",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_136",
        )

        put_location_schedule_for_user_135 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_135',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_state_administrative_132').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_136 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_136',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_137",
            no_task="if_d_uri_present_138",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_137 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_137',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_state_administrative_132').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_138 = rail.IfOperator(
            task_id='if_d_uri_present_138',
            test='''{{ result('put_user2_state_administrative_132').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_139",
            no_task="log_t_y_p_e_stateadministrative_uri_140",
        )

        log_forlookuplogs_139 = rail.PythonOperator(
            task_id='log_forlookuplogs_139',
            python_callable=lambda:  '''SA user profile created successfully'''
        )

        log_t_y_p_e_stateadministrative_uri_140 = rail.PythonOperator(
            task_id='log_t_y_p_e_stateadministrative_uri_140',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri(
                "State administrative")
        )

        update_dropdown_value_141 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_141',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_state_administrative_132').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_stateadministrative_uri_140') }}"
            }
        )

        put_user2_statelegislative_142 = rail.RepliconServiceOperator(
            task_id='put_user2_statelegislative_142',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "sl",
                        "parameterCorrelationId": null
                    },
                    "firstname": "State legislative",
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
                        "loginName": rail.result('log_loginname_31') + "sl",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - State Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_143 = rail.SetVariableOperator(
            task_id='insert_to_list_143',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_statelegislative_142').uri }}",
                "subuseruri": "{{ result('put_user2_federal_legislative_102').uri }}"
            }
        )

        if_request_locationuri_present_144 = rail.IfOperator(
            task_id='if_request_locationuri_present_144',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_145",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_146",
        )

        put_location_schedule_for_user_145 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_145',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_statelegislative_142').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_146 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_146',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedassupervisor_147",
            no_task="if_d_uri_present_148",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedassupervisor_147 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedassupervisor_147',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_statelegislative_142').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_148 = rail.IfOperator(
            task_id='if_d_uri_present_148',
            test='''{{ result('put_user2_statelegislative_142').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_149",
            no_task="log_t_y_p_e_statelegislative_uri_150",
        )

        log_forlookuplogs_149 = rail.PythonOperator(
            task_id='log_forlookuplogs_149',
            python_callable=lambda:  '''SL user profile created successfully'''
        )

        log_t_y_p_e_statelegislative_uri_150 = rail.PythonOperator(
            task_id='log_t_y_p_e_statelegislative_uri_150',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri("State legislative")
        )

        update_dropdown_value_151 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_151',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_statelegislative_142').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_statelegislative_uri_150') }}"
            }
        )

        def get_log_lookup_152(delimeter):
            log_forlookuplogs = []
            log_forlookuplogs_109 = rail.result('log_forlookuplogs_109')
            if log_forlookuplogs_109:
                log_forlookuplogs.append(log_forlookuplogs_109)
            log_forlookuplogs_119 = rail.result('log_forlookuplogs_119')
            if log_forlookuplogs_119:
                log_forlookuplogs.append(log_forlookuplogs_119)
            log_forlookuplogs_129 = rail.result('log_forlookuplogs_129')
            if log_forlookuplogs_129:
                log_forlookuplogs.append(log_forlookuplogs_129)
            log_forlookuplogs_139 = rail.result('log_forlookuplogs_139')
            if log_forlookuplogs_139:
                log_forlookuplogs.append(log_forlookuplogs_139)
            log_forlookuplogs_149 = rail.result('log_forlookuplogs_149')
            if log_forlookuplogs_149:
                log_forlookuplogs.append(log_forlookuplogs_149)
            return rail.smartjoin_by_delim(log_forlookuplogs, delimeter, delimeter)

        log_log_153 = rail.PythonOperator(
            task_id='log_log_153',
            python_callable=lambda:  get_log_lookup_152('|')
        )

        if_log_log_153_present_154 = rail.IfOperator(
            task_id='if_log_log_153_present_154',
            test='''{{ result('log_log_153') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_155",
            no_task="c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156",
        )

        nrdc_user_import_logs_add_entry_155 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_155',
            message="User Add",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "{{ result('log_log_153') }}|{{ dag_run_ecid() }} ",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156 = rail.IfOperator(
            task_id='c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156',
            test='''{{ dag_run.conf.c4orc3_present == 'C4 and C3' }}''',
            yes_task="put_user2_federal_legislative_157",
            no_task="c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211",
        )

        put_user2_federal_legislative_157 = rail.RepliconServiceOperator(
            task_id='put_user2_federal_legislative_157',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "fl",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Federal Legislative",
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
                        "loginName": rail.result('log_loginname_31') + "fl",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Federal Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                "useruri": "{{ result('put_user2_federal_legislative_157').uri }}",
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
                "userUri": "{{ result('put_user2_federal_legislative_157').uri }}",
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
                "userUri": "{{ result('put_user2_federal_legislative_157').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_163 = rail.IfOperator(
            task_id='if_d_uri_present_163',
            test='''{{ result('put_user2_federal_legislative_157').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_164",
            no_task="log_t_y_p_e_federal_legislative_uri_165",
        )

        log_forlookuplogs_164 = rail.PythonOperator(
            task_id='log_forlookuplogs_164',
            python_callable=lambda:  '''FL user profile created successfully'''
        )

        log_t_y_p_e_federal_legislative_uri_165 = rail.PythonOperator(
            task_id='log_t_y_p_e_federal_legislative_uri_165',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri(
                "Federal Legislative")
        )

        update_dropdown_value_166 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_166',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_federal_legislative_157').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_federal_legislative_uri_165') }}"
            }
        )

        put_user2_local_administrative_167 = rail.RepliconServiceOperator(
            task_id='put_user2_local_administrative_167',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "la",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Local administrative",
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
                        "loginName": rail.result('log_loginname_31') + "la",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Local Administrative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_168 = rail.SetVariableOperator(
            task_id='insert_to_list_168',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_local_administrative_167').uri }}",
                "subuseruri": "{{ result('put_user2_c4_user_74').uri }}"
            }
        )

        if_request_locationuri_present_169 = rail.IfOperator(
            task_id='if_request_locationuri_present_169',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_170",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_171",
        )

        put_location_schedule_for_user_170 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_170',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_local_administrative_167').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_171 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_171',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_172",
            no_task="if_d_uri_present_173",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_172 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_172',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_local_administrative_167').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_173 = rail.IfOperator(
            task_id='if_d_uri_present_173',
            test='''{{ result('put_user2_local_administrative_167').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_174",
            no_task="log_t_y_p_e_localadministrative_uri_175",
        )

        log_forlookuplogs_174 = rail.PythonOperator(
            task_id='log_forlookuplogs_174',
            python_callable=lambda:  '''LA user profile created successfully'''
        )

        log_t_y_p_e_localadministrative_uri_175 = rail.PythonOperator(
            task_id='log_t_y_p_e_localadministrative_uri_175',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri(
                "Local administrative")
        )

        update_dropdown_value_176 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_176',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_local_administrative_167').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_localadministrative_uri_175') }}"
            }
        )

        put_user2_local_legislative_177 = rail.RepliconServiceOperator(
            task_id='put_user2_local_legislative_177',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "ll",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Local legislative",
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
                        "loginName": rail.result('log_loginname_31') + "ll",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Local Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_178 = rail.SetVariableOperator(
            task_id='insert_to_list_178',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_local_legislative_177').uri }}",
                "subuseruri": "{{ result('put_user2_c4_user_74').uri }}"
            }
        )

        if_request_locationuri_present_179 = rail.IfOperator(
            task_id='if_request_locationuri_present_179',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_180",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_181",
        )

        put_location_schedule_for_user_180 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_180',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_local_legislative_177').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_181 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_181',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_182",
            no_task="if_d_uri_present_183",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_182 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_182',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_local_legislative_177').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_183 = rail.IfOperator(
            task_id='if_d_uri_present_183',
            test='''{{ result('put_user2_local_legislative_177').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_184",
            no_task="log_t_y_p_e_locallegislative_uri_185",
        )

        log_forlookuplogs_184 = rail.PythonOperator(
            task_id='log_forlookuplogs_184',
            python_callable=lambda:  '''LL user profile created successfully'''
        )

        log_t_y_p_e_locallegislative_uri_185 = rail.PythonOperator(
            task_id='log_t_y_p_e_locallegislative_uri_185',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri("Local legislative")
        )

        update_dropdown_value_186 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_186',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_local_legislative_177').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_locallegislative_uri_185') }}"
            }
        )

        put_user2_state_administrative_187 = rail.RepliconServiceOperator(
            task_id='put_user2_state_administrative_187',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "sa",
                        "parameterCorrelationId": null
                    },
                    "firstname": "State administrative",
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
                        "loginName": rail.result('log_loginname_31') + "sa",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - State Administrative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_188 = rail.SetVariableOperator(
            task_id='insert_to_list_188',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_state_administrative_187').uri }}",
                "subuseruri": "{{ result('put_user2_c4_user_74').uri }}"
            }
        )

        if_request_locationuri_present_189 = rail.IfOperator(
            task_id='if_request_locationuri_present_189',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_190",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_191",
        )

        put_location_schedule_for_user_190 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_190',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_state_administrative_187').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_191 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_191',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_192",
            no_task="if_d_uri_present_193",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_192 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_192',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_state_administrative_187').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_193 = rail.IfOperator(
            task_id='if_d_uri_present_193',
            test='''{{ result('put_user2_state_administrative_187').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_194",
            no_task="log_t_y_p_e_stateadministrative_uri_195",
        )

        log_forlookuplogs_194 = rail.PythonOperator(
            task_id='log_forlookuplogs_194',
            python_callable=lambda:  '''SA user profile created successfully'''
        )

        log_t_y_p_e_stateadministrative_uri_195 = rail.PythonOperator(
            task_id='log_t_y_p_e_stateadministrative_uri_195',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_cust_dropdown_uri(
                "State administrative")
        )

        update_dropdown_value_196 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_196',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_state_administrative_187').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_stateadministrative_uri_195') }}"
            }
        )

        put_user2_statelegislative_197 = rail.RepliconServiceOperator(
            task_id='put_user2_statelegislative_197',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "sl",
                        "parameterCorrelationId": null
                    },
                    "firstname": "State legislative",
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
                        "loginName": rail.result('log_loginname_31') + "sl",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - State Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_198 = rail.SetVariableOperator(
            task_id='insert_to_list_198',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_statelegislative_197').uri }}",
                "subuseruri": "{{ result('put_user2_c4_user_74').uri }}"
            }
        )

        if_request_locationuri_present_199 = rail.IfOperator(
            task_id='if_request_locationuri_present_199',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_200",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_201",
        )

        put_location_schedule_for_user_200 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_200',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_statelegislative_197').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_201 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_201',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_202",
            no_task="if_d_uri_present_203",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_202 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_202',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_statelegislative_197').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_203 = rail.IfOperator(
            task_id='if_d_uri_present_203',
            test='''{{ result('put_user2_statelegislative_197').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_204",
            no_task="log_t_y_p_e_statelegislative_uri_205",
        )

        log_forlookuplogs_204 = rail.PythonOperator(
            task_id='log_forlookuplogs_204',
            python_callable=lambda:  '''SL user profile created successfully'''
        )

        log_t_y_p_e_statelegislative_uri_205 = rail.PythonOperator(
            task_id='log_t_y_p_e_statelegislative_uri_205',
            python_callable=lambda:  get_cust_dropdown_uri("State legislative")
        )

        update_dropdown_value_206 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_206',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_statelegislative_197').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_statelegislative_uri_205') }}"
            }
        )

        def get_log_lookup_207(delimeter):
            log_forlookuplogs = []
            log_forlookuplogs_164 = rail.result('log_forlookuplogs_164')
            if log_forlookuplogs_164:
                log_forlookuplogs.append(log_forlookuplogs_164)
            log_forlookuplogs_174 = rail.result('log_forlookuplogs_174')
            if log_forlookuplogs_174:
                log_forlookuplogs.append(log_forlookuplogs_174)
            log_forlookuplogs_184 = rail.result('log_forlookuplogs_184')
            if log_forlookuplogs_184:
                log_forlookuplogs.append(log_forlookuplogs_184)
            log_forlookuplogs_194 = rail.result('log_forlookuplogs_194')
            if log_forlookuplogs_194:
                log_forlookuplogs.append(log_forlookuplogs_194)
            log_forlookuplogs_204 = rail.result('log_forlookuplogs_204')
            if log_forlookuplogs_204:
                log_forlookuplogs.append(log_forlookuplogs_204)
            return rail.smartjoin_by_delim(log_forlookuplogs, delimeter, delimeter)

        log_splitandjoinedtoremoveextraspace_208 = rail.PythonOperator(
            task_id='log_splitandjoinedtoremoveextraspace_208',
            python_callable=lambda:  get_log_lookup_207('|')
        )

        if_log_splitandjoinedtoremoveextraspace_208_present_209 = rail.IfOperator(
            task_id='if_log_splitandjoinedtoremoveextraspace_208_present_209',
            test='''{{ result('log_splitandjoinedtoremoveextraspace_208') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_210",
            no_task="c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211",
        )

        nrdc_user_import_logs_add_entry_210 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_210',
            message="User Add",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "{{ result('log_splitandjoinedtoremoveextraspace_208') }}|{{ dag_run_ecid() }} ",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211 = rail.IfOperator(
            task_id='c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211',
            test='''{{ dag_run.conf.c4orc3_present == 'C3 and Delegate' }}''',
            yes_task="put_user2_federal_legislative_212",
            no_task="c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266",
        )

        put_user2_federal_legislative_212 = rail.RepliconServiceOperator(
            task_id='put_user2_federal_legislative_212',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "fl",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Federal Legislative",
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
                        "loginName": rail.result('log_loginname_31') + "fl",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Federal Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                "useruri": "{{ result('put_user2_federal_legislative_212').uri }}",
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
                "userUri": "{{ result('put_user2_federal_legislative_212').uri }}",
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
                "userUri": "{{ result('put_user2_federal_legislative_212').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_218 = rail.IfOperator(
            task_id='if_d_uri_present_218',
            test='''{{ result('put_user2_federal_legislative_212').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_219",
            no_task="log_t_y_p_e_federal_legislative_uri_220",
        )

        log_forlookuplogs_219 = rail.PythonOperator(
            task_id='log_forlookuplogs_219',
            python_callable=lambda:  '''FL user profile created successfully'''
        )

        log_t_y_p_e_federal_legislative_uri_220 = rail.PythonOperator(
            task_id='log_t_y_p_e_federal_legislative_uri_220',
            python_callable=lambda:  get_cust_dropdown_uri(
                "Federal Legislative")
        )

        update_dropdown_value_221 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_221',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_federal_legislative_212').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_federal_legislative_uri_220') }}"
            }
        )

        put_user2_local_administrative_222 = rail.RepliconServiceOperator(
            task_id='put_user2_local_administrative_222',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "la",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Local administrative",
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
                        "loginName": rail.result('log_loginname_31') + "la",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Local Administrative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_223 = rail.SetVariableOperator(
            task_id='insert_to_list_223',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_local_administrative_222').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_224 = rail.IfOperator(
            task_id='if_request_locationuri_present_224',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_225",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_226",
        )

        put_location_schedule_for_user_225 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_225',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_local_administrative_222').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_226 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_226',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_227",
            no_task="if_d_uri_present_228",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_227 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_227',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_local_administrative_222').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_228 = rail.IfOperator(
            task_id='if_d_uri_present_228',
            test='''{{ result('put_user2_local_administrative_222').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_229",
            no_task="log_t_y_p_e_localadministrative_uri_230",
        )

        log_forlookuplogs_229 = rail.PythonOperator(
            task_id='log_forlookuplogs_229',
            python_callable=lambda:  '''LA user profile created successfully'''
        )

        log_t_y_p_e_localadministrative_uri_230 = rail.PythonOperator(
            task_id='log_t_y_p_e_localadministrative_uri_230',
            python_callable=lambda:  get_cust_dropdown_uri(
                "Local administrative")
        )

        update_dropdown_value_231 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_231',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_local_administrative_222').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_localadministrative_uri_230') }}"
            }
        )

        put_user2_local_legislative_232 = rail.RepliconServiceOperator(
            task_id='put_user2_local_legislative_232',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "ll",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Local legislative",
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
                        "loginName": rail.result('log_loginname_31') + "ll",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Local Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_233 = rail.SetVariableOperator(
            task_id='insert_to_list_233',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_local_legislative_232').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_234 = rail.IfOperator(
            task_id='if_request_locationuri_present_234',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_235",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_236",
        )

        put_location_schedule_for_user_235 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_235',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_local_legislative_232').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_236 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_236',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_237",
            no_task="if_d_uri_present_238",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_237 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_237',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_local_legislative_232').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_238 = rail.IfOperator(
            task_id='if_d_uri_present_238',
            test='''{{ result('put_user2_local_legislative_232').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_239",
            no_task="log_t_y_p_e_locallegislative_uri_240",
        )

        log_forlookuplogs_239 = rail.PythonOperator(
            task_id='log_forlookuplogs_239',
            python_callable=lambda:  '''LL user profile created successfully'''
        )

        log_t_y_p_e_locallegislative_uri_240 = rail.PythonOperator(
            task_id='log_t_y_p_e_locallegislative_uri_240',
            python_callable=lambda:  get_cust_dropdown_uri("Local legislative")
        )

        update_dropdown_value_241 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_241',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_local_legislative_232').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_locallegislative_uri_240') }}"
            }
        )

        put_user2_state_administrative_242 = rail.RepliconServiceOperator(
            task_id='put_user2_state_administrative_242',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "sa",
                        "parameterCorrelationId": null
                    },
                    "firstname": "State administrative",
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
                        "loginName": rail.result('log_loginname_31') + "sa",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - State Administrative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_243 = rail.SetVariableOperator(
            task_id='insert_to_list_243',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_state_administrative_242').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_244 = rail.IfOperator(
            task_id='if_request_locationuri_present_244',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_245",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_246",
        )

        put_location_schedule_for_user_245 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_245',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_state_administrative_242').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_246 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_246',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_247",
            no_task="if_d_uri_present_248",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_247 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_247',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_state_administrative_242').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_248 = rail.IfOperator(
            task_id='if_d_uri_present_248',
            test='''{{ result('put_user2_state_administrative_242').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_249",
            no_task="log_t_y_p_e_stateadministrative_uri_250",
        )

        log_forlookuplogs_249 = rail.PythonOperator(
            task_id='log_forlookuplogs_249',
            python_callable=lambda:  '''SA user profile created successfully'''
        )

        log_t_y_p_e_stateadministrative_uri_250 = rail.PythonOperator(
            task_id='log_t_y_p_e_stateadministrative_uri_250',
            python_callable=lambda:  get_cust_dropdown_uri(
                "State administrative")
        )

        update_dropdown_value_251 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_251',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_state_administrative_242').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_stateadministrative_uri_250') }}"
            }
        )

        put_user2_statelegislative_252 = rail.RepliconServiceOperator(
            task_id='put_user2_statelegislative_252',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "sl",
                        "parameterCorrelationId": null
                    },
                    "firstname": "State legislative",
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
                        "loginName": rail.result('log_loginname_31') + "sl",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - State Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_253 = rail.SetVariableOperator(
            task_id='insert_to_list_253',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_statelegislative_252').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_254 = rail.IfOperator(
            task_id='if_request_locationuri_present_254',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_255",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_256",
        )

        put_location_schedule_for_user_255 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_255',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_statelegislative_252').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_256 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_256',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_257",
            no_task="if_d_uri_present_258",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_257 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_257',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_statelegislative_252').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_258 = rail.IfOperator(
            task_id='if_d_uri_present_258',
            test='''{{ result('put_user2_statelegislative_252').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_259",
            no_task="log_t_y_p_e_statelegislative_uri_260",
        )

        log_forlookuplogs_259 = rail.PythonOperator(
            task_id='log_forlookuplogs_259',
            python_callable=lambda:  '''SL user profile created successfully'''
        )

        log_t_y_p_e_statelegislative_uri_260 = rail.PythonOperator(
            task_id='log_t_y_p_e_statelegislative_uri_260',
            python_callable=lambda:  get_cust_dropdown_uri("State legislative")
        )

        update_dropdown_value_261 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_261',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_statelegislative_252').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_statelegislative_uri_260') }}"
            }
        )

        def get_log_lookup_263(delimeter):
            log_forlookuplogs = []
            log_forlookuplogs_219 = rail.result('log_forlookuplogs_219')
            if log_forlookuplogs_219:
                log_forlookuplogs.append(log_forlookuplogs_219)
            log_forlookuplogs_229 = rail.result('log_forlookuplogs_229')
            if log_forlookuplogs_229:
                log_forlookuplogs.append(log_forlookuplogs_229)
            log_forlookuplogs_239 = rail.result('log_forlookuplogs_239')
            if log_forlookuplogs_239:
                log_forlookuplogs.append(log_forlookuplogs_239)
            log_forlookuplogs_249 = rail.result('log_forlookuplogs_249')
            if log_forlookuplogs_249:
                log_forlookuplogs.append(log_forlookuplogs_249)
            log_forlookuplogs_259 = rail.result('log_forlookuplogs_259')
            if log_forlookuplogs_259:
                log_forlookuplogs.append(log_forlookuplogs_259)
            return rail.smartjoin_by_delim(log_forlookuplogs, delimeter, delimeter)

        log_splitandjoinedtoremoveextraspace_263 = rail.PythonOperator(
            task_id='log_splitandjoinedtoremoveextraspace_263',
            python_callable=lambda:  get_log_lookup_263('|')
        )

        if_log_splitandjoinedtoremoveextraspace_263_present_264 = rail.IfOperator(
            task_id='if_log_splitandjoinedtoremoveextraspace_263_present_264',
            test='''{{ result('log_splitandjoinedtoremoveextraspace_263') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_265",
            no_task="c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266",
        )

        nrdc_user_import_logs_add_entry_265 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_265',
            message="User Add",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Success",
                "details": "{{ result('log_splitandjoinedtoremoveextraspace_263') }}|{{ dag_run_ecid() }} ",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
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
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
            python_callable=lambda:  '''C4 secondary  user profile created successfully'''
        )

        put_user2_federal_legislative_274 = rail.RepliconServiceOperator(
            task_id='put_user2_federal_legislative_274',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "fl",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Federal Legislative",
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
                        "loginName": rail.result('log_loginname_31') + "fl",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Federal Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                "useruri": "{{ result('put_user2_federal_legislative_274').uri }}",
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
                "userUri": "{{ result('put_user2_federal_legislative_274').uri }}",
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
                "userUri": "{{ result('put_user2_federal_legislative_274').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_280 = rail.IfOperator(
            task_id='if_d_uri_present_280',
            test='''{{ result('put_user2_federal_legislative_274').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_281",
            no_task="log_t_y_p_e_federal_legislative_uri_282",
        )

        log_forlookuplogs_281 = rail.PythonOperator(
            task_id='log_forlookuplogs_281',
            python_callable=lambda:  '''FL user profile created successfully'''
        )

        log_t_y_p_e_federal_legislative_uri_282 = rail.PythonOperator(
            task_id='log_t_y_p_e_federal_legislative_uri_282',
            python_callable=lambda:  get_cust_dropdown_uri(
                "Federal Legislative")
        )

        update_dropdown_value_283 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_283',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_federal_legislative_274').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_federal_legislative_uri_282') }}"
            }
        )

        put_user2_local_administrative_284 = rail.RepliconServiceOperator(
            task_id='put_user2_local_administrative_284',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "la",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Local administrative",
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
                        "loginName": rail.result('log_loginname_31') + "la",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Local Administrative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_285 = rail.SetVariableOperator(
            task_id='insert_to_list_285',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_local_administrative_284').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_286 = rail.IfOperator(
            task_id='if_request_locationuri_present_286',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_287",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_288",
        )

        put_location_schedule_for_user_287 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_287',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_local_administrative_284').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_288 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_288',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_289",
            no_task="if_d_uri_present_290",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_289 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_289',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_local_administrative_284').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_290 = rail.IfOperator(
            task_id='if_d_uri_present_290',
            test='''{{ result('put_user2_local_administrative_284').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_291",
            no_task="log_t_y_p_e_localadministrative_uri_292",
        )

        log_forlookuplogs_291 = rail.PythonOperator(
            task_id='log_forlookuplogs_291',
            python_callable=lambda:  '''LA user profile created successfully'''
        )

        log_t_y_p_e_localadministrative_uri_292 = rail.PythonOperator(
            task_id='log_t_y_p_e_localadministrative_uri_292',
            python_callable=lambda:  get_cust_dropdown_uri(
                "Local administrative")
        )

        update_dropdown_value_293 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_293',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_local_administrative_284').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_localadministrative_uri_292') }}"
            }
        )

        put_user2_local_legislative_294 = rail.RepliconServiceOperator(
            task_id='put_user2_local_legislative_294',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "ll",
                        "parameterCorrelationId": null
                    },
                    "firstname": "Local legislative",
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
                        "loginName": rail.result('log_loginname_31') + "ll",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - Local Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_295 = rail.SetVariableOperator(
            task_id='insert_to_list_295',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_local_legislative_294').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_296 = rail.IfOperator(
            task_id='if_request_locationuri_present_296',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_297",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_298",
        )

        put_location_schedule_for_user_297 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_297',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_local_legislative_294').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_298 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_298',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_299",
            no_task="if_d_uri_present_300",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_299 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_299',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_local_legislative_294').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_300 = rail.IfOperator(
            task_id='if_d_uri_present_300',
            test='''{{ result('put_user2_local_legislative_294').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_301",
            no_task="log_t_y_p_e_locallegislative_uri_302",
        )

        log_forlookuplogs_301 = rail.PythonOperator(
            task_id='log_forlookuplogs_301',
            python_callable=lambda:  '''LL user profile created successfully'''
        )

        log_t_y_p_e_locallegislative_uri_302 = rail.PythonOperator(
            task_id='log_t_y_p_e_locallegislative_uri_302',
            python_callable=lambda:  get_cust_dropdown_uri("Local legislative")
        )

        update_dropdown_value_303 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_303',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_local_legislative_294').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_locallegislative_uri_302') }}"
            }
        )

        put_user2_state_administrative_304 = rail.RepliconServiceOperator(
            task_id='put_user2_state_administrative_304',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "sa",
                        "parameterCorrelationId": null
                    },
                    "firstname": "State administrative",
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
                        "loginName": rail.result('log_loginname_31') + "sa",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - State Administrative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_305 = rail.SetVariableOperator(
            task_id='insert_to_list_305',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_state_administrative_304').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_306 = rail.IfOperator(
            task_id='if_request_locationuri_present_306',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_307",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_308",
        )

        put_location_schedule_for_user_307 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_307',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_state_administrative_304').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_308 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_308',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_309",
            no_task="if_d_uri_present_310",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_309 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_309',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_state_administrative_304').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_310 = rail.IfOperator(
            task_id='if_d_uri_present_310',
            test='''{{ result('put_user2_state_administrative_304').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_311",
            no_task="log_t_y_p_e_stateadministrative_uri_312",
        )

        log_forlookuplogs_311 = rail.PythonOperator(
            task_id='log_forlookuplogs_311',
            python_callable=lambda:  '''SA user profile created successfully'''
        )

        log_t_y_p_e_stateadministrative_uri_312 = rail.PythonOperator(
            task_id='log_t_y_p_e_stateadministrative_uri_312',
            python_callable=lambda:  get_cust_dropdown_uri(
                "State administrative")
        )

        update_dropdown_value_313 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_313',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_state_administrative_304').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_stateadministrative_uri_312') }}"
            }
        )

        put_user2_statelegislative_314 = rail.RepliconServiceOperator(
            task_id='put_user2_statelegislative_314',
            endpoint="/services/importService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginname_31') + "sl",
                        "parameterCorrelationId": null
                    },
                    "firstname": "State legislative",
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
                        "loginName": rail.result('log_loginname_31') + "sl",
                        "password": "Replicon12"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": json.loads(json.dumps(rail.result('log_f_i_n_a_lpermissiontopass_23'))),
                    "policySets": [
                        {
                            "uri": null,
                            "name": "C3 - State Legislative"
                        }
                    ],
                    "employeeType": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":employee-type:1",
                        "name": null
                    },
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
                    "policyDataAccessScopes": json.loads(json.dumps(rail.result('log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22'))),
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        insert_to_list_315 = rail.SetVariableOperator(
            task_id='insert_to_list_315',
            append=True,
            name='{{ result("declare_list_51").name }}',
            value={
                "useruri": "{{ result('put_user2_statelegislative_314').uri }}",
                "subuseruri": "{{ result('put_user2_delegate_user_83').uri }}"
            }
        )

        if_request_locationuri_present_316 = rail.IfOperator(
            task_id='if_request_locationuri_present_316',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="put_location_schedule_for_user_317",
            no_task="if_log_checkif_zach_shankuserexists_c_r11_71_present_318",
        )

        put_location_schedule_for_user_317 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_317',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_statelegislative_314').uri }}",
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

        if_log_checkif_zach_shankuserexists_c_r11_71_present_318 = rail.IfOperator(
            task_id='if_log_checkif_zach_shankuserexists_c_r11_71_present_318',
            test='''{{ result('log_checkif_zach_shankuserexists_c_r11_71') | is_truthy  and result('log_checkif_zach_shankifitsenabled_72') | is_truthy }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_319",
            no_task="if_d_uri_present_320",
        )

        update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_319 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_319',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2_statelegislative_314').uri }}",
                "supervisorUri": "{{ result('log_checkif_zach_shankuserexists_c_r11_71') }}",
                "dateRange": null
            }
        )

        if_d_uri_present_320 = rail.IfOperator(
            task_id='if_d_uri_present_320',
            test='''{{ result('put_user2_statelegislative_314').uri | is_truthy }}''',
            yes_task="log_forlookuplogs_321",
            no_task="log_t_y_p_e_statelegislative_uri_322",
        )

        log_forlookuplogs_321 = rail.PythonOperator(
            task_id='log_forlookuplogs_321',
            python_callable=lambda:  '''SL user profile created successfully'''
        )

        log_t_y_p_e_statelegislative_uri_322 = rail.PythonOperator(
            task_id='log_t_y_p_e_statelegislative_uri_322',
            python_callable=lambda:  get_cust_dropdown_uri("State legislative")
        )

        update_dropdown_value_323 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_323',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_statelegislative_314').uri }}",
                "customFieldUri": "{{ result('log_u_d_f_uri_type_45') }}",
                "customFieldDropDownOptionUri": "{{ result('log_t_y_p_e_statelegislative_uri_322') }}"
            }
        )

        def get_log_lookup_324(delimeter):
            log_forlookuplogs = []
            log_forlookuplogs_273 = rail.result('log_forlookuplogs_273')
            if log_forlookuplogs_273:
                log_forlookuplogs.append(log_forlookuplogs_273)
            log_forlookuplogs_281 = rail.result('log_forlookuplogs_281')
            if log_forlookuplogs_281:
                log_forlookuplogs.append(log_forlookuplogs_281)
            log_forlookuplogs_291 = rail.result('log_forlookuplogs_291')
            if log_forlookuplogs_291:
                log_forlookuplogs.append(log_forlookuplogs_291)
            log_forlookuplogs_301 = rail.result('log_forlookuplogs_301')
            if log_forlookuplogs_301:
                log_forlookuplogs.append(log_forlookuplogs_301)
            log_forlookuplogs_311 = rail.result('log_forlookuplogs_311')
            if log_forlookuplogs_311:
                log_forlookuplogs.append(log_forlookuplogs_311)
            log_forlookuplogs_321 = rail.result('log_forlookuplogs_321')
            if log_forlookuplogs_321:
                log_forlookuplogs.append(log_forlookuplogs_321)
            return rail.smartjoin_by_delim(log_forlookuplogs, delimeter, delimeter)

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
                "details": "{{ result('log_splitandjoinedtoremoveextraspace_325') }}|{{ dag_run_ecid() }} ",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        # catch_328 = rail.EmptyOperator(
        #     task_id='catch_328',
        #     trigger_rule='one_failed',
        # )

        # nrdc_user_import_logs_add_entry_329 = rail.WriteLogOperator(
        #     task_id='nrdc_user_import_logs_add_entry_329',
        #     #log="{{ fixme result('create_log') }}",
        #     message="fixme get message from prop ",
        #     severity="fixme get severity from prop ",
        #     properties={
        #         "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
        #         "status": "Error",
        #         "details": "Error while creating the 1 or multiple user profiles. Error: #{_('data.catch.catch_328') }}|{{ dag_run_ecid() }} ",
        #         "action": "Add"
        #     }
        # )

        def has_subuser():
            subuser_info = rail.get_dag_run_var(
                rail.result('declare_list_51')['name'])
            return bool(subuser_info)

        if_declare_list_51_list_items_greater_than_0_330 = rail.IfOperator(
            task_id='if_declare_list_51_list_items_greater_than_0_330',
            test=has_subuser,
            yes_task="get_51_list_331",
            no_task="catch_338",
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
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
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

        catch_338 = rail.EmptyOperator(
            task_id='catch_338',
            trigger_rule='one_failed',
        )

        nrdc_user_import_logs_add_entry_339 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_339',
            message="User Add",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Error",
                "details": "Error while creating the 1 or multiple user profiles. Error: #{_('data.catch.catch_338') }}|{{ dag_run_ecid() }} ",
                "action": "Add",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_request_firstname_blank_3
        if_request_firstname_blank_3 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_4 >> stop_5 >> log_to_sumo
        if_request_firstname_blank_3 >> rail.Label(
            'No') >> if_request_emailaddress_blank_6
        if_request_emailaddress_blank_6 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_7 >> stop_8 >> log_to_sumo
        if_request_emailaddress_blank_6 >> rail.Label(
            'No') >> if_request_department_blank_9
        if_request_department_blank_9 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_10 >> stop_11 >> log_to_sumo
        if_request_department_blank_9 >> rail.Label(
            'No') >> if_request_logonname_blank_12
        if_request_logonname_blank_12 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_13 >> stop_14 >> log_to_sumo
        if_request_logonname_blank_12 >> rail.Label(
            'No') >> declare_list_15 >> insert_to_list_16 >> if_request_locationuri_present_17
        if_request_locationuri_present_17 >> rail.Label(
            'Yes') >> log_policydataaccessscopeforthepermission_18 >> insert_to_list_19 >> else_20 >> \
            log_policydataaccessscopeforthepermissionwithoutlocation_21 >> log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22
        if_request_locationuri_present_17 >> rail.Label(
            'No') >> log_f_i_n_a_l_p_o_l_i_c_y_restrictriontopass_22 >> log_f_i_n_a_lpermissiontopass_23 >> if_request_whencreated_not_contains_24
        if_request_whencreated_not_contains_24 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_25 >> stop_26 >> log_to_sumo
        if_request_whencreated_not_contains_24 >> rail.Label(
            'No') >> log_startdate_27 >> log_startday_28 >> log_startmonth_29 >> log_start_year_30 >> log_loginname_31 >> search_users_32 >> \
            log_presenceofexistingloginname_33 >> if_log_presenceofexistingloginname_33_present_34
        if_log_presenceofexistingloginname_33_present_34 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_35 >> stop_36 >> log_to_sumo
        if_log_presenceofexistingloginname_33_present_34 >> rail.Label(
            'No') >> get_enabled_departments_37 >> log_department_uri_38 >> if_log_department_uri_38_blank_39
        if_log_department_uri_38_blank_39 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_40 >> stop_41 >> log_to_sumo
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
            if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82
        if_log_checkif_zara_aktheruserexisits_c4supervisot_68_present_80 >> rail.Label(
            'No') >> if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82
        if_request_c4orc3present_contains_c4_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_73 >> rail.Label(
            'No') >> if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82
        if_request_c4orc3present_contains_delegate_c_r_e_a_t_e_s1_p_r_o_f_i_l_eprimaryprofile_82 >> rail.Label(
            'Yes') >> put_user2_delegate_user_83 >> updateing_s_s_o_i_d_84 >> if_request_locationuri_present_85
        if_request_locationuri_present_85 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_86 >> log_type_delegate_uri_87
        if_request_locationuri_present_85 >> rail.Label(
            'No') >> log_type_delegate_uri_87 >> update_dropdown_value_88 >> nrdc_user_import_logs_add_entry_89 >> \
            log_forlookuplogs_90 >> if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91
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
            if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101
        if_request_c4orc3present_equals_to_delegateand1_c_r_e_a_t_e_s1_p_r_o_f_i_l_esecondary_c4profile_91 >> rail.Label(
            'No') >> if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101
        if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101 >> rail.Label(
            'Yes') >> put_user2_federal_legislative_102 >> updateing_s_s_o_i_d_103 >> if_request_locationuri_present_104
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
            'Yes') >> log_forlookuplogs_109 >> log_type_federal_legislative_uri_110
        if_d_uri_present_108 >> rail.Label(
            'No') >> log_type_federal_legislative_uri_110 >> update_dropdown_value_111 >> \
            put_user2_local_administrative_112 >> insert_to_list_113 >> if_request_locationuri_present_114
        if_request_locationuri_present_114 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_115 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_116
        if_request_locationuri_present_114 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_116
        if_log_checkif_zach_shankuserexists_c_r11_71_present_116 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_117 >> \
            if_d_uri_present_118
        if_log_checkif_zach_shankuserexists_c_r11_71_present_116 >> rail.Label(
            'No') >> if_d_uri_present_118
        if_d_uri_present_118 >> rail.Label(
            'Yes') >> log_forlookuplogs_119 >> log_t_y_p_e_localadministrative_uri_120
        if_d_uri_present_118 >> rail.Label(
            'No') >> log_t_y_p_e_localadministrative_uri_120 >> update_dropdown_value_121 >> \
            put_user2_local_legislative_122 >> insert_to_list_123 >> if_request_locationuri_present_124
        if_request_locationuri_present_124 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_125 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_126
        if_request_locationuri_present_124 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_126
        if_log_checkif_zach_shankuserexists_c_r11_71_present_126 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_127 >> \
            if_d_uri_present_128
        if_log_checkif_zach_shankuserexists_c_r11_71_present_126 >> rail.Label(
            'No') >> if_d_uri_present_128
        if_d_uri_present_128 >> rail.Label(
            'Yes') >> log_forlookuplogs_129 >> log_t_y_p_e_locallegislative_uri_130
        if_d_uri_present_128 >> rail.Label(
            'No') >> log_t_y_p_e_locallegislative_uri_130 >> update_dropdown_value_131 >> \
            put_user2_state_administrative_132 >> insert_to_list_133 >> if_request_locationuri_present_134
        if_request_locationuri_present_134 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_135 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_136
        if_request_locationuri_present_134 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_136
        if_log_checkif_zach_shankuserexists_c_r11_71_present_136 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankasthesupervisor_137 >> \
            if_d_uri_present_138
        if_log_checkif_zach_shankuserexists_c_r11_71_present_136 >> rail.Label(
            'No') >> if_d_uri_present_138
        if_d_uri_present_138 >> rail.Label(
            'Yes') >> log_forlookuplogs_139 >> log_t_y_p_e_stateadministrative_uri_140
        if_d_uri_present_138 >> rail.Label(
            'No') >> log_t_y_p_e_stateadministrative_uri_140 >> update_dropdown_value_141 >> \
            put_user2_statelegislative_142 >> insert_to_list_143 >> if_request_locationuri_present_144
        if_request_locationuri_present_144 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_145 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_146
        if_request_locationuri_present_144 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_146
        if_log_checkif_zach_shankuserexists_c_r11_71_present_146 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedassupervisor_147 >> \
            if_d_uri_present_148
        if_log_checkif_zach_shankuserexists_c_r11_71_present_146 >> rail.Label(
            'No') >> if_d_uri_present_148
        if_d_uri_present_148 >> rail.Label(
            'Yes') >> log_forlookuplogs_149 >> log_t_y_p_e_statelegislative_uri_150
        if_d_uri_present_148 >> rail.Label(
            'No') >> log_t_y_p_e_statelegislative_uri_150 >> update_dropdown_value_151 >> \
            log_log_153 >> if_log_log_153_present_154
        if_log_log_153_present_154 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_155 >> c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156
        if_log_log_153_present_154 >> rail.Label(
            'No') >> c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156
        if_request_c4orc3present_equals_to_c3only_c_r_e_a_t_e_s5_p_r_o_f_i_l_e_s_c3primaryprofile_101 >> rail.Label(
            'No') >> c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156
        c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156 >> rail.Label(
            'Yes') >> put_user2_federal_legislative_157 >> insert_to_list_158 >> if_request_locationuri_present_159
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
            'Yes') >> log_forlookuplogs_164 >> log_t_y_p_e_federal_legislative_uri_165
        if_d_uri_present_163 >> rail.Label(
            'No') >> log_t_y_p_e_federal_legislative_uri_165 >> update_dropdown_value_166 >> put_user2_local_administrative_167 >> \
            insert_to_list_168 >> if_request_locationuri_present_169
        if_request_locationuri_present_169 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_170 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_171
        if_request_locationuri_present_169 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_171
        if_log_checkif_zach_shankuserexists_c_r11_71_present_171 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_172 >> \
            if_d_uri_present_173
        if_log_checkif_zach_shankuserexists_c_r11_71_present_171 >> rail.Label(
            'No') >> if_d_uri_present_173
        if_d_uri_present_173 >> rail.Label(
            'Yes') >> log_forlookuplogs_174 >> log_t_y_p_e_localadministrative_uri_175
        if_d_uri_present_173 >> rail.Label(
            'No') >> log_t_y_p_e_localadministrative_uri_175 >> update_dropdown_value_176 >> \
            put_user2_local_legislative_177 >> insert_to_list_178 >> if_request_locationuri_present_179
        if_request_locationuri_present_179 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_180 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_181
        if_request_locationuri_present_179 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_181
        if_log_checkif_zach_shankuserexists_c_r11_71_present_181 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_182 >> \
            if_d_uri_present_183
        if_log_checkif_zach_shankuserexists_c_r11_71_present_181 >> rail.Label(
            'No') >> if_d_uri_present_183
        if_d_uri_present_183 >> rail.Label(
            'Yes') >> log_forlookuplogs_184 >> log_t_y_p_e_locallegislative_uri_185
        if_d_uri_present_183 >> rail.Label(
            'No') >> log_t_y_p_e_locallegislative_uri_185 >> update_dropdown_value_186 >> \
            put_user2_state_administrative_187 >> insert_to_list_188 >> if_request_locationuri_present_189
        if_request_locationuri_present_189 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_190 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_191
        if_request_locationuri_present_189 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_191
        if_log_checkif_zach_shankuserexists_c_r11_71_present_191 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_192 >> \
            if_d_uri_present_193
        if_log_checkif_zach_shankuserexists_c_r11_71_present_191 >> rail.Label(
            'No') >> if_d_uri_present_193
        if_d_uri_present_193 >> rail.Label(
            'Yes') >> log_forlookuplogs_194 >> log_t_y_p_e_stateadministrative_uri_195
        if_d_uri_present_193 >> rail.Label(
            'No') >> log_t_y_p_e_stateadministrative_uri_195 >> update_dropdown_value_196 >> \
            put_user2_statelegislative_197 >> insert_to_list_198 >> if_request_locationuri_present_199
        if_request_locationuri_present_199 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_200 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_201
        if_request_locationuri_present_199 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_201
        if_log_checkif_zach_shankuserexists_c_r11_71_present_201 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_202 >> \
            if_d_uri_present_203
        if_log_checkif_zach_shankuserexists_c_r11_71_present_201 >> rail.Label(
            'No') >> if_d_uri_present_203
        if_d_uri_present_203 >> rail.Label(
            'Yes') >> log_forlookuplogs_204 >> log_t_y_p_e_statelegislative_uri_205
        if_d_uri_present_203 >> rail.Label(
            'No') >> log_t_y_p_e_statelegislative_uri_205 >> update_dropdown_value_206 >> \
            log_splitandjoinedtoremoveextraspace_208 >> if_log_splitandjoinedtoremoveextraspace_208_present_209
        if_log_splitandjoinedtoremoveextraspace_208_present_209 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_210 >> c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211
        if_log_splitandjoinedtoremoveextraspace_208_present_209 >> rail.Label(
            'No') >> c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211
        c4orc3present_c4andc3_creates5_c3profile_ssecondary_c3profileswhen_c3and_c4_156 >> rail.Label(
            'No') >> c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211
        c4orc3_equals_to_c3anddelegate_creates5c3profilescrea_c3and_delegate_211 >> rail.Label(
            'Yes') >> put_user2_federal_legislative_212 >> insert_to_list_213 >> if_request_locationuri_present_214
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
            'Yes') >> log_forlookuplogs_219 >> log_t_y_p_e_federal_legislative_uri_220
        if_d_uri_present_218 >> rail.Label(
            'No') >> log_t_y_p_e_federal_legislative_uri_220 >> update_dropdown_value_221 >> \
            put_user2_local_administrative_222 >> insert_to_list_223 >> if_request_locationuri_present_224
        if_request_locationuri_present_224 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_225 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_226
        if_request_locationuri_present_224 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_226
        if_log_checkif_zach_shankuserexists_c_r11_71_present_226 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_227 >> \
            if_d_uri_present_228
        if_log_checkif_zach_shankuserexists_c_r11_71_present_226 >> rail.Label(
            'No') >> if_d_uri_present_228
        if_d_uri_present_228 >> rail.Label(
            'Yes') >> log_forlookuplogs_229 >> log_t_y_p_e_localadministrative_uri_230
        if_d_uri_present_228 >> rail.Label(
            'No') >> log_t_y_p_e_localadministrative_uri_230 >> update_dropdown_value_231 >> \
            put_user2_local_legislative_232 >> insert_to_list_233 >> if_request_locationuri_present_234
        if_request_locationuri_present_234 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_235 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_236
        if_request_locationuri_present_234 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_236
        if_log_checkif_zach_shankuserexists_c_r11_71_present_236 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_237 >> \
            if_d_uri_present_238
        if_log_checkif_zach_shankuserexists_c_r11_71_present_236 >> rail.Label(
            'No') >> if_d_uri_present_238
        if_d_uri_present_238 >> rail.Label(
            'Yes') >> log_forlookuplogs_239 >> \
            log_t_y_p_e_locallegislative_uri_240
        if_d_uri_present_238 >> rail.Label(
            'No') >> log_t_y_p_e_locallegislative_uri_240 >> update_dropdown_value_241 >> put_user2_state_administrative_242 >> \
            insert_to_list_243 >> if_request_locationuri_present_244
        if_request_locationuri_present_244 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_245 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_246
        if_request_locationuri_present_244 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_246
        if_log_checkif_zach_shankuserexists_c_r11_71_present_246 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_247 >> \
            if_d_uri_present_248
        if_log_checkif_zach_shankuserexists_c_r11_71_present_246 >> rail.Label(
            'No') >> if_d_uri_present_248
        if_d_uri_present_248 >> rail.Label(
            'Yes') >> log_forlookuplogs_249 >> log_t_y_p_e_stateadministrative_uri_250
        if_d_uri_present_248 >> rail.Label(
            'No') >> log_t_y_p_e_stateadministrative_uri_250 >> update_dropdown_value_251 >> \
            put_user2_statelegislative_252 >> insert_to_list_253 >> if_request_locationuri_present_254
        if_request_locationuri_present_254 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_255 >> \
            if_log_checkif_zach_shankuserexists_c_r11_71_present_256
        if_request_locationuri_present_254 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_256
        if_log_checkif_zach_shankuserexists_c_r11_71_present_256 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_257 >> \
            if_d_uri_present_258
        if_log_checkif_zach_shankuserexists_c_r11_71_present_256 >> rail.Label(
            'No') >> if_d_uri_present_258
        if_d_uri_present_258 >> rail.Label(
            'Yes') >> log_forlookuplogs_259 >> log_t_y_p_e_statelegislative_uri_260
        if_d_uri_present_258 >> rail.Label(
            'No') >> log_t_y_p_e_statelegislative_uri_260 >> update_dropdown_value_261 >> log_splitandjoinedtoremoveextraspace_263 >> \
            if_log_splitandjoinedtoremoveextraspace_263_present_264
        if_log_splitandjoinedtoremoveextraspace_263_present_264 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_265 >> \
            c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266
        if_log_splitandjoinedtoremoveextraspace_263_present_264 >> rail.Label(
            'No') >> c4orc3present_equals_delegateandall_6_seconprofilesc3andc4whenc3c4anddelegate_266
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
            'No') >> log_forlookuplogs_273 >> put_user2_federal_legislative_274 >> insert_to_list_275 >> if_request_locationuri_present_276
        if_request_locationuri_present_276 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_277 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_278
        if_request_locationuri_present_276 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_278
        if_log_checkif_zach_shankuserexists_c_r11_71_present_278 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_279 >> if_d_uri_present_280
        if_log_checkif_zach_shankuserexists_c_r11_71_present_278 >> rail.Label(
            'No') >> if_d_uri_present_280
        if_d_uri_present_280 >> rail.Label(
            'Yes') >> log_forlookuplogs_281 >> log_t_y_p_e_federal_legislative_uri_282
        if_d_uri_present_280 >> rail.Label(
            'No') >> log_t_y_p_e_federal_legislative_uri_282 >> update_dropdown_value_283 >> \
            put_user2_local_administrative_284 >> insert_to_list_285 >> if_request_locationuri_present_286
        if_request_locationuri_present_286 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_287 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_288
        if_request_locationuri_present_286 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_288
        if_log_checkif_zach_shankuserexists_c_r11_71_present_288 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_289 >> if_d_uri_present_290
        if_log_checkif_zach_shankuserexists_c_r11_71_present_288 >> rail.Label(
            'No') >> if_d_uri_present_290
        if_d_uri_present_290 >> rail.Label(
            'Yes') >> log_forlookuplogs_291 >> log_t_y_p_e_localadministrative_uri_292
        if_d_uri_present_290 >> rail.Label(
            'No') >> log_t_y_p_e_localadministrative_uri_292 >> update_dropdown_value_293 >> \
            put_user2_local_legislative_294 >> insert_to_list_295 >> if_request_locationuri_present_296
        if_request_locationuri_present_296 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_297 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_298
        if_request_locationuri_present_296 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_298
        if_log_checkif_zach_shankuserexists_c_r11_71_present_298 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_299 >> if_d_uri_present_300
        if_log_checkif_zach_shankuserexists_c_r11_71_present_298 >> rail.Label(
            'No') >> if_d_uri_present_300
        if_d_uri_present_300 >> rail.Label(
            'Yes') >> log_forlookuplogs_301 >> log_t_y_p_e_locallegislative_uri_302
        if_d_uri_present_300 >> rail.Label(
            'No') >> log_t_y_p_e_locallegislative_uri_302 >> update_dropdown_value_303 >> \
            put_user2_state_administrative_304 >> insert_to_list_305 >> if_request_locationuri_present_306
        if_request_locationuri_present_306 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_307 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_308
        if_request_locationuri_present_306 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_308
        if_log_checkif_zach_shankuserexists_c_r11_71_present_308 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_309 >> if_d_uri_present_310
        if_log_checkif_zach_shankuserexists_c_r11_71_present_308 >> rail.Label(
            'No') >> if_d_uri_present_310
        if_d_uri_present_310 >> rail.Label(
            'Yes') >> log_forlookuplogs_311 >> log_t_y_p_e_stateadministrative_uri_312
        if_d_uri_present_310 >> rail.Label(
            'No') >> log_t_y_p_e_stateadministrative_uri_312 >> update_dropdown_value_313 >> \
            put_user2_statelegislative_314 >> insert_to_list_315 >> if_request_locationuri_present_316
        if_request_locationuri_present_316 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_317 >> if_log_checkif_zach_shankuserexists_c_r11_71_present_318
        if_request_locationuri_present_316 >> rail.Label(
            'No') >> if_log_checkif_zach_shankuserexists_c_r11_71_present_318
        if_log_checkif_zach_shankuserexists_c_r11_71_present_318 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_zach_shankassignedasthesupervisor_319 >> if_d_uri_present_320
        if_log_checkif_zach_shankuserexists_c_r11_71_present_318 >> rail.Label(
            'No') >> if_d_uri_present_320
        if_d_uri_present_320 >> rail.Label(
            'Yes') >> log_forlookuplogs_321 >> log_t_y_p_e_statelegislative_uri_322
        if_d_uri_present_320 >> rail.Label(
            'No') >> log_t_y_p_e_statelegislative_uri_322 >> update_dropdown_value_323 >> \
            log_splitandjoinedtoremoveextraspace_325 >> if_log_splitandjoinedtoremoveextraspace_325_present_326
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
            catch_338 >> nrdc_user_import_logs_add_entry_339 >> log_to_sumo
        if_declare_list_51_list_items_greater_than_0_330 >> rail.Label(
            'No') >> catch_338

    return dag


rail.for_each_instance(create_dag)
