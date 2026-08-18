
from datetime import timedelta, datetime
import itertools
from ge.user_sync_netherlands.netherlands_master_mapper import netherlands_master_mapper
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_netherlands_child_add_supervisor_v1_0_{config.instance}',
        description=f'GE_netherlands_Child_Add Supervisor V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
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
            no_task='search_users_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_3',
            end_task='ey_user_import_logs_update_entry_47',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
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
                'useruri': row['cells'][1]['uri'],
                'employeetype': row['cells'][4]['textValue'] if 'textValue' in row['cells'][4] else None,
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_3 = rail.RepliconServicePageOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled',
                    'urn:replicon:user-list-column:employee-type'
                ],
                "sort": [],
                "filterExpression": {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisorloginname'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['supervisorloginname'])
        )

        if_log_getsupervisor_uri_4_blank_5 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_blank_5',
            test='''{{ result('search_users_3') | is_falsy }}''',
            yes_task="stop_6",
            no_task="if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7",
        )

        stop_6 = rail.FailOperator(
            task_id='stop_6',
            message='''Supervisor "{{ dag_run.conf.supervisorloginname }}" not available'''
        )

        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7 = rail.IfOperator(
            task_id='if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="if_log_getsupervisor_status_8_equals_to_false_9",
            no_task="log_errorfor_supervisorand_userslogin_nameissame_39"

        )

        if_log_getsupervisor_status_8_equals_to_false_9 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_8_equals_to_false_9',
            test='''{{ result('search_users_3').status == 'False' }}''',
            yes_task="if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11",
            no_task="if_log_getsupervisor_status_8_equals_to_true_15",
        )

        if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11 = rail.IfOperator(
            task_id='if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11',
            test='''{{ result('search_users_3').employeetype | lower != 'foreign supervisor' }}''',
            yes_task="log_errorwhensupervisorisdisabled_12",
            no_task="_adhoc_http_action_14",
        )

        log_errorwhensupervisorisdisabled_12 = rail.PythonOperator(
            task_id='log_errorwhensupervisorisdisabled_12',
            python_callable=lambda:  rail.render_template(
                '''Supervsior assignment/update is not done as supervsior with login name "{{ dag_run.conf.supervisorloginname }}" is disabled''')
        )

        _adhoc_http_action_14 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_14',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('search_users_3').useruri }}"
            }
        )

        if_log_getsupervisor_status_8_equals_to_true_15 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_8_equals_to_true_15',
            test='''{{ result('search_users_3').status == 'True' or result('search_users_3').employeetype == 'Foreign Supervisor' }}''',
            yes_task="log_supervisor_permissiontobeassigned_17",
            no_task="ge_supervisor_assignment_table_update_entry_48",
        )

        def get_mapper_permissions(mapper_type, identifier_1):
            employee_permissions = list(filter(
                lambda x: x['legacy_payroll_id'] == mapper_type
                and x['legal_entity'] == identifier_1, netherlands_master_mapper))
            permissions = []
            for permission in employee_permissions:
                if permission['value'] not in permissions:
                    permissions.append(permission['value'])
            return rail.smartjoin_by_delim(permissions, ',')

        log_supervisor_permissiontobeassigned_17 = rail.PythonOperator(
            task_id='log_supervisor_permissiontobeassigned_17',
            python_callable=lambda: get_mapper_permissions(
                'Permission', 'Supervisor')
        )

        _adhoc_http_action_18 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_18',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_3').useruri }}"
            }
        )

        def get_supervision_permission(permission_uri):
            permissionset = rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_18'), 'policyUri', permission_uri, 'permissionSet')
            return permissionset['name'] if permissionset else None

        log_checkifthe_supervisor_permissionisassigned_19 = rail.PythonOperator(
            task_id='log_checkifthe_supervisor_permissionisassigned_19',
            python_callable=lambda: get_supervision_permission(
                'urn:replicon:policy:supervision')
        )

        log_checkifthe_user_permissionisassigned_20 = rail.PythonOperator(
            task_id='log_checkifthe_user_permissionisassigned_20',
            python_callable=lambda: get_supervision_permission(
                'urn:replicon:policy:user')
        )

        def is_permission_present():
            if rail.result('log_checkifthe_supervisor_permissionisassigned_19') is None or \
                    rail.result('log_checkifthe_user_permissionisassigned_20') is None or \
                rail.result('log_checkifthe_supervisor_permissionisassigned_19') not in rail.result('log_supervisor_permissiontobeassigned_17') \
                    or rail.result('log_checkifthe_user_permissionisassigned_20') not in rail.result('log_supervisor_permissiontobeassigned_17'):
                return True
            return False

        if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21 = rail.IfOperator(
            task_id='if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21',
            test=is_permission_present,
            yes_task="_adhoc_http_action_22",
            no_task="if_action_downcase_equals_to_add_30",
        )

        _adhoc_http_action_22 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_22',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        log_permission_namestobeassigned_23 = rail.PythonOperator(
            task_id='log_permission_namestobeassigned_23',
            python_callable=lambda:  rail.result(
                'log_supervisor_permissiontobeassigned_17').split(",")
        )

        create_list_24 = rail.EmptyOperator(
            task_id='create_list_24',
        )

        foreach_create_list_24_25 = rail.ForEachOperator(
            task_id='foreach_create_list_24_25',
            items=lambda: rail.result(
                'log_supervisor_permissiontobeassigned_17').split(","),
            start_task='log_permission_uri_27',
            end_task='foreach_create_list_24_25_end'
        )

        log_permission_uri_27 = rail.PythonOperator(
            task_id='log_permission_uri_27',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_22'), 'name', rail.result('foreach_create_list_24_25'), 'uri')
        )

        assign_permission_set_to_user_28 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_28',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_3').useruri }}",
                "permissionSetUri": "{{ result('log_permission_uri_27') }}"
            }
        )

        foreach_create_list_24_25_end = rail.EmptyOperator(
            task_id='foreach_create_list_24_25_end',
        )

        if_action_downcase_equals_to_add_30 = rail.IfOperator(
            task_id='if_action_downcase_equals_to_add_30',
            test='''{{ dag_run.conf.action | lower =='add' }}''',
            yes_task="update_initial_supervisor_31",
            no_task="if_action_downcase_equals_to_update_32",
        )

        update_initial_supervisor_31 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_31',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_users_3').useruri }}",
                "scheduleEntries": []
            }
        )

        if_action_downcase_equals_to_update_32 = rail.IfOperator(
            task_id='if_action_downcase_equals_to_update_32',
            test='''{{ dag_run.conf.action | lower =='update' }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_34",
            no_task="ey_user_import_logs_search_entries_40",
        )

        update_supervisor_assignment_schedule_over_date_range_34 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_34',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_users_3')['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['supervisoreffectivedate'], '%d/%m/%Y').year,
                        "month": datetime.strptime(dag_run.conf['supervisoreffectivedate'], '%d/%m/%Y').month,
                        "day": datetime.strptime(dag_run.conf['supervisoreffectivedate'], '%d/%m/%Y').day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_errorfor_supervisorand_userslogin_nameissame_39 = rail.PythonOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame_39',
            python_callable=lambda:  "Supervisor not assigned since the user and supervisor SSO ID are same"
        )

        ey_user_import_logs_search_entries_40 = rail.PythonOperator(
            task_id='ey_user_import_logs_search_entries_40',
            python_callable=lambda:  "true"
        )

        if_entry_col5_present_41 = rail.IfOperator(
            task_id='if_entry_col5_present_41',
            test='''{{ result('ey_user_import_logs_search_entries_40') | is_truthy }}''',
            yes_task="ey_user_import_logs_update_entry_42",
            no_task="ge_supervisor_assignment_table_update_entry_43",
        )

        def get_status():
            status = "Skipped"
            if rail.result('log_errorfor_supervisorand_userslogin_nameissame_39') or rail.result('log_errorwhensupervisorisdisabled_12'):
                status = "Exception"
            if rail.result('log_supervisor_permissiontobeassigned_17'):
                status = "Success"
            return status

        def get_details():
            details = [
                rail.result(
                    'log_errorfor_supervisorand_userslogin_nameissame_39'),
                rail.result('log_errorwhensupervisorisdisabled_12')
            ]
            return rail.smartjoin_by_delim(details, ';')

        ey_user_import_logs_update_entry_42 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_update_entry_42',
            message="na",
            severity="AddUpdate",
            properties=lambda dag_run: {
                "action": "Update",
                "status": get_status(),
                "child_job_id": get_dagrun_ecid(dag_run),
                "details": get_details(),
                "OHRID": dag_run.conf['loginname'],
                "username": dag_run.conf['loginname']
            }
        )

        ge_supervisor_assignment_table_update_entry_43 = rail.EmptyOperator(
            task_id='ge_supervisor_assignment_table_update_entry_43',
        )

        ey_user_import_logs_update_entry_47 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_update_entry_47',
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "action": "Update",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.loginname }}",
                "username": "{{ dag_run.conf.loginname }}"
            }
        )

        ge_supervisor_assignment_table_update_entry_48 = rail.EmptyOperator(
            task_id='ge_supervisor_assignment_table_update_entry_48',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> ey_user_import_logs_update_entry_47
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> if_log_getsupervisor_uri_4_blank_5
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'Yes') >> stop_6 >> if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'No') >> if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7
        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7 >> rail.Label(
            'No') >> log_errorfor_supervisorand_userslogin_nameissame_39 >> if_entry_col5_present_41
        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7 >> rail.Label(
            'Yes') >> if_log_getsupervisor_status_8_equals_to_false_9
        if_log_getsupervisor_status_8_equals_to_false_9 >> rail.Label(
            'Yes') >> if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11
        if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11 >> rail.Label(
            'Yes') >> log_errorwhensupervisorisdisabled_12 >> if_log_getsupervisor_status_8_equals_to_true_15
        if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11 >> rail.Label(
            'No') >> _adhoc_http_action_14 >> if_log_getsupervisor_status_8_equals_to_true_15
        if_log_getsupervisor_status_8_equals_to_false_9 >> rail.Label(
            'No') >> if_log_getsupervisor_status_8_equals_to_true_15
        if_log_getsupervisor_status_8_equals_to_true_15 >> rail.Label(
            'No') >> ge_supervisor_assignment_table_update_entry_48
        if_log_getsupervisor_status_8_equals_to_true_15 >> rail.Label(
            'Yes') >> log_supervisor_permissiontobeassigned_17 >> _adhoc_http_action_18 >> \
            log_checkifthe_supervisor_permissionisassigned_19 >> log_checkifthe_user_permissionisassigned_20 >> \
            if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21
        if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21 >> rail.Label(
            'Yes') >> _adhoc_http_action_22 >> log_permission_namestobeassigned_23 >> create_list_24 >> \
            foreach_create_list_24_25 >> log_permission_uri_27 >> assign_permission_set_to_user_28 >> foreach_create_list_24_25_end
        foreach_create_list_24_25 >> foreach_create_list_24_25_end >> if_action_downcase_equals_to_add_30
        if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21 >> rail.Label(
            'No') >> if_action_downcase_equals_to_add_30
        if_action_downcase_equals_to_add_30 >> rail.Label(
            'Yes') >> update_initial_supervisor_31 >> if_action_downcase_equals_to_update_32
        if_action_downcase_equals_to_add_30 >> rail.Label(
            'No') >> if_action_downcase_equals_to_update_32
        if_action_downcase_equals_to_update_32 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_34 >> ey_user_import_logs_search_entries_40
        if_action_downcase_equals_to_update_32 >> rail.Label(
            'No') >> ey_user_import_logs_search_entries_40 >> if_entry_col5_present_41
        if_entry_col5_present_41 >> rail.Label(
            'Yes') >> ey_user_import_logs_update_entry_42 >> ge_supervisor_assignment_table_update_entry_43
        if_entry_col5_present_41 >> rail.Label(
            'No') >> ge_supervisor_assignment_table_update_entry_43 >> ey_user_import_logs_update_entry_47 >> \
            ge_supervisor_assignment_table_update_entry_48 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
