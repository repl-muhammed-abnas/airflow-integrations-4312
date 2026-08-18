from datetime import timedelta
from airflow.models import Variable
import itertools
from ge.user_sync_poland.utils import request_payload, custom_methods
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_supervisor_dag_id,
        description=f'GE POLAND User Import Add Supervisor Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='filter_user_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='filter_user_logs',
            end_task='on_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        filter_user_logs = rail.FilterLogEntriesOperator(
            task_id='filter_user_logs',
            log='{{ dag_run.conf.user_log }}',
            properties={
                "OHRID": '{{ dag_run.conf.loginname }}'
            },
            remove_filtered_entries=True
        )

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
            page_handler=custom_methods.page_handler,
            all_result_data_handler=lambda response, dag_run: custom_methods.compose_user_details(
                response, dag_run.conf['supervisorloginname'])
        )

        if_log_getsupervisor_uri_4_blank_5 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_blank_5',
            test='''{{ result('search_users_3') | is_falsy }}''',
            yes_task="dummy_filter_user_logs",
            no_task="if_supervisorloginname_not_equals_userloginname_7",
        )

        if_supervisorloginname_not_equals_userloginname_7 = rail.IfOperator(
            task_id='if_supervisorloginname_not_equals_userloginname_7',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="if_log_getsupervisor_status_equals_to_false_9",
            no_task="dummy_filter_user_logs"
        )

        if_log_getsupervisor_status_equals_to_false_9 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_equals_to_false_9',
            test='''{{ result('search_users_3').status == 'False' }}''',
            yes_task="if_log_getsupervisor_employee_type_not_equals_to_foreignsupervisor_11",
            no_task="if_log_getsupervisor_status_8_equals_to_true_15",
        )

        if_log_getsupervisor_employee_type_not_equals_to_foreignsupervisor_11 = rail.IfOperator(
            task_id='if_log_getsupervisor_employee_type_not_equals_to_foreignsupervisor_11',
            test='''{{ result('search_users_3').employeetype | lower != 'foreign supervisor' }}''',
            yes_task="log_errorwhensupervisorisdisabled_12",
            no_task="enable_login_foreign_supervisor_14",
        )

        log_errorwhensupervisorisdisabled_12 = rail.PythonOperator(
            task_id='log_errorwhensupervisorisdisabled_12',
            python_callable=lambda:  rail.render_template(
                '''Supervsior assignment/update is not done as supervsior with login name "{{ dag_run.conf.supervisorloginname }}" is disabled''')
        )

        enable_login_foreign_supervisor_14 = rail.RepliconServiceOperator(
            task_id='enable_login_foreign_supervisor_14',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('search_users_3').useruri }}"
            }
        )

        if_log_getsupervisor_status_8_equals_to_true_15 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_8_equals_to_true_15',
            test='''{{ result('search_users_3').status == 'True' or result('search_users_3').employeetype == 'Foreign Supervisor' }}''',
            yes_task="log_supervisor_permission_to_be_assigned_17",
            no_task="dummy_filter_user_logs",
        )

        def get_mapper_permissions(entry_type, identifier_1, master_mapper):
            employee_permissions = list(filter(
                lambda x: x['type'] == entry_type
                and x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == identifier_1, master_mapper))
            permissions = []
            for permission in employee_permissions:
                if permission['value'] not in permissions:
                    permissions.append(permission['value'])
            return rail.smartjoin_by_delim(permissions, ',')

        log_supervisor_permission_to_be_assigned_17 = rail.PythonOperator(
            task_id='log_supervisor_permission_to_be_assigned_17',
            python_callable=lambda: get_mapper_permissions(
                'Permission', 'Supervisor', config.POLAND_MASTER_MAPPER)
        )

        get_assigned_premissions_for_supervisor_18 = rail.RepliconServiceOperator(
            task_id='get_assigned_premissions_for_supervisor_18',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_3').useruri }}"
            }
        )

        def get_supervision_permission(permission_uri):
            permissionset = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_premissions_for_supervisor_18'), 'policyUri', permission_uri, 'permissionSet')
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
                rail.result('log_checkifthe_supervisor_permissionisassigned_19') not in rail.result('log_supervisor_permission_to_be_assigned_17') \
                    or rail.result('log_checkifthe_user_permissionisassigned_20') not in rail.result('log_supervisor_permission_to_be_assigned_17'):
                return True
            return False

        if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21 = rail.IfOperator(
            task_id='if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21',
            test=is_permission_present,
            yes_task="get_required_permission_uris_list_22",
            no_task="if_action_downcase_equals_to_add_30",
        )

        get_required_permission_uris_list_22 = rail.RepliconServiceOperator(
            task_id='get_required_permission_uris_list_22',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda res: [rail.find_first_by_attr_and_get_attr(res, 'name', permission, 'uri', '') for permission in rail.result(
                'log_supervisor_permission_to_be_assigned_17').split(",") if rail.find_first_by_attr_and_get_attr(res, 'name', permission, 'uri', '')]
        )

        add_missing_supervisor_permissions_28 = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permissions_28',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data=lambda: {
                "user": {
                    "uri": rail.result('search_users_3')['useruri']
                },
                "modifications": {
                    "permissionSetsToApply": {
                        "permissionSetUrisToAssign": rail.result('get_required_permission_uris_list_22'),
                        "policyUrisToRemovePermissionSet": []
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
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
            no_task="dummy_filter_user_logs"
        )

        update_supervisor_assignment_schedule_over_date_range_34 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_34',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_users_3')['useruri'],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf['supervisoreffectivedate'], config.DATE_DEFAULT_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        dummy_filter_user_logs = rail.EmptyOperator(
            task_id="dummy_filter_user_logs",
        )

        is_filtered_userlogs = rail.IfOperator(
            task_id='is_filtered_userlogs',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries',
            no_task='on_error'
        )

        update_userlog_entries = rail.WriteLogOperator(
            task_id='update_userlog_entries',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            items="{{ result('filter_user_logs') }}",
            properties=lambda item, dag_run: {
                "OHRID": item['properties']['OHRID'],
                "action": item['properties']['action'],
                "status": custom_methods.get_supervisor_status(item['properties']['status'], dag_run),
                "details": custom_methods.get_log_details_post_supervisor_assignment(item['properties']['details'], dag_run),
                "username": item['properties']['username']
            }
        )

        update_supervisor_assignment_logs = rail.WriteLogOperator(
            task_id='update_supervisor_assignment_logs',
            message='post_processing_entry',
            log='{{ dag_run.conf.supervisor_log }}',
            severity='Completed',
            properties=lambda dag_run: {
                "username": dag_run.conf['loginname'],
                'useruri': dag_run.conf['useruri'],
                'supervisorloginname': dag_run.conf['supervisorloginname'],
                'action': dag_run.conf['action'],
                'status': 'completed',
                'supervisoreffectivedate': dag_run.conf['supervisoreffectivedate'],
                'supervisorusername': dag_run.conf['supervisorusername'],
                'user_log': dag_run.conf['user_log']
            },
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        is_entries_present_error = rail.IfOperator(
            task_id='is_entries_present_error',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries_error',
        )

        update_userlog_entries_error = rail.WriteLogOperator(
            task_id='update_userlog_entries_error',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            severity='Error',
            items="{{ result('filter_user_logs') }}",
            properties=lambda item: {
                "OHRID": item['properties']['OHRID'],
                "action": item['properties']['action'],
                "status": 'Error',
                "details": item['properties']['details'] + rail.render_template(";{{get_error_message()}}"),
                "username": item['properties']['username']
            }
        )

        update_supervisor_assignment_error_logs = rail.WriteLogOperator(
            task_id='update_supervisor_assignment_error_logs',
            message='post_processing_error_entry',
            log='{{ dag_run.conf.supervisor_log }}',
            severity='Error',
            properties=lambda dag_run: {
                "username": dag_run.conf['loginname'],
                'useruri': dag_run.conf['useruri'],
                'supervisorloginname': dag_run.conf['supervisorloginname'],
                'action': dag_run.conf['action'],
                'status': 'Error',
                'supervisoreffectivedate': dag_run.conf['supervisoreffectivedate'],
                'supervisorusername': dag_run.conf['supervisorusername'],
                'user_log': dag_run.conf['user_log']
            },
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> on_error
        can_run_batch_task >> rail.Label(
            'No') >> filter_user_logs >> search_users_3
        search_users_3 >> if_log_getsupervisor_uri_4_blank_5
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'Yes') >> dummy_filter_user_logs
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'No') >> if_supervisorloginname_not_equals_userloginname_7
        if_supervisorloginname_not_equals_userloginname_7 >> rail.Label(
            'No') >> dummy_filter_user_logs
        if_supervisorloginname_not_equals_userloginname_7 >> rail.Label(
            'Yes') >> if_log_getsupervisor_status_equals_to_false_9
        if_log_getsupervisor_status_equals_to_false_9 >> rail.Label(
            'Yes') >> if_log_getsupervisor_employee_type_not_equals_to_foreignsupervisor_11
        if_log_getsupervisor_employee_type_not_equals_to_foreignsupervisor_11 >> rail.Label(
            'Yes') >> log_errorwhensupervisorisdisabled_12 >> if_log_getsupervisor_status_8_equals_to_true_15
        if_log_getsupervisor_employee_type_not_equals_to_foreignsupervisor_11 >> rail.Label(
            'No') >> enable_login_foreign_supervisor_14 >> if_log_getsupervisor_status_8_equals_to_true_15
        if_log_getsupervisor_status_equals_to_false_9 >> rail.Label(
            'No') >> if_log_getsupervisor_status_8_equals_to_true_15
        if_log_getsupervisor_status_8_equals_to_true_15 >> rail.Label(
            'No') >> dummy_filter_user_logs
        if_log_getsupervisor_status_8_equals_to_true_15 >> rail.Label(
            'Yes') >> log_supervisor_permission_to_be_assigned_17 >> get_assigned_premissions_for_supervisor_18 >> \
            log_checkifthe_supervisor_permissionisassigned_19 >> log_checkifthe_user_permissionisassigned_20 >> \
            if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21
        if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21 >> rail.Label(
            'Yes') >> get_required_permission_uris_list_22 >> add_missing_supervisor_permissions_28 >> if_action_downcase_equals_to_add_30
        if_log_supervisor_permission_17_not_contains_checkifthe_supervisor_permission_19message_21 >> rail.Label(
            'No') >> if_action_downcase_equals_to_add_30
        if_action_downcase_equals_to_add_30 >> rail.Label(
            'Yes') >> update_initial_supervisor_31 >> if_action_downcase_equals_to_update_32
        if_action_downcase_equals_to_add_30 >> rail.Label(
            'No') >> if_action_downcase_equals_to_update_32
        if_action_downcase_equals_to_update_32 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_34 >> dummy_filter_user_logs
        if_action_downcase_equals_to_update_32 >> rail.Label(
            'No') >> dummy_filter_user_logs

        dummy_filter_user_logs >> is_filtered_userlogs

        is_filtered_userlogs >> rail.Label(
            'No') >> on_error
        is_filtered_userlogs >> rail.Label(
            'Yes') >> update_userlog_entries >> update_supervisor_assignment_logs >> on_error

        on_error >> is_entries_present_error >> rail.Label(
            'Yes') >> update_userlog_entries_error >> update_supervisor_assignment_error_logs

    return dag


rail.for_each_instance(create_dag)
