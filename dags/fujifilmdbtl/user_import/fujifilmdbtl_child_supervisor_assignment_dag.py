from datetime import timedelta, datetime
from pendulum import now
from airflow.models import Variable
import rail
from fujifilmdbtl.user_import.utils import request_payload, python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_child_supervisor_assignment_{config.instance}',
        description=f'FDT_Child Workflow to assign supervisor {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

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
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_users_3 = rail.RepliconServiceOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_search_user_payload_for_supervisor(
                dag_run.conf['supervisorid']),
            data_handler=lambda response: response['rows']
        )

        user_search_result_list = rail.PythonOperator(
            task_id='user_search_result_list',
            python_callable=lambda: python_callable.get_search_user_details(
                rail.result('search_users_3'))
        )

        log_getsupervisor_uri_4 = rail.PythonOperator(
            task_id='log_getsupervisor_uri_4',
            python_callable=lambda dag_run: (rail.find_first_by_attr_and_get_attr(rail.result(
                'user_search_result_list'), 'employeeid', dag_run.conf['supervisorid'], 'uri', "")) if rail.result('user_search_result_list') else null
        )

        if_request_supervisorloginname_not_equals_to_loginname_5 = rail.IfOperator(
            task_id='if_request_supervisorloginname_not_equals_to_loginname_5',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="if_log_getsupervisor_uri_4_present_6",
            no_task="log_errorfor_supervisorand_userslogin_nameissame_31",
        )

        if_log_getsupervisor_uri_4_present_6 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_present_6',
            test='''{{ result('log_getsupervisor_uri_4') | is_truthy }}''',
            yes_task="log_getsupervisor_status_7",
            no_task="if_log_getsupervisor_uri_4_blank_28",
        )

        log_getsupervisor_status_7 = rail.PythonOperator(
            task_id='log_getsupervisor_status_7',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'user_search_result_list'), 'enabled', "True", 'enabled', '')
        )

        if_log_getsupervisor_status_7_equals_to_true_8 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_7_equals_to_true_8',
            test='''{{ result('log_getsupervisor_status_7') == 'True' }}''',
            yes_task="_adhoc_http_action_10",
            no_task="log_errorwhensupervisorisdisabled_27",
        )

        _adhoc_http_action_10 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_10',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_getsupervisor_uri_4') }}"
            }
        )

        log_checkifthe_supervisor_permissionisassigned_11 = rail.PythonOperator(
            task_id='log_checkifthe_supervisor_permissionisassigned_11',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_10'), 'policyUri', 'urn:replicon:policy:supervision', 'uri', "") if rail.result(
                    '_adhoc_http_action_10') else null
        )

        if_log_checkifthe_supervisor_permissionisassigned_11_blank_12 = rail.IfOperator(
            task_id='if_log_checkifthe_supervisor_permissionisassigned_11_blank_12',
            test='''{{ result('log_checkifthe_supervisor_permissionisassigned_11') | is_falsy }}''',
            yes_task="_adhoc_http_action_13",
            no_task="if_request_action_equals_to_add_17",
        )

        _adhoc_http_action_13 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_13',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        log_requiredpermissiontoassign_14 = rail.PythonOperator(
            task_id='log_requiredpermissiontoassign_14',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_13'), 'displayText', "Supervisor", 'uri', "") if rail.result('_adhoc_http_action_13') else null
        )

        if_log_requiredpermissiontoassign_14_present_15 = rail.IfOperator(
            task_id='if_log_requiredpermissiontoassign_14_present_15',
            test='''{{ result('log_requiredpermissiontoassign_14') | is_truthy }}''',
            yes_task="assign_supervsior_permission_set_to_user_16",
            no_task="if_request_action_equals_to_add_17",
        )

        assign_supervsior_permission_set_to_user_16 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_16',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('log_getsupervisor_uri_4') }}",
                "permissionSetUri": "{{ result('log_requiredpermissiontoassign_14') }}"
            }
        )

        if_request_action_equals_to_add_17 = rail.IfOperator(
            task_id='if_request_action_equals_to_add_17',
            test='''{{ dag_run.conf.action == 'Add' }}''',
            yes_task="update_initial_supervisor_18",
            no_task="if_request_action_equals_to_update_19",
        )

        update_initial_supervisor_18 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_18',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('log_getsupervisor_uri_4') }}",
                "scheduleEntries": []
            }
        )

        if_request_action_equals_to_update_19 = rail.IfOperator(
            task_id='if_request_action_equals_to_update_19',
            test='''{{ dag_run.conf.action == 'Update' }}''',
            yes_task="log_supervisor_effective_dateday_20",
            no_task="if_log_getsupervisor_uri_4_blank_28",
        )

        log_supervisor_effective_dateday_20 = rail.PythonOperator(
            task_id='log_supervisor_effective_dateday_20',
            python_callable=lambda:  now().strftime("%d")
        )

        log_supervisor_effective_date_month_21 = rail.PythonOperator(
            task_id='log_supervisor_effective_date_month_21',
            python_callable=lambda:  now().strftime("%m")
        )

        log_supervisor_effective_date_year_22 = rail.PythonOperator(
            task_id='log_supervisor_effective_date_year_22',
            python_callable=lambda: now().strftime("%Y")
        )

        update_supervisor_assignment_schedule_over_date_range_23 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_23',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('log_getsupervisor_uri_4') }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_supervisor_effective_date_year_22') }}",
                        "month": "{{ result('log_supervisor_effective_date_month_21') }}",
                        "day": "{{ result('log_supervisor_effective_dateday_20') }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_errorwhensupervisorisdisabled_27 = rail.PythonOperator(
            task_id='log_errorwhensupervisorisdisabled_27',
            python_callable=lambda: "Supervsior assignment/update is not done for user {{ dag_run.conf.loginname }} as supervsior with employee id {{ dag_run.conf.supervisorid }} is disabled in Replicon."
        )

        if_log_getsupervisor_uri_4_blank_28 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_blank_28',
            test='''{{ result('log_getsupervisor_uri_4') | is_falsy }}''',
            yes_task="log_erroras_supervisorisnotavailable_29",
            no_task="fdt_user_import_logs_search_entries_32",
        )

        log_erroras_supervisorisnotavailable_29 = rail.PythonOperator(
            task_id='log_erroras_supervisorisnotavailable_29',
            python_callable=lambda:  "Supervisor is not updated as the supervisor with id {{ dag_run.conf.supervisorid }} is not available"
        )

        log_errorfor_supervisorand_userslogin_nameissame_31 = rail.PythonOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame_31',
            python_callable=lambda:  '''Supervisor is not updated as the \"Login name\" for user and supervisor is same on the input file'''
        )

        fdt_user_import_logs_search_entries_32 = rail.FilterLogEntriesOperator(
            task_id='fdt_user_import_logs_search_entries_32',
            log="{{ dag_run.conf.userimportlogtable }}",
            properties={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{dag_run.conf.childjobid}}",
            },
            remove_filtered_entries=True,
        )

        if_first_id_present_33 = rail.IfOperator(
            task_id='if_first_id_present_33',
            test=lambda: rail.result(
                'fdt_user_import_logs_search_entries_32', 'length') > 0,
            yes_task="fdt_user_import_logs_update_entry_34",
            no_task="fdt_supervisor_assignment_table_update_entry_35",
        )

        fdt_user_import_logs_update_entry_34 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_update_entry_34',
            log="{{ dag_run.conf.userimportlogtable }}",
            items="{{ result('fdt_user_import_logs_search_entries_32') }}",
            message="Post-Processing Supervisor Assignment",
            severity="na",
            properties=lambda item: {
                "parentjobid": item['properties']['parentjobid'],
                "username": item['properties']['username'],
                "loginname": item['properties']['loginname'],
                "emplid": item['properties']['emplid'],
                "action": item['properties']['action'],
                "status": python_callable.get_final_supervisor_assignment_entry_status(item),
                "details": python_callable.get_final_supervisor_assignment_entry_details(item)
            }
        )

        fdt_supervisor_assignment_table_update_entry_35 = rail.WriteLogOperator(
            task_id='fdt_supervisor_assignment_table_update_entry_35',
            log="{{ dag_run.conf.supervisorassignmentlookuptable }}",
            message="na",
            severity="na",
            properties={
                "userloginname": "{{ dag_run.conf.loginname }}",
                "user_uri": "{{ dag_run.conf.useruri }}",
                "user_name": "{{ dag_run.conf.username }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "supervisor_id": "{{ dag_run.conf.supervisorid }}",
                "action":"{{ dag_run.conf.action }}",
                "emplid": "{{ dag_run.conf.employeeid }}",
                "status": "completed"
            }
        )

        if_first_id_present_38 = rail.IfOperator(
            task_id='if_first_id_present_38',
            trigger_rule='one_failed',
            test=lambda: rail.result(
                'fdt_user_import_logs_search_entries_32', 'length') > 0,
            yes_task="fdt_user_import_logs_update_entry_39",
            no_task="fdt_supervisor_assignment_table_update_entry_40",
        )

        fdt_user_import_logs_update_entry_39 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_update_entry_39',
            log="{{ dag_run.conf.userimportlogtable }}",
            items="{{ result('fdt_user_import_logs_search_entries_32') }}",
            message="na",
            severity="Error",
            properties=lambda item: {
                "parentjobid": item['properties']['parentjobid'],
                "username": item['properties']['username'],
                "loginname": item['properties']['loginname'],
                "emplid": item['properties']['emplid'],
                "action": item['properties']['action'],
                "status": "Error",
                "details": item['properties']['action'] + ";" + rail.render_template("{{get_error_message()}}")
            }
        )

        fdt_supervisor_assignment_table_update_entry_40 = rail.WriteLogOperator(
            task_id='fdt_supervisor_assignment_table_update_entry_40',
            log="{{ dag_run.conf.supervisorassignmentlookuptable }}",
            message="na",
            severity="na",
            properties={
                "userloginname": "{{ dag_run.conf.loginname }}",
                "user_uri": "{{ dag_run.conf.useruri }}",
                "user_name": "{{ dag_run.conf.username }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "supervisor_id": "{{ dag_run.conf.supervisorid }}",
                "action":"{{ dag_run.conf.action }}",
                "emplid": "{{ dag_run.conf.employeeid }}",
                "status": "completed"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> user_search_result_list >> log_getsupervisor_uri_4 >> if_request_supervisorloginname_not_equals_to_loginname_5
        if_request_supervisorloginname_not_equals_to_loginname_5 >> rail.Label(
            'Yes') >> if_log_getsupervisor_uri_4_present_6
        if_log_getsupervisor_uri_4_present_6 >> rail.Label(
            'Yes') >> log_getsupervisor_status_7 >> if_log_getsupervisor_status_7_equals_to_true_8
        if_log_getsupervisor_status_7_equals_to_true_8 >> rail.Label(
            'Yes') >> _adhoc_http_action_10 >> log_checkifthe_supervisor_permissionisassigned_11 >> if_log_checkifthe_supervisor_permissionisassigned_11_blank_12
        if_log_checkifthe_supervisor_permissionisassigned_11_blank_12 >> rail.Label(
            'Yes') >> _adhoc_http_action_13 >> log_requiredpermissiontoassign_14 >> if_log_requiredpermissiontoassign_14_present_15
        if_log_requiredpermissiontoassign_14_present_15 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_16
        if_log_requiredpermissiontoassign_14_present_15 >> rail.Label(
            'No') >> if_request_action_equals_to_add_17
        if_log_checkifthe_supervisor_permissionisassigned_11_blank_12 >> rail.Label(
            'No') >> if_request_action_equals_to_add_17
        if_request_action_equals_to_add_17 >> rail.Label(
            'Yes') >> update_initial_supervisor_18 >> if_request_action_equals_to_update_19
        if_request_action_equals_to_add_17 >> rail.Label(
            'No') >> if_request_action_equals_to_update_19
        if_request_action_equals_to_update_19 >> rail.Label(
            'Yes') >> log_supervisor_effective_dateday_20 >> log_supervisor_effective_date_month_21 >> log_supervisor_effective_date_year_22 >> update_supervisor_assignment_schedule_over_date_range_23 >> if_log_getsupervisor_uri_4_blank_28
        if_request_action_equals_to_update_19 >> rail.Label('No') >> if_log_getsupervisor_uri_4_blank_28 >> rail.Label(
            'No') >> fdt_user_import_logs_search_entries_32
        if_log_getsupervisor_status_7_equals_to_true_8 >> rail.Label(
            'No') >> log_errorwhensupervisorisdisabled_27 >> if_log_getsupervisor_uri_4_blank_28
        if_log_getsupervisor_uri_4_present_6 >> rail.Label(
            'No') >> if_log_getsupervisor_uri_4_blank_28
        if_log_getsupervisor_uri_4_blank_28 >> rail.Label(
            'Yes') >> log_erroras_supervisorisnotavailable_29 >> fdt_user_import_logs_search_entries_32
        if_log_getsupervisor_uri_4_blank_28 >> rail.Label(
            'No') >> fdt_user_import_logs_search_entries_32
        if_request_supervisorloginname_not_equals_to_loginname_5 >> rail.Label(
            'No') >> log_errorfor_supervisorand_userslogin_nameissame_31 >> fdt_user_import_logs_search_entries_32 >> if_first_id_present_33
        if_first_id_present_33 >> rail.Label(
            'Yes') >> fdt_user_import_logs_update_entry_34 >> fdt_supervisor_assignment_table_update_entry_35
        if_first_id_present_33 >> rail.Label(
            'No') >> fdt_supervisor_assignment_table_update_entry_35 >> if_first_id_present_38
        if_first_id_present_38 >> rail.Label(
            'Yes') >> fdt_user_import_logs_update_entry_39 >> fdt_supervisor_assignment_table_update_entry_40
        if_first_id_present_38 >> rail.Label(
            'No') >> fdt_supervisor_assignment_table_update_entry_40 >> finish

    return dag


rail.for_each_instance(create_dag)
