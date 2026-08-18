from datetime import timedelta
from airflow.models import Variable
import rail
from ascendmaterials.user_import.utils import request_payload, python_callable, response_filter

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.supervisor_dag_id,
        description=f'Ascend_Child_Add Supervisor {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
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
            no_task='filter_user_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='filter_user_logs',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        filter_user_logs = rail.FilterLogEntriesOperator(
            task_id='filter_user_logs',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            properties={
                'userloginname': '{{ dag_run.conf["loginname"] }}'
            },
            remove_filtered_entries=True
        )

        search_users = rail.RepliconServiceOperator(
            task_id='search_users',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_user_3_payload_data,
            data_handler=response_filter.get_supervisor
        )

        if_supervisorloginname_ne_loginname = rail.IfOperator(
            task_id='if_supervisorloginname_ne_loginname',
            test='''{{ dag_run.conf["supervisorloginname"] != dag_run.conf["loginname"] }}''',
            yes_task="if_getsupervisor_uri_present",
            no_task="log_errorfor_supervisorand_userslogin_nameissame",
        )

        if_getsupervisor_uri_present = rail.IfOperator(
            task_id='if_getsupervisor_uri_present',
            test='''{{ result('search_users').uri | is_truthy }}''',
            yes_task="if_getsupervisor_status_7_eq_true",
            no_task="if_getsupervisor_uri_4_blank",
        )

        if_getsupervisor_status_7_eq_true = rail.IfOperator(
            task_id='if_getsupervisor_status_7_eq_true',
            test='''{{ result('search_users').status == 'True' }}''',
            yes_task="get_assigned_permission_sets",
            no_task="log_errorwhensupervisorisdisabled",
        )

        get_assigned_permission_sets = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users').uri }}"
            }
        )

        log_supervisor_permissionisassigned = rail.PythonOperator(
            task_id='log_supervisor_permissionisassigned',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_permission_sets'), 'policyUri',
                'urn:replicon:policy:supervision', 'permissionSet.uri', '') if rail.result('get_assigned_permission_sets') and rail.result('get_assigned_permission_sets')[0]['policyUri'] else None
        )

        if_the_supervisor_permissionisassigned_10_blank = rail.IfOperator(
            task_id='if_the_supervisor_permissionisassigned_10_blank',
            test='''{{ result('log_supervisor_permissionisassigned') | is_falsy }}''',
            yes_task="get_all_permission_sets",
            no_task="if_action_eq_add",
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Supervisor', 'uri', '')
        )

        if_requiredpermissiontoassign_present = rail.IfOperator(
            task_id='if_requiredpermissiontoassign_present',
            test='''{{ result('get_all_permission_sets') | is_truthy }}''',
            yes_task="assign_supervsior_permission_set_to_user",
            no_task="if_action_eq_add",
        )

        assign_supervsior_permission_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users').uri }}",
                "permissionSetUri": "{{ result('get_all_permission_sets') }}"
            }
        )

        if_action_eq_add = rail.IfOperator(
            task_id='if_action_eq_add',
            test='''{{ dag_run.conf["action"] == 'Add' }}''',
            yes_task="update_initial_supervisor",
            no_task="if_action_eq_update",
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "initialSupervisorUri": "{{ result('search_users').uri }}",
                "scheduleEntries": []
            }
        )

        if_action_eq_update = rail.IfOperator(
            task_id='if_action_eq_update',
            test='''{{ dag_run.conf["action"] == 'Update' }}''',
            yes_task="update_supervisor_schedule",
            no_task="catch_1",
        )

        update_supervisor_schedule = rail.RepliconServiceOperator(
            task_id='update_supervisor_schedule',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "supervisorUri": rail.result('search_users')['uri'],
                "dateRange": {
                    "startDate": python_callable.get_today_date(),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        catch_1 = rail.EmptyOperator(
            task_id='catch_1',
            trigger_rule='none_failed',
        )

        log_errorwhensupervisorisdisabled = rail.PythonOperator(
            task_id='log_errorwhensupervisorisdisabled',
            python_callable=lambda dag_run: "Supervsior assignment/update is not done for user " + str(dag_run.conf["loginname"]) +
            " as supervsior with login name " +
            str(dag_run.conf["supervisorloginname"]) +
            " is disabled in Replicon."
        )

        if_getsupervisor_uri_4_blank = rail.IfOperator(
            task_id='if_getsupervisor_uri_4_blank',
            test='''{{ result('search_users').uri | is_falsy }}''',
            yes_task="log_erroras_supervisorisnotavailable",
            no_task="if_filter_user_logs_present",
        )

        log_erroras_supervisorisnotavailable = rail.PythonOperator(
            task_id='log_erroras_supervisorisnotavailable',
            python_callable=lambda dag_run: (
                "Supervisor is not assigned/updated as the supervisor with login name " +
                str(dag_run.conf.get("supervisorloginname", "")) +
                " is not available in Replicon."
            )
        )

        log_errorfor_supervisorand_userslogin_nameissame = rail.PythonOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame',
            python_callable=lambda: '''Supervisor is not updated as the "Login name" for user and supervisor is same on the input file'''
        )

        if_filter_user_logs_present = rail.IfOperator(
            task_id='if_filter_user_logs_present',
            test='''{{ result('filter_user_logs', 'length') > 0 }}''',
            yes_task="log_update_entry_1",
            no_task="catch_and_log_errors",
        )

        log_update_entry_1 = rail.WriteLogOperator(
            task_id='log_update_entry_1',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            items="{{ result('filter_user_logs') }}",
            properties=lambda item: {
                "userloginname": item['properties']['userloginname'],
                "username": item['properties'].get('username', ''),
                "action": item['properties']['action'],
                "status": python_callable.get_supervisor_status(item['properties'].get('status', 'Success')),
                "details": python_callable.get_detail_message_34(item['properties'].get('details', ''))
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "username": "",
                "userloginname": dag_run.conf["loginname"],
                "action": dag_run.conf["action"],
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> filter_user_logs >> search_users
        search_users >> if_supervisorloginname_ne_loginname
        if_supervisorloginname_ne_loginname >> rail.Label(
            'Yes') >> if_getsupervisor_uri_present
        if_getsupervisor_uri_present >> rail.Label(
            'Yes') >> if_getsupervisor_status_7_eq_true
        if_getsupervisor_status_7_eq_true >> rail.Label(
            'Yes') >> get_assigned_permission_sets >> log_supervisor_permissionisassigned >> if_the_supervisor_permissionisassigned_10_blank
        if_the_supervisor_permissionisassigned_10_blank >> rail.Label(
            'Yes') >> get_all_permission_sets >> if_requiredpermissiontoassign_present
        if_requiredpermissiontoassign_present >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user >> if_action_eq_add
        if_requiredpermissiontoassign_present >> rail.Label(
            'No') >> if_action_eq_add
        if_the_supervisor_permissionisassigned_10_blank >> rail.Label(
            'No') >> if_action_eq_add
        if_action_eq_add >> rail.Label(
            'Yes') >> update_initial_supervisor >> if_action_eq_update
        if_action_eq_add >> rail.Label(
            'No') >> if_action_eq_update
        if_action_eq_update >> rail.Label(
            'Yes') >> update_supervisor_schedule >> catch_1
        if_action_eq_update >> rail.Label(
            'No') >> catch_1 >> if_getsupervisor_uri_4_blank
        if_getsupervisor_status_7_eq_true >> rail.Label(
            'No') >> log_errorwhensupervisorisdisabled >> if_getsupervisor_uri_4_blank
        if_getsupervisor_uri_present >> rail.Label(
            'No') >> if_getsupervisor_uri_4_blank
        if_getsupervisor_uri_4_blank >> rail.Label(
            'Yes') >> log_erroras_supervisorisnotavailable >> if_filter_user_logs_present
        if_getsupervisor_uri_4_blank >> rail.Label(
            'No') >> if_filter_user_logs_present
        if_supervisorloginname_ne_loginname >> rail.Label(
            'No') >> log_errorfor_supervisorand_userslogin_nameissame >> if_filter_user_logs_present
        if_filter_user_logs_present >> rail.Label(
            'Yes') >> log_update_entry_1 >> catch_and_log_errors
        if_filter_user_logs_present >> rail.Label(
            'No') >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
