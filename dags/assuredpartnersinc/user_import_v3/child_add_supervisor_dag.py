from datetime import timedelta
from airflow.models import Variable
from assuredpartnersinc.user_import_v3.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_supervisor_dag_id,
        description=f'Assured Partners User Import Add Supervisor {config.instance}',
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
            no_task='search_supervisor_in_replicon'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_supervisor_in_replicon',
            end_task='catch_and_log_error_in_user_logs',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": "{{ dag_run.conf.supervisorloginname }}",
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: {
                'uri': res[0]['userDetails']['uri'],
                'employee_id': res[0]['userDetails']['employeeId'],
                'status': res[0]['userDetails']['isEnabled'],
                'permissionsets': res[0]['permissionSets'],
            } if res else []
        )

        is_supervisor_not_exists = rail.IfOperator(
            task_id='is_supervisor_not_exists',
            test="{{result('search_supervisor_in_replicon') | is_falsy }}",
            yes_task='add_exception_entry_to_supervisor_user_logs',
            no_task='if_request_supervisorloginname_not_equals_user_loginname_11'
        )

        add_exception_entry_to_supervisor_user_logs = rail.WriteLogOperator(
            task_id='add_exception_entry_to_supervisor_user_logs',
            log="{{ dag_run.conf.user_temp_log }}",
            message='supervisor_user_logs',
            properties=lambda dag_run: {
                "entry_type": "Exception entry",
                "childjobid": dag_run.conf['childjobid'],
                "status": "Exception",
                "details": "Supervisor with ID - " + dag_run.conf['supervisorloginname'] + "not found in Replicon"
            }
        )

        supervisor_assignment_table_update_entry_9 = rail.WriteLogOperator(
            task_id='supervisor_assignment_table_update_entry_9',
            log="{{ dag_run.conf.supervisor_assignment_log }}",
            message='updated supervisor entry',
            properties=lambda dag_run: {
                "job_id": dag_run.conf['parentjobid'],
                "username": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['supervisorloginname'],
                "action": dag_run.conf['action'],
                "childjobid": dag_run.conf['childjobid'],
                "supervisoreffectivedate": dag_run.conf['supervisoreffectivedate'],
                "status": "completed",
                "supervisorusername": dag_run.conf['supervisorusername']
            }
        )

        if_request_supervisorloginname_not_equals_user_loginname_11 = rail.IfOperator(
            task_id='if_request_supervisorloginname_not_equals_user_loginname_11',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="is_supervisor_enabled",
            no_task="log_error_supervisor_and_user_loginname_same_31",
        )

        is_supervisor_enabled = rail.IfOperator(
            task_id='is_supervisor_enabled',
            test=lambda: rail.result(
                'search_supervisor_in_replicon')['status'],
            yes_task='is_supervisor_permission_not_assigned',
            no_task='log_error_supervisor_is_disabled_29'
        )

        is_supervisor_permission_not_assigned = rail.IfOperator(
            task_id='is_supervisor_permission_not_assigned',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'search_supervisor_in_replicon')['permissionsets'], 'name', 'Supervisor')),
            yes_task="get_supervisor_permission_uri_in_replicon",
            no_task="if_action_downcase_equals_to_add_21",
        )

        get_supervisor_permission_uri_in_replicon = rail.RepliconServiceOperator(
            task_id='get_supervisor_permission_uri_in_replicon',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', 'Supervisor', 'uri')
        )

        assign_permission_set_to_user_19 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_19',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_supervisor_in_replicon').uri }}",
                "permissionSetUri": "{{ result('get_supervisor_permission_uri_in_replicon') }}"
            }
        )

        if_action_downcase_equals_to_add_21 = rail.IfOperator(
            task_id='if_action_downcase_equals_to_add_21',
            test=lambda dag_run: dag_run.conf['action'].lower() == 'add',
            yes_task="update_initial_supervisor_22",
            no_task="if_action_downcase_equals_to_update_23",
        )

        update_initial_supervisor_22 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_22',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_supervisor_in_replicon').uri }}",
                "scheduleEntries": []
            }
        )

        if_action_downcase_equals_to_update_23 = rail.IfOperator(
            task_id='if_action_downcase_equals_to_update_23',
            test=lambda dag_run: dag_run.conf['action'].lower() == 'update',
            yes_task="update_supervisor_assignment_schedule_over_date_range_25",
            no_task="add_processed_entry_to_supervisor_user_logs",
        )

        update_supervisor_assignment_schedule_over_date_range_25 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_25',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon')['uri'],
                "dateRange": {
                    "startDate": python_callable.get_split_date(dag_run.conf['supervisoreffectivedate']),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_error_supervisor_is_disabled_29 = rail.PythonOperator(
            task_id='log_error_supervisor_is_disabled_29',
            python_callable=lambda dag_run:  "Supervsior assignment/update is not done as user " +
            dag_run.conf['supervisorloginname'] + " is disabled"
        )

        log_error_supervisor_and_user_loginname_same_31 = rail.PythonOperator(
            task_id='log_error_supervisor_and_user_loginname_same_31',
            python_callable=lambda:  "Supervisor not assigned since the user and manager IDs are same"
        )

        add_processed_entry_to_supervisor_user_logs = rail.WriteLogOperator(
            task_id='add_processed_entry_to_supervisor_user_logs',
            log="{{ dag_run.conf.user_temp_log }}",
            message='supervisor_user_logs',
            properties=lambda dag_run: {
                "entry_type": "Processed entry",
                "childjobid": dag_run.conf['childjobid'],
                "status": "Exception" if (rail.result('log_error_supervisor_and_user_loginname_same_31') or rail.result(
                    'log_error_supervisor_is_disabled_29')) else "Success",
                "details": rail.result('log_error_supervisor_and_user_loginname_same_31') if rail.result(
                    'log_error_supervisor_and_user_loginname_same_31') else (rail.result('log_error_supervisor_is_disabled_29') if rail.result(
                        'log_error_supervisor_is_disabled_29') else '')
            }
        )

        supervisor_assignment_table_update_entry_35 = rail.WriteLogOperator(
            task_id='supervisor_assignment_table_update_entry_35',
            log="{{ dag_run.conf.supervisor_assignment_log }}",
            message='updated supervisor entry',
            properties=lambda dag_run: {
                "job_id": dag_run.conf['parentjobid'],
                "username": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['supervisorloginname'],
                "action": dag_run.conf['action'],
                "childjobid": dag_run.conf['childjobid'],
                "supervisoreffectivedate": dag_run.conf['supervisoreffectivedate'],
                "status": "completed",
                "supervisorusername": dag_run.conf['supervisorusername']
            }
        )

        catch_and_log_error_in_user_logs = rail.WriteLogOperator(
            task_id='catch_and_log_error_in_user_logs',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.user_temp_log }}",
            message='supervisor_user_logs',
            properties=lambda dag_run: {
                "entry_type": "Error entry",
                "childjobid": dag_run.conf['childjobid'],
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}")
            }
        )

        supervisor_assignment_table_update_entry_as_error = rail.WriteLogOperator(
            task_id='supervisor_assignment_table_update_entry_as_error',
            trigger_rule='all_success',
            log="{{ dag_run.conf.supervisor_assignment_log }}",
            message='updated supervisor entry',
            properties=lambda dag_run: {
                "job_id": dag_run.conf['parentjobid'],
                "username": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['supervisorloginname'],
                "action": dag_run.conf['action'],
                "childjobid": dag_run.conf['childjobid'],
                "supervisoreffectivedate": dag_run.conf['supervisoreffectivedate'],
                "status": "completed",
                "supervisorusername": dag_run.conf['supervisorusername']
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error_in_user_logs
        can_run_batch_task >> rail.Label(
            'No') >> search_supervisor_in_replicon

        search_supervisor_in_replicon >> is_supervisor_not_exists

        is_supervisor_not_exists >> rail.Label(
            'Yes') >> add_exception_entry_to_supervisor_user_logs >> supervisor_assignment_table_update_entry_9 >> catch_and_log_error_in_user_logs
        is_supervisor_not_exists >> rail.Label(
            'No') >> if_request_supervisorloginname_not_equals_user_loginname_11

        if_request_supervisorloginname_not_equals_user_loginname_11 >> rail.Label(
            'No') >> log_error_supervisor_and_user_loginname_same_31 >> add_processed_entry_to_supervisor_user_logs
        if_request_supervisorloginname_not_equals_user_loginname_11 >> rail.Label(
            'Yes') >> is_supervisor_enabled

        is_supervisor_enabled >> rail.Label(
            'No') >> log_error_supervisor_is_disabled_29 >> add_processed_entry_to_supervisor_user_logs
        is_supervisor_enabled >> rail.Label(
            'Yes') >> is_supervisor_permission_not_assigned

        is_supervisor_permission_not_assigned >> rail.Label(
            'No') >> if_action_downcase_equals_to_add_21
        is_supervisor_permission_not_assigned >> rail.Label(
            'Yes') >> get_supervisor_permission_uri_in_replicon >> assign_permission_set_to_user_19 >> if_action_downcase_equals_to_add_21

        if_action_downcase_equals_to_add_21 >> rail.Label(
            'No') >> if_action_downcase_equals_to_update_23
        if_action_downcase_equals_to_add_21 >> rail.Label(
            'Yes') >> update_initial_supervisor_22 >> if_action_downcase_equals_to_update_23

        if_action_downcase_equals_to_update_23 >> rail.Label(
            'No') >> add_processed_entry_to_supervisor_user_logs
        if_action_downcase_equals_to_update_23 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_25 >> add_processed_entry_to_supervisor_user_logs

        add_processed_entry_to_supervisor_user_logs >> supervisor_assignment_table_update_entry_35 >> catch_and_log_error_in_user_logs >> supervisor_assignment_table_update_entry_as_error

    return dag


rail.for_each_instance(create_dag)
