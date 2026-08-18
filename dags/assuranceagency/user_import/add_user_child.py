from datetime import timedelta
import rail
from assuranceagency.user_import.utils import python_callable
from assuranceagency.user_import.utils import request_payload
from airflow.models import Variable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'assuranceagency_user_import_add_user_child_{config.instance}',
        description=f'assuranceagency_user_import_add_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_add_user_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                 config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='validate_employeetype_and_manager'
            )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='validate_employeetype_and_manager',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            )

        validate_employeetype_and_manager = rail.IfOperator(
            task_id="validate_employeetype_and_manager",
            test="{{ dag_run.conf.employeetype == 'E' and dag_run.conf.manager.lower() != 'yes'}}",
            yes_task="log_user_not_allowed_import",
            no_task="split_start_date"
        )

        log_user_not_allowed_import = rail.WriteLogOperator(
            task_id='log_user_not_allowed_import',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Skipped",
            properties={
                "username" : "{{ dag_run.conf.firstname }}" + " " + "{{ dag_run.conf.lastname }}",
                "login_name": "{{ dag_run.conf.loginname }}",
                "emplid" : "{{ dag_run.conf.employeeid }}",
                "action" : "{{ dag_run.conf.type }}",
                "status": "Skipped",
                "details": 'Employee type exempt with manager set to "{{ dag_run.conf.manager }}" not allowed in Replicon'
            }
        )

        split_start_date = rail.PythonOperator(
            task_id = "split_start_date",
            python_callable=python_callable.split_startdate
        )

        get_exception_log = rail.PythonOperator(
            task_id = "get_exception_log",
            python_callable=python_callable.get_exception_logs
        )

        add_user = rail.RepliconServiceOperator(
            task_id = "add_user",
            endpoint = "/services/ImportService1.svc/PutUser3",
            data = request_payload.create_user_payload
        )

        assign_time_off_types = rail.RepliconServiceOperator(
            task_id = "assign_time_off_types",
            endpoint = "/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data = python_callable.get_timeoff_type
        )

        is_supervisor_uri_present = rail.IfOperator(
            task_id="is_supervisor_uri_present",
            test="{{ dag_run.conf.supervisoruri | is_truthy }}",
            yes_task="get_supervisor_details",
            no_task="log_to_supervisor_lookup"
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id = "get_supervisor_details",
            endpoint = "/services/ImportService1.svc/BulkGetUsers3",
            data = request_payload.get_supervisor_data
        )

        is_supervisor_is_enabled = rail.IfOperator(
            task_id="is_supervisor_is_enabled",
            test="{{ result('get_supervisor_details')[0].userDetails.isEnabled | lower() == 'true' }}",
            yes_task="get_supervisor_permission_sets",
            no_task="log_to_supervisor_lookup"
        )

        get_supervisor_permission_sets = rail.PythonOperator(
            task_id = "get_supervisor_permission_sets",
            python_callable=lambda: {
                'manager_permission' : rail.find_first_by_attr_and_get_attr(
                    rail.result('get_supervisor_details')[0]['permissionSets'], 'displayText', 'Manager', 'uri', ''),
                'enduser_permission' : rail.find_first_by_attr_and_get_attr(
                    rail.result('get_supervisor_details')[0]['permissionSets'], 'displayText', 'End user with reports view', 'uri', '')
            }
        )

        check_if_manager_permission_absent = rail.IfOperator(
            task_id="check_if_manager_permission_absent",
            test="{{ result('get_supervisor_permission_sets').manager_permission | is_falsy }}",
            yes_task="assign_manager_permission_to_supervisor",
            no_task="check_if_enduser_permission_absent"
        )

        assign_manager_permission_to_supervisor = rail.RepliconServiceOperator(
            task_id = "assign_manager_permission_to_supervisor",
            endpoint = "/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.supervisoruri }}",
                "permissionSetUri": "{{ dag_run.conf['supervisorpermissionuri'] }}"
            }
        )

        check_if_enduser_permission_absent = rail.IfOperator(
            task_id="check_if_enduser_permission_absent",
            test="{{ result('get_supervisor_permission_sets').enduser_permission | is_falsy }}",
            yes_task="assign_enduser_permission_to_supervisor",
            no_task="assign_initial_supervisor"
        )

        assign_enduser_permission_to_supervisor = rail.RepliconServiceOperator(
            task_id = "assign_enduser_permission_to_supervisor",
            endpoint = "/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.supervisoruri }}",
                "permissionSetUri": "{{ dag_run.conf['enduserpermissionformanager'] }}"
            }
        )

        assign_initial_supervisor = rail.RepliconServiceOperator(
            task_id = "assign_initial_supervisor",
            endpoint = "/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = {
                "userUri": "{{ result('add_user').uri }}",
                "supervisorUri": "{{ dag_run.conf.supervisoruri }}"
            }
        )

        log_to_supervisor_lookup = rail.WriteLogOperator(
            task_id='log_to_supervisor_lookup',
            log = "{{ dag_run.conf.supervisor_logger }}",
            message="na",
            severity="Skipped",
            properties={
                "userloginname" : "{{ dag_run.conf.loginname }}",
                "useruri" : "{{ result('add_user').uri }}",
                "username" : "{{ dag_run.conf.firstname }}" + " " + "{{ dag_run.conf.lastname }}",
                "supervisorloginname" : "{{ dag_run.conf.initialsupervisorloginname }}",
                "emplid" : "{{ dag_run.conf.employeeid }}",
                "action" : "Add",
                "status": ""
            }
        )

        log_user_import = rail.WriteLogOperator(
            task_id='log_user_import',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity=lambda: 'Success' if rail.result('get_exception_log')['exc_present'] is False else 'Exception',
            properties=python_callable.get_status_and_details_for_add
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = "{{ dag_run.conf.logger }}",
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "username" : "{{ dag_run.conf.firstname }}" + " " + "{{ dag_run.conf.lastname }}",
                "login_name": "{{ dag_run.conf.loginname }}",
                "emplid" : "{{ dag_run.conf.employeeid }}",
                "action" : "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> validate_employeetype_and_manager

        validate_employeetype_and_manager >> rail.Label('Yes') >> log_user_not_allowed_import >> catch_and_log_errors
        validate_employeetype_and_manager >> rail.Label('No') >> split_start_date >> get_exception_log >> add_user >> \
        assign_time_off_types >> is_supervisor_uri_present

        is_supervisor_uri_present >> rail.Label('Yes') >> get_supervisor_details
        is_supervisor_uri_present >> rail.Label('No') >> log_to_supervisor_lookup >> log_user_import >> catch_and_log_errors

        get_supervisor_details >> is_supervisor_is_enabled

        is_supervisor_is_enabled >> rail.Label('Yes') >> get_supervisor_permission_sets >> check_if_manager_permission_absent
        is_supervisor_is_enabled >> rail.Label('No') >> log_to_supervisor_lookup >> log_user_import >> catch_and_log_errors

        check_if_manager_permission_absent >> rail.Label('Yes') >> assign_manager_permission_to_supervisor >> check_if_enduser_permission_absent
        check_if_manager_permission_absent >> rail.Label('No') >> check_if_enduser_permission_absent

        check_if_enduser_permission_absent >> rail.Label('Yes') >> assign_enduser_permission_to_supervisor >> assign_initial_supervisor
        check_if_enduser_permission_absent >> rail.Label('No') >> assign_initial_supervisor

        assign_initial_supervisor >> log_user_import >> catch_and_log_errors

        catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
