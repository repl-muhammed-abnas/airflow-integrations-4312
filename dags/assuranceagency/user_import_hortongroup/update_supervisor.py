from datetime import timedelta
import rail
from assuranceagency.user_import_hortongroup.utils.request_payload import get_today_date_format
from assuranceagency.user_import_hortongroup.utils.python_callable import get_exceptions
from assuranceagency.user_import_hortongroup.utils import python_callable
from assuranceagency.user_import_hortongroup.utils import request_payload
from airflow.models import Variable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'assuranceagency_user_import_hortongroup_update_supervisor_from_logs_child_{config.instance}',
        description=f'assuranceagency_user_import_hortongroup_update_supervisor_from_logs_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_update_supervisor_child,
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
            no_task='get_supervisor_uri'
            )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_supervisor_uri',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            )

        get_supervisor_uri = rail.RepliconServiceOperator(
            task_id='get_supervisor_uri',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.get_supervisordetails,
            data_handler = python_callable.get_supervisor_uri_by_loginname
        )

        if_initial_supervisor_equal_loginname = rail.IfOperator(
            task_id="if_initial_supervisor_equal_loginname",
            test="{{ dag_run.conf.supervisorloginname == dag_run.conf.userloginname }}",
            yes_task="log_same_user_and_supervisor_exception",
            no_task="if_supervisoruri_present"
        )

        log_same_user_and_supervisor_exception = rail.PythonOperator(
            task_id='log_same_user_and_supervisor_exception',
            python_callable=lambda: 'Supervisor not updated  - Supervisor login name is same as User login name'
        )

        if_supervisoruri_present = rail.IfOperator(
            task_id="if_supervisoruri_present",
            test="{{ result('get_supervisor_uri') | is_truthy }}",
            yes_task="get_supervisor_details",
            no_task="log_supervisor_not_available"
        )

        log_supervisor_not_available = rail.PythonOperator(
            task_id='log_supervisor_not_available',
            python_callable=lambda dag_run: 'Supervisor is not updated as the supervisor with \
                    login name "' + dag_run.conf['supervisorloginname'] + '" is not available'
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id = "get_supervisor_details",
            endpoint = "/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "uri": "{{ result('get_supervisor_uri') }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        is_supervisor_is_enabled = rail.IfOperator(
            task_id="is_supervisor_is_enabled",
            test="{{ result('get_supervisor_details')[0].userDetails.isEnabled | lower() == 'true' }}",
            yes_task="get_supervisor_permission_sets",
            no_task="log_supervisor_is_disabled"
        )

        log_supervisor_is_disabled = rail.PythonOperator(
            task_id='log_supervisor_is_disabled',
            python_callable=lambda dag_run: 'Supervsior assignment/update is not done for user "' + dag_run.conf['userloginname'] + '" \
                    as supervsior with loginname "' + dag_run.conf['supervisorloginname'] + '" is disabled in Replicon.'
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
                "userUri": "{{ result('get_supervisor_uri') }}",
                "permissionSetUri": "{{ dag_run.conf['supervisorpermissionuri'] }}"
            }
        )

        check_if_enduser_permission_absent = rail.IfOperator(
            task_id="check_if_enduser_permission_absent",
            test="{{ result('get_supervisor_permission_sets').enduser_permission | is_falsy }}",
            yes_task="assign_enduser_permission_to_supervisor",
            no_task="check_if_action_is_add"
        )

        assign_enduser_permission_to_supervisor = rail.RepliconServiceOperator(
            task_id = "assign_enduser_permission_to_supervisor",
            endpoint = "/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ result('get_supervisor_uri') }}",
                "permissionSetUri": "{{ dag_run.conf['enduserpermissionformanager'] }}"
            }
        )

        check_if_action_is_add = rail.IfOperator(
            task_id="check_if_action_is_add",
            test="{{ dag_run.conf.action | lower() == 'add' }}",
            yes_task="update_initial_supervisor",
            no_task="check_if_action_is_update"
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id = "update_initial_supervisor",
            endpoint = "/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('get_supervisor_uri') }}"
            }
        )

        check_if_action_is_update = rail.IfOperator(
            task_id="check_if_action_is_update",
            test="{{ dag_run.conf.action | lower() == 'update' }}",
            yes_task="update_supervisor_assignmentoverdaterange",
            no_task="log_no_action_found"
        )

        update_supervisor_assignmentoverdaterange = rail.RepliconServiceOperator(
            task_id = "update_supervisor_assignmentoverdaterange",
            endpoint = "/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('get_supervisor_uri') }}",
                "dateRange": {
                    "startDate": get_today_date_format()
                }
            }
        )

        log_no_action_found = rail.PythonOperator(
            task_id='log_no_action_found',
            python_callable=lambda: 'The action field is neither add or update'
        )

        search_userimport_logs_for_user_and_delete_to_update = rail.FilterLogEntriesOperator(
            task_id='search_userimport_logs_for_user_and_delete_to_update',
            log="{{ dag_run.conf.logger }}",
            properties={
                "emplid": "{{dag_run.conf.emplid}}"
            },
            remove_filtered_entries=True
        )

        load_found_logs_entry = rail.PythonOperator(
            task_id='load_found_logs_entry',
            python_callable=lambda: rail.load_all_records(rail.result(
                'search_userimport_logs_for_user_and_delete_to_update'))
        )

        if_entry_is_present = rail.IfOperator(
            task_id='if_entry_is_present',
            test='''{{ result('search_userimport_logs_for_user_and_delete_to_update','length') > 0 | is_truthy }}''',
            yes_task="add_updated_log",
            no_task="catch_and_log_errors",
        )

        add_updated_log = rail.WriteLogOperator(
            task_id='add_updated_log',
            log="{{dag_run.conf.logger}}",
            message='na',
            severity=lambda: 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['status']),
            properties=lambda: {
                "username": rail.result('load_found_logs_entry')[0]['properties']['username'],
                "login_name": rail.result('load_found_logs_entry')[0]['properties']['login_name'],
                "emplid": rail.result('load_found_logs_entry')[0]['properties']['emplid'],
                "action": rail.result('load_found_logs_entry')[0]['properties']['action'],
                "status": 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['status']),
                "details": rail.result('load_found_logs_entry')[0]['properties']['details'] + ',' + get_exceptions()
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log = "{{ dag_run.conf.logger }}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "username" : "{{ dag_run.conf.username }}",
                "login_name": "{{ dag_run.conf.userloginname }}",
                "emplid" : "{{ dag_run.conf.emplid }}",
                "action" : "{{ dag_run.conf.action }}",
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_supervisor_uri

        get_supervisor_uri >> if_initial_supervisor_equal_loginname

        if_initial_supervisor_equal_loginname >> rail.Label('Yes') >> log_same_user_and_supervisor_exception >> \
            search_userimport_logs_for_user_and_delete_to_update
        if_initial_supervisor_equal_loginname >> rail.Label('No') >> if_supervisoruri_present

        if_supervisoruri_present >> rail.Label('Yes') >> get_supervisor_details >> is_supervisor_is_enabled
        if_supervisoruri_present >> rail.Label('No') >> log_supervisor_not_available >> search_userimport_logs_for_user_and_delete_to_update

        is_supervisor_is_enabled >> rail.Label('Yes') >> get_supervisor_permission_sets >> check_if_manager_permission_absent
        is_supervisor_is_enabled >> rail.Label('No') >> log_supervisor_is_disabled >> search_userimport_logs_for_user_and_delete_to_update

        check_if_manager_permission_absent >> rail.Label('Yes') >> assign_manager_permission_to_supervisor >> check_if_enduser_permission_absent
        check_if_manager_permission_absent >> rail.Label('No') >> check_if_enduser_permission_absent

        check_if_enduser_permission_absent >> rail.Label('Yes') >> assign_enduser_permission_to_supervisor >> check_if_action_is_add
        check_if_enduser_permission_absent >> rail.Label('No') >> check_if_action_is_add

        check_if_action_is_add >> rail.Label('Yes') >> update_initial_supervisor >> search_userimport_logs_for_user_and_delete_to_update
        check_if_action_is_add >> rail.Label('No') >> check_if_action_is_update

        check_if_action_is_update >> rail.Label('Yes') >> update_supervisor_assignmentoverdaterange >> search_userimport_logs_for_user_and_delete_to_update
        check_if_action_is_update >> rail.Label('No') >> log_no_action_found >> search_userimport_logs_for_user_and_delete_to_update

        search_userimport_logs_for_user_and_delete_to_update >> load_found_logs_entry >> if_entry_is_present
        if_entry_is_present >> rail.Label('Yes') >> add_updated_log >> catch_and_log_errors
        if_entry_is_present >> rail.Label('No') >> catch_and_log_errors

        catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
