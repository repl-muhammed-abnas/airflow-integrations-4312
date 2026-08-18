from datetime import timedelta
from airflow.models import Variable
import rail
from lendingclub.user_import.utils import python_callable
from lendingclub.user_import.utils.request_payload import get_today_dateformat_payload
from lendingclub.user_import.utils.python_callable import get_exceptions


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'lendingclub_user_import_update_supervisor_child_{config.instance}',
        description=f'lendingclub_user_import_update_supervisor_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.update_supervisor_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_supervisor_login_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_supervisor_login_present',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        is_supervisor_login_present = rail.IfOperator(
            task_id='is_supervisor_login_present',
            test="{{ dag_run.conf.managerid | is_truthy }}",
            yes_task="search_for_user_with_empid",
            no_task="log_no_supervisor_loginname",
        )

        search_for_user_with_empid = rail.RepliconServiceOperator(
            task_id='search_for_user_with_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data = {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:login-name"
                ]
            },
            data_handler=python_callable.get_userdata_list_for_managerid
        )

        if_supervisor_not_equals_user = rail.IfOperator(
            task_id='if_supervisor_not_equals_user',
            test="{{ result('search_for_user_with_empid') | is_truthy and \
                dag_run.conf.loginid != result('search_for_user_with_empid')[0]['loginname'] }}",
            yes_task="check_if_single_manageruseruri_present",
            no_task="if_supervisor_not_present",
        )

        if_supervisor_not_present = rail.IfOperator(
            task_id='if_supervisor_not_present',
            test="{{ result('search_for_user_with_empid') | is_falsy }}",
            yes_task="log_supervisor_absent",
            no_task="log_supervisor_equals_user",
        )

        check_if_single_manageruseruri_present = rail.IfOperator(
            task_id='check_if_single_manageruseruri_present',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) == 1 ),
            yes_task="get_manager_details",
            no_task="check_if_manageruri_absent",
        )

        get_manager_details = rail.RepliconServiceOperator(
            task_id='get_manager_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "uri": "{{ result('search_for_user_with_empid')[0]['uri'] }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_manager_details_present_and_enabled = rail.IfOperator(
            task_id='if_manager_details_present_and_enabled',
            test="{{ result('get_manager_details') | is_truthy and result('get_manager_details')[0]['userDetails']['isEnabled'] }}",
            yes_task="get_assigned_permissionset_foruser",
            no_task="log_supervisor_absent",
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ result('search_for_user_with_empid')[0]['uri'] }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri', '')
        )

        if_supervisor_permission_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_assigned',
            test="{{ result('get_assigned_permissionset_foruser') | is_truthy }}",
            yes_task="if_type_equals_add",
            no_task="log_supervisor_not_assigned",
        )

        if_type_equals_add = rail.IfOperator(
            task_id='if_type_equals_add',
            test="{{ dag_run.conf.type == 'add' }}",
            yes_task="update_supervisor_for_add",
            no_task="update_supervisor",
        )

        update_supervisor_for_add = rail.RepliconServiceOperator(
            task_id='update_supervisor_for_add',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_for_user_with_empid')[0]['uri'] }}"
            }
        )

        update_supervisor = rail.RepliconServiceOperator(
            task_id='update_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_for_user_with_empid')[0]['uri'] }}",
                "dateRange": {
                    "startDate" : get_today_dateformat_payload()
                }
            }
        )

        check_if_manageruri_absent = rail.IfOperator(
            task_id='check_if_manageruri_absent',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) == 0 ),
            yes_task="log_supervisor_absent",
            no_task="search_userimport_logs_for_user_and_delete_to_update",
        )

        log_no_supervisor_loginname = rail.PythonOperator(
            task_id='log_no_supervisor_loginname',
            python_callable=lambda dag_run: "Supervisor not assigned since Supervisor loginname is not present"
        )

        log_supervisor_equals_user = rail.PythonOperator(
            task_id='log_supervisor_equals_user',
            python_callable=lambda dag_run: "Supervisor not assigned since Supervisor and user are same"
        )

        log_supervisor_not_assigned = rail.PythonOperator(
            task_id='log_supervisor_not_assigned',
            python_callable=lambda dag_run: "Supervisor not assigned since Supervisor doesn't have the required permissions assigned"
        )

        log_supervisor_absent = rail.PythonOperator(
            task_id='log_supervisor_absent',
            python_callable=lambda dag_run: "Supervisor not assigned since supervisor isn't present in replicon"
        )

        search_userimport_logs_for_user_and_delete_to_update = rail.FilterLogEntriesOperator(
            task_id='search_userimport_logs_for_user_and_delete_to_update',
            log="{{ dag_run.conf.logger }}",
            properties={
                "UserID": "{{ dag_run.conf.loginid }}" + "|" + "{{ dag_run.conf.empid }}"
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
            no_task="catch_and_log_error",
        )

        add_updated_log = rail.WriteLogOperator(
            task_id='add_updated_log',
            log="{{dag_run.conf.logger}}",
            message='na',
            severity=lambda: 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['Status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['Status']),
            properties=lambda: {
                "UserID": rail.result('load_found_logs_entry')[0]['properties']['UserID'],
                "Action": rail.result('load_found_logs_entry')[0]['properties']['Action'],
                "Status": 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['Status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['Status']),
                "Details": rail.result('load_found_logs_entry')[0]['properties']['Details'] + ',' + get_exceptions()
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "UserID": "{{ dag_run.conf.loginid }}" + "|" + "{{ dag_run.conf.empid }}",
                "Action": "{{ dag_run.conf.type }}",
                "Status": "Error",
                "Details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> is_supervisor_login_present

        is_supervisor_login_present >> rail.Label('Yes') >> search_for_user_with_empid >> if_supervisor_not_equals_user
        is_supervisor_login_present >> rail.Label('No') >> log_no_supervisor_loginname >> search_userimport_logs_for_user_and_delete_to_update

        if_supervisor_not_equals_user >> rail.Label('Yes') >> check_if_single_manageruseruri_present
        if_supervisor_not_equals_user >> rail.Label('No') >> if_supervisor_not_present

        if_supervisor_not_present >> rail.Label('Yes') >> log_supervisor_absent >> search_userimport_logs_for_user_and_delete_to_update
        if_supervisor_not_present >> rail.Label('No') >> log_supervisor_equals_user >> search_userimport_logs_for_user_and_delete_to_update

        check_if_single_manageruseruri_present >> rail.Label('Yes') >> get_manager_details >> if_manager_details_present_and_enabled
        check_if_single_manageruseruri_present >> rail.Label('No') >> check_if_manageruri_absent

        if_manager_details_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_assigned
        if_manager_details_present_and_enabled >> rail.Label('No') >> log_supervisor_absent >> search_userimport_logs_for_user_and_delete_to_update

        if_supervisor_permission_assigned >> rail.Label('Yes') >> if_type_equals_add
        if_supervisor_permission_assigned >> rail.Label('No') >> log_supervisor_not_assigned >> search_userimport_logs_for_user_and_delete_to_update

        if_type_equals_add >> rail.Label('Yes') >> update_supervisor_for_add >> search_userimport_logs_for_user_and_delete_to_update
        if_type_equals_add >> rail.Label('No') >> update_supervisor >> search_userimport_logs_for_user_and_delete_to_update

        check_if_manageruri_absent >> rail.Label('Yes') >> log_supervisor_absent >> search_userimport_logs_for_user_and_delete_to_update
        check_if_manageruri_absent >> rail.Label('No') >> search_userimport_logs_for_user_and_delete_to_update

        search_userimport_logs_for_user_and_delete_to_update >> load_found_logs_entry >> if_entry_is_present
        if_entry_is_present >> rail.Label('Yes') >> add_updated_log >> catch_and_log_error
        if_entry_is_present >> rail.Label('No') >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
