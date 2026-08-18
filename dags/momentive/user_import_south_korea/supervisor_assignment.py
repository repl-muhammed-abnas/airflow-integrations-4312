from datetime import timedelta
from airflow.models import Variable
import rail
from momentive.user_import_south_korea.utils import request_payload, python_callable
from momentive.user_import_south_korea.utils.python_callable import get_exceptions

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'momentive_userimport_supervisor_assignment_child_{config.instance}',
        description=f'momentive_userimport_supervisor_assignment_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.supervisor_assignment_child_dag_active_runs,
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
            no_task='get_all_permissionsets'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_permissionsets',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: {
                'supervisor': rail.find_first_by_attr_and_get_attr(
                    response,'name',"Supervisor - Edit",'uri')
            }
        )

        if_managerid_equals_loginname = rail.IfOperator(
            task_id='if_managerid_equals_loginname',
            test="{{ dag_run.conf.managerid != dag_run.conf.loginname }}",
            yes_task="search_for_user_with_empid",
            no_task="catch_and_log_error",
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

        check_if_multiple_manageruseruri_present = rail.IfOperator(
            task_id='check_if_multiple_manageruseruri_present',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) > 1 ),
            yes_task="log_multiple_user_for_same_managerid",
            no_task="if_supervisor_present",
        )

        log_multiple_user_for_same_managerid = rail.PythonOperator(
            task_id='log_multiple_user_for_same_managerid',
            python_callable=lambda: 'Supervisor not assigned sincemultiple users found with same EMP id'
        )

        if_supervisor_present = rail.IfOperator(
            task_id='if_supervisor_present',
            test="{{ result('search_for_user_with_empid') | is_truthy and \
                result('search_for_user_with_empid')[0].uri | is_truthy}}",
            yes_task="get_manager_details",
            no_task="if_supervisor_email_present",
        )

        get_manager_details = rail.RepliconServiceOperator(
            task_id='get_manager_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = request_payload.get_manager_details_payload
        )

        if_manager_details_present_and_enabled = rail.IfOperator(
            task_id='if_manager_details_present_and_enabled',
            test="{{ result('get_manager_details') | is_truthy and result('get_manager_details')[0]['userDetails']['isEnabled'] | is_truthy }}",
            yes_task="get_assigned_permissionset_foruser",
            no_task="if_manager_disabled",
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ result('search_for_user_with_empid')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.uri', '')
        )

        if_supervisor_permission_not_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_not_assigned',
            test="{{ result('get_assigned_permissionset_foruser') | is_falsy }}",
            yes_task="add_missing_supervisor_permission",
            no_task="if_type_is_add",
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=request_payload.add_missing_supervisor_permission_payload
        )

        if_type_is_add = rail.IfOperator(
            task_id='if_type_is_add',
            test="{{ dag_run.conf.type == 'add' }}",
            yes_task="update_supervisor_25",
            no_task="update_supervisor_27",
        )

        update_supervisor_25 = rail.RepliconServiceOperator(
            task_id='update_supervisor_25',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_for_user_with_empid')[0]['uri']
            }
        )

        update_supervisor_27 = rail.RepliconServiceOperator(
            task_id='update_supervisor_27',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_for_user_with_empid')[0]['uri'],
                "dateRange":{
                    "startDate": request_payload.get_datetime_obj(dag_run.conf['sup_change_effective_date'])
                }
            }
        )

        if_manager_disabled = rail.IfOperator(
            task_id='if_manager_disabled',
            test="{{ result('get_manager_details')[0]['userDetails']['isEnabled'] | is_falsy }}",
            yes_task="log_supervisor_disabled",
            no_task="if_supervisor_email_present",
        )

        log_supervisor_disabled = rail.PythonOperator(
            task_id='log_supervisor_disabled',
            python_callable=lambda: 'Supervisor not assigned since supervisor is disabled in Replicon'
        )

        if_supervisor_email_present = rail.IfOperator(
            task_id='if_supervisor_email_present',
            test="{{ dag_run.conf.sup_email | is_truthy }}",
            yes_task="create_supervisor",
            no_task="log_foreign_supervisor_not_received",
        )

        create_supervisor = rail.RepliconServiceOperator(
            task_id = "create_supervisor",
            endpoint = "/services/ImportService1.svc/PutUser3",
            data = request_payload.create_supervisor_payload
        )

        remove_all_timeoffs = rail.RepliconServiceOperator(
            task_id='remove_all_timeoffs',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ result('create_supervisor').uri }}",
                'timeOffTypeUris': []
            }
        )

        if_supervisor_uri_present = rail.IfOperator(
            task_id='if_supervisor_uri_present',
            test="{{ result('create_supervisor').uri | is_truthy }}",
            yes_task="if_type_is_add_36",
            no_task="log_foreign_supervisor_not_received",
        )

        if_type_is_add_36 = rail.IfOperator(
            task_id='if_type_is_add_36',
            test="{{ dag_run.conf.type == 'add' }}",
            yes_task="update_supervisor_38",
            no_task="update_supervisor_40",
        )

        update_supervisor_38 = rail.RepliconServiceOperator(
            task_id='update_supervisor_38',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('create_supervisor')['uri']
            }
        )

        update_supervisor_40 = rail.RepliconServiceOperator(
            task_id='update_supervisor_40',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('create_supervisor')['uri'],
                "dateRange":{
                    "startDate": request_payload.get_datetime_obj(dag_run.conf['sup_change_effective_date'])
                }
            }
        )

        log_created_and_updated = rail.PythonOperator(
            task_id='log_created_and_updated',
            python_callable=lambda: 'Foreign supervisor created and updated for the user'
        )

        log_foreign_supervisor_not_received = rail.PythonOperator(
            task_id='log_foreign_supervisor_not_received',
            python_callable=lambda: 'Supervisor not updated since Foreign supervisor ID was not received'
        )

        search_userimport_logs_for_user_and_delete_to_update = rail.FilterLogEntriesOperator(
            task_id='search_userimport_logs_for_user_and_delete_to_update',
            log="{{ dag_run.conf.logger }}",
            properties={
                "userid": "{{dag_run.conf.userid}}"
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
            severity=lambda: 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['status']),
            properties=lambda: {
                "userid": rail.result('load_found_logs_entry')[0]['properties']['userid'],
                "username": rail.result('load_found_logs_entry')[0]['properties']['username'],
                "action": rail.result('load_found_logs_entry')[0]['properties']['action'],
                "country": rail.result('load_found_logs_entry')[0]['properties']['country'],
                "status": 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['status']),
                "details": rail.result('load_found_logs_entry')[0]['properties']['details'] + ',' + \
                    get_exceptions() if get_exceptions() else rail.result('log_created_and_updated')
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            trigger_rule='one_failed',
            message="Error",
            severity='Error',
            properties={
                "userid": "{{ result('load_found_logs_entry')[0].properties.userid'] }}",
                "username": "{{ result('load_found_logs_entry')[0].properties.username }}",
                "action": "{{ result('load_found_logs_entry')[0].properties.action }}",
                "country": "{{ result('load_found_logs_entry')[0].properties.country }}",
                "status": 'Error',
                "details": "{{ result('load_found_logs_entry')[0].properties.details }}" + '' + "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_all_permissionsets

        get_all_permissionsets >> if_managerid_equals_loginname

        if_managerid_equals_loginname >> rail.Label('Yes') >> search_for_user_with_empid >> check_if_multiple_manageruseruri_present
        if_managerid_equals_loginname >> rail.Label('No') >> catch_and_log_error

        check_if_multiple_manageruseruri_present >> rail.Label('Yes') >> log_multiple_user_for_same_managerid >> search_userimport_logs_for_user_and_delete_to_update
        check_if_multiple_manageruseruri_present >> rail.Label('No') >> if_supervisor_present

        if_supervisor_present >> rail.Label('Yes') >> get_manager_details >> if_manager_details_present_and_enabled
        if_supervisor_present >> rail.Label('No') >> if_supervisor_email_present

        if_manager_details_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_not_assigned
        if_manager_details_present_and_enabled >> rail.Label('No') >> if_manager_disabled

        if_supervisor_permission_not_assigned >> rail.Label('Yes') >> add_missing_supervisor_permission >> if_type_is_add
        if_supervisor_permission_not_assigned >> rail.Label('No') >> if_type_is_add

        if_type_is_add >> rail.Label('Yes') >> update_supervisor_25 >> search_userimport_logs_for_user_and_delete_to_update
        if_type_is_add >> rail.Label('No') >> update_supervisor_27 >> search_userimport_logs_for_user_and_delete_to_update

        if_manager_disabled >> rail.Label('Yes') >> log_supervisor_disabled >> search_userimport_logs_for_user_and_delete_to_update
        if_manager_disabled >> rail.Label('No') >> if_supervisor_email_present

        if_supervisor_email_present >> rail.Label('Yes') >> create_supervisor >> remove_all_timeoffs >> if_supervisor_uri_present
        if_supervisor_email_present >> rail.Label('No') >> log_foreign_supervisor_not_received >> search_userimport_logs_for_user_and_delete_to_update

        if_supervisor_uri_present >> rail.Label('Yes') >> if_type_is_add_36
        if_supervisor_uri_present >> rail.Label('No') >> log_foreign_supervisor_not_received >> search_userimport_logs_for_user_and_delete_to_update

        if_type_is_add_36 >> rail.Label('Yes') >> update_supervisor_38 >> log_created_and_updated >> search_userimport_logs_for_user_and_delete_to_update
        if_type_is_add_36 >> rail.Label('No') >> update_supervisor_40 >> log_created_and_updated >> search_userimport_logs_for_user_and_delete_to_update

        search_userimport_logs_for_user_and_delete_to_update >> load_found_logs_entry >> if_entry_is_present

        if_entry_is_present >> rail.Label('Yes') >> add_updated_log >> catch_and_log_error
        if_entry_is_present >> rail.Label('No') >> catch_and_log_error

        catch_and_log_error

        catch_and_log_error >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
