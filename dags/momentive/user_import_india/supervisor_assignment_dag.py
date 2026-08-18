from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from momentive.user_import_india.utils.python_callable import split_date_string, get_userdata_list_for_managerid, get_exceptions
from momentive.user_import_india.utils import request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_india_user_sync_supervisor_assignment_dag_id,
        description=f'Momentive_User Import India_Supervisor assignment {config.instance}',
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
            no_task='get_all_permission_sets'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_permission_sets',
            end_task='catch_and_log_error_find_entry',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: {
                "supervisor": rail.find_first_by_attr_and_get_attr(response, 'displayText', "Supervisor", 'uri', '')
            }
        )

        if_request_supervisorloginname_present_5 = rail.IfOperator(
            task_id='if_request_supervisorloginname_present_5',
            test='''{{ dag_run.conf.supervisorloginname | is_truthy }}''',
            yes_task="get_split_dates",
            no_task="catch_and_log_error_find_entry",
        )

        get_split_dates = rail.PythonOperator(
            task_id="get_split_dates",
            python_callable=lambda dag_run: {
                "todays_date": split_date_string(datetime.now()),
                "sup_eff_date": split_date_string(dag_run.conf['sup_change_effectivedate'])
            }
        )

        if_supervisorloginname_not_equals_to_loginname_8 = rail.IfOperator(
            task_id='if_supervisorloginname_not_equals_to_loginname_8',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="search_for_user_with_empid",
            no_task="catch_and_log_error_find_entry",
        )

        search_for_user_with_empid = rail.RepliconServiceOperator(
            task_id='search_for_user_with_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:login-name"
                ]
            },
            data_handler=get_userdata_list_for_managerid
        )

        if_multiple_users_found_11 = rail.IfOperator(
            task_id='if_multiple_users_found_11',
            test=lambda: rail.result('search_for_user_with_empid') and len(
                rail.result('search_for_user_with_empid')) > 1,
            yes_task="search_userimport_logs_for_user_and_delete_to_update",
            no_task="if_supervisor_present",
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
            data=request_payload.get_manager_details_payload
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
            data={
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
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_for_user_with_empid')[0]['uri']
            }
        )

        update_supervisor_27 = rail.RepliconServiceOperator(
            task_id='update_supervisor_27',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_for_user_with_empid')[0]['uri'],
                "dateRange": {
                    "startDate": split_date_string(dag_run.conf['sup_change_effective_date'], 'datetime')
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
            task_id="create_supervisor",
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.create_supervisor_payload
        )

        if_supervisor_uri_present = rail.IfOperator(
            task_id='if_supervisor_uri_present',
            test="{{ result('create_supervisor').uri | is_truthy }}",
            yes_task="if_type_is_add_23",
            no_task="log_foreign_supervisor_not_received",
        )

        if_type_is_add_23 = rail.IfOperator(
            task_id='if_type_is_add_23',
            test="{{ dag_run.conf.type == 'add' }}",
            yes_task="update_supervisor_24",
            no_task="update_supervisor_26",
        )

        update_supervisor_24 = rail.RepliconServiceOperator(
            task_id='update_supervisor_24',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('create_supervisor')['uri']
            }
        )

        update_supervisor_26 = rail.RepliconServiceOperator(
            task_id='update_supervisor_26',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('create_supervisor')['uri'],
                "dateRange": {
                    "startDate": split_date_string(dag_run.conf['sup_change_effective_date'], 'datetime')
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
            log="{{ dag_run.conf.user_import_logs }}",
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
            no_task="catch_and_log_error_find_entry",
        )

        add_updated_log = rail.WriteLogOperator(
            task_id='add_updated_log',
            log="{{dag_run.conf.user_import_logs}}",
            message='na',
            severity=lambda: 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['status']),
            properties=lambda: {
                "jobid": rail.result('load_found_logs_entry')[0]['properties']['parentjobid'],
                "userid": rail.result('load_found_logs_entry')[0]['properties']['userid'],
                "username": rail.result('load_found_logs_entry')[0]['properties']['username'],
                "action": rail.result('load_found_logs_entry')[0]['properties']['action'],
                "country": rail.result('load_found_logs_entry')[0]['properties']['country'],
                "status": 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['status']),
                "details": rail.result('load_found_logs_entry')[0]['properties']['details'] + ',' +
                get_exceptions() if get_exceptions() else rail.result('log_created_and_updated'),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
            }
        )

        catch_and_log_error_find_entry = rail.FilterLogEntriesOperator(
            task_id="catch_and_log_error_find_entry",
            log='{{ dag_run.conf.user_import_logs}}',
            trigger_rule='one_failed',
            properties={
                "jobid": "{{dag_run.conf.parentjobid}}",
                "userid": "{{dag_run.conf.userid}}"
            },
            remove_filtered_entries=True
        )

        load_found_log_entry_for_catch_and_log = rail.PythonOperator(
            task_id='load_found_log_entry_for_catch_and_log',
            trigger_rule='all_success',
            python_callable=lambda: rail.load_all_records(
                rail.result('catch_and_log_error_find_entry'))
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.user_import_logs}}',
            trigger_rule='all_success',
            message="na",
            severity='Error',
            properties={
                "jobid": "{{ result('load_found_log_entry_for_catch_and_log')[0].properties.parentjobid }}",
                "userid": "{{ result('load_found_log_entry_for_catch_and_log')[0].properties.userid }}",
                "username": "{{ result('load_found_log_entry_for_catch_and_log')[0].properties.username }}",
                "action": "{{ result('load_found_log_entry_for_catch_and_log')[0].properties.action }}",
                "status": 'Error',
                "details": "{{ result('load_found_log_entry_for_catch_and_log')[0].properties.details }}" + ';' + "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error_find_entry
        can_run_batch_task >> rail.Label('No') >> get_all_permission_sets

        get_all_permission_sets >> if_request_supervisorloginname_present_5

        if_request_supervisorloginname_present_5 >> rail.Label(
            'No') >> catch_and_log_error_find_entry
        if_request_supervisorloginname_present_5 >> rail.Label(
            'Yes') >> get_split_dates >> if_supervisorloginname_not_equals_to_loginname_8

        if_supervisorloginname_not_equals_to_loginname_8 >> rail.Label(
            'No') >> catch_and_log_error_find_entry
        if_supervisorloginname_not_equals_to_loginname_8 >> rail.Label(
            'Yes') >> search_for_user_with_empid >> if_multiple_users_found_11

        if_multiple_users_found_11 >> rail.Label(
            'Yes') >> search_userimport_logs_for_user_and_delete_to_update
        if_multiple_users_found_11 >> rail.Label('No') >> if_supervisor_present

        if_supervisor_present >> rail.Label(
            'Yes') >> get_manager_details >> if_manager_details_present_and_enabled
        if_supervisor_present >> rail.Label(
            'No') >> if_supervisor_email_present

        if_manager_details_present_and_enabled >> rail.Label(
            'Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_not_assigned
        if_manager_details_present_and_enabled >> rail.Label(
            'No') >> if_manager_disabled

        if_supervisor_permission_not_assigned >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> if_type_is_add
        if_supervisor_permission_not_assigned >> rail.Label(
            'No') >> if_type_is_add

        if_type_is_add >> rail.Label(
            'Yes') >> update_supervisor_25 >> search_userimport_logs_for_user_and_delete_to_update
        if_type_is_add >> rail.Label(
            'No') >> update_supervisor_27 >> search_userimport_logs_for_user_and_delete_to_update

        if_manager_disabled >> rail.Label(
            'Yes') >> log_supervisor_disabled >> search_userimport_logs_for_user_and_delete_to_update
        if_manager_disabled >> rail.Label('No') >> if_supervisor_email_present

        if_supervisor_email_present >> rail.Label(
            'Yes') >> create_supervisor >> if_supervisor_uri_present
        if_supervisor_email_present >> rail.Label(
            'No') >> log_foreign_supervisor_not_received >> search_userimport_logs_for_user_and_delete_to_update

        if_supervisor_uri_present >> rail.Label('Yes') >> if_type_is_add_23
        if_supervisor_uri_present >> rail.Label(
            'No') >> log_foreign_supervisor_not_received >> search_userimport_logs_for_user_and_delete_to_update

        if_type_is_add_23 >> rail.Label(
            'Yes') >> update_supervisor_24 >> log_created_and_updated >> search_userimport_logs_for_user_and_delete_to_update
        if_type_is_add_23 >> rail.Label(
            'No') >> update_supervisor_26 >> log_created_and_updated >> search_userimport_logs_for_user_and_delete_to_update

        search_userimport_logs_for_user_and_delete_to_update >> load_found_logs_entry >> if_entry_is_present

        if_entry_is_present >> rail.Label(
            'Yes') >> add_updated_log >> catch_and_log_error_find_entry
        if_entry_is_present >> rail.Label(
            'No') >> catch_and_log_error_find_entry

        catch_and_log_error_find_entry >> load_found_log_entry_for_catch_and_log >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
