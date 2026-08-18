import rail
from impervainc.user_sync.utils import python_callable, request_payload, response_filter

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_supervisor_assignment_child,
        description=f'impervainc supervisor assignment child dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,
                "displayText", "Supervisor", "uri")
        )

        if_supervisor_loginname_present = rail.IfOperator(
            task_id='if_supervisor_loginname_present',
            test="{{dag_run.conf.supervisorloginname | is_truthy}}",
            yes_task="search_supervisor_in_replicon",
            no_task="catch_error",
        )

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id="search_supervisor_in_replicon",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_user_payload,
            data_handler=response_filter.get_filtered_supervisor_data
        )

        if_supervisor_uri_present_and_status_enable = rail.IfOperator(
            task_id='if_supervisor_uri_present_and_status_enable',
            test="{{result('search_supervisor_in_replicon') | is_truthy and result('search_supervisor_in_replicon')[0].uri | is_truthy and \
                result('search_supervisor_in_replicon')[0].status | is_truthy}}",
            yes_task="get_supervisor_assigned_permissionsets",
            no_task="search_user_sync_log_to_delete_and_update_25",
        )

        get_supervisor_assigned_permissionsets = rail.RepliconServiceOperator(
            task_id='get_supervisor_assigned_permissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_supervisor_in_replicon')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.uri', '')
        )

        if_supervisor_policyuri_present = rail.IfOperator(
            task_id='if_supervisor_policyuri_present',
            test="{{result('get_supervisor_assigned_permissionsets') | is_truthy}}",
            yes_task="if_type_is_add",
            no_task="search_user_sync_log_to_delete_and_update_21",
        )

        if_type_is_add = rail.IfOperator(
            task_id='if_type_is_add',
            test="{{dag_run.conf.type.lower() == 'add'}}",
            yes_task="assign_initial_supervisor",
            no_task="assign_supervisor_with_effectivedate"
        )

        assign_initial_supervisor = rail.RepliconServiceOperator(
            task_id='assign_initial_supervisor',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data= {
                'userUri': "{{dag_run.conf.useruri}}",
                'supervisorUri': "{{result('search_supervisor_in_replicon')[0].uri}}",
                'dateRange': None
            }
        )

        assign_supervisor_with_effectivedate = rail.RepliconServiceOperator(
            task_id='assign_supervisor_with_effectivedate',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('search_supervisor_in_replicon')[0]['uri'],
                'dateRange': {
                    "startDate":python_callable.get_current_date_time()
                }
            }
        )

        search_user_sync_log_to_delete_and_update_21 = rail.FilterLogEntriesOperator(
            task_id='search_user_sync_log_to_delete_and_update_21',
            log="{{ dag_run.conf.user_sync_log }}",
            properties={
                "loginname": "{{dag_run.conf.loginname}}"
            },
            remove_filtered_entries=True
        )

        if_entry_is_present_22 = rail.IfOperator(
            task_id='if_entry_is_present_22',
            test='''{{ result('search_user_sync_log_to_delete_and_update_21','length') > 0 | is_truthy }}''',
            yes_task="load_found_logs_entry_23",
            no_task="catch_error",
        )

        load_found_logs_entry_23 = rail.PythonOperator(
            task_id='load_found_logs_entry_23',
            python_callable=lambda: rail.load_all_records(rail.result(
                'search_user_sync_log_to_delete_and_update_21'))
        )

        update_user_log_to_error_23 = rail.WriteLogOperator(
            task_id='update_user_log_to_error_23',
            message="NA",
            log="{{ dag_run.conf.user_sync_log }}",
            severity='Error',
            properties=lambda: {
                "parentjobid": rail.result('load_found_logs_entry_23')[0]['properties']['parentjobid'],
                "childjobid": rail.result('load_found_logs_entry_23')[0]['properties']['childjobid'],
                "loginname": rail.result('load_found_logs_entry_23')[0]['properties']['loginname'],
                "employeeid": rail.result('load_found_logs_entry_23')[0]['properties']['employeeid'],
                "status": 'Error' if 'Error' in rail.result('load_found_logs_entry_23')[0]['properties']['status'] else 'Warning',
                "reason": 'Supervisor not assigned, since supervisor  doesn\'t have necessary permission' + ' ,' + \
                    rail.result('load_found_logs_entry_23')[0]['properties']['reason'],
                "action": rail.result('load_found_logs_entry_23')[0]['properties']['action'],
                "country": rail.result('load_found_logs_entry_23')[0]['properties']['country']
            }
        )

        search_user_sync_log_to_delete_and_update_25 = rail.FilterLogEntriesOperator(
            task_id='search_user_sync_log_to_delete_and_update_25',
            log="{{ dag_run.conf.user_sync_log }}",
            properties={
                "loginname": "{{dag_run.conf.loginname}}"
            },
            remove_filtered_entries=True
        )

        if_entry_is_present_26 = rail.IfOperator(
            task_id='if_entry_is_present_26',
            test='''{{ result('search_user_sync_log_to_delete_and_update_25','length') > 0 | is_truthy }}''',
            yes_task="load_found_logs_entry_27",
            no_task="catch_error",
        )

        load_found_logs_entry_27 = rail.PythonOperator(
            task_id='load_found_logs_entry_27',
            python_callable=lambda: rail.load_all_records(rail.result(
                'search_user_sync_log_to_delete_and_update_25'))
        )

        update_user_log_to_error_27 = rail.WriteLogOperator(
            task_id='update_user_log_to_error_27',
            message="NA",
            log="{{ dag_run.conf.user_sync_log }}",
            severity='Error',
            properties=lambda: {
                "parentjobid": rail.result('load_found_logs_entry_27')[0]['properties']['parentjobid'],
                "childjobid": rail.result('load_found_logs_entry_27')[0]['properties']['childjobid'],
                "loginname": rail.result('load_found_logs_entry_27')[0]['properties']['loginname'],
                "employeeid": rail.result('load_found_logs_entry_27')[0]['properties']['employeeid'],
                "status": 'Error' if 'Error' in rail.result('load_found_logs_entry_27')[0]['properties']['status'] else 'Warning',
                "reason": 'Supervisor not assigned, since supervisor is not available to assign in Replicon' + ' ,' + \
                    rail.result('load_found_logs_entry_27')[0]['properties']['reason'],
                "action": rail.result('load_found_logs_entry_27')[0]['properties']['action'],
                "country": rail.result('load_found_logs_entry_27')[0]['properties']['country']
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        search_user_sync_log_to_delete_and_update = rail.FilterLogEntriesOperator(
            task_id='search_user_sync_log_to_delete_and_update',
            log="{{ dag_run.conf.user_sync_log }}",
            properties={
                "loginname": "{{dag_run.conf.loginname}}"
            },
            remove_filtered_entries=True
        )

        if_entry_is_present = rail.IfOperator(
            task_id='if_entry_is_present',
            test='''{{ result('search_user_sync_log_to_delete_and_update','length') > 0 | is_truthy }}''',
            yes_task="load_found_logs_entry_31",
            no_task="log_to_sumo",
        )

        load_found_logs_entry_31 = rail.PythonOperator(
            task_id='load_found_logs_entry_31',
            python_callable=lambda: rail.load_all_records(rail.result(
                'search_user_sync_log_to_delete_and_update'))
        )

        def get_error_message():
            error_message = rail.render_template("{{get_error_message()}}")
            message = error_message + rail.render_template("{{ecid()}}")
            return message

        update_user_log_to_error = rail.WriteLogOperator(
            task_id='update_user_log_to_error',
            message="NA",
            log="{{ dag_run.conf.user_sync_log }}",
            severity='Error',
            properties=lambda: {
                "parentjobid": rail.result('load_found_logs_entry_31')[0]['properties']['parentjobid'],
                "childjobid": rail.result('load_found_logs_entry_31')[0]['properties']['childjobid'],
                "loginname": rail.result('load_found_logs_entry_31')[0]['properties']['loginname'],
                "employeeid": rail.result('load_found_logs_entry_31')[0]['properties']['employeeid'],
                "status": 'Error',
                "reason": 'Supervisor not assigned,' + get_error_message() + ',' + \
                    rail.result('load_found_logs_entry_31')[0]['properties']['reason'],
                "action": rail.result('load_found_logs_entry_31')[0]['properties']['action'],
                "country": rail.result('load_found_logs_entry_31')[0]['properties']['country']
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        get_all_permissionsets >> if_supervisor_loginname_present >> rail.Label(
            "Yes") >> search_supervisor_in_replicon >> if_supervisor_uri_present_and_status_enable >> rail.Label(
            "Yes") >> get_supervisor_assigned_permissionsets >> if_supervisor_policyuri_present >> rail.Label(
            "Yes") >> if_type_is_add >> rail.Label("Yes") >> assign_initial_supervisor >> catch_error
        if_type_is_add >> rail.Label("No") >> assign_supervisor_with_effectivedate >> catch_error
        if_supervisor_policyuri_present >> rail.Label(
            "No") >> search_user_sync_log_to_delete_and_update_21 >> if_entry_is_present_22 >> rail.Label(
            "Yes") >> load_found_logs_entry_23 >> update_user_log_to_error_23 >> catch_error
        if_entry_is_present_22 >> rail.Label(
            "No") >> catch_error
        if_supervisor_uri_present_and_status_enable >> rail.Label(
            "No") >> search_user_sync_log_to_delete_and_update_25 >> if_entry_is_present_26 >> rail.Label(
            "Yes") >> load_found_logs_entry_27 >> update_user_log_to_error_27 >> catch_error
        if_entry_is_present_26 >> rail.Label(
            "No") >> catch_error
        if_supervisor_loginname_present >> rail.Label(
            "No") >> catch_error
        catch_error >> search_user_sync_log_to_delete_and_update >> if_entry_is_present >> rail.Label(
            "Yes") >> load_found_logs_entry_31 >> update_user_log_to_error >> log_to_sumo
        if_entry_is_present >> rail.Label(
            "No") >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
