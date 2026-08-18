
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'omd_singapore_user_import_add_supervisor_child_{config.instance}',
        description=f'OMD Singapore User Import Add Supervisor V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_supervisor_user_by_id'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_supervisor_user_by_id',
            end_task='searchentries_in_logslookuptable',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_supervisor_user_by_id=rail.RepliconServiceOperator(
            task_id='search_supervisor_user_by_id',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "100",
              "columnUris": [
                  "urn:replicon:user-list-column:login-name",
                  "urn:replicon:user-list-column:employee-id",
                  "urn:replicon:user-list-column:enabled"
              ],
              "sort": [],
              "filterExpression": {
                  "leftExpression": {
                      "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                  },
                  "operatorUri": "urn:replicon:filter-operator:text-search",
                  "rightExpression": {
                      "value": {
                          "text": "{{dag_run.conf.supervisorloginname}}"
                      }
                  }
              }
            },
            data_handler=lambda response,dag_run: list(filter(lambda x: x['cells'][1]['textValue'] == dag_run.conf['supervisorloginname'],response['rows']))
        )

        if_multiple_profiles_found=rail.IfOperator(
            task_id='if_multiple_profiles_found',
            test=lambda: bool(rail.result('search_supervisor_user_by_id') and len(rail.result('search_supervisor_user_by_id')) > 1),
            yes_task="search_entries_in_logs_lookuptable",
            no_task="get_supervisor_uri_by_id",
        )

        search_entries_in_logs_lookuptable=rail.FilterLogEntriesOperator(
            task_id='search_entries_in_logs_lookuptable',
            log="{{ dag_run.conf.logslookuptable }}",
            properties={
                'username': "{{ dag_run.conf.loginname }}",
                'jobid': "{{ dag_run.conf.parentjobid}}"
            }
        )

        load_searched_entry = rail.PythonOperator(
            task_id = 'load_searched_entry',
            python_callable=lambda: rail.load_all_records(rail.result('search_entries_in_logs_lookuptable'))
        )

        if_entry_present=rail.IfOperator(
            task_id='if_entry_present',
            test='''{{ result('search_entries_in_logs_lookuptable','length') > 0 }}''',
            yes_task="delete_entry_to_update_log",
            no_task="delete_entry_to_update_supervisor_lookuptable",
        )

        delete_entry_to_update_log=rail.FilterLogEntriesOperator(
            task_id='delete_entry_to_update_log',
            log="{{ dag_run.conf.logslookuptable }}",
            properties={
                'username': "{{ dag_run.conf.loginname }}",
                'jobid': "{{ dag_run.conf.parentjobid}}"
            },
            remove_filtered_entries=True
        )

        update_log=rail.WriteLogOperator(
            task_id= 'update_log',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity=lambda: "Error" if "Error" in rail.result('load_searched_entry')[0]['status'] else "Exception",
            properties=lambda dag_run: {
                "employeeid": rail.result('load_searched_entry')[0]['employeeid'],
                "username": rail.result('load_searched_entry')[0]['username'],
                "status": "Error" if "Error" in rail.result('load_searched_entry')[0]['status'] else "Exception",
                "action": rail.result('load_searched_entry')[0]['action'],
                "details": ';'.join((("Multiple users found with supervisor ID" + dag_run.conf['supervisorloginname']) 
                            if "No change to the user record in Replicon" in rail.result('load_searched_entry')[0]['details']
                            else ( str(rail.result('load_searched_entry')[0]['details']) + "," + "Multiple users found with supervisor ID" +
                            dag_run.conf['supervisorloginname'])).split(',')),
                "jobid": rail.result('load_searched_entry')[0]['jobid'],
                "childjobid": rail.result('load_searched_entry')[0]['childjobid']
            }
        )

        delete_entry_to_update_supervisor_lookuptable=rail.FilterLogEntriesOperator(
            task_id='delete_entry_to_update_supervisor_lookuptable',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            properties={
                'username': "{{ dag_run.conf.loginname }}",
                'supervisorloginname': "{{ dag_run.conf.supervisorloginname}}"
            },
            remove_filtered_entries=True
        )

        update_entry_in_supervisor_lookuptable = rail.WriteLogOperator(
            task_id = 'update_entry_in_supervisor_lookuptable',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            severity="Completed",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "username": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['supervisorloginname'],
                "action": dag_run.conf['action'],
                "status": "Completed",
                "childjobid": dag_run.conf['childjobid']
            }
        )

        get_supervisor_uri_by_id=rail.PythonOperator(
            task_id='get_supervisor_uri_by_id',
            python_callable= lambda: rail.result('search_supervisor_user_by_id')[0]['cells'][0]['uri']
                                if rail.result('search_supervisor_user_by_id') and
                                rail.result('search_supervisor_user_by_id')[0]['cells'][0]['textValue'] else null
        )

        if_uri_present=rail.IfOperator(
            task_id='if_uri_present',
            test='''{{ result('get_supervisor_uri_by_id') | is_truthy }}''',
            yes_task="get_supervisor_user_status",
            no_task="log_user_with_this_id_not_present",
        )

        get_supervisor_user_status=rail.PythonOperator(
            task_id='get_supervisor_user_status',
            python_callable= lambda: rail.result('search_supervisor_user_by_id')[0]['cells'][2]['textValue']
        )

        if_user_status_present_and_true=rail.IfOperator(
            task_id='if_user_status_present_and_true',
            test='''{{ result('get_supervisor_user_status') | is_truthy  and result('get_supervisor_user_status') == 'True' }}''',
            yes_task="get_assigned_permission_sets",
            no_task="log_user_with_this_id_disabled",
        )

        get_assigned_permission_sets=rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('get_supervisor_uri_by_id') }}"
            }
        )

        if_supervision_permission_not_present=rail.IfOperator(
            task_id='if_supervision_permission_not_present',
            test=lambda: lambda: not bool(rail.find_first_by_attr_and_get_attr(
                            rail.result('get_assigned_permission_sets_for_user'),'policyUri','urn:replicon:policy:supervision','permissionSet.name','')
                            if rail.result('get_assigned_permission_sets_for_user')[0]['policyUri'] else null),
            yes_task="assign_permission_set_to_user",
            no_task="if_action_equals_to_add",
        )

        assign_permission_set_to_user=rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_supervisor_uri_by_id') }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        if_action_equals_to_add=rail.IfOperator(
            task_id='if_action_equals_to_add',
            test='''{{ dag_run.conf.action.lower() =='add' }}''',
            yes_task="update_initial_supervisor",
            no_task="if_action_equals_to_update",
        )

        update_initial_supervisor=rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('get_supervisor_uri_by_id') }}",
                "scheduleEntries": []
            }
        )

        if_action_equals_to_update=rail.IfOperator(
            task_id='if_action_equals_to_update',
            test='''{{ dag_run.conf.action.lower()=='update' }}''',
            yes_task="get_supervisor_effective_date",
            no_task="final_log",
        )

        get_supervisor_effective_date=rail.PythonOperator(
            task_id='get_supervisor_effective_date',
            python_callable= lambda: {
                "day": datetime.now().day,
                "month": datetime.now().month,
                "year": datetime.now().year
            }
        )

        update_supervisor_assignment_schedule_over_date_range=rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('get_supervisor_uri_by_id') }}",
                "dateRange": {
                    "startDate": {
                    "year": "{{ result('get_supervisor_effective_date').year }}",
                    "month": "{{ result('get_supervisor_effective_date').month }}",
                    "day": "{{ result('get_supervisor_effective_date').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_user_with_this_id_disabled=rail.PythonOperator(
            task_id='log_user_with_this_id_disabled',
            python_callable= lambda dag_run:  "Supervisor not assigned since the user with ID: " + dag_run.conf['supervisorloginname'] + " is disabled"
        )

        log_user_with_this_id_not_present=rail.PythonOperator(
            task_id='log_user_with_this_id_not_present',
            python_callable= lambda dag_run:  "Supervisor not assigned since the user with ID: " + dag_run.conf['supervisorloginname'] +"is not available"
        )

        final_log=rail.PythonOperator(
            task_id='final_log',
            python_callable= lambda: (rail.result('log_user_with_this_id_disabled') + "," + (rail.result('log_user_with_this_id_not_present') if
                                rail.result('log_user_with_this_id_not_present') else '')) if
                                rail.result('log_user_with_this_id_disabled') else ( rail.result('log_user_with_this_id_not_present') if
                                rail.result('log_user_with_this_id_not_present') else '')
        )

        search_entries_in_logslookuptable=rail.FilterLogEntriesOperator(
            task_id='search_entries_in_logslookuptable',
            log="{{ dag_run.conf.logslookuptable }}",
            properties={
                'username': "{{ dag_run.conf.loginname }}",
                'jobid': "{{ dag_run.conf.parentjobid}}"
            }
        )

        load_found_entry = rail.PythonOperator(
            task_id = 'load_found_entry',
            python_callable=lambda: rail.load_all_records(rail.result('search_entries_in_logslookuptable'))
        )

        is_entry_present=rail.IfOperator(
            task_id='is_entry_present',
            test='''{{ result('search_entries_in_logslookuptable','length') > 0 }}''',
            yes_task="delete_entry_to_updatelog",
            no_task="delete_entry_to_update_supervisorlookuptable",
        )

        delete_entry_to_updatelog=rail.FilterLogEntriesOperator(
            task_id='delete_entry_to_updatelog',
            log="{{ dag_run.conf.logslookuptable }}",
            properties={
                'username': "{{ dag_run.conf.loginname }}",
                'jobid': "{{ dag_run.conf.parentjobid}}"
            },
            remove_filtered_entries=True
        )

        update_log_in_logs=rail.WriteLogOperator(
            task_id= 'update_log_in_logs',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity=lambda: "Error" if "Error" in rail.result('load_found_entry')[0]['status'] else "Exception",
            properties=lambda: {
                "employeeid": rail.result('load_found_entry')[0]['employeeid'],
                "username": rail.result('load_found_entry')[0]['username'],
                "status": "Error" if "Error" in rail.result('load_found_entry')[0]['status']
                                    else ( ( "Exception" if rail.result('load_found_entry')[0]['status']
                                        else ( "Exception" if rail.result('load_found_entry')[0]['status']
                                            else ( "Error" if rail.result('final_log') else rail.result('load_found_entry')[0]['status'] )) )
                                        if "Skipped" in rail.result('load_found_entry')[0]['status']
                                                else (rail.result('load_found_entry')[0]['status'])),
                "action": rail.result('load_found_entry')[0]['action'],
                "details": ";".join((rail.result('load_found_entry')[0]['details'] + "," + rail.result('final_log')).split(',')),
                "jobid": rail.result('load_found_entry')[0]['jobid'],
                "childjobid": rail.result('load_found_entry')[0]['childjobid'] + "|" + rail.render_template('{{dag_run_ecid()}}')
            }
        )

        delete_entry_to_update_supervisorlookuptable=rail.FilterLogEntriesOperator(
            task_id='delete_entry_to_update_supervisorlookuptable',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            properties={
                'username': "{{ dag_run.conf.loginname }}",
                'supervisorloginname': "{{ dag_run.conf.supervisorloginname}}"
            },
            remove_filtered_entries=True
        )

        update_entry_in_supervisorlookuptable = rail.WriteLogOperator(
            task_id = 'update_entry_in_supervisorlookuptable',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            severity="Completed",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "username": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['supervisorloginname'],
                "action": dag_run.conf['action'],
                "status": "Completed",
                "childjobid": dag_run.conf['childjobid']
            }
        )

        searchentries_in_logslookuptable=rail.FilterLogEntriesOperator(
            task_id='searchentries_in_logslookuptable',
            trigger_rule = 'one_failed',
            log="{{ dag_run.conf.logslookuptable }}",
            properties={
                'username': "{{ dag_run.conf.loginname }}",
                'jobid': "{{ dag_run.conf.parentjobid}}"
            }
        )

        log_found_entry = rail.PythonOperator(
            task_id = 'log_found_entry',
            python_callable=lambda: rail.load_all_records(rail.result('searchentries_in_logslookuptable'))
        )

        is_any_entry_present=rail.IfOperator(
            task_id='is_any_entry_present',
            test='''{{ result('searchentries_in_logslookuptable','length') > 0 }}''',
            yes_task="deleteentry_to_updatelog",
            no_task="delete_entry_to_update_supervisorlookuptable_on_error",
        )

        deleteentry_to_updatelog=rail.FilterLogEntriesOperator(
            task_id='deleteentry_to_updatelog',
            log="{{ dag_run.conf.logslookuptable }}",
            properties={
                'username': "{{ dag_run.conf.loginname }}",
                'jobid': "{{ dag_run.conf.parentjobid}}"
            },
            remove_filtered_entries=True
        )

        update_log_on_error=rail.WriteLogOperator(
            task_id= 'update_log_on_error',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity=lambda: rail.result('log_found_entry')[0]['status'],
            properties=lambda dag_run: {
                "employeeid": rail.result('log_found_entry')[0]['employeeid'],
                "username": rail.result('log_found_entry')[0]['username'],
                "status": rail.result('log_found_entry')[0]['status'],
                "action": rail.result('log_found_entry')[0]['action'],
                "details": ';'.join((rail.result('log_found_entry')[0]['details'] + "," + rail.render_template("{{get_error_message()}}")).split(',')),
                "jobid": rail.result('log_found_entry')[0]['jobid'] + "|" + rail.render_template("{{dag_run_ecid()}}"),
                "childjobid": rail.result('log_found_entry')[0]['jobid']
            }
        )

        delete_entry_to_update_supervisorlookuptable_on_error=rail.FilterLogEntriesOperator(
            task_id='delete_entry_to_update_supervisorlookuptable_on_error',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            properties={
                'username': "{{ dag_run.conf.loginname }}",
                'supervisorloginname': "{{ dag_run.conf.supervisorloginname}}"
            },
            remove_filtered_entries=True
        )

        update_entry_in_supervisorlookuptable_on_error = rail.WriteLogOperator(
            task_id = 'update_entry_in_supervisorlookuptable_on_error',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            severity="Completed",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "username": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['supervisorloginname'],
                "action": dag_run.conf['action'],
                "status": "Completed",
                "childjobid": dag_run.conf['childjobid']
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> searchentries_in_logslookuptable
        can_run_batch_task >> rail.Label('No') >> search_supervisor_user_by_id
        search_supervisor_user_by_id >> if_multiple_profiles_found
        if_multiple_profiles_found >> rail.Label('Yes')  >> search_entries_in_logs_lookuptable >> load_searched_entry >> if_entry_present
        if_entry_present >> rail.Label('Yes')  >> delete_entry_to_update_log >> update_log >> delete_entry_to_update_supervisor_lookuptable
        if_entry_present >> rail.Label(
            'No') >> delete_entry_to_update_supervisor_lookuptable >> update_entry_in_supervisor_lookuptable >> searchentries_in_logslookuptable
        if_multiple_profiles_found >> rail.Label('No') >> get_supervisor_uri_by_id >> if_uri_present
        if_uri_present >> rail.Label('Yes')  >> get_supervisor_user_status >> if_user_status_present_and_true
        if_user_status_present_and_true >> rail.Label('Yes')  >> get_assigned_permission_sets >> if_supervision_permission_not_present
        if_supervision_permission_not_present >> rail.Label('Yes')  >> assign_permission_set_to_user >> if_action_equals_to_add
        if_supervision_permission_not_present >> rail.Label('No') >> if_action_equals_to_add
        if_action_equals_to_add >> rail.Label('Yes')  >> update_initial_supervisor >> if_action_equals_to_update
        if_action_equals_to_add >> rail.Label('No') >> if_action_equals_to_update
        if_action_equals_to_update >> rail.Label('Yes')  >> get_supervisor_effective_date >> update_supervisor_assignment_schedule_over_date_range >> final_log
        if_action_equals_to_update >> rail.Label('No') >> final_log
        if_user_status_present_and_true >> rail.Label('No') >> log_user_with_this_id_disabled >> final_log
        if_uri_present >> rail.Label(
            'No') >> log_user_with_this_id_not_present >> final_log >> search_entries_in_logslookuptable >> load_found_entry >> is_entry_present
        is_entry_present >> rail.Label('Yes')  >> delete_entry_to_updatelog >> update_log_in_logs >> delete_entry_to_update_supervisorlookuptable
        delete_entry_to_update_supervisorlookuptable >> update_entry_in_supervisorlookuptable >> searchentries_in_logslookuptable
        is_entry_present >> rail.Label('No') >> delete_entry_to_update_supervisorlookuptable >> update_entry_in_supervisorlookuptable
        update_entry_in_supervisorlookuptable >> searchentries_in_logslookuptable >> log_found_entry >> is_any_entry_present
        is_any_entry_present >> rail.Label('Yes') >> deleteentry_to_updatelog >> update_log_on_error
        update_log_on_error >> delete_entry_to_update_supervisorlookuptable_on_error >> update_entry_in_supervisorlookuptable_on_error
        is_any_entry_present >> rail.Label(
            'No') >> delete_entry_to_update_supervisorlookuptable_on_error >> update_entry_in_supervisorlookuptable_on_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
