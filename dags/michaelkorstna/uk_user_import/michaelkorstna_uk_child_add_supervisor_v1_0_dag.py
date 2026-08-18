
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_uk_user_import_add_supervisor_child_{config.instance}',
        description=f'MichaelKorsTnA UK_Child_Add Supervisor V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_users_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_supervisor_uri_and_status(response, dag_run):
            users_found = response['rows']
            supervisor = {}
            for user in users_found:
                if user['cells'][0]['textValue'] == dag_run.conf['supervisorloginname']:
                    supervisor = user
                    break
            return {
                'uri': supervisor['cells'][0]['uri'] if supervisor else '',
                'status': supervisor['cells'][1]['textValue'] if supervisor else '',
                'employeetype': supervisor['cells'][2].get('textValue') if supervisor else ''
            }

        search_users_3 = rail.RepliconServiceOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:employee-type"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{ dag_run.conf.supervisorloginname }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_supervisor_uri_and_status
        )

        if_log_getsupervisor_uri_4_blank_5 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_blank_5',
            test='''{{ result('search_users_3').uri | is_falsy }}''',
            yes_task="stop_6",
            no_task="if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7",
        )

        stop_6 = rail.FailOperator(
            task_id='stop_6',
            message='''Supervisor "{{ dag_run.conf.supervisorloginname }}" not available'''
        )

        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7 = rail.IfOperator(
            task_id='if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="if_log_getsupervisor_status_8_equals_to_false_9",
            no_task="log_errorfor_supervisorand_userslogin_nameissame_40",
        )

        if_log_getsupervisor_status_8_equals_to_false_9 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_8_equals_to_false_9',
            test='''{{ result('search_users_3').status == 'False' }}''',
            yes_task="if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11",
            no_task="if_log_getsupervisor_status_8_equals_to_true_15",
        )

        if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11 = rail.IfOperator(
            task_id='if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11',
            test='''{{ result('search_users_3').employeetype != 'Foreign Supervisor' }}''',
            yes_task="log_errorwhensupervisorisdisabled_12",
            no_task="_adhoc_http_action_14",
        )

        log_errorwhensupervisorisdisabled_12 = rail.PythonOperator(
            task_id='log_errorwhensupervisorisdisabled_12',
            python_callable=lambda dag_run:  "Supervsior assignment/update is not done user " +
            dag_run.conf['supervisorloginname'] + " is disabled"
        )

        _adhoc_http_action_14 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_14',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('search_users_3').uri }}"
            }
        )

        if_log_getsupervisor_status_8_equals_to_true_15 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_8_equals_to_true_15',
            test='''{{ result('search_users_3').status == 'True'  or result('search_users_3').employeetype == 'Foreign Supervisor' }}''',
            yes_task="get_permissions_from_mapper",
            no_task="michael_kors_gmbh_user_sync_logs_search_entries_41",
        )

        get_permissions_from_mapper = rail.PythonOperator(
            task_id='get_permissions_from_mapper',
            python_callable=lambda dag_run:  list({permission['value'] for permission in list(filter(lambda x: x["type"] == "Permission" and x[
                "identifier__1"] == "Supervisor" and x["country"] == dag_run.conf['country'], config.michael_kors_gmbh_user_sync_master_mapper_uk))})
        )

        _adhoc_http_action_18 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_18',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_3').uri }}",
            },
            data_handler=lambda response: {
                'supervisorpermission': rail.find_first_by_attr_and_get_attr(response,'policyUri','urn:replicon:policy:supervision','permissionSet.name',''),
                'userpermission': rail.find_first_by_attr_and_get_attr(response,'policyUri','urn:replicon:policy:user','permissionSet.name',''),
                'schedulemanagementpermission': rail.find_first_by_attr_and_get_attr(response, 'policyUri',
                    'urn:replicon:policy:schedule-management', 'permissionSet.name', '')
            }
        )

        if_log_supervisor_permissiontobeassigned_17_not_contains_dataloggerlog_checkifthe_supervisor_permissionisassigned_19message_22 = rail.IfOperator(
            task_id='if_log_supervisor_permissiontobeassigned_17_not_contains_dataloggerlog_checkifthe_supervisor_permissionisassigned_19message_22',
            test=lambda: (rail.result('_adhoc_http_action_18')['supervisorpermission'] not in rail.result('get_permissions_from_mapper')) or (rail.result(
                '_adhoc_http_action_18')['userpermission'] not in rail.result('get_permissions_from_mapper')) or (rail.result(
                '_adhoc_http_action_18')['schedulemanagementpermission'] not in rail.result('get_permissions_from_mapper')) or not (rail.result(
                '_adhoc_http_action_18')['supervisorpermission']) or not (rail.result('_adhoc_http_action_18')['userpermission']) or not (rail.result(
                '_adhoc_http_action_18')['schedulemanagementpermission']),
            yes_task="_adhoc_http_action_23",
            no_task="if_action_downcase_equals_to_add_31",
        )

        _adhoc_http_action_23 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_23',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        foreach_create_list_25_26 = rail.ForEachOperator(
            task_id='foreach_create_list_25_26',
            items=lambda: rail.result('get_permissions_from_mapper'),
            start_task='assign_permission_set_to_user_29',
            end_task='foreach_create_list_25_26_end'
        )


        assign_permission_set_to_user_29 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_29',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: {
                "userUri": rail.result('search_users_3')['uri'],
                "permissionSetUri": rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_23'), 'name', (rail.result(
                    'foreach_create_list_25_26')).strip(), 'uri', '')
            }
        )

        foreach_create_list_25_26_end = rail.EmptyOperator(
            task_id='foreach_create_list_25_26_end',
        )

        if_action_downcase_equals_to_add_31 = rail.IfOperator(
            task_id='if_action_downcase_equals_to_add_31',
            test=lambda dag_run: (dag_run.conf['action']).lower() == 'add',
            yes_task="update_initial_supervisor_32",
            no_task="if_action_downcase_equals_to_update_33",
        )

        update_initial_supervisor_32 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_32',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_users_3').uri }}",
                "scheduleEntries": []
            }
        )

        if_action_downcase_equals_to_update_33 = rail.IfOperator(
            task_id='if_action_downcase_equals_to_update_33',
            test=lambda dag_run: (dag_run.conf['action']).lower() == 'update',
            yes_task="invoke_custom_ruby_code_supervisor_effective_date_34",
            no_task="if_error_occured",
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, "%d/%m/%Y")
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        invoke_custom_ruby_code_supervisor_effective_date_34 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_supervisor_effective_date_34',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['supervisoreffectivedate'])
        )

        update_supervisor_assignment_schedule_over_date_range_35 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_35',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_users_3').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('invoke_custom_ruby_code_supervisor_effective_date_34').year }}",
                        "month": "{{ result('invoke_custom_ruby_code_supervisor_effective_date_34').month }}",
                        "day": "{{ result('invoke_custom_ruby_code_supervisor_effective_date_34').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_error_occured = rail.IfOperator(
            task_id='if_error_occured',
            trigger_rule='all_done',
            test="{{ get_error_message() | is_truthy}}",
            yes_task='log_error_37',
            no_task='michael_kors_gmbh_user_sync_logs_search_entries_41'
        )

        log_error_37 = rail.PythonOperator(
            task_id='log_error_37',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_errorfor_supervisorand_userslogin_nameissame_40 = rail.PythonOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame_40',
            python_callable=lambda:  "Supervisor not assigned since the " +
            "user and manager IDs are same"
        )

        michael_kors_gmbh_user_sync_logs_search_entries_41 = rail.FilterLogEntriesOperator(
            task_id='michael_kors_gmbh_user_sync_logs_search_entries_41',
            log="{{dag_run.conf.userimportlogtable}}",
            properties={
                'jobid': "{{dag_run.conf.callerjobid}}",
                'childjobid': "{{dag_run.conf.childjobid}}"
            },
            remove_filtered_entries=True
        )

        load_found_entry = rail.PythonOperator(
            task_id='load_found_entry',
            python_callable=lambda: rail.load_all_records(rail.result(
                'michael_kors_gmbh_user_sync_logs_search_entries_41'))
        )

        if_entry_col5_present_42 = rail.IfOperator(
            task_id='if_entry_col5_present_42',
            test='''{{ result('michael_kors_gmbh_user_sync_logs_search_entries_41','length') > 0 }}''',
            yes_task="michael_kors_gmbh_user_sync_logs_update_entry_43",
            no_task="michael_kors_gmbh_supervisor_assignment_table_update_entry_44",
        )

        michael_kors_gmbh_user_sync_logs_update_entry_43 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_user_sync_logs_update_entry_43',
            log="{{dag_run.conf.userimportlogtable}}",
            message='na',
            severity='na',
            properties=lambda: {
                'loginname': rail.result('load_found_entry')[0]['properties']['loginname'],
                'action': rail.result('load_found_entry')[0]['properties']['action'],
                'status': 'Error' if ('Error' in rail.result('load_found_entry')[0]['properties']['status']) else (('Exception' if rail.result(
                    'log_errorfor_supervisorand_userslogin_nameissame_40') else ('Exception' if rail.result(
                    'log_errorwhensupervisorisdisabled_12') else ('Error' if rail.result('log_error_37') else ('Success' if rail.result(
                    'get_permissions_from_mapper') else rail.result(
                    'load_found_entry')[0]['properties']['status'])))) if ('Skipped' in rail.result(
                    'load_found_entry')[0]['properties']['status']) else (rail.result('load_found_entry')[0]['properties']['status'])),
                'details': rail.smartjoin_by_delim((('Supervisor added/updated successfully' if rail.result('get_permissions_from_mapper') else "") if (
                    'No change to the user record in Replicon' in rail.result('load_found_entry')[0]['properties']['details']) else (rail.result(
                    'load_found_entry')[0]['properties']['details'] + "," + (rail.result('log_errorfor_supervisorand_userslogin_nameissame_40') if rail.result(
                    'log_errorfor_supervisorand_userslogin_nameissame_40') else '') + ',' + (rail.result(
                    'log_errorwhensupervisorisdisabled_12') if rail.result('log_errorwhensupervisorisdisabled_12') else '') + ',' + (rail.result(
                    'log_error_37') if rail.result('log_error_37') else ''))).split(','), ';'),
                'jobid': rail.result('load_found_entry')[0]['properties']['jobid'],
                'childjobid': rail.result('load_found_entry')[0]['properties']['childjobid'],
                'username': rail.result('load_found_entry')[0]['properties']['username'],
            }
        )

        michael_kors_gmbh_supervisor_assignment_table_update_entry_44 = rail.FilterLogEntriesOperator(
            task_id='michael_kors_gmbh_supervisor_assignment_table_update_entry_44',
            log="{{dag_run.conf.supervisorlookup}}",
            properties={
                'useruri': "{{dag_run.conf.useruri}}",
                'supervisorloginname': "{{dag_run.conf.supervisorloginname}}"
            },
            remove_filtered_entries=True
        )

        update_supervisor_entry_to_completed = rail.WriteLogOperator(
            task_id='update_supervisor_entry_to_completed',
            log="{{dag_run.conf.supervisorlookup}}",
            message='na',
            severity='na',
            properties={
                'jobid': "{{dag_run.conf.callerjobid}}",
                'username': "{{dag_run.conf.loginname}}",
                'useruri': "{{dag_run.conf.useruri}}",
                'supervisorloginname': "{{dag_run.conf.supervisorloginname}}",
                'action': "{{dag_run.conf.action}}",
                'childjobid': "{{dag_run.conf.childjobid}}",
                'supervisoreffectivedate': "{{dag_run.conf.supervisoreffectivedate}}",
                'status': 'completed',
                'supervisorusername': "{{dag_run.conf.supervisorusername}}",
                'country': "{{dag_run.conf.country}}"
            }

        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        michael_kors_gmbh_user_sync_logs_search_entries_46 = rail.FilterLogEntriesOperator(
            task_id='michael_kors_gmbh_user_sync_logs_search_entries_46',
            log="{{dag_run.conf.userimportlogtable}}",
            properties={
                'jobid': "{{dag_run.conf.callerjobid}}",
                'childjobid': "{{dag_run.conf.childjobid}}"
            },
            remove_filtered_entries=True
        )

        load_found_entry_on_error = rail.PythonOperator(
            task_id='load_found_entry_on_error',
            python_callable=lambda: rail.load_all_records(rail.result(
                'michael_kors_gmbh_user_sync_logs_search_entries_46'))
        )

        if_first_id_present_47 = rail.IfOperator(
            task_id='if_first_id_present_47',
            test='''{{ result('michael_kors_gmbh_user_sync_logs_search_entries_46','length') > 0 }}''',
            yes_task="michael_kors_gmbh_user_sync_logs_update_entry_48",
            no_task="michael_kors_gmbh_supervisor_assignment_table_update_entry_49",
        )

        michael_kors_gmbh_user_sync_logs_update_entry_48 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_user_sync_logs_update_entry_48',
            log="{{dag_run.conf.userimportlogtable}}",
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                'loginname': rail.result('load_found_entry_on_error')[0]['properties']['loginname'],
                'action': rail.result('load_found_entry_on_error')[0]['properties']['action'],
                'status': 'Error',
                'details': rail.smartjoin_by_delim((rail.result('load_found_entry_on_error')[0]['properties']['details'] + "," +
                    rail.render_template("{{get_error_message()}}")).split(','), ';'),
                'jobid': dag_run.conf['callerjobid'],
                'childjobid': rail.result('load_found_entry_on_error')[0]['properties']['childjobid'],
                'username': rail.result('load_found_entry_on_error')[0]['properties']['username']
            }
        )

        michael_kors_gmbh_supervisor_assignment_table_update_entry_49 = rail.FilterLogEntriesOperator(
            task_id='michael_kors_gmbh_supervisor_assignment_table_update_entry_49',
            log="{{dag_run.conf.supervisorlookup}}",
            properties={
                'useruri': "{{dag_run.conf.useruri}}",
                'supervisorloginname': "{{dag_run.conf.supervisorloginname}}"
            },
            remove_filtered_entries=True
        )

        update_supervisor_entry_on_error = rail.WriteLogOperator(
            task_id='update_supervisor_entry_on_error',
            log="{{dag_run.conf.supervisorlookup}}",
            message='na',
            severity='Error',
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "loginname": "{{dag_run.conf.loginname}}",
                "useruri": "{{dag_run.conf.useruri}}",
                "supervisorloginname": "{{dag_run.conf.supervisorloginname}}",
                "action": "{{dag_run.conf.action}}",
                "childjobid": "{{dag_run.conf.childjobid}}",
                "supervisoreffectivedate": "{{dag_run.conf.supervisoreffectivedate}}",
                "status": 'completed',
                "supervisorusername": "{{dag_run.conf.supervisorusername}}",
                "country": "{{dag_run.conf.country}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> if_log_getsupervisor_uri_4_blank_5
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'Yes') >> stop_6 >> catch_error
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'No') >> if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7
        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7 >> rail.Label(
            'Yes') >> if_log_getsupervisor_status_8_equals_to_false_9
        if_log_getsupervisor_status_8_equals_to_false_9 >> rail.Label(
            'Yes') >> if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11
        if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11 >> rail.Label(
            'Yes') >> log_errorwhensupervisorisdisabled_12 >> if_log_getsupervisor_status_8_equals_to_true_15
        if_log_getsupervisor_employee_type_10_not_equals_to_foreignsupervisor_11 >> rail.Label(
            'No') >> _adhoc_http_action_14 >> if_log_getsupervisor_status_8_equals_to_true_15
        if_log_getsupervisor_status_8_equals_to_false_9 >> rail.Label(
            'No') >> if_log_getsupervisor_status_8_equals_to_true_15
        if_log_getsupervisor_status_8_equals_to_true_15 >> rail.Label(
            'Yes') >> get_permissions_from_mapper >> _adhoc_http_action_18
        _adhoc_http_action_18 >> if_log_supervisor_permissiontobeassigned_17_not_contains_dataloggerlog_checkifthe_supervisor_permissionisassigned_19message_22
        if_log_supervisor_permissiontobeassigned_17_not_contains_dataloggerlog_checkifthe_supervisor_permissionisassigned_19message_22 >> rail.Label(
            'Yes') >> _adhoc_http_action_23 >> foreach_create_list_25_26 >> assign_permission_set_to_user_29 >> foreach_create_list_25_26_end
        foreach_create_list_25_26 >> foreach_create_list_25_26_end >> if_action_downcase_equals_to_add_31
        if_log_supervisor_permissiontobeassigned_17_not_contains_dataloggerlog_checkifthe_supervisor_permissionisassigned_19message_22 >> rail.Label(
            'No') >> if_action_downcase_equals_to_add_31
        if_action_downcase_equals_to_add_31 >> rail.Label(
            'Yes') >> update_initial_supervisor_32 >> if_action_downcase_equals_to_update_33
        if_action_downcase_equals_to_add_31 >> rail.Label(
            'No') >> if_action_downcase_equals_to_update_33
        if_action_downcase_equals_to_update_33 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_supervisor_effective_date_34 >> update_supervisor_assignment_schedule_over_date_range_35 >> if_error_occured
        if_action_downcase_equals_to_update_33 >> rail.Label(
            'No') >> if_error_occured
        if_error_occured >> rail.Label(
            'Yes') >> log_error_37 >> michael_kors_gmbh_user_sync_logs_search_entries_41
        if_error_occured >> rail.Label(
            'No') >> michael_kors_gmbh_user_sync_logs_search_entries_41
        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_7 >> rail.Label(
            'No') >> log_errorfor_supervisorand_userslogin_nameissame_40 >> michael_kors_gmbh_user_sync_logs_search_entries_41
        michael_kors_gmbh_user_sync_logs_search_entries_41 >> load_found_entry >> if_entry_col5_present_42
        if_entry_col5_present_42 >> rail.Label(
            'Yes') >> michael_kors_gmbh_user_sync_logs_update_entry_43 >> michael_kors_gmbh_supervisor_assignment_table_update_entry_44
        if_entry_col5_present_42 >> rail.Label(
            'No') >> michael_kors_gmbh_supervisor_assignment_table_update_entry_44 >> update_supervisor_entry_to_completed
        update_supervisor_entry_to_completed >> catch_error >> michael_kors_gmbh_user_sync_logs_search_entries_46 >> load_found_entry_on_error
        load_found_entry_on_error >> if_first_id_present_47
        if_first_id_present_47 >> rail.Label(
            'Yes') >> michael_kors_gmbh_user_sync_logs_update_entry_48 >> michael_kors_gmbh_supervisor_assignment_table_update_entry_49
        if_first_id_present_47 >> rail.Label(
            'No') >> michael_kors_gmbh_supervisor_assignment_table_update_entry_49 >> update_supervisor_entry_on_error >> log_to_sumo
        if_log_getsupervisor_status_8_equals_to_true_15 >> rail.Label(
            'No') >> michael_kors_gmbh_user_sync_logs_search_entries_41
    return dag


rail.for_each_instance(create_dag)
