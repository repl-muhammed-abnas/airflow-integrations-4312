
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'baylorcollegeofmedicine_assign_supervisor_child_{config.instance}',
        description=f'BaylorCollegeOfMedicine_Child_Assign Supervisor {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_user,
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
            matching_users = list(filter(
                lambda user: user['cells'][0]['textValue'] == dag_run.conf['supervisorloginname'], users_found))
            return {
                'matchingusersfound': len(matching_users),
                'uri': matching_users[0]['cells'][0]['uri'] if matching_users else '',
                'status': matching_users[0]['cells'][1]['textValue'] if matching_users else ''
            }

        search_users_3 = rail.RepliconServiceOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
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

        if_pluckuri_smart_joinnil_not_equals_to_dataworkato_service3cd9c331requestloginname_5 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_not_equals_to_dataworkato_service3cd9c331requestloginname_5',
            test=lambda dag_run: dag_run.conf['loginname'] != dag_run.conf['supervisorloginname'],
            yes_task="if_pluckuri_smart_joinnil_present_6",
            no_task="log_errorfor_supervisorand_userslogin_nameissame_27",
        )

        if_pluckuri_smart_joinnil_present_6 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_present_6',
            test=lambda: bool(rail.result('search_users_3')['uri']),
            yes_task="if_pluckuri_length_greater_than_1_7",
            no_task="if_pluckuri_smart_joinnil_blank_24",
        )

        if_pluckuri_length_greater_than_1_7 = rail.IfOperator(
            task_id='if_pluckuri_length_greater_than_1_7',
            test=lambda: rail.result('search_users_3')[
                'matchingusersfound'] > 1,
            yes_task="log_errorasmultipleusershavethesameemployeeid_8",
            no_task="if_smart_join_presence_equals_to_true_10",
        )

        log_errorasmultipleusershavethesameemployeeid_8 = rail.PythonOperator(
            task_id='log_errorasmultipleusershavethesameemployeeid_8',
            python_callable=lambda dag_run:  'Supervisor is not assigned/updated as multiple users have the same employee id as "' +
            dag_run.conf['supervisorloginname'] + '" in Replicon'
        )

        if_smart_join_presence_equals_to_true_10 = rail.IfOperator(
            task_id='if_smart_join_presence_equals_to_true_10',
            test=lambda: rail.result('search_users_3')[
                'status'] == 'True',
            yes_task="_adhoc_http_action_11",
            no_task="log_errorwhensupervisorisdisabled_23",
        )

        _adhoc_http_action_11 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_11',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_3').uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri', '') if (response and response[0]['policyUri']) else ''
        )

        if_pluckuri_smart_joinnil_blank_12 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_blank_12',
            test=lambda: not bool(rail.result('_adhoc_http_action_11')),
            yes_task="assign_permission_set_to_user_supervisor_13",
            no_task="if_request_action_equals_to_add_15",
        )

        assign_permission_set_to_user_supervisor_13 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_supervisor_13',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data={
                "userUri": "{{ result('search_users_3').uri }}",
                "permissionSetUris": [
                    "{{ dag_run.conf.supervisorpermission }}",
                    "{{ dag_run.conf.basicuserwithreports }}"
                ]
            }
        )

        if_request_action_equals_to_add_15 = rail.IfOperator(
            task_id='if_request_action_equals_to_add_15',
            test='''{{ dag_run.conf.action == 'Add' }}''',
            yes_task="update_initial_supervisor_16",
            no_task="if_request_action_equals_to_update_17",
        )

        update_initial_supervisor_16 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_16',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_users_3').uri }}",
                "scheduleEntries": []
            }
        )

        if_request_action_equals_to_update_17 = rail.IfOperator(
            task_id='if_request_action_equals_to_update_17',
            test='''{{ dag_run.conf.action == 'Update' }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_18",
            no_task="if_pluckuri_smart_joinnil_blank_24",
        )

        update_supervisor_assignment_schedule_over_date_range_18 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_18',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_users_3').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ dag_run.conf.today.year }}",
                        "month": "{{ dag_run.conf.today.month }}",
                        "day": "{{ dag_run.conf.today.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_errorwhensupervisorisdisabled_23 = rail.PythonOperator(
            task_id='log_errorwhensupervisorisdisabled_23',
            python_callable=lambda dag_run:  'Supervsior assignment/update is not done as "' +
            dag_run.conf['supervisorloginname'] + '" is disabled'
        )

        if_pluckuri_smart_joinnil_blank_24 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_blank_24',
            test=lambda: not bool(rail.result('search_users_3')['uri']),
            yes_task="log_erroras_supervisorisnotavailable_25",
            no_task="log_final_exceptions",
        )

        log_erroras_supervisorisnotavailable_25 = rail.PythonOperator(
            task_id='log_erroras_supervisorisnotavailable_25',
            python_callable=lambda dag_run:  'Supervisor is not assigned/updated as "' +
            dag_run.conf['supervisorloginname'] +
            '" is not available in Replicon'
        )

        log_errorfor_supervisorand_userslogin_nameissame_27 = rail.PythonOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame_27',
            python_callable=lambda:  'Supervisor is not assigned/updated as the "Login name" for user' +
            ' and supervisor is same on the input file'
        )

        baylorcollegeofmedicine_user_import_logs_search_entries_29 = rail.FilterLogEntriesOperator(
            task_id='baylorcollegeofmedicine_user_import_logs_search_entries_29',
            log="{{dag_run.conf.userimportlogslookup}}",
            properties={
                "jobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{dag_run.conf.childjobid}}"
            },
            remove_filtered_entries=True
        )

        load_found_entry = rail.PythonOperator(
            task_id='load_found_entry',
            python_callable=lambda: rail.load_all_records(rail.result(
                'baylorcollegeofmedicine_user_import_logs_search_entries_29'))
        )

        def get_exception_message():
            return rail.smartjoin_by_delim(((rail.result('log_errorfor_supervisorand_userslogin_nameissame_27') if rail.result(
                'log_errorfor_supervisorand_userslogin_nameissame_27') else '') + ',' + (rail.result('log_errorwhensupervisorisdisabled_23') if rail.result(
                'log_errorwhensupervisorisdisabled_23') else '') + ',' + (rail.result('log_erroras_supervisorisnotavailable_25') if rail.result(
                'log_erroras_supervisorisnotavailable_25') else '') + ',' + (rail.result(
                'log_errorasmultipleusershavethesameemployeeid_8') if rail.result(
                'log_errorasmultipleusershavethesameemployeeid_8') else '')).split(','), ',')

        log_final_exceptions = rail.PythonOperator(
            task_id='log_final_exceptions',
            python_callable=get_exception_message
        )

        if_entry_col5_present_30 = rail.IfOperator(
            task_id='if_entry_col5_present_30',
            test='''{{ result('baylorcollegeofmedicine_user_import_logs_search_entries_29','length') > 0 | is_truthy }}''',
            yes_task="baylorcollegeofmedicine_user_import_logs_update_entry_31",
            no_task="baylorcollegeofmedicine_supervisor_assignment_logs_update_entry_32",
        )

        baylorcollegeofmedicine_user_import_logs_update_entry_31 = rail.WriteLogOperator(
            task_id='baylorcollegeofmedicine_user_import_logs_update_entry_31',
            log="{{dag_run.conf.userimportlogslookup}}",
            message='na',
            severity='na',
            properties=lambda: {
                "loginname": rail.result('load_found_entry')[0]['properties']['loginname'],
                "action": rail.result('load_found_entry')[0]['properties']['action'],
                "status": 'Error' if 'Error' in rail.result('load_found_entry')[0]['properties']['status'] else ('Exception' if rail.result(
                    'log_final_exceptions') else rail.result('load_found_entry')[0]['properties']['jobid']),
                "details": (('Partially Updated - ' + ',' + rail.result('log_final_exceptions')) if rail.result(
                    'log_final_exceptions') else rail.result('load_found_entry')[0]['properties']['details']) if (
                    'No change to the user record in Replicon' in rail.result('load_found_entry')[0]['properties']['details']) else ((((
                        'Partially added - ' if 'add' in (rail.result(
                            'load_found_entry')[0]['properties']['action']).lower() else 'Partially updated - ') + rail.result(
                        'log_final_exceptions')) if rail.result('log_final_exceptions') else (rail.result(
                            'load_found_entry')[0]['properties']['details'])) if 'successfully' in (rail.result(
                                'load_found_entry')[0]['properties']['details']).lower() else (rail.result(
                                    'load_found_entry')[0]['properties']['details'] + (',' + rail.result('log_final_exceptions') if rail.result(
                                        'log_final_exceptions') else ''))),
                "jobid": rail.result('load_found_entry')[0]['properties']['jobid'],
                "childjobid": rail.result('load_found_entry')[0]['properties']['childjobid'] + ' - ' + rail.render_template("{{dag_run_ecid()}}"),
                "firstname": '',
                "lastname": ''
            }
        )

        baylorcollegeofmedicine_supervisor_assignment_logs_update_entry_32 = rail.FilterLogEntriesOperator(
            task_id='baylorcollegeofmedicine_supervisor_assignment_logs_update_entry_32',
            log="{{dag_run.conf.supervisorlookup}}",
            properties={
                "username": "{{dag_run.conf.loginname}}",
                "supervisorloginname": "{{dag_run.conf.supervisorloginname}}"
            },
            remove_filtered_entries=True
        )

        load_found_supervisor_entry = rail.PythonOperator(
            task_id='load_found_supervisor_entry',
            python_callable=lambda: rail.load_all_records(rail.result(
                'baylorcollegeofmedicine_supervisor_assignment_logs_update_entry_32'))
        )

        update_supervisor_entry_to_completed = rail.WriteLogOperator(
            task_id='update_supervisor_entry_to_completed',
            log="{{dag_run.conf.supervisorlookup}}",
            message='na',
            severity='na',
            properties=lambda: {
                "jobid": rail.result('load_found_supervisor_entry')[0]['properties']['jobid'],
                "username": rail.result('load_found_supervisor_entry')[0]['properties']['username'],
                "useruri": rail.result('load_found_supervisor_entry')[0]['properties']['useruri'],
                "supervisorloginname": rail.result('load_found_supervisor_entry')[0]['properties']['supervisorloginname'],
                "action": rail.result('load_found_supervisor_entry')[0]['properties']['action'],
                "childjobid": rail.result('load_found_supervisor_entry')[0]['properties']['childjobid'],
                "status": 'completed'
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> if_pluckuri_smart_joinnil_not_equals_to_dataworkato_service3cd9c331requestloginname_5
        if_pluckuri_smart_joinnil_not_equals_to_dataworkato_service3cd9c331requestloginname_5 >> rail.Label(
            'Yes') >> if_pluckuri_smart_joinnil_present_6
        if_pluckuri_smart_joinnil_present_6 >> rail.Label(
            'Yes') >> if_pluckuri_length_greater_than_1_7
        if_pluckuri_length_greater_than_1_7 >> rail.Label(
            'Yes') >> log_errorasmultipleusershavethesameemployeeid_8 >> if_pluckuri_smart_joinnil_blank_24
        if_pluckuri_length_greater_than_1_7 >> rail.Label(
            'No') >> if_smart_join_presence_equals_to_true_10
        if_smart_join_presence_equals_to_true_10 >> rail.Label(
            'Yes') >> _adhoc_http_action_11 >> if_pluckuri_smart_joinnil_blank_12
        if_pluckuri_smart_joinnil_blank_12 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_supervisor_13 >> if_request_action_equals_to_add_15
        if_pluckuri_smart_joinnil_blank_12 >> rail.Label(
            'No') >> if_request_action_equals_to_add_15
        if_request_action_equals_to_add_15 >> rail.Label(
            'Yes') >> update_initial_supervisor_16 >> if_request_action_equals_to_update_17
        if_request_action_equals_to_add_15 >> rail.Label(
            'No') >> if_request_action_equals_to_update_17
        if_request_action_equals_to_update_17 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_18 >> if_pluckuri_smart_joinnil_blank_24
        if_request_action_equals_to_update_17 >> rail.Label(
            'No') >> if_pluckuri_smart_joinnil_blank_24
        if_smart_join_presence_equals_to_true_10 >> rail.Label(
            'No') >> log_errorwhensupervisorisdisabled_23 >> if_pluckuri_smart_joinnil_blank_24
        if_pluckuri_smart_joinnil_present_6 >> rail.Label(
            'No') >> if_pluckuri_smart_joinnil_blank_24
        if_pluckuri_smart_joinnil_blank_24 >> rail.Label(
            'Yes') >> log_erroras_supervisorisnotavailable_25 >> log_final_exceptions
        if_pluckuri_smart_joinnil_blank_24 >> rail.Label(
            'No') >> log_final_exceptions
        if_pluckuri_smart_joinnil_not_equals_to_dataworkato_service3cd9c331requestloginname_5 >> rail.Label(
            'No') >> log_errorfor_supervisorand_userslogin_nameissame_27 >> log_final_exceptions >> baylorcollegeofmedicine_user_import_logs_search_entries_29
        baylorcollegeofmedicine_user_import_logs_search_entries_29 >> load_found_entry >> if_entry_col5_present_30
        if_entry_col5_present_30 >> rail.Label(
            'Yes') >> baylorcollegeofmedicine_user_import_logs_update_entry_31 >> baylorcollegeofmedicine_supervisor_assignment_logs_update_entry_32
        if_entry_col5_present_30 >> rail.Label(
            'No') >> baylorcollegeofmedicine_supervisor_assignment_logs_update_entry_32 >> load_found_supervisor_entry >> update_supervisor_entry_to_completed
        update_supervisor_entry_to_completed >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
