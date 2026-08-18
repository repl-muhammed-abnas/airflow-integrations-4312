
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'npsg_user_import_supervisor_assignment_child_{config.instance}',
        description=f'NPSG_Supervisor Assignment {config.instance}',
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

        def get_supervisoruser_details(response, dag_run):
            users_found = response['rows']
            today_date = datetime.now()
            required_user = {}
            for user in users_found:
                if user['cells'][1]['textValue'] == dag_run.conf['supervisorloginname']:
                    required_user = user
                    break
            return {
                'user': required_user if users_found and required_user else '',
                'uri': required_user['cells'][0]['uri'] if users_found and required_user else '',
                'name': required_user['cells'][3]['textValue'] if users_found and required_user else '',
                'status': required_user['cells'][2]['textValue'] if users_found and required_user else False,
                'todayday': int(today_date.strftime('%d')),
                'todaymonth': int(today_date.strftime('%m')),
                'todayyear': int(today_date.strftime('%Y')),
                'today': today_date.strftime("%m/%d/%Y")
            }

        search_users_3 = rail.RepliconServiceOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:user-name",
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
            data_handler=get_supervisoruser_details
        )

        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5 = rail.IfOperator(
            task_id='if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="if_output_urioutput_present_6",
            no_task="log_errorfor_supervisorand_userslogin_nameissame_25",
        )

        if_output_urioutput_present_6 = rail.IfOperator(
            task_id='if_output_urioutput_present_6',
            test='''{{ result('search_users_3').uri | is_truthy }}''',
            yes_task="_adhoc_http_action_8",
            no_task="if_output_urioutput_blank_22",
        )

        _adhoc_http_action_8 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_8',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_3').uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'permissionSet.name', 'Manager', 'permissionSet.uri', '') if response[0]['policyUri'] else ''
        )

        if_log_checkifthe_supervisor_permissionisassigned_9_blank_10 = rail.IfOperator(
            task_id='if_log_checkifthe_supervisor_permissionisassigned_9_blank_10',
            test='''{{ result('_adhoc_http_action_8') | is_falsy }}''',
            yes_task="assign_supervsior_permission_set_to_user_manager_11",
            no_task="if_request_action_equals_to_add_15",
        )

        assign_supervsior_permission_set_to_user_manager_11 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_11',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_3').uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        if_request_action_equals_to_add_15 = rail.IfOperator(
            task_id='if_request_action_equals_to_add_15',
            test='''{{ dag_run.conf.action == 'add' }}''',
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
            test='''{{ dag_run.conf.action == 'update' }}''',
            yes_task="get_supervisoreffective_dateobject",
            no_task="if_error_present",
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring,"%m/%d/%Y")
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        get_supervisoreffective_dateobject = rail.PythonOperator(
            task_id = 'get_supervisoreffective_dateobject',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['supeffectivedate'])
        )

        update_supervisor_assignment_schedule_over_date_range_19 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_19',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run:{
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_users_3')['uri'],
                "dateRange": {
                    "startDate": rail.result('get_supervisoreffective_dateobject'),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_error_present = rail.IfOperator(
            task_id='if_error_present',
            trigger_rule='all_done',
            test="{{get_error_message() | is_truthy}}",
            yes_task='log_error_21',
            no_task='if_output_urioutput_blank_22'
        )

        log_error_21 = rail.PythonOperator(
            task_id='log_error_21',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        if_output_urioutput_blank_22 = rail.IfOperator(
            task_id='if_output_urioutput_blank_22',
            test='''{{ result('search_users_3').uri | is_falsy }}''',
            yes_task="log_erroras_supervisorisnotavailable_23",
            no_task="npsg_user_import_logs_search_entries_26",
        )

        log_erroras_supervisorisnotavailable_23 = rail.PythonOperator(
            task_id='log_erroras_supervisorisnotavailable_23',
            python_callable=lambda dag_run:  "Supervisor is not updated as the supervisor with login name " +
            dag_run.conf['supervisorloginname'] + " is not available"
        )

        log_errorfor_supervisorand_userslogin_nameissame_25 = rail.PythonOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame_25',
            python_callable=lambda dag_run:  "Supervisor is not updated as the " + dag_run.conf['supervisorloginname'] +
            " for user and supervisor is same on the input file"
        )

        npsg_user_import_logs_search_entries_26 = rail.FilterLogEntriesOperator(
            task_id='npsg_user_import_logs_search_entries_26',
            log="{{dag_run.conf.userimportlogtable}}",
            properties={
                "childjob": "{{dag_run.conf.childjobid}}"
            },
            remove_filtered_entries=True
        )

        load_found_entry = rail.PythonOperator(
            task_id='load_found_entry',
            python_callable=lambda: rail.load_all_records(
                rail.result('npsg_user_import_logs_search_entries_26'))
        )

        if_first_id_present_27 = rail.IfOperator(
            task_id='if_first_id_present_27',
            test='''{{ result('npsg_user_import_logs_search_entries_26','length') > 0 }}''',
            yes_task="npsg_user_import_logs_update_entry_28",
            no_task="npsg_supervisor_check_update_entry_29",
        )

        npsg_user_import_logs_update_entry_28 = rail.WriteLogOperator(
            task_id='npsg_user_import_logs_update_entry_28',
            log="{{dag_run.conf.userimportlogtable}}",
            message="na",
            severity="na",
            properties=lambda: {
                'empid': rail.result('load_found_entry')[0]['properties']['empid'],
                'username': rail.result('load_found_entry')[0]['properties']['username'],
                'action': "Error" if 'Error' in rail.result('load_found_entry')[0]['properties']['action'] else ('Exception' if rail.result(
                    'log_errorfor_supervisorand_userslogin_nameissame_25') else ('Exception' if rail.result(
                    'log_erroras_supervisorisnotavailable_23') else ('Exception' if rail.result(
                    'log_error_21') else rail.result('load_found_entry')[0]['properties']['action']))),
                'status': rail.smartjoin_by_delim(((rail.result('load_found_entry')[0]['properties']['status'] + "," + (rail.result(
                    'log_errorfor_supervisorand_userslogin_nameissame_25') if rail.result(
                    'log_errorfor_supervisorand_userslogin_nameissame_25') else '') + "," + (rail.result(
                    'log_error_21') if rail.result('log_error_21') else '') + "," + (rail.result(
                    'log_erroras_supervisorisnotavailable_23') if rail.result('log_erroras_supervisorisnotavailable_23') else '')).split(",")), ";"),
                'details': rail.result('load_found_entry')[0]['properties']['details'],
                'parentjob': rail.result('load_found_entry')[0]['properties']['parentjob'],
                'childjob': rail.result('load_found_entry')[0]['properties']['childjob'],
            }
        )

        npsg_supervisor_check_update_entry_29 = rail.FilterLogEntriesOperator(
            task_id='npsg_supervisor_check_update_entry_29',
            log="{{dag_run.conf.supervisorlookup}}",
            properties={
                "username": "{{dag_run.conf.username}}",
                "useruri": "{{dag_run.conf.useruri}}",
                "jobid": "{{dag_run.conf.parentjobid}}"
            },
            remove_filtered_entries=True
        )

        load_supervisor_entry = rail.PythonOperator(
            task_id='load_supervisor_entry',
            python_callable=lambda: rail.load_all_records(
                rail.result('npsg_supervisor_check_update_entry_29'))
        )

        update_supervisor_lookup_entry = rail.WriteLogOperator(
            task_id='update_supervisor_lookup_entry',
            log="{{dag_run.conf.supervisorlookup}}",
            message="na",
            severity="na",
            properties=lambda: {
                "jobid": rail.result('load_supervisor_entry')[0]['properties']['jobid'],
                "userempid": rail.result('load_supervisor_entry')[0]['properties']['userempid'],
                "useruri": rail.result('load_supervisor_entry')[0]['properties']['useruri'],
                "username": rail.result('load_supervisor_entry')[0]['properties']['username'],
                "supervisorempid": rail.result('load_supervisor_entry')[0]['properties']['supervisorempid'],
                "action": "completed",
                "childjobid": rail.result('load_supervisor_entry')[0]['properties']['childjobid'],
                "status": rail.result('load_supervisor_entry')[0]['properties']['status'],
                "effectivedate": rail.result('load_supervisor_entry')[0]['properties']['effectivedate']
            }
        )

        catch_error = rail.FilterLogEntriesOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.userimportlogtable}}",
            properties={
                'childjob': "{{dag_run.conf.childjobid}}",
                'username': "{{dag_run.conf.username}}"
            },
            remove_filtered_entries=True
        )

        load_found_entry_on_error = rail.PythonOperator(
            task_id='load_found_entry_on_error',
            python_callable=lambda: rail.load_all_records(
                rail.result('catch_error'))
        )

        if_first_id_present_32 = rail.IfOperator(
            task_id='if_first_id_present_32',
            test='''{{ result('catch_error','length') > 0 }}''',
            yes_task="npsg_user_import_logs_update_entry_33",
            no_task="npsg_supervisor_check_update_entry_34",
        )

        npsg_user_import_logs_update_entry_33 = rail.WriteLogOperator(
            task_id='npsg_user_import_logs_update_entry_33',
            log="{{dag_run.conf.userimportlogtable}}",
            message="na",
            severity="na",
            properties=lambda: {
                'empid': rail.result('load_found_entry')[0]['properties']['parentjob'],
                'username': rail.result('load_found_entry')[0]['properties']['empid'],
                'action': "Supervisor Update",
                'status': rail.smartjoin_by_delim(((rail.result('load_found_entry')[0]['properties']['status'] + "," +
                    rail.render_template("{{get_error_message()}}")).split(",")), ";"),
                'details': rail.result('load_found_entry')[0]['properties']['details'],
                'parentjob': rail.result('load_found_entry')[0]['properties']['parentjob'],
                'childjob': rail.result('load_found_entry')[0]['properties']['childjob'],
            }
        )

        npsg_supervisor_check_update_entry_34 = rail.FilterLogEntriesOperator(
            task_id='npsg_supervisor_check_update_entry_34',
            log="{{dag_run.conf.supervisorlookup}}",
            properties={
                "username": "{{dag_run.conf.username}}",
                "useruri": "{{dag_run.conf.useruri}}",
                "jobid": "{{dag_run.conf.parentjobid}}"
            },
            remove_filtered_entries=True
        )

        load_supervisor_entry_onerror = rail.PythonOperator(
            task_id='load_supervisor_entry_onerror',
            python_callable=lambda: rail.load_all_records(
                rail.result('npsg_supervisor_check_update_entry_34'))
        )

        update_supervisor_lookup_entry_onerror = rail.WriteLogOperator(
            task_id='update_supervisor_lookup_entry_onerror',
            log="{{dag_run.conf.supervisorlookup}}",
            message="na",
            severity="na",
            properties=lambda: {
                "jobid": rail.result('load_supervisor_entry_onerror')[0]['properties']['jobid'],
                "userempid": rail.result('load_supervisor_entry_onerror')[0]['properties']['userempid'],
                "useruri": rail.result('load_supervisor_entry_onerror')[0]['properties']['useruri'],
                "username": rail.result('load_supervisor_entry_onerror')[0]['properties']['username'],
                "supervisorempid": rail.result('load_supervisor_entry_onerror')[0]['properties']['supervisorempid'],
                "action": "completed",
                "childjobid": rail.result('load_supervisor_entry_onerror')[0]['properties']['childjobid'],
                "status": rail.result('load_supervisor_entry_onerror')[0]['properties']['status'],
                "effectivedate": rail.result('load_supervisor_entry_onerror')[0]['properties']['effectivedate']
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5
        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5 >> rail.Label(
            'Yes') >> if_output_urioutput_present_6
        if_output_urioutput_present_6 >> rail.Label(
            'Yes') >> _adhoc_http_action_8 >> if_log_checkifthe_supervisor_permissionisassigned_9_blank_10
        if_log_checkifthe_supervisor_permissionisassigned_9_blank_10 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_manager_11 >> if_request_action_equals_to_add_15
        if_log_checkifthe_supervisor_permissionisassigned_9_blank_10 >> rail.Label(
            'No') >> if_request_action_equals_to_add_15
        if_request_action_equals_to_add_15 >> rail.Label(
            'Yes') >> update_initial_supervisor_16 >> if_request_action_equals_to_update_17
        if_request_action_equals_to_add_15 >> rail.Label(
            'No') >> if_request_action_equals_to_update_17
        if_request_action_equals_to_update_17 >> rail.Label(
            'Yes') >> get_supervisoreffective_dateobject >> update_supervisor_assignment_schedule_over_date_range_19 >> if_error_present
        if_request_action_equals_to_update_17 >> rail.Label(
            'No') >> if_error_present
        if_error_present >> rail.Label(
            'Yes') >> log_error_21 >> if_output_urioutput_blank_22
        if_error_present >> rail.Label('No') >> if_output_urioutput_blank_22
        if_output_urioutput_present_6 >> rail.Label(
            'No') >> if_output_urioutput_blank_22
        if_output_urioutput_blank_22 >> rail.Label(
            'Yes') >> log_erroras_supervisorisnotavailable_23 >> npsg_user_import_logs_search_entries_26
        if_output_urioutput_blank_22 >> rail.Label(
            'No') >> npsg_user_import_logs_search_entries_26
        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5 >> rail.Label(
            'No') >> log_errorfor_supervisorand_userslogin_nameissame_25 >> npsg_user_import_logs_search_entries_26 >> load_found_entry
        load_found_entry >> if_first_id_present_27
        if_first_id_present_27 >> rail.Label(
            'Yes') >> npsg_user_import_logs_update_entry_28 >> npsg_supervisor_check_update_entry_29
        if_first_id_present_27 >> rail.Label(
            'No') >> npsg_supervisor_check_update_entry_29 >> load_supervisor_entry >> update_supervisor_lookup_entry >> catch_error
        catch_error >> load_found_entry_on_error >> if_first_id_present_32
        if_first_id_present_32 >> rail.Label(
            'Yes') >> npsg_user_import_logs_update_entry_33 >> npsg_supervisor_check_update_entry_34
        if_first_id_present_32 >> rail.Label(
            'No') >> npsg_supervisor_check_update_entry_34 >> load_supervisor_entry_onerror >> update_supervisor_lookup_entry_onerror >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
