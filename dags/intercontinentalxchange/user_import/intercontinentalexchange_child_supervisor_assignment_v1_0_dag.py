import itertools
from datetime import timedelta, datetime
import pendulum
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalexchangechild_supervisorassignmentv10_{config.instance}',
        description=f'IntercontinentalExchange - Child_Supervisor Assignment V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            no_task='search_users_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result, username):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            existing_user = list(filter(lambda x: x['employeeid'] == username, map(lambda row: {
                'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'loginname': row['cells'][1]['textValue'],
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))

            return existing_user[0] if existing_user else {}

        search_users_3 = rail.RepliconServicePageOperator(
            task_id="search_users_3",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisorempid']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result, dag_run: all_result_data_handler(
                result, dag_run.conf['supervisorempid'])
        )

        invoke_custom_ruby_code_4 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_4',
            python_callable=lambda: {
                "todayday": pendulum.now(config.pacific_timezone).day,
                "todaymonth": pendulum.now(config.pacific_timezone).month,
                "todayyear": pendulum.now(config.pacific_timezone).year,
                "today": pendulum.now(config.pacific_timezone).strftime("%m_%d_%Y")
            }
        )

        if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_5 = rail.IfOperator(
            task_id='if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_5',
            test='''{{ dag_run.conf.supervisorempid != dag_run.conf.userid }}''',
            yes_task="if_output_urioutput_present_6",
            no_task="log_errorfor_supervisorand_userslogin_nameissame_25",
        )

        if_output_urioutput_present_6 = rail.IfOperator(
            task_id='if_output_urioutput_present_6',
            test='''{{ result('search_users_3') | is_truthy }}''',
            yes_task="_adhoc_http_action_8",
            no_task="if_output_urioutput_blank_22",
        )

        _adhoc_http_action_8 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_8',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_3').useruri }}"
            }
        )

        def get_existing_super_permission(permission_task):
            assigned_permissions = [perm['permissionSet'] for perm in rail.result(
                permission_task)] if rail.result(permission_task) else []
            return rail.find_first_by_attr_and_get_attr(
                assigned_permissions, 'name', "Supervisor", 'uri')

        log_checkifthe_supervisor_permissionisassigned_9 = rail.PythonOperator(
            task_id='log_checkifthe_supervisor_permissionisassigned_9',
            python_callable=lambda: get_existing_super_permission(
                '_adhoc_http_action_8')
        )

        if_log_checkifthe_supervisor_permissionisassigned_9_blank_10 = rail.IfOperator(
            task_id='if_log_checkifthe_supervisor_permissionisassigned_9_blank_10',
            test='''{{ result('log_checkifthe_supervisor_permissionisassigned_9') | is_falsy }}''',
            yes_task="assign_supervsior_permission_set_to_user_supervisor_11",
            no_task="if_request_action_equals_to_add_15",
        )

        assign_supervsior_permission_set_to_user_supervisor_11 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_supervisor_11',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_3').useruri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
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
                "initialSupervisorUri": "{{ result('search_users_3').useruri }}",
                "scheduleEntries": []
            }
        )

        if_request_action_equals_to_update_17 = rail.IfOperator(
            task_id='if_request_action_equals_to_update_17',
            test='''{{ dag_run.conf.action == 'Update' }}''',
            yes_task="date_split_18",
            no_task="if_output_urioutput_blank_22",
        )

        date_split_18 = rail.EmptyOperator(
            task_id='date_split_18',
        )

        update_supervisor_assignment_schedule_over_date_range_19 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_19',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_users_3')['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['supeffectivedate'], '%m_%d_%Y').year,
                        "month": datetime.strptime(dag_run.conf['supeffectivedate'], '%m_%d_%Y').month,
                        "day":  datetime.strptime(dag_run.conf['supeffectivedate'], '%m_%d_%Y').day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_output_urioutput_blank_22 = rail.IfOperator(
            task_id='if_output_urioutput_blank_22',
            test='''{{ result('search_users_3') | is_falsy }}''',
            yes_task="log_erroras_supervisorisnotavailable_23",
            no_task="catch_30",
        )

        log_erroras_supervisorisnotavailable_23 = rail.WriteLogOperator(
            task_id='log_erroras_supervisorisnotavailable_23',
            message="Supervisor is not updated as the supervisor",
            severity="Exception",
            properties=lambda dag_run: {
                "Empid": dag_run.conf['employeeid'],
                "Username": dag_run.conf['username'],
                "Action": "Exception",
                "Status": "Supervisor is not updated as the supervisor with employee id " +
                dag_run.conf['supervisorempid'] + " is not available",
                "Details": "",
                "Jobid": get_dagrun_ecid(dag_run)
            }
        )

        log_errorfor_supervisorand_userslogin_nameissame_25 = rail.WriteLogOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame_25',
            message="Supervisor is not updated as the supervisor",
            severity="Exception",
            properties=lambda dag_run: {
                "Empid": dag_run.conf['employeeid'],
                "Username": dag_run.conf['username'],
                "Action": "Exception",
                "Status": "Supervisor is not updated as the Employee ID for user and supervisor is same on the input file",
                "Details": "",
                "Jobid": get_dagrun_ecid(dag_run)
            }
        )

        catch_30 = rail.EmptyOperator(
            task_id='catch_30',
            trigger_rule='one_failed',
        )

        log_superviosr_process_errror_31 = rail.WriteLogOperator(
            task_id='log_superviosr_process_errror_31',
            message="{{ get_error_message() }}",
            severity="Error",
            properties=lambda dag_run: {
                "Empid": dag_run.conf['employeeid'],
                "Username": dag_run.conf['username'],
                "Action": "Error",
                "Status": "{{ get_error_message() }}",
                "Details": "",
                "Jobid": get_dagrun_ecid(dag_run)
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> invoke_custom_ruby_code_4 >> if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_5
        if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_5 >> rail.Label(
            'Yes') >> if_output_urioutput_present_6
        if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_5 >> rail.Label('No') >> \
            log_errorfor_supervisorand_userslogin_nameissame_25 >> log_to_sumo
        if_output_urioutput_present_6 >> rail.Label(
            'Yes') >> _adhoc_http_action_8 >> log_checkifthe_supervisor_permissionisassigned_9 >> if_log_checkifthe_supervisor_permissionisassigned_9_blank_10
        if_log_checkifthe_supervisor_permissionisassigned_9_blank_10 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_supervisor_11 >> if_request_action_equals_to_add_15
        if_log_checkifthe_supervisor_permissionisassigned_9_blank_10 >> rail.Label(
            'No') >> if_request_action_equals_to_add_15
        if_request_action_equals_to_add_15 >> rail.Label(
            'Yes') >> update_initial_supervisor_16 >> if_request_action_equals_to_update_17
        if_request_action_equals_to_add_15 >> rail.Label(
            'No') >> if_request_action_equals_to_update_17
        if_request_action_equals_to_update_17 >> rail.Label(
            'Yes') >> date_split_18 >> update_supervisor_assignment_schedule_over_date_range_19 >> if_output_urioutput_blank_22
        if_request_action_equals_to_update_17 >> rail.Label(
            'No') >> if_output_urioutput_blank_22
        if_output_urioutput_present_6 >> rail.Label(
            'No') >> if_output_urioutput_blank_22
        if_output_urioutput_blank_22 >> rail.Label(
            'Yes') >> log_erroras_supervisorisnotavailable_23 >> catch_30
        if_output_urioutput_blank_22 >> rail.Label(
            'No') >> catch_30 >> log_superviosr_process_errror_31 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
