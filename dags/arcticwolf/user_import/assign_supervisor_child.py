
from datetime import timedelta
from airflow.models import Variable
import rail
from arcticwolf.user_import.utils import request_payload, response_filter, python_callable_methods

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.assign_supervisor_child_dagid,
        description=f'Arcticwolf_Child_Assign Supervisor {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
            no_task='search_supervisor'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_supervisor',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_supervisor = rail.RepliconServiceOperator(
            task_id='search_supervisor',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_supervisor_payload,
            data_handler=response_filter.get_supervisor_uri_and_status_assign_supervisor
        )

        if_loginname_not_equals_requestloginname = rail.IfOperator(
            task_id='if_loginname_not_equals_requestloginname',
            test=lambda dag_run: dag_run.conf['employeeid'] != dag_run.conf['supervisor'],
            yes_task="if_supervisor_uri_is_present",
            no_task="log_error_supervisor_and_user_is_same",
        )

        if_supervisor_uri_is_present = rail.IfOperator(
            task_id='if_supervisor_uri_is_present',
            test=lambda: bool(rail.result('search_supervisor')['uri']),
            yes_task="if_matching_supervisor_length_greater_than_one",
            no_task="if_supervisor_uri_is_not_present",
        )

        if_matching_supervisor_length_greater_than_one = rail.IfOperator(
            task_id='if_matching_supervisor_length_greater_than_one',
            test=lambda: rail.result('search_supervisor')[
                'matchingusersfound'] > 1,
            yes_task="log_error_multiple_same_user",
            no_task="if_supervisor_user_is_enabled",
        )

        log_error_multiple_same_user = rail.PythonOperator(
            task_id='log_error_multiple_same_user',
            python_callable=lambda dag_run:  'Supervisor is not assigned/updated as multiple users have the same employee id as "' +
            dag_run.conf['supervisor'] + '" in Replicon'
        )

        if_supervisor_user_is_enabled = rail.IfOperator(
            task_id='if_supervisor_user_is_enabled',
            test=lambda: rail.result('search_supervisor')[
                'status'] == 'True',
            yes_task="get_assigned_permission_for_supervisor",
            no_task="log_error_supervisor_is_disabled",
        )

        get_assigned_permission_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_for_supervisor',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_supervisor').uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri', '') if (response and response[0]['policyUri']) else ''
        )

        if_supervisor_permission_not_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_not_assigned',
            test=lambda: not bool(rail.result(
                'get_assigned_permission_for_supervisor')),
            yes_task="assign_permission_set_to_user_supervisor",
            no_task="if_request_action_equals_to_add",
        )

        assign_permission_set_to_user_supervisor = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_supervisor',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data={
                "userUri": "{{ result('search_supervisor').uri }}",
                "permissionSetUris": [
                    "{{ dag_run.conf.supervisorpermission }}"
                ]
            }
        )

        if_request_action_equals_to_add = rail.IfOperator(
            task_id='if_request_action_equals_to_add',
            test='''{{ dag_run.conf.action == 'Add' }}''',
            yes_task="update_initial_supervisor",
            no_task="if_request_action_equals_to_update",
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_supervisor').uri }}",
                "scheduleEntries": []
            }
        )

        if_request_action_equals_to_update = rail.IfOperator(
            task_id='if_request_action_equals_to_update',
            test='''{{ dag_run.conf.action == 'Update' }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range",
            no_task="if_supervisor_uri_is_not_present",
        )

        update_supervisor_assignment_schedule_over_date_range = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_supervisor').uri }}",
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

        log_error_supervisor_is_disabled = rail.PythonOperator(
            task_id='log_error_supervisor_is_disabled',
            python_callable=lambda dag_run:  'Supervsior assignment/update is not done as "' +
            dag_run.conf['supervisor'] + '" is disabled'
        )

        if_supervisor_uri_is_not_present = rail.IfOperator(
            task_id='if_supervisor_uri_is_not_present',
            test=lambda: not bool(rail.result('search_supervisor')['uri']),
            yes_task="log_error_supervisor_not_available",
            no_task="log_final_exceptions",
        )

        log_error_supervisor_not_available = rail.PythonOperator(
            task_id='log_error_supervisor_not_available',
            python_callable=lambda dag_run:  'Supervisor is not assigned/updated as "' +
            dag_run.conf['supervisor'] +
            '" is not available in Replicon'
        )

        log_error_supervisor_and_user_is_same = rail.PythonOperator(
            task_id='log_error_supervisor_and_user_is_same',
            python_callable=lambda:  'Supervisor is not assigned/updated as the "Login name" for user' +
            ' and supervisor is same on the input file'
        )

        arcticwolfof_user_import_logs_search_entries = rail.FilterLogEntriesOperator(
            task_id='arcticwolfof_user_import_logs_search_entries',
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
                'arcticwolfof_user_import_logs_search_entries'))
        )

        log_final_exceptions = rail.PythonOperator(
            task_id='log_final_exceptions',
            python_callable=python_callable_methods.get_exception_message
        )

        if_user_import_logs_search_entries_has_data = rail.IfOperator(
            task_id='if_user_import_logs_search_entries_has_data',
            test='''{{ result('arcticwolfof_user_import_logs_search_entries','length') > 0 | is_truthy }}''',
            yes_task="arcticwolf_user_import_logs_update_entry",
            no_task="arcticwolfofmedicine_supervisor_assignment_logs_update_entry",
        )

        arcticwolf_user_import_logs_update_entry = rail.WriteLogOperator(
            task_id='arcticwolf_user_import_logs_update_entry',
            log="{{dag_run.conf.userimportlogslookup}}",
            message='na',
            severity='na',
            properties=lambda dag_run: {
                "loginname": rail.result('load_found_entry')[0]['properties']['loginname'],
                "action": rail.result('load_found_entry')[0]['properties']['action'],
                "status": 'Error' if 'Error' in rail.result('load_found_entry')[0]['properties']['status'] else ('Exception' if rail.result(
                    'log_final_exceptions') else 'Success'),
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
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname']
            }
        )

        arcticwolfofmedicine_supervisor_assignment_logs_update_entry = rail.FilterLogEntriesOperator(
            task_id='arcticwolfofmedicine_supervisor_assignment_logs_update_entry',
            log="{{dag_run.conf.supervisorlookup}}",
            properties={
                "employeeid": "{{dag_run.conf.employeeid}}",
                "supervisor": "{{dag_run.conf.supervisor}}"
            },
            remove_filtered_entries=True
        )

        load_found_supervisor_entry = rail.PythonOperator(
            task_id='load_found_supervisor_entry',
            python_callable=lambda: rail.load_all_records(rail.result(
                'arcticwolfofmedicine_supervisor_assignment_logs_update_entry'))
        )

        update_supervisor_entry_to_completed = rail.WriteLogOperator(
            task_id='update_supervisor_entry_to_completed',
            log="{{dag_run.conf.supervisorlookup}}",
            message='na',
            severity='na',
            properties=lambda: {
                "jobid": rail.result('load_found_supervisor_entry')[0]['properties']['jobid'],
                "username": rail.result('load_found_supervisor_entry')[0]['properties']['username'],
                "employeeid": rail.result('load_found_supervisor_entry')[0]['properties']['employeeid'],
                "useruri": rail.result('load_found_supervisor_entry')[0]['properties']['useruri'],
                "supervisor": rail.result('load_found_supervisor_entry')[0]['properties']['supervisor'],
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
        can_run_batch_task >> rail.Label('No') >> search_supervisor
        search_supervisor >> if_loginname_not_equals_requestloginname
        if_loginname_not_equals_requestloginname >> rail.Label(
            'Yes') >> if_supervisor_uri_is_present
        if_supervisor_uri_is_present >> rail.Label(
            'Yes') >> if_matching_supervisor_length_greater_than_one
        if_matching_supervisor_length_greater_than_one >> rail.Label(
            'Yes') >> log_error_multiple_same_user >> if_supervisor_uri_is_not_present
        if_matching_supervisor_length_greater_than_one >> rail.Label(
            'No') >> if_supervisor_user_is_enabled
        if_supervisor_user_is_enabled >> rail.Label(
            'Yes') >> get_assigned_permission_for_supervisor >> if_supervisor_permission_not_assigned
        if_supervisor_permission_not_assigned >> rail.Label(
            'Yes') >> assign_permission_set_to_user_supervisor >> if_request_action_equals_to_add
        if_supervisor_permission_not_assigned >> rail.Label(
            'No') >> if_request_action_equals_to_add
        if_request_action_equals_to_add >> rail.Label(
            'Yes') >> update_initial_supervisor >> if_request_action_equals_to_update
        if_request_action_equals_to_add >> rail.Label(
            'No') >> if_request_action_equals_to_update
        if_request_action_equals_to_update >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range >> if_supervisor_uri_is_not_present
        if_request_action_equals_to_update >> rail.Label(
            'No') >> if_supervisor_uri_is_not_present
        if_supervisor_user_is_enabled >> rail.Label(
            'No') >> log_error_supervisor_is_disabled >> if_supervisor_uri_is_not_present
        if_supervisor_uri_is_present >> rail.Label(
            'No') >> if_supervisor_uri_is_not_present
        if_supervisor_uri_is_not_present >> rail.Label(
            'Yes') >> log_error_supervisor_not_available >> log_final_exceptions
        if_supervisor_uri_is_not_present >> rail.Label(
            'No') >> log_final_exceptions
        if_loginname_not_equals_requestloginname >> rail.Label(
            'No') >> log_error_supervisor_and_user_is_same >> log_final_exceptions >> arcticwolfof_user_import_logs_search_entries
        arcticwolfof_user_import_logs_search_entries >> load_found_entry >> if_user_import_logs_search_entries_has_data
        if_user_import_logs_search_entries_has_data >> rail.Label(
            'Yes') >> arcticwolf_user_import_logs_update_entry >> arcticwolfofmedicine_supervisor_assignment_logs_update_entry
        if_user_import_logs_search_entries_has_data >> rail.Label(
            'No') >> arcticwolfofmedicine_supervisor_assignment_logs_update_entry >> load_found_supervisor_entry >> update_supervisor_entry_to_completed
        update_supervisor_entry_to_completed >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
