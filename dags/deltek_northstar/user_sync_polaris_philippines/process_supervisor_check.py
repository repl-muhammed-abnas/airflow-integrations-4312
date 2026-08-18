from datetime import timedelta
from airflow.models import Variable
import rail
from deltek_northstar.user_sync_polaris_philippines.utils import request_payload, response_filter


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.processs_supervisor,
        description='Deltek Costpoint User Import - Process Supervisor',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_supervisor,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='filter_user_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='filter_user_logs',
            end_task='on_error',
        )

        filter_user_logs = rail.FilterLogEntriesOperator(
            task_id='filter_user_logs',
            log='{{ dag_run.conf.user_log }}',
            properties={
                'loginname': '{{ dag_run.conf.loginname }}'
            },
            remove_filtered_entries=True
        )

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_supervisor_data_with_manager_id,
            data_handler=response_filter.map_supervisor_list_data
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: bool(rail.result('search_supervisor_in_replicon')),
            yes_task='is_supervisor_disabled',
            no_task='log_supervisor_not_present'
        )

        log_supervisor_not_present = rail.EmptyOperator(
            task_id="log_supervisor_not_present",
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: rail.result('search_supervisor_in_replicon')[
                0]['status'],
            yes_task='log_supervisor_disabled_in_replicon',
            no_task='get_missing_supervisor_permission'
        )

        log_supervisor_disabled_in_replicon = rail.EmptyOperator(
            task_id="log_supervisor_disabled_in_replicon"
        )

        get_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data=lambda: {
                'userUri': "{{ dag_run.conf.useruri}}"
            },
            data_handler=response_filter.is_assign_supervisorpermission
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permission') | is_truthy }}",
            yes_task='add_missing_supervisor_permission',
            no_task='is_new_user_supervisor_assignment'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'permissionSetUri': dag_run.conf['supervisor_permission_uri']
            }
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=lambda dag_run: dag_run.conf['action'] == 'Add',
            yes_task='update_supervisor_schedule_for_user',
            no_task='get_effective_supervisor_of_user'
        )

        get_effective_supervisor_of_user  = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data={
                "userUri": "{{ dag_run.conf.useruri}}",
                "asOfDate": request_payload.get_today_date(config)
            }
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=request_payload.validate_supervisor_changed,
            yes_task='update_supervisor_schedule_for_user',
            no_task='same_supervisor_already_assigned'
        )

        same_supervisor_already_assigned = rail.EmptyOperator(
            task_id="same_supervisor_already_assigned",
        )

        update_supervisor_schedule_for_user = rail.RepliconServiceOperator(
            task_id="update_supervisor_schedule_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon')['uri'],
                "dateRange": None if dag_run.conf['action'] == 'Add' else {
                    "startDate": request_payload.get_replicon_date(dag_run.conf['effectivedate'])
                }
            }
        )

        dummy_filter_user_logs = rail.EmptyOperator(
            task_id="dummy_filter_user_logs",
        )

        is_filtered_userlogs = rail.IfOperator(
            task_id='is_filtered_userlogs',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries',
            no_task='on_error'
        )

        update_userlog_entries = rail.WriteLogOperator(
            task_id='update_userlog_entries',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            items="{{ result('filter_user_logs') }}",
            properties=lambda item, dag_run: {
                'lastname': item['properties']['lastname'],
                'firstname': item['properties']['firstname'],
                'loginname':  item['properties']['loginname'],
                'employeeid': item['properties']['employeeid'],
                'useruri': item['properties']['useruri'],
                'manager': item['properties']['manager'],
                'action': item['properties']['action'],
                'status': 'Update',
                'details':request_payload.get_supervisor_message(item['properties']['action'], dag_run),
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        is_entries_present_error = rail.IfOperator(
            task_id='is_entries_present_error',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries_error',
            no_task='log_to_sumo'
        )

        update_userlog_entries_error = rail.WriteLogOperator(
            task_id='update_userlog_entries_error',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            severity='Error',
            items="{{ result('filter_user_logs') }}",
            properties={
                'lastname': '{{ item.properties.lastname }}',
                'firstname': '{{ item.properties.firstname }}',
                'loginname': '{{ item.properties.loginname }}',
                'employeeid': '{{ item.properties.employeeid }}',
                'useruri': '{{ item.properties.useruri }}',
                'manager': '{{ item.properties.manager }}',
                'action': '{{ item.properties.action }}',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> on_error
        can_run_batch_task >> rail.Label('No') >> filter_user_logs

        filter_user_logs >> search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label('No') >> log_supervisor_not_present
        log_supervisor_not_present >> dummy_filter_user_logs

        is_supervisor_exists >> rail.Label('Yes') >> is_supervisor_disabled >> rail.Label('Yes') >> log_supervisor_disabled_in_replicon
        log_supervisor_disabled_in_replicon >> dummy_filter_user_logs
        is_supervisor_disabled >> rail.Label('No') >> get_missing_supervisor_permission >> should_add_missing_permissions >> rail.Label(
            "Yes") >> add_missing_supervisor_permission >> is_new_user_supervisor_assignment
        should_add_missing_permissions >> rail.Label(
            "No") >> is_new_user_supervisor_assignment

        is_new_user_supervisor_assignment >> rail.Label('Yes') >> update_supervisor_schedule_for_user
        is_new_user_supervisor_assignment >> rail.Label('No') >> get_effective_supervisor_of_user >> is_supervisor_changed
        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user >> dummy_filter_user_logs
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> dummy_filter_user_logs
        dummy_filter_user_logs >> is_filtered_userlogs >> rail.Label('Yes') >> update_userlog_entries >> on_error
        is_filtered_userlogs >> rail.Label('No') >> on_error >> is_entries_present_error  >> rail.Label('Yes') >> update_userlog_entries_error >> log_to_sumo
        is_entries_present_error  >> rail.Label('No') >> log_to_sumo



    return dag


rail.for_each_instance(create_child_dag_wbs)
