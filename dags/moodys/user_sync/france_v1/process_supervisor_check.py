from datetime import datetime, timedelta
from airflow.models import Variable
import rail

from moodys.user_sync.france_v1.utils import request_payload, response_filter


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.processs_supervisor_dag_id,
        description='Moodys User Sync - Process Supervisor',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_supervisor,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
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
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_data_for_supervisor_payload,
            response_filter=response_filter.map_supervisor_list_data
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: rail.result('search_supervisor_in_replicon') != [],
            yes_task='is_supervisor_end_date_in_past',
            no_task='has_supervisor_details'
        )

        has_supervisor_details = rail.IfOperator(
            task_id='has_supervisor_details',
            test=lambda dag_run: dag_run.conf['supervisorfirstname'] and dag_run.conf['supervisorlastname'],
            yes_task='create_spervisor',
            no_task='log_supervisor_not_created'
        )

        create_spervisor = rail.RepliconServiceOperator(
            task_id="create_spervisor",
            endpoint="/services/importService1.svc/PutUser3",
            data=lambda dag_run: {
                'user': {
                    'target': {
                        'loginName': dag_run.conf['supervisorid']
                    },
                    'firstname': dag_run.conf['supervisorfirstname'],
                    'lastname': dag_run.conf['supervisorlastname'],
                    'emailAddress': dag_run.conf['supervisoremailid'] if dag_run.conf['supervisoremailid'] else None,
                    'employeeId': dag_run.conf['supervisorid'],
                    'securityConfiguration': {
                        'enabledAuthenticationTypeUris': [
                            'urn:replicon:user-authentication-type:sso'
                        ],
                        'isLoginEnabled': 'true',
                        'loginName': dag_run.conf['supervisorid'],
                        'SSOName': dag_run.conf['supervisorid']
                    },
                    "timeZone": {
                        "uri": dag_run.conf['timezone'],
                        "IANAName": None
                    }
                }
            }
        )

        log_supervisor_not_created = rail.EmptyOperator(
            task_id="log_supervisor_not_created",
        )

        is_supervisor_end_date_in_past = rail.IfOperator(
            task_id='is_supervisor_end_date_in_past',
            test=lambda: datetime.now() > datetime.strptime(
                rail.result('search_supervisor_in_replicon')[0]['enddate'], "%m-%d-%Y")
            if rail.result('search_supervisor_in_replicon') and rail.result('search_supervisor_in_replicon')[0]['enddate'] else False,
            yes_task='log_supervisor_end_date_in_past',
            no_task='is_supervisor_disabled'
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: rail.result('search_supervisor_in_replicon')[
                0]['status'] == 'False',
            yes_task='log_supervisor_disabled_in_replicon',
            no_task='get_missing_supervisor_permissions'
        )

        log_supervisor_disabled_in_replicon = rail.EmptyOperator(
            task_id="log_supervisor_disabled_in_replicon"
        )

        get_missing_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data=lambda: {
                'userUri': rail.result('create_spervisor')['uri']
                if not rail.result('search_supervisor_in_replicon') else
                rail.result('search_supervisor_in_replicon')[0]['uri']
            },
            log_response=True,
            data_handler=response_filter.get_missing_permissions
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permissions') | length > 0 }}",
            yes_task='add_missing_supervisor_permissions',
            no_task='is_new_user_supervisor_assignment'
        )

        add_missing_supervisor_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_missing_supervisor_permissions'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda item: {
                'userUri': rail.result('create_spervisor')['uri']
                if not rail.result('search_supervisor_in_replicon') else
                rail.result('search_supervisor_in_replicon')[0]['uri'],
                'permissionSetUri': item
            }
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=lambda dag_run: dag_run.conf['action'] == 'Add',
            yes_task='update_supervisor_schedule_for_user',
            no_task='get_effective_supervisor_of_user'
        )

        log_supervisor_end_date_in_past = rail.EmptyOperator(
            task_id="log_supervisor_end_date_in_past",
        )

        get_effective_supervisor_of_user = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data={
                "userUri": "{{ dag_run.conf.useruri}}",
                "asOfDate": request_payload.get_today_date()
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
                "supervisorUri": rail.result('create_spervisor')['uri']
                if not rail.result('search_supervisor_in_replicon') else
                rail.result('search_supervisor_in_replicon')[0]['uri'],
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
            properties=lambda item: {
                "countryid":  item['properties']['countryid'],
                "loginname":  item['properties']['loginname'],
                "lastname":  item['properties']['lastname'],
                "firstname": item['properties']['firstname'],
                'action': item['properties']['action'],
                'status': request_payload.get_supervisor_status(),
                'details': request_payload.get_supervisor_message(item['properties']['action'])
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
                'countryid': '{{ item.properties.countryid }}',
                'loginname': '{{ item.properties.loginname }}',
                'lastname': '{{ item.properties.lastname }}',
                'firstname': '{{ item.properties.firstname }}',
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

        filter_user_logs >> search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label(
            'No') >> has_supervisor_details

        has_supervisor_details >> rail.Label(
            'Yes') >> create_spervisor >> get_missing_supervisor_permissions
        has_supervisor_details >> rail.Label(
            'No') >> log_supervisor_not_created >> dummy_filter_user_logs

        is_supervisor_exists >> rail.Label('Yes') >> is_supervisor_end_date_in_past >> rail.Label(
            'No') >> is_supervisor_disabled >> rail.Label('Yes') >> log_supervisor_disabled_in_replicon >> dummy_filter_user_logs
        is_supervisor_disabled >> rail.Label(
            'No') >> get_missing_supervisor_permissions
        is_supervisor_end_date_in_past >> rail.Label(
            'Yes') >> log_supervisor_end_date_in_past >> dummy_filter_user_logs

        get_missing_supervisor_permissions >> should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions >> is_new_user_supervisor_assignment
        should_add_missing_permissions >> rail.Label(
            'No') >> is_new_user_supervisor_assignment

        is_new_user_supervisor_assignment >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user
        is_new_user_supervisor_assignment >> rail.Label(
            'No') >> get_effective_supervisor_of_user >> is_supervisor_changed
        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user >> dummy_filter_user_logs
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> dummy_filter_user_logs

        dummy_filter_user_logs >> is_filtered_userlogs >> rail.Label(
            'Yes') >> update_userlog_entries >> on_error
        is_filtered_userlogs >> rail.Label('No') >> on_error >> is_entries_present_error >> rail.Label(
            'Yes') >> update_userlog_entries_error >> log_to_sumo
        is_entries_present_error >> rail.Label('No') >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
