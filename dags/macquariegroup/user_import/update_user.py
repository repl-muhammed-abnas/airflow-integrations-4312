from datetime import timedelta
import rail
from macquariegroup.user_import.utils import data_handlers
from macquariegroup.user_import.utils.request_payload import get_update_payload, get_today_date, get_search_supervisor_payload
from macquariegroup.user_import.utils.custom_methods import map_supervisor_list, get_log_message
from airflow.models import Variable

#pylint: disable=too-many-statements
def create_update_user_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'macquarie_user_import_update_users_child_{config.instance}',
        description=f'Macquarie User Import update user child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,

    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "create_update_user_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_update_user_log',
            end_task="catch_and_log_error",
        )

        create_update_user_log = rail.CreateLogOperator(
            task_id='create_update_user_log'
        )

        search_user_with_loginname = rail.RepliconServiceOperator(
            task_id="search_user_with_loginname",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "loginName": "{{dag_run.conf.login_name}}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
        )

        def callable_is_different_user_found(dag_run):
            search_user_with_loginname = rail.result(
                "search_user_with_loginname")

            # no user with login name
            if not search_user_with_loginname:
                return True

            # user with new login name already present
            if search_user_with_loginname[0]['userDetails']['uri'] != dag_run.conf['user_uri']:
                return False

            return True

        is_different_user_found = rail.PythonOperator(
            task_id="is_different_user_found",
            python_callable=callable_is_different_user_found
        )

        can_update_user = rail.IfOperator(
            task_id="can_update_user",
            test="{{result('is_different_user_found') | is_truthy}}",
            yes_task="get_user_details",
            no_task="log_user_found_with_loginname"
        )

        log_user_found_with_loginname = rail.WriteLogOperator(
            task_id="log_user_found_with_loginname",
            severity="Exception",
            log="{{result('create_update_user_log')}}",
            message="User with login name - {{dag_run.conf.login_name}} already present",
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.first_name}}" + "." + "{{dag_run.conf.last_name}}",
                'employee_id': "{{dag_run.conf.emp_id}}",
                'action': 'Update',
                'status': 'Exception',
                "details": "User with login name - {{dag_run.conf.login_name}} already present",
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{dag_run.conf.user_uri}}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_effectivegroup_membership = rail.RepliconServiceOperator(
            task_id="get_effectivegroup_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{dag_run.conf.user_uri}}",
                "dateRange": None
            },
            data_handler=data_handlers.get_effectivegroup_membership_filter
        )

        get_users_current_timesheet_end_date = rail.RepliconServiceOperator(
            task_id="get_users_current_timesheet_end_date",
            endpoint="services/TimesheetService1.svc/GetNextTimesheetDueDate",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "asOfDate": get_today_date()
            }
        )

        create_update_payload = rail.PythonOperator(
            task_id="create_update_payload",
            python_callable=get_update_payload
        )

        update_user = rail.RepliconServiceOperator(
            task_id="update_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data="{{result('create_update_payload') | to_json}}"
        )

        is_update_failed = rail.IfOperator(
            task_id="is_update_failed",
            test="{{ result('update_user').errors | is_truthy }}",
            yes_task="log_update_failed",
            no_task="is_recovery_enabled_is_no"
        )

        log_update_failed = rail.WriteLogOperator(
            task_id="log_update_failed",
            message="{{ result('update_user').errors.DisplayText }}",
            severity="Error",
            log="{{result('create_update_user_log')}}",
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.first_name}}" + "." + "{{dag_run.conf.last_name}}",
                'employee_id': "{{dag_run.conf.emp_id}}",
                "action": "Update",
                "Status": "Error",
                "details": "User Updating failed; {{ result('update_user').errors.DisplayText }}"
            }
        )

        is_recovery_enabled_is_no = rail.IfOperator(
            task_id='is_recovery_enabled_is_no',
            test='{{ dag_run.conf.recovery_enabled == "No" }}',
            yes_task='enable_login',
            no_task='is_supervisor_present'
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        get_timesheeturis_to_delete = rail.RepliconServiceOperator(
            task_id='get_timesheeturis_to_delete',
            endpoint="/services/TimesheetListService1.svc/GetData",
            data=data_handlers.get_timesheet_details_payload,
            data_handler=data_handlers.get_timesheet_uris
        )

        is_timesheet_uris_to_delete = rail.IfOperator(
            task_id='is_timesheet_uris_to_delete',
            test="{{ result('get_timesheeturis_to_delete') | length > 0 }}",
            yes_task="create_timesheet_delete_batch",
            no_task="generate_timesheet_period",
        )

        create_timesheet_delete_batch = rail.RepliconServiceOperator(
            task_id='create_timesheet_delete_batch',
            endpoint="/services/TimesheetService1.svc/CreateTimesheetDeleteBatch",
            data=lambda: {
                "timesheetUris": rail.result('get_timesheeturis_to_delete'),
                "deleteOptionUri": "urn:replicon:timesheet-delete-option:delete-overlapping-time-and-payable-time-entries"
            }
        )

        execute_timesheet_delete_batch = rail.RepliconServiceOperator(
            task_id='execute_timesheet_delete_batch',
            endpoint="/services/TimesheetService1.svc/ExecuteTimesheetDeleteBatch",
            data={
                "timesheetDeleteBatchUri": "{{ result('create_timesheet_delete_batch') }}"
            }
        )

        generate_timesheet_period = rail.RepliconServiceOperator(
            task_id="generate_timesheet_period",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "date": get_today_date(),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        is_supervisor_present = rail.IfOperator(
            task_id="is_supervisor_present",
            test="{{dag_run.conf.supervisor | is_truthy}}",
            yes_task="search_supervisor_in_replicon",
            no_task="log_success"
        )

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id="search_supervisor_in_replicon",
            endpoint="/services/UserListService1.svc/GetData",
            data=get_search_supervisor_payload,
            response_filter=map_supervisor_list
        )

        get_effective_supervisor_of_user = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data={
                "userUri": "{{ dag_run.conf.user_uri}}",
                "asOfDate": get_today_date()
            }
        )

        # pylint: disable=too-many-return-statements
        def bool_can_log_for_supervisor_processing(dag_run):
            if not dag_run.conf['supervisor']:
                return False

            # supervisor not found in replicon not as enabled nor disabled
            if dag_run.conf['supervisor'] and not rail.result("search_supervisor_in_replicon"):
                return False

            # Supervisor is disabled in Replicon, default will get assigned
            if dag_run.conf['supervisor'] and rail.result("search_supervisor_in_replicon")[0]['enabled'] is False:
                return True
            # supervisor is not assigned to user
            if not rail.result("get_effective_supervisor_of_user"):
                return True

            # more than 1 supervisor found with same emp ID
            if len(rail.result("search_supervisor_in_replicon")) > 1:
                return False

            # comparing supervisor uri
            if rail.result("search_supervisor_in_replicon")[0]['useruri'] != rail.result("get_effective_supervisor_of_user")['supervisor']['uri']:
                return True

            return False

        can_log_for_supervisor_processing = rail.IfOperator(
            task_id="can_log_for_supervisor_processing",
            test=bool_can_log_for_supervisor_processing,
            yes_task="log_for_supervisor_assignment",
            no_task="log_success"
        )

        log_for_supervisor_assignment = rail.WriteLogOperator(
            task_id="log_for_supervisor_assignment",
            severity="process",
            log="{{dag_run.conf.supervisor_log}}",
            message="Process Supervisor assignment",
            properties={
                "file_name": "{{dag_run.conf.file_name}}",
                'user_name': "{{dag_run.conf.first_name}}" + "." + "{{dag_run.conf.last_name}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "action": "Update",
                "user_uri": "{{ dag_run.conf.user_uri }}",
                "supervisor": "{{ dag_run.conf.supervisor}}",
                "can_assign_default": "{{ dag_run.conf.supervisor and result('search_supervisor_in_replicon')[0].enabled == False}}",
                "default_supervisor_uri": "{{ dag_run.conf.default_supervisor_uri }}",
                "supervisor_status": "{{result('search_supervisor_in_replicon')[0].enabled if result('search_supervisor_in_replicon') else 'NA'}}",
                "supervisor_uri": "{{ result('search_supervisor_in_replicon')[0].useruri if result('search_supervisor_in_replicon') else ''}}",
                "user_employee_id": "{{dag_run.conf.emp_id}}",
                "supervisor_permission": "Gen3 Supervisor"
            }
        )

        log_success = rail.WriteLogOperator(
            task_id="log_success",
            severity='Success',
            message='User Updated successfully',
            log="{{result('create_update_user_log')}}",
            properties=lambda dag_run: {
                'userloginname': dag_run.conf['login_name'],
                'user_name': dag_run.conf['first_name'] + "." + dag_run.conf['last_name'],
                'employee_id': dag_run.conf['emp_id'],
                'action': 'Update',
                'status': 'Success',
                'details': get_log_message('create_update_payload', 'update')
            }
        )

        can_send_general_email_notification = rail.IfOperator(
            task_id="can_send_general_email_notification",
            # Recovery Enabled is updated from 'No' to 'Yes'
            test=lambda dag_run: dag_run.conf['recovery_enabled'].lower() == "no",
            yes_task="send_general_email_notification"
        )

        send_general_email_notification = rail.TriggerDagRunOperator(
            task_id="send_general_email_notification",
            trigger_dag_id=f"macquarie_user_import_send_recovery_enabled_emails_child_{config.instance}",
            conf=lambda dag_run: {
                **dag_run.conf,
                **{
                    "log": rail.result('create_update_user_log'),
                    "useruri": dag_run.conf['user_uri'],
                    "action": "Update",
                    'user_name': f"{dag_run.conf['first_name']}.{dag_run.conf['last_name']}",
                }
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            log="{{result('create_update_user_log')}}",
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.first_name}}" + "." + "{{dag_run.conf.last_name}}",
                'employee_id': "{{dag_run.conf.emp_id}}",
                'action': 'Update',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> rail.Label("On Error") >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_update_user_log
        create_update_user_log >> search_user_with_loginname >> is_different_user_found >>\
            can_update_user >> rail.Label("No") >> log_user_found_with_loginname >> rail.Label(
                "On Error") >> catch_and_log_error
        can_update_user >> rail.Label("No") >> get_user_details >> \
            get_effectivegroup_membership >> get_users_current_timesheet_end_date >> create_update_payload >> update_user

        update_user >> is_update_failed >> rail.Label(
            "No") >> is_recovery_enabled_is_no

        is_recovery_enabled_is_no >> rail.Label(
            "No") >> is_supervisor_present \
            >> rail.Label("Yes") >> search_supervisor_in_replicon >> get_effective_supervisor_of_user \
            >> can_log_for_supervisor_processing >> rail.Label("Yes") >>\
            log_for_supervisor_assignment >> log_success

        is_update_failed >> rail.Label("Yes") >> log_update_failed >> rail.Label(
            "On Error") >> catch_and_log_error
        is_supervisor_present >> rail.Label("No") >> log_success
        can_log_for_supervisor_processing >> rail.Label("No") >> log_success >> can_send_general_email_notification >>\
            rail.Label("Yes") >>send_general_email_notification >> rail.Label(
                "On Error") >> catch_and_log_error >> log_to_sumo

        is_recovery_enabled_is_no >> rail.Label(
            "Yes") >> enable_login >> get_timesheeturis_to_delete >> is_timesheet_uris_to_delete

        is_timesheet_uris_to_delete >> rail.Label(
            "Yes") >> create_timesheet_delete_batch >> execute_timesheet_delete_batch >> generate_timesheet_period >>\
            is_supervisor_present

        is_timesheet_uris_to_delete >> rail.Label(
            "No") >> generate_timesheet_period

    return dag


rail.for_each_instance(create_update_user_dag)
