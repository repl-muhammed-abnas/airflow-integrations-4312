from datetime import timedelta
from airflow.models import Variable
import rail

from ttecholdingsinc.user_sync_v1.utils import request_payload, response_filter
from ttecholdingsinc.user_sync_v1.tasks.process_supervisor import process_supervisor_assignment_task_group

null= None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_update_users_dagid,
        description='TTEC HOLDINGS INC - User Sync Process Update Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_update_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_info'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_info',
            end_task='catch_and_log_errors',
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": '{{ dag_run.conf.useruri }}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            data_handler=lambda res: res[0]
        )

        is_status_terminated = rail.IfOperator(
            task_id="is_status_terminated",
            test=lambda dag_run: dag_run.conf['employee_status'] =='Terminated',
            yes_task="is_enddate_greater_than_start_date",
            no_task="is_rehire_user"
        )

        is_enddate_greater_than_start_date = rail.IfOperator(
            task_id ='is_enddate_greater_than_start_date',
            test = request_payload.validate_enddate,
            yes_task="update_employee_endate",
            no_task="log_endate_exception"
        )

        update_employee_endate = rail.RepliconServiceOperator(
            task_id='update_employee_endate',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(dag_run.conf['start_date']),
                    "endDate": request_payload.get_replicon_date(dag_run.conf['end_date'])
                }
            }
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/securityservice1.svc/DisableLogin',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        log_endate_exception = rail.WriteLogOperator(
            task_id = 'log_endate_exception',
            log = '{{ dag_run.conf.user_log }}',
            message = "User not Disabled,End date Prior to Start date",
            severity='Exception',
            properties ={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "last_name": "{{ dag_run.conf.last_name }}",
                "first_name": "{{ dag_run.conf.first_name }}",
                "action": "Validation",
                "status": "Exception",
                'details': "User not Disabled,End date Prior to Start date",
            }
        )

        log_disabled_success = rail.WriteLogOperator(
            task_id = 'log_disabled_success',
            log = '{{ dag_run.conf.user_log }}',
            message = "User Disabled Successfully",
            severity='Success',
            properties = {
                 "employee_id": "{{ dag_run.conf.employee_id }}",
                "last_name": "{{ dag_run.conf.last_name }}",
                "first_name": "{{ dag_run.conf.first_name }}",
                "action": "Disable",
                "status": "Success",
                'details': "User Disabled Successfully",
            }
        )

        is_rehire_user = rail.IfOperator(
            task_id="is_rehire_user",
            test=request_payload.validate_rehire,
            yes_task="enable_login",
            no_task="get_current_udf_values"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        get_current_udf_values = rail.PythonOperator(
            task_id='get_current_udf_values',
            python_callable=lambda: rail.result('get_user_info')[
                'userDetails']['customFieldValues']
        )

        get_effective_user_groupmembership = rail.RepliconServiceOperator(
            task_id='get_effective_user_groupmembership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                "userUri": "{{dag_run.conf.useruri}}",
                "dateRange": null
            },
            data_handler=response_filter.get_effective_user_groupmembership_filter
        )

        get_assigned_policy_to_user = rail.RepliconServiceOperator(
            task_id='get_assigned_policy_to_user',
            endpoint='/services/PolicySetService1.svc/GetAssignedPolicySetsForUser',
            data={
                "userUri": "{{dag_run.conf.useruri}}"
            },
            data_handler=response_filter.map_assigned_policy_to_user
        )

        apply_user_modifications = rail.RepliconServiceOperator(
            task_id='apply_user_modifications',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.apply_user_modifications_payload,
        )

        is_user_update_failed = rail.IfOperator(
            task_id = "is_user_update_failed",
            test="{{ result('apply_user_modifications').errors | is_truthy }}",
            yes_task="log_update_user_failed",
            no_task="search_supervisor_in_replicon"
        )

        log_update_user_failed = rail.WriteLogOperator(
            task_id='log_update_user_failed',
            log = '{{ dag_run.conf.user_log }}',
            message="{{ result('apply_user_modifications').errors }}",
            severity='Error',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf['employee_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                "action": "Rehire" if request_payload.validate_rehire(dag_run) else 'Update',
                'status': 'Error',
                'details': rail.result('apply_user_modifications')['errors']
            }
        )

        process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
            'useruri', 'update_user')

        is_user_rehire = rail.IfOperator(
            task_id="is_user_rehire",
            test=request_payload.validate_rehire,
            yes_task="process_time_off_type_assignment_rehire_user",
            no_task="log_user_completion"
        )

        process_time_off_type_assignment_rehire_user = rail.TriggerDagRunOperator(
            task_id='process_time_off_type_assignment_rehire_user',
            trigger_dag_id=config.process_timeoff_type_assignment_new_user_dagid,
            conf=lambda dag_run:{
                'file_name': dag_run.conf['file_name'],
                "employee_id": dag_run.conf['employee_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                'start_date': dag_run.conf['start_date'],
                'useruri': dag_run.conf['useruri'],
                'required_timeoff_types_names': dag_run.conf['get_required_time_off_types_names'],
                "user_log": dag_run.conf['user_log'],
                "action": 'rehire',
                'disabled_supervisor_exception_present': request_payload.get_task_state('log_supervisor_disabled_in_replicon') == 'success'
            },
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_time_off_type_assignment_rehire_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_type_assignment_rehire_user',
            dag_runs='{{ result("process_time_off_type_assignment_rehire_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_time_off_type_error_logs_rehire_user = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_off_type_error_logs_rehire_user',
            dag_runs='{{ result("process_time_off_type_assignment_rehire_user") }}',
            dagrun_task_id='catch_and_log_errors',
            flatten=True,
        )

        gather_time_off_type_exception_logs_rehire_user = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_off_type_exception_logs_rehire_user',
            dag_runs='{{ result("process_time_off_type_assignment_rehire_user") }}',
            dagrun_task_id='log_time_off_type_not_available',
            flatten=True,
        )

        has_any_error_or_exceptions_present_rehire_user = rail.IfOperator(
            task_id="has_any_error_or_exceptions_present_rehire_user",
            test="{{ result('gather_time_off_type_error_logs_rehire_user') | is_truthy \
                or result('gather_time_off_type_exception_logs_rehire_user') | is_truthy }}",
            yes_task= 'log_error_or_exceptions_present_rehire_user',
            no_task='log_user_completion'
        )

        log_error_or_exceptions_present_rehire_user = rail.EmptyOperator(
            task_id='log_error_or_exceptions_present_rehire_user'
        )

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log = '{{ dag_run.conf.user_log }}',
            message=request_payload.get_update_user_message,
            severity=request_payload.get_update_user_severity,
            properties=lambda dag_run: {
                "employee_id": dag_run.conf['employee_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                "action": "Update",
                "status": request_payload.get_update_user_severity(dag_run),
                'details': request_payload.get_update_user_message(dag_run)
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "employee_id": "{{dag_run.conf.employee_id}}",
                "last_name": "{{dag_run.conf.last_name}}",
                "first_name": "{{dag_run.conf.first_name}}",
                "action": "Update",
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_user_info

        get_user_info >> is_status_terminated >> rail.Label("No") >> is_rehire_user
        is_status_terminated >> rail.Label("Yes") >> is_enddate_greater_than_start_date >> rail.Label("No") >> log_endate_exception >> catch_and_log_errors
        is_enddate_greater_than_start_date >> rail.Label("No") >> update_employee_endate >> disable_login >> log_disabled_success >> catch_and_log_errors

        is_rehire_user >> rail.Label('No') >> get_current_udf_values
        is_rehire_user >> rail.Label('Yes') >> enable_login >> get_current_udf_values >> get_effective_user_groupmembership >> get_assigned_policy_to_user
        get_assigned_policy_to_user >> apply_user_modifications
        apply_user_modifications >> is_user_update_failed >> rail.Label('Yes') >> log_update_user_failed >> catch_and_log_errors
        is_user_update_failed >> rail.Label('No') >> process_supervisor_entry
        process_supervisor_exit >> is_user_rehire >> rail.Label('Yes') >> process_time_off_type_assignment_rehire_user
        is_user_rehire >> rail.Label('No') >> log_user_completion
        process_time_off_type_assignment_rehire_user >> wait_for_process_time_off_type_assignment_rehire_user
        wait_for_process_time_off_type_assignment_rehire_user >> gather_time_off_type_error_logs_rehire_user >> gather_time_off_type_exception_logs_rehire_user
        gather_time_off_type_exception_logs_rehire_user >> has_any_error_or_exceptions_present_rehire_user >> rail.Label('No') >> log_user_completion
        has_any_error_or_exceptions_present_rehire_user >> rail.Label('Yes') >> log_error_or_exceptions_present_rehire_user >> catch_and_log_errors

        log_user_completion >> catch_and_log_errors >> log_to_sumo


    return dag

rail.for_each_instance(create_child_dag)
