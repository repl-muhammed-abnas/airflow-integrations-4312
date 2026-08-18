from datetime import timedelta
import json
from airflow.models import Variable
import rail

from deltek_northstar.user_sync_polaris_philippines.utils import request_payload, response_filter, python_callable
from deltek_northstar.user_sync_polaris_philippines.tasks.process_supervisor import process_supervisor_assignment_task_group

null= None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_update_users,
        description='Deltek Costpoint User Import - Process Update Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_update_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_data',
            end_task='catch_and_log_errors',
        )

        get_user_data = rail.RepliconServiceOperator(
            task_id='get_user_data',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": '{{ dag_run.conf.useruri }}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_termination_date_present = rail.IfOperator(
            task_id="if_termination_date_present",
            test=lambda dag_run: dag_run.conf['termination_date'],
            yes_task="is_enddate_greater_than_start_date",
            no_task="empty_is_user_disabled"
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
                    "startDate": request_payload.get_replicon_date(dag_run.conf['current_hire_date']),
                    "endDate": request_payload.get_replicon_date(dag_run.conf['termination_date'])
                }
            }
        )

        if_can_be_disabled = rail.IfOperator(
            task_id ='if_can_be_disabled',
            test = lambda dag_run: python_callable.if_termination_date_in_past(dag_run, config.time_zone),
            yes_task="disable_login",
            no_task="empty_is_user_disabled"
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
                "lastname": "{{dag_run.conf.last_name}}",
                "firstname": "{{dag_run.conf.first_name}}",
                "loginname": "{{dag_run.conf.email_id}}",
                "employeeid": "{{dag_run.conf.empl_id}}",
                'useruri': "{{ dag_run.conf.useruri }}",
                'manager': "{{ dag_run.conf.mgr_empl_id }}",
                'action': 'Validation',
                'status': "Exception",
                'details': "User not Disabled,End date Prior to Start date"
            }
        )

        log_disabled_success = rail.WriteLogOperator(
            task_id = 'log_disabled_success',
            log = '{{ dag_run.conf.user_log }}',
            message = "User Disabled Successfully",
            severity='Success',
            properties = {
                "lastname": "{{dag_run.conf.last_name}}",
                "firstname": "{{dag_run.conf.first_name}}",
                "loginname": "{{dag_run.conf.email_id}}",
                "employeeid": "{{dag_run.conf.empl_id}}",
                'useruri': "{{ dag_run.conf.useruri }}",
                'manager': "{{ dag_run.conf.mgr_empl_id }}",
                'action': 'Disable',
                'status': "Success",
                'details': "User Disabled Successfully"
            }
        )

        empty_is_user_disabled = rail.EmptyOperator(
            task_id='empty_is_user_disabled'
        )

        is_user_disabled =  rail.IfOperator(
            task_id="is_user_disabled",
            test=lambda dag_run : not bool(rail.result('get_user_data')[0]['userDetails']['isEnabled']) and not dag_run.conf['termination_date'],
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
            python_callable=lambda: rail.result('get_user_data')[0][
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

        get_user_assigned_role_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_assigned_role_from_replicon',
            endpoint='/services/ResourceService1.svc/BulkGetProjectRoleAssignmentScheduleForUsers',
            data= lambda dag_run: {
                "userUris": [dag_run.conf['useruri']],
                "dateRange": {
                    "startDate": request_payload.get_today_date(config),
                    "endDate": request_payload.get_today_date(config),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        update_existing_user = rail.RepliconServiceOperator(
            task_id='update_existing_user',
            endpoint='/services/importservice2.svc/CreateUserOrApplyModifications',
            data=lambda dag_run: request_payload.get_create_update_user_payload(config, dag_run, "update_user"),
        )

        if_user_start_date_changed = rail.IfOperator(
            task_id='if_user_start_date_changed',
            test=lambda dag_run: python_callable.is_user_start_date_changed(dag_run, config.instance),
            yes_task='get_default_time_off_type_policy_schedule_for_user',
            no_task='is_supervisor_in_api_response'
        )
        
        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            items=lambda dag_run: dag_run.conf["timeoff_types_available"],
            data={
                "timeOffAccount": {
                    "userUri": '{{dag_run.conf.useruri}}',
                    "timeOffTypeUri": "{{ item.uri }}"
                }
            }
        )

        replace_effective_date = rail.PythonOperator(
            task_id="replace_effective_date",
            python_callable=python_callable.get_updated_effective_date
        )

        assign_default_timeoff_policy_for_assignee = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_default_timeoff_policy_for_assignee',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            items=lambda dag_run: dag_run.conf["timeoff_types_available"],
            data=lambda dag_run, item: {
                "timeOffAccount": {
                    "userUri": rail.render_template('{{dag_run.conf.useruri}}'),
                    "timeOffTypeUri": item['uri']
                },
                "policySetScheduleEntries": json.loads(json.dumps(rail.result('replace_effective_date')
                    [dag_run.conf["timeoff_types_available"].index(item)])
                        .replace('"script"', '"scriptTarget"').replace('"description": null', '"description": "effective"'))
            }
        )

        is_supervisor_in_api_response = rail.IfOperator(
            task_id='is_supervisor_in_api_response',
            test=lambda dag_run: bool(dag_run.conf['mgr_empl_id']),
            yes_task='if_user_is_supervisor',
            no_task='log_user_completion'
        )

        if_user_is_supervisor = rail.IfOperator(
            task_id='if_user_is_supervisor',
            test=lambda dag_run: dag_run.conf['mgr_empl_id'] == dag_run.conf['empl_id'],
            yes_task='log_user_supervisor_same',
            no_task='search_supervisor_in_replicon'
        )

        log_user_supervisor_same = rail.EmptyOperator(
            task_id='log_user_supervisor_same'
        )

        process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
            'useruri', 'update_user', config)

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log = '{{ dag_run.conf.user_log }}',
            message=request_payload.get_update_user_message,
            severity=request_payload.get_update_user_severity,
            properties=lambda dag_run: {
                'lastname': dag_run.conf['last_name'],
                'firstname': dag_run.conf['first_name'],
                'loginname':  dag_run.conf['email_id'],
                'employeeid': dag_run.conf['empl_id'],
                'useruri': dag_run.conf['useruri'],
                'manager': dag_run.conf['mgr_empl_id'],
                'action': 'Update',
                'status': request_payload.get_update_user_severity(),
                'details': request_payload.get_update_user_message(),
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "lastname": "{{dag_run.conf.last_name}}",
                "firstname": "{{dag_run.conf.first_name}}",
                "loginname": "{{dag_run.conf.email_id}}",
                "employeeid": "{{dag_run.conf.empl_id}}",
                'useruri': '{{dag_run.conf.useruri}}',
                'manager': '{{dag_run.conf.mgr_empl_id}}',
                'action': 'Update',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_user_data

        get_user_data >> if_termination_date_present >> rail.Label("No") >> empty_is_user_disabled
        if_termination_date_present >> rail.Label("Yes") >> is_enddate_greater_than_start_date >> rail.Label(
            "No") >> log_endate_exception >> catch_and_log_errors
        is_enddate_greater_than_start_date >> rail.Label("Yes") >> update_employee_endate >> if_can_be_disabled

        if_can_be_disabled >> rail.Label('Yes') >> disable_login >> log_disabled_success >> catch_and_log_errors
        if_can_be_disabled >> rail.Label('No') >> empty_is_user_disabled

        empty_is_user_disabled >> is_user_disabled >> rail.Label('Yes') >> enable_login >> get_current_udf_values
        is_user_disabled >> rail.Label('No') >> get_current_udf_values

        get_current_udf_values >> get_effective_user_groupmembership
        get_effective_user_groupmembership >> get_user_assigned_role_from_replicon >> update_existing_user >> if_user_start_date_changed
        if_user_start_date_changed >> rail.Label('Yes') >> get_default_time_off_type_policy_schedule_for_user >> \
        replace_effective_date >> assign_default_timeoff_policy_for_assignee >> is_supervisor_in_api_response
        if_user_start_date_changed >> rail.Label('No') >> is_supervisor_in_api_response

        is_supervisor_in_api_response >> rail.Label('No') >> log_user_completion
        is_supervisor_in_api_response >> rail.Label('Yes') >> if_user_is_supervisor

        if_user_is_supervisor >> rail.Label('No') >> process_supervisor_entry
        if_user_is_supervisor >> rail.Label('Yes') >> log_user_supervisor_same >> log_user_completion
        process_supervisor_exit >> log_user_completion >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
