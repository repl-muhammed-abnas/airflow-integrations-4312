from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from strayeruniversity.user_sync_v3.utils import request_payload


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'strayeruniversity_usersync_disable_user_child_v3_{config.instance}',
        description=f'strayeruniversity_usersync_disable_user_child_v3_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.disable_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details_for_disable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details_for_disable',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_details_for_disable = rail.RepliconServiceOperator(
            task_id='get_user_details_for_disable',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}"
                    }
                ]
            }
        )

        get_startingbalance_script_uri = rail.RepliconServiceOperator(
            task_id='get_startingbalance_script_uri',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetActiveScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        get_all_customfield_dropdowns = rail.RepliconServiceOperator(
            task_id='get_all_customfield_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ dag_run.conf.employee_status_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['employeestatus'], 'uri', '')
        )

        if_emp_status_is_T_is_enabled_true = rail.IfOperator(
            task_id='if_emp_status_is_T_is_enabled_true',
            test='''{{ dag_run.conf.employeestatus == 'T' and result('get_user_details_for_disable')[0].userDetails.isEnabled | is_truthy }}''',
            yes_task="disable_user",
            no_task="if_emp_status_is_T_is_enabled_false",
        )

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_empstatus_uri_present = rail.IfOperator(
            task_id='if_empstatus_uri_present',
            test='''{{ result('get_all_customfield_dropdowns') | is_truthy }}''',
            yes_task="update_dropdown_value",
            no_task="if_termdate_present",
        )

        update_dropdown_value = rail.RepliconServiceOperator(
            task_id='update_dropdown_value',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.employee_status_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_all_customfield_dropdowns') }}"
            }
        )

        if_termdate_present = rail.IfOperator(
            task_id='if_termdate_present',
            test='''{{ dag_run.conf.termdate | is_truthy }}''',
            yes_task="update_employmentdaterange",
            no_task="log_userdisable_success",
        )

        update_employmentdaterange = rail.RepliconServiceOperator(
            task_id='update_employmentdaterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_date_for_disableuser
        )

        get_time_off_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_timeoff_type_assignments_present = rail.IfOperator(
            task_id='if_timeoff_type_assignments_present',
            test='''{{ result('get_time_off_type_assignments_for_user') | is_truthy }}''',
            yes_task="trigger_assign_0_balance_timeoff",
            no_task="trigger_remove_future_time_off_bookings",
        )

        trigger_assign_0_balance_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_assign_0_balance_timeoff',
            items=lambda: rail.result(
                'get_time_off_type_assignments_for_user'),
            trigger_dag_id=f'strayeruniversity_usersync_assign_0_balance_timeoff_child_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "timeoffuri": item,
                "useruri": dag_run.conf['useruri'],
                "terminationdate": dag_run.conf['termdate'],
                "username": dag_run.conf['username'],
                "emplid": dag_run.conf['emplid'],
                "logger": dag_run.conf['logger'],
                "scripttarget": rail.result('get_startingbalance_script_uri')
            }
        )

        wait_for_completion_trigger_assign_0_balance_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_assign_0_balance_timeoff',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_assign_0_balance_timeoff") }}'
        )

        trigger_remove_future_time_off_bookings = rail.TriggerDagRunOperator(
            task_id='trigger_remove_future_time_off_bookings',
            trigger_dag_id=f'strayeruniversity_usersync_remove_future_time_off_bookings_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "terminationdate": dag_run.conf['termdate']
            }
        )

        wait_for_completion_trigger_remove_future_time_off_bookings = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_remove_future_time_off_bookings',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_remove_future_time_off_bookings") }}'
        )

        gather_results_from_dag_run_remove_future_time_off_bookings = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_dag_run_remove_future_time_off_bookings',
            dag_runs="{{result('trigger_remove_future_time_off_bookings')}}",
            dagrun_task_id="final_response_from_dag",
            target='result'
        )

        if_error_in_gather_reponse_from_dag_run_remove_future_time_off_bookings = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_dag_run_remove_future_time_off_bookings',
            test=lambda: bool(rail.result("gather_results_from_dag_run_remove_future_time_off_bookings") and "Error" in (rail.result(
                "gather_results_from_dag_run_remove_future_time_off_bookings")[0])),
            yes_task="fail_with_error_in_remove_future_time_off_bookings",
            no_task="log_userdisable_success",
        )

        fail_with_error_in_remove_future_time_off_bookings = rail.FailOperator(
            task_id='fail_with_error_in_remove_future_time_off_bookings',
            message="Error in removing future timeoff bookings"
        )

        if_emp_status_is_T_is_enabled_false = rail.IfOperator(
            task_id='if_emp_status_is_T_is_enabled_false',
            test='''{{ dag_run.conf.employeestatus == 'T' and result('get_user_details_for_disable')[0].userDetails.isEnabled | is_falsy }}''',
            yes_task="log_userdisable_skipped",
            no_task="catch_and_log_error",
        )

        log_userdisable_success = rail.WriteLogOperator(
            task_id="log_userdisable_success",
            log='{{ dag_run.conf.logger}}',
            message="Success",
            severity="Success",
            properties=lambda dag_run: {
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Disable user",
                "status": "Success" + ((" ; " + str(rail.result("gather_results_from_dag_run_remove_future_time_off_bookings")[0])) if rail.result("gather_results_from_dag_run_remove_future_time_off_bookings") else ""),
                'details': get_dagrun_ecid(dag_run),
            }
        )

        log_userdisable_skipped = rail.WriteLogOperator(
            task_id="log_userdisable_skipped",
            log='{{ dag_run.conf.logger}}',
            message="Skipped",
            severity="Skipped",
            properties=lambda dag_run: {
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Disable user",
                "status": "Skipped",
                'details': get_dagrun_ecid(dag_run),
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Disable user",
                "status": "Error",
                "details": "{{ dag_run_ecid() }}" + "-" + "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_user_details_for_disable

        get_user_details_for_disable >> get_startingbalance_script_uri >> get_all_customfield_dropdowns >> \
            if_emp_status_is_T_is_enabled_true

        if_emp_status_is_T_is_enabled_true >> rail.Label(
            'Yes') >> disable_user >> if_empstatus_uri_present
        if_emp_status_is_T_is_enabled_true >> rail.Label(
            'No') >> if_emp_status_is_T_is_enabled_false

        if_empstatus_uri_present >> rail.Label(
            'Yes') >> update_dropdown_value >> if_termdate_present
        if_empstatus_uri_present >> rail.Label('No') >> if_termdate_present

        if_termdate_present >> rail.Label(
            'Yes') >> update_employmentdaterange >> get_time_off_type_assignments_for_user >> if_timeoff_type_assignments_present
        if_termdate_present >> rail.Label('No') >> log_userdisable_success

        if_timeoff_type_assignments_present >> rail.Label(
            'Yes') >> trigger_assign_0_balance_timeoff >> wait_for_completion_trigger_assign_0_balance_timeoff >> trigger_remove_future_time_off_bookings
        if_timeoff_type_assignments_present >> rail.Label(
            'No') >> trigger_remove_future_time_off_bookings

        trigger_remove_future_time_off_bookings >> wait_for_completion_trigger_remove_future_time_off_bookings >> gather_results_from_dag_run_remove_future_time_off_bookings >> if_error_in_gather_reponse_from_dag_run_remove_future_time_off_bookings

        if_error_in_gather_reponse_from_dag_run_remove_future_time_off_bookings >> rail.Label(
            'Yes') >> fail_with_error_in_remove_future_time_off_bookings >> log_userdisable_success
        if_error_in_gather_reponse_from_dag_run_remove_future_time_off_bookings >> rail.Label(
            'No') >> log_userdisable_success

        if_emp_status_is_T_is_enabled_false >> rail.Label(
            'Yes') >> log_userdisable_skipped >> catch_and_log_error
        if_emp_status_is_T_is_enabled_false >> rail.Label(
            'No') >> catch_and_log_error

        log_userdisable_success >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
