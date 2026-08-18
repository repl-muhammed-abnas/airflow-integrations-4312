from datetime import timedelta
from airflow.models import Variable
import re
from ge.user_sync_poland.utils import custom_methods, request_payload
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_assign_timeoff_policy_annual_leave_on_termination_dag_id,
        description=f'GE POLAND User Import Assign TimeOff Policy Annual Leave On Termination Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_required_value_to_calculate_termination_accrual_3_13'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_required_value_to_calculate_termination_accrual_3_13',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_required_value_to_calculate_termination_accrual_3_13 = rail.PythonOperator(
            task_id='log_required_value_to_calculate_termination_accrual_3_13',
            python_callable=lambda dag_run: custom_methods.get_required_value_to_calculate_termination_accrual(
                config.POLAND_MASTER_MAPPER, config.DATE_DEFAULT_FORMAT, dag_run)
        )

        get_user_time_off_type_policy_summary_14 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_14',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda res: res['policiesByTimeOffType'] if res else null
        )

        check_if_user_timeoff_policy_present_15 = rail.IfOperator(
            task_id='check_if_user_timeoff_policy_present_15',
            test="{{ result('get_user_time_off_type_policy_summary_14') | is_truthy }}",
            yes_task='get_past_timeoff_policy_lines_and_required_date_16_29',
            no_task='catch_and_log_error'
        )

        get_past_timeoff_policy_lines_and_required_date_16_29 = rail.PythonOperator(
            task_id='get_past_timeoff_policy_lines_and_required_date_16_29',
            python_callable=lambda dag_run: custom_methods.get_past_policy_lines_and_date_for_balance_daterange(rail.result(
                'get_user_time_off_type_policy_summary_14'), config.DATE_DEFAULT_FORMAT, dag_run)
        )

        get_timeoff_booking_hours_for_user_for_required_timeoff_in_daterange_32_36 = rail.RepliconServiceOperator(
            task_id='get_timeoff_booking_hours_for_user_for_required_timeoff_in_daterange_32_36',
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_user_timeoff_booking_details_payload(
                config.DATE_DEFAULT_FORMAT, dag_run),
            data_handler=custom_methods.get_sum_timeoff_booking_hours
        )

        get_starting_balance_script_uri_33 = rail.RepliconServiceOperator(
            task_id='get_starting_balance_script_uri_33',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', "Starting Balance Set To", 'uri', '')
        )

        log_final_policy_lines_with_disable_user_policy_line_37_44 = rail.PythonOperator(
            task_id='log_final_policy_lines_with_disable_user_policy_line_37_44',
            python_callable=lambda dag_run:  custom_methods.final_policy_lines_with_disable_user_policy_line(rail.result(
                'get_past_timeoff_policy_lines_and_required_date_16_29')['past_policy_lines'], rail.result(
                    'log_required_value_to_calculate_termination_accrual_3_13'), rail.result(
                        'get_timeoff_booking_hours_for_user_for_required_timeoff_in_daterange_32_36'), rail.result(
                            'get_starting_balance_script_uri_33'), config.DATE_DEFAULT_FORMAT, dag_run)
        )

        put_user_time_off_account_policy_set_schedule_45 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_45',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_final_policy_lines_with_disable_user_policy_line_37_44')
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in updating time off policy annual leave on termination : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or "Success"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> log_required_value_to_calculate_termination_accrual_3_13

        log_required_value_to_calculate_termination_accrual_3_13 >> get_user_time_off_type_policy_summary_14 >> check_if_user_timeoff_policy_present_15

        check_if_user_timeoff_policy_present_15 >> rail.Label(
            'No') >> catch_and_log_error
        check_if_user_timeoff_policy_present_15 >> rail.Label(
            'Yes') >> get_past_timeoff_policy_lines_and_required_date_16_29

        get_past_timeoff_policy_lines_and_required_date_16_29 >> get_timeoff_booking_hours_for_user_for_required_timeoff_in_daterange_32_36 \
            >> get_starting_balance_script_uri_33 >> log_final_policy_lines_with_disable_user_policy_line_37_44

        log_final_policy_lines_with_disable_user_policy_line_37_44 >> put_user_time_off_account_policy_set_schedule_45 >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
