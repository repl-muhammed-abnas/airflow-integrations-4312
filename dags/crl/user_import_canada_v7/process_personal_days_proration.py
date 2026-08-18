# V2.7 - Proration of [CAN] Jour personnel/Personal Days
#
# Sibling DAG to process_special_accrual_time_off_type.py. Handles BOTH
# outbound (going on leave) and return (coming back from leave) Personal Days
# policy adjustments via a single mode-dispatch step. All business rules live
# in the personal_days_proration_mapper; this DAG only orchestrates the calls.
#
# DEPLOYMENT SCOPE: V2.7 is currently enabled only on instances/trial.py.
# prod.py and sandbox.py do NOT define process_personal_days_proration_dagid
# nor the six mapper constants (LEAVE_OUT_IMPACT_RULES, BUCKET_TABLE, etc.) -
# create_child_dag below returns None for those instances, and the trigger in
# process_update_users.py is guarded by getattr(...). Before rolling V2.7 to
# prod/sandbox, mirror the imports + constants from instances/trial.py.

from datetime import timedelta
from airflow.models import Variable
import rail

from crl.user_import_canada_v7.utils import request_payload, python_callable_methods, response_filter

null = None


def create_child_dag(config):
    # V2.7 is deployed only to instances that define the dagid + mapper
    # constants. For other instances (prod, sandbox) this DAG is skipped.
    if not getattr(config, 'process_personal_days_proration_dagid', None):
        return None

    with rail.create_airflow_dag(
        dag_id=config.process_personal_days_proration_dagid,
        description='CRL User Import - Personal Days Proration (V2.7)',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_personal_days_proration,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='determine_mode'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='determine_mode',
            end_task='catch_and_log_errors',
        )

        determine_mode = rail.PythonOperator(
            task_id='determine_mode',
            python_callable=lambda dag_run: python_callable_methods.determine_personal_days_mode(dag_run, config)
        )

        is_mode_actionable = rail.IfOperator(
            task_id='is_mode_actionable',
            test=lambda: rail.result('determine_mode') in ('outbound', 'return', 'cleanup'),
            yes_task='get_user_time_off_policy_summary',
            no_task='log_noop'
        )

        # data_handler flattens response['policiesByTimeOffType'] into a list
        # of {timeoff_type_name, timeoff_type_uri, enabled, policy} dicts so
        # find_first_by_attr_and_get_attr can locate Personal Days. Without
        # the handler the raw response's nested shape leaves the lookup as
        # None and the DAG short-circuits to log_personal_days_not_assigned.
        # Mirrors process_special_accrual_time_off_type.py.
        get_user_time_off_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_time_off_policy_summary',
            endpoint='/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary',
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=response_filter.assigned_timeoffs_types_to_user
        )

        # Needed to resolve the URI of "[CAN] Personal Days Update {std_hrs}"
        # template used by return-mode to clone accrual/reset/limitation rules.
        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_filtered_time_off_types
        )

        resolve_personal_days_timeoff_type_uri = rail.PythonOperator(
            task_id='resolve_personal_days_timeoff_type_uri',
            python_callable=lambda: python_callable_methods.resolve_personal_days_timeoff_type_uri(config)
        )

        is_personal_days_assigned = rail.IfOperator(
            task_id='is_personal_days_assigned',
            test=lambda: bool(rail.result('resolve_personal_days_timeoff_type_uri')),
            yes_task='should_write_personal_days_policy',
            no_task='log_personal_days_not_assigned'
        )

        # Optimization #1: short-circuit cleanup mode when there is no
        # future-dated integration line to drop (PUT would be a redundant
        # rewrite). Outbound/return always pass through.
        should_write_personal_days_policy = rail.PythonOperator(
            task_id='should_write_personal_days_policy',
            python_callable=lambda dag_run: python_callable_methods.should_write_personal_days_policy(dag_run, config)
        )

        is_write_needed = rail.IfOperator(
            task_id='is_write_needed',
            test=lambda: bool(rail.result('should_write_personal_days_policy')),
            yes_task='is_balance_summary_needed',
            no_task='log_noop'
        )

        # Optimization #2: skip GetBalanceSummaryForAccount unless the mode
        # is outbound AND the action is prorate_at_leave_start. Everything
        # else (zero_immediately, buffer_then_zero, return, cleanup) doesn't
        # use the balance value.
        is_balance_summary_needed = rail.IfOperator(
            task_id='is_balance_summary_needed',
            test=lambda dag_run: python_callable_methods.is_personal_days_balance_summary_needed(dag_run, config),
            yes_task='get_personal_days_balance_summary',
            no_task='resolve_personal_days_update_template_uri'
        )

        get_personal_days_balance_summary = rail.RepliconServiceOperator(
            task_id='get_personal_days_balance_summary',
            endpoint='/services/TimeOffService2.svc/GetBalanceSummaryForAccount',
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('resolve_personal_days_timeoff_type_uri'),
                },
                "asOfDate": request_payload.get_replicon_date(dag_run.conf['change_effective_date']),
            }
        )

        # Resolve "[CAN] Personal Days Update {std_hrs}" template URI. Returns
        # None for non-return modes (skipping the fetch entirely) and for
        # std_hrs values outside {37.5, 30, 22.5}.
        resolve_personal_days_update_template_uri = rail.PythonOperator(
            task_id='resolve_personal_days_update_template_uri',
            # Two upstream paths merge here (balance-summary fetched OR skipped).
            trigger_rule='none_failed_min_one_success',
            python_callable=lambda dag_run: python_callable_methods.resolve_personal_days_update_template_uri(dag_run, config)
        )

        is_personal_days_update_template_resolved = rail.IfOperator(
            task_id='is_personal_days_update_template_resolved',
            test=lambda: bool(rail.result('resolve_personal_days_update_template_uri')),
            yes_task='get_personal_days_update_template_schedule',
            no_task='build_personal_days_policy_line'
        )

        # Fetches the default policy schedule of the Update template; result
        # is consumed by build_personal_days_policy_line to clone accrual /
        # reset / limitation / validation rules onto the return-mode line.
        get_personal_days_update_template_schedule = rail.RepliconServiceOperator(
            task_id='get_personal_days_update_template_schedule',
            endpoint='/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType',
            data=lambda: {
                "timeOffTypeUri": rail.result('resolve_personal_days_update_template_uri')
            }
        )

        build_personal_days_policy_line = rail.PythonOperator(
            task_id='build_personal_days_policy_line',
            # trigger_rule='none_failed_min_one_success' lets this run whether
            # we came via the get_personal_days_balance_summary path OR skipped
            # directly from is_balance_summary_needed=No.
            trigger_rule='none_failed_min_one_success',
            python_callable=lambda dag_run: python_callable_methods.build_personal_days_policy_line(dag_run, config)
        )

        # Cleanup mode produces an empty policy line list - we still need to
        # PUT the (filtered) historical schedule to drop the future-dated
        # zero-balance line that the outbound write placed. So unlike the
        # previous flow, we do NOT short-circuit on "no new line"; the
        # pipeline always runs once we reach build_personal_days_policy_line.
        get_personal_days_historical_policy_lines = rail.PythonOperator(
            task_id='get_personal_days_historical_policy_lines',
            python_callable=lambda dag_run: python_callable_methods.get_personal_days_historical_policy_lines(dag_run, config)
        )

        get_all_personal_days_policy_to_assign = rail.PythonOperator(
            task_id='get_all_personal_days_policy_to_assign',
            python_callable=python_callable_methods.get_all_personal_days_policy_to_assign
        )

        put_personal_days_timeoff_policy = rail.RepliconServiceOperator(
            task_id='put_personal_days_timeoff_policy',
            endpoint='/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule',
            data=request_payload.put_personal_days_timeoff_policy
        )

        log_personal_days_proration_success = rail.WriteLogOperator(
            task_id='log_personal_days_proration_success',
            log='{{ dag_run.conf.user_log }}',
            severity='Success',
            message="{{ 'Personal Days policy updated (' + result('determine_mode') + ')' }}",
            properties={
                'employee_id': '{{ dag_run.conf.emp_id }}',
                'first_name': '{{ dag_run.conf.first_name }}',
                'last_name': '{{ dag_run.conf.last_name }}',
                'action': 'Update',
                'status': 'Success',
                'details': "{{ 'Personal Days ' + result('determine_mode') + ' policy line written' }}",
            }
        )

        log_personal_days_not_assigned = rail.WriteLogOperator(
            task_id='log_personal_days_not_assigned',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Personal Days time-off-type not assigned to user; skipping V2.7 proration',
            properties={
                'employee_id': '{{ dag_run.conf.emp_id }}',
                'first_name': '{{ dag_run.conf.first_name }}',
                'last_name': '{{ dag_run.conf.last_name }}',
                'action': 'Update',
                'status': 'Exception',
                'details': 'Personal Days time-off-type not assigned to user; skipping V2.7 proration',
            }
        )

        log_noop = rail.WriteLogOperator(
            task_id='log_noop',
            log='{{ dag_run.conf.user_log }}',
            severity='Success',
            message='No Personal Days proration required',
            properties={
                'employee_id': '{{ dag_run.conf.emp_id }}',
                'first_name': '{{ dag_run.conf.first_name }}',
                'last_name': '{{ dag_run.conf.last_name }}',
                'action': 'Update',
                'status': 'Success',
                'details': 'No Personal Days proration required',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employee_id': '{{ dag_run.conf.emp_id }}',
                'first_name': '{{ dag_run.conf.first_name }}',
                'last_name': '{{ dag_run.conf.last_name }}',
                'action': 'Update',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done',
        )

        # Graph wiring
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> determine_mode

        determine_mode >> is_mode_actionable
        is_mode_actionable >> rail.Label('No') >> log_noop
        is_mode_actionable >> rail.Label('Yes') >> get_user_time_off_policy_summary
        get_user_time_off_policy_summary >> get_all_time_off_types >> resolve_personal_days_timeoff_type_uri >> is_personal_days_assigned
        is_personal_days_assigned >> rail.Label('No') >> log_personal_days_not_assigned >> catch_and_log_errors
        is_personal_days_assigned >> rail.Label('Yes') >> should_write_personal_days_policy >> is_write_needed
        is_write_needed >> rail.Label('No') >> log_noop
        is_write_needed >> rail.Label('Yes') >> is_balance_summary_needed
        is_balance_summary_needed >> rail.Label('Yes') >> get_personal_days_balance_summary >> resolve_personal_days_update_template_uri
        is_balance_summary_needed >> rail.Label('No') >> resolve_personal_days_update_template_uri
        resolve_personal_days_update_template_uri >> is_personal_days_update_template_resolved
        is_personal_days_update_template_resolved >> rail.Label('Yes') >> get_personal_days_update_template_schedule >> build_personal_days_policy_line
        is_personal_days_update_template_resolved >> rail.Label('No') >> build_personal_days_policy_line
        build_personal_days_policy_line >> get_personal_days_historical_policy_lines
        get_personal_days_historical_policy_lines >> get_all_personal_days_policy_to_assign
        get_all_personal_days_policy_to_assign >> put_personal_days_timeoff_policy >> log_personal_days_proration_success
        log_personal_days_proration_success >> catch_and_log_errors
        log_noop >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
