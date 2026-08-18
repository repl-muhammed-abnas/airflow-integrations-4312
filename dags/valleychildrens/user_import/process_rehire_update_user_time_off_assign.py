from datetime import timedelta
from airflow.models import Variable
import rail

from valleychildrens.user_import.utils import request_payload, response_filter

null = None


def _user_timeoff_type_names(conf):
    """Parse pipe-separated 'timeofftypes' from the conf (master flattens
    it from the user's master_mapper 'Time Off Types' column)."""
    names_str = conf.get('timeofftypes') or ''
    return {n.strip() for n in names_str.split('|') if n.strip()}


def _filter_user_timeoff_types():
    """Filter get_all_timeoff_types to only the types listed in the user's
    mapper 'timeofftypes'."""
    from airflow.operators.python import get_current_context
    ctx = get_current_context()
    user_names = _user_timeoff_type_names(ctx['dag_run'].conf)
    if not user_names:
        return []
    all_types = rail.result('get_all_timeoff_types') or []
    out = []
    for t in all_types:
        if not isinstance(t, dict):
            continue
        display = t.get('displayText') or t.get('name')
        if display in user_names:
            out.append(t)
    return out


def _is_pto_type(time_off_mapper, timeoff_type_item):
    """Return True if this timeoff type has rows in time_off_mapper
    (meaning it has FTE-based accrual policy and must use the PTO child DAG)."""
    if not time_off_mapper or not isinstance(timeoff_type_item, dict):
        return False
    type_name = (timeoff_type_item.get('displayText')
                 or timeoff_type_item.get('name') or '').strip()
    return any(
        isinstance(row, dict) and (row.get('time_off_type') or '').strip() == type_name
        for row in time_off_mapper
    )


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_rehire_update_user_time_off_assign_dagid,
        description='ValleyChildrens User Import - Rehire Update User Time Off Assign',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_rehire_update_user_time_off_assign,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='clear_end_date',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='clear_end_date',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        clear_end_date = rail.RepliconServiceOperator(
            task_id='clear_end_date',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'employmentDateRange': {
                    'startDate': request_payload.to_date_struct(dag_run.conf.get('startdate')),
                    'endDate': null,
                },
            },
        )

        get_all_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data={},
        )

        filter_user_timeoff_types = rail.PythonOperator(
            task_id='filter_user_timeoff_types',
            python_callable=_filter_user_timeoff_types,
            show_return_value_in_logs=False,
        )

        get_user_timeoff_summary = rail.RepliconServiceOperator(
            task_id='get_user_timeoff_summary',
            endpoint='/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary',
            data=lambda dag_run: {'userUri': dag_run.conf['useruri']},
            data_handler=response_filter.filter_active_policies,
        )

        process_fte_change_for_each_policy = rail.TriggerDagRunForEachItemOperator(
            task_id='process_fte_change_for_each_policy',
            items="{{ result('get_user_timeoff_summary') | to_json }}",
            trigger_dag_id=config.process_time_off_policy_update_on_fte_change_dagid,
            conf=lambda item, dag_run: request_payload.get_process_time_off_policy_update_on_fte_change_conf(
                {**dict(dag_run.conf), **item, 'effectivedate': dag_run.conf.get('startdate')},
                config, dag_run.conf.get('log_id')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_fte_change = rail.WaitForDagRunsSensor(
            task_id='wait_fte_change',
            dag_runs="{{ result('process_fte_change_for_each_policy') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_balance_per_policy = rail.ForEachOperator(
            task_id='get_balance_per_policy',
            items="{{ result('get_user_timeoff_summary') | to_json }}",
            start_task='get_balance_for_account',
            end_task='get_balance_per_policy_end',
        )

        get_balance_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_for_account',
            endpoint='/services/TimeOffService2.svc/GetBalanceSummaryForAccount',
            data=lambda dag_run: {
                'timeOffAccount': {
                    'userUri': dag_run.conf['useruri'],
                    'timeOffTypeUri': (rail.result('get_balance_per_policy').get('timeOffType') or {}).get('uri'),
                },
            },
        )

        trigger_payout_for_policy = rail.TriggerDagRunOperator(
            task_id='trigger_payout_for_policy',
            trigger_dag_id=config.process_timeoff_policy_payoutbalance_dagid,
            conf=lambda dag_run: request_payload.get_process_timeoff_policy_payoutbalance_conf(
                {
                    **dict(dag_run.conf),
                    'time_off_type_uri': (rail.result('get_balance_per_policy').get('timeOffType') or {}).get('uri'),
                    'timeofftypeuri': (rail.result('get_balance_per_policy').get('timeOffType') or {}).get('uri'),
                    'effectivedate': dag_run.conf.get('startdate'),
                },
                config, dag_run.conf.get('log_id')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        get_balance_per_policy_end = rail.EmptyOperator(
            task_id='get_balance_per_policy_end',
        )

        assign_user_timeoff_types = rail.RepliconServiceOperator(
            task_id='assign_user_timeoff_types',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'timeOffTypeUris': [
                    t['uri'] for t in (rail.result('filter_user_timeoff_types') or [])
                    if isinstance(t, dict) and t.get('uri')
                ],
            },
        )

        process_each_timeoff_type = rail.ForEachOperator(
            task_id='process_each_timeoff_type',
            items="{{ result('filter_user_timeoff_types') | to_json }}",
            start_task='get_default_timeoff_policy_schedule',
            end_task='process_each_timeoff_type_end',
        )

        get_default_timeoff_policy_schedule = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_policy_schedule',
            endpoint='/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser',
            data=lambda dag_run: {
                'timeOffAccount': {
                    'userUri': dag_run.conf['useruri'],
                    'timeOffTypeUri': rail.result('process_each_timeoff_type')['uri'],
                },
            },
        )

        is_pto_type = rail.IfOperator(
            task_id='is_pto_type',
            test=lambda: _is_pto_type(
                config.TIME_OFF_MAPPER,
                rail.result('process_each_timeoff_type'),
            ),
            yes_task='trigger_add_pto_for_type',
            no_task='put_default_policy_for_type',
        )

        trigger_add_pto_for_type = rail.TriggerDagRunOperator(
            task_id='trigger_add_pto_for_type',
            trigger_dag_id=config.process_time_off_policy_add_pto_dagid,
            conf=lambda dag_run: request_payload.get_process_time_off_policy_add_pto_conf(
                {
                    **dict(dag_run.conf),
                    'timeofftypeuri': rail.result('process_each_timeoff_type')['uri'],
                    'time_off_type_uri': rail.result('process_each_timeoff_type')['uri'],
                    'time_off_type': (rail.result('process_each_timeoff_type').get('displayText')
                                       or rail.result('process_each_timeoff_type').get('name')),
                    'effectivedate': dag_run.conf.get('startdate'),
                },
                config, dag_run.conf.get('log_id')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        put_default_policy_for_type = rail.RepliconServiceOperator(
            task_id='put_default_policy_for_type',
            endpoint='/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule',
            data=lambda dag_run: {
                'timeOffAccount': {
                    'userUri': dag_run.conf['useruri'],
                    'timeOffTypeUri': rail.result('process_each_timeoff_type')['uri'],
                },
                'policySetScheduleEntries': request_payload.build_policy_set_schedule_entries(
                    dag_run,
                    request_payload.resolve_time_off_policy_uri(
                        dag_run,
                        rail.result('get_default_timeoff_policy_schedule'),
                        rail.result('get_user_timeoff_summary'),
                    ),
                ),
            },
        )

        process_each_timeoff_type_end = rail.EmptyOperator(
            task_id='process_each_timeoff_type_end',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log='{{ dag_run.conf["log_id"] }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf.get('employeeid'),
                'action': 'RehireUpdateUserTimeOffAssign',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> clear_end_date
        clear_end_date >> get_all_timeoff_types >> filter_user_timeoff_types >> get_user_timeoff_summary
        get_user_timeoff_summary >> process_fte_change_for_each_policy >> wait_fte_change
        wait_fte_change >> get_balance_per_policy >> get_balance_for_account >> trigger_payout_for_policy >> get_balance_per_policy_end
        get_balance_per_policy >> get_balance_per_policy_end
        get_balance_per_policy_end >> assign_user_timeoff_types >> process_each_timeoff_type \
            >> get_default_timeoff_policy_schedule >> is_pto_type
        is_pto_type >> rail.Label('Yes') >> trigger_add_pto_for_type >> process_each_timeoff_type_end
        is_pto_type >> rail.Label('No') >> put_default_policy_for_type >> process_each_timeoff_type_end
        process_each_timeoff_type >> process_each_timeoff_type_end >> catch_and_log_error
    return dag

rail.for_each_instance(create_child_dag)
