# pylint: disable=line-too-long
from datetime import timedelta
from airflow.models import Variable
import rail
from momentive.user_import_thailand.utils import python_callable, request_payload

null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_thailand_user_sync_child_update_user_timeoff_assign_id,
        description=f'Momentive_thailand_user_sync_update_user_timeoff_assign_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_assigned_timeoff_types'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_assigned_timeoff_types',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Recipe step 14: current time-off type assignments for the user.
        get_assigned_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_assigned_timeoff_types',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers",
            data={
                "userUris": [
                    "{{ dag_run.conf.useruri }}"
                ]
            }
        )

        # Recipe step 15: enabled time off types (source for matching displayText -> uri).
        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        # Recipe step 19: only proceed when the incoming timeofftypes list is present.
        if_timeofftypes_present = rail.IfOperator(
            task_id='if_timeofftypes_present',
            test="{{ dag_run.conf.timeofftypes | is_truthy }}",
            yes_task='final_list_of_timeoff_uris',
            no_task='catch_error'
        )

        # Recipe steps 23-31: map each incoming display name to its enabled URI.
        final_list_of_timeoff_uris = rail.PythonOperator(
            task_id='final_list_of_timeoff_uris',
            python_callable=python_callable.final_timeofftype_uris
        )

        # Recipe step 29: only assign when at least one matched URI is present.
        if_final_uris_present = rail.IfOperator(
            task_id='if_final_uris_present',
            test=lambda: bool(rail.result('final_list_of_timeoff_uris')),
            yes_task='assign_required_timeofftypes',
            no_task='foreach_annual_leave_type'
        )

        # Recipe step 31: replace the user's time-off type assignments with the incoming set.
        assign_required_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_required_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.assign_timeofftypes_payload
        )

        # Recipe steps 39-123: rebuild the annual-leave policy-set schedule per type.
        foreach_annual_leave_type = rail.ForEachOperator(
            task_id='foreach_annual_leave_type',
            items=config.ANNUAL_LEAVE_TYPES,
            start_task='if_annual_leave_type_requested',
            end_task='foreach_annual_leave_type_end'
        )

        # Recipe step 39: only rebuild when this annual-leave type is in the incoming set.
        if_annual_leave_type_requested = rail.IfOperator(
            task_id='if_annual_leave_type_requested',
            test=python_callable.is_annual_leave_type_requested,
            yes_task='log_annual_leave_enabled_uri',
            no_task='foreach_annual_leave_type_end'
        )

        # Recipe step 43: enabled URI for the current loop type.
        log_annual_leave_enabled_uri = rail.PythonOperator(
            task_id='log_annual_leave_enabled_uri',
            python_callable=python_callable.annual_leave_enabled_uri
        )

        # Recipe step 41: rebuild when not currently assigned OR the record is a rehire.
        if_not_assigned_or_rehire = rail.IfOperator(
            task_id='if_not_assigned_or_rehire',
            test=python_callable.not_assigned_or_rehire,
            yes_task='get_user_timeoff_policy_summary',
            no_task='foreach_annual_leave_type_end'
        )

        # Recipe step 45: the user's per-type policy schedules.
        get_user_timeoff_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_timeoff_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_timeoff_policy_summary_payload
        )

        # Recipe step 61: default policy-set schedule for the current type.
        get_default_policyset_for_type = rail.RepliconServiceOperator(
            task_id='get_default_policyset_for_type',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=request_payload.get_default_policyset_for_type_payload
        )

        # Recipe steps 48-71: past entries (effectiveDate < today, transformed) + new entry.
        build_annual_leave_policy_entries = rail.PythonOperator(
            task_id='build_annual_leave_policy_entries',
            python_callable=request_payload.build_annual_leave_policy_entries
        )

        # Recipe steps 76/78: write the rebuilt policy-set schedule for the type.
        put_user_timeoff_policy_schedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_annual_leave_policy_schedule_payload
        )

        foreach_annual_leave_type_end = rail.EmptyOperator(task_id='foreach_annual_leave_type_end')

        # Leaf return contract (Japan): capture any failure as a message the parent
        # update_user gathers via dagrun_task_id='final_response_from_dag'.
        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in timeoff update for user ; {{ get_error_message() }}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        # ---- wiring ----
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_assigned_timeoff_types

        get_assigned_timeoff_types >> get_enabled_timeoff_types >> if_timeofftypes_present
        if_timeofftypes_present >> rail.Label('No') >> catch_error
        if_timeofftypes_present >> rail.Label('Yes') >> final_list_of_timeoff_uris >> if_final_uris_present

        if_final_uris_present >> rail.Label('Yes') >> assign_required_timeofftypes >> foreach_annual_leave_type
        if_final_uris_present >> rail.Label('No') >> foreach_annual_leave_type

        foreach_annual_leave_type >> if_annual_leave_type_requested
        if_annual_leave_type_requested >> rail.Label('No') >> foreach_annual_leave_type_end
        if_annual_leave_type_requested >> rail.Label('Yes') >> log_annual_leave_enabled_uri >> if_not_assigned_or_rehire

        if_not_assigned_or_rehire >> rail.Label('No') >> foreach_annual_leave_type_end
        if_not_assigned_or_rehire >> rail.Label('Yes') >> get_user_timeoff_policy_summary \
            >> get_default_policyset_for_type >> build_annual_leave_policy_entries \
            >> put_user_timeoff_policy_schedule >> foreach_annual_leave_type_end

        foreach_annual_leave_type >> foreach_annual_leave_type_end >> catch_error

    return dag


rail.for_each_instance(create_dag)
