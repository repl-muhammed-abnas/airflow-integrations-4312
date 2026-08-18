# pylint: disable=line-too-long
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_thailand_user_sync_child_add_timeoff_new_user_dag_id,
        description=f'Momentive_thailand_user_sync_Timeoff_add_new_user_child_{config.instance}',
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
            no_task='get_enabled_timeoff_types'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_enabled_timeoff_types',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Recipe step 2: enabled time off types (the source for matching displayText -> uri).
        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        # Recipe steps 5-9: split the piped `timeofftypes` into trimmed names.
        declare_timeofftypenameslist = rail.SetVariableOperator(
            task_id='declare_timeofftypenameslist',
            append=False,
            name='timeofftypenameslist',
            value=lambda dag_run: [item.strip() for item in dag_run.conf['timeofftypes'].split("|")] if dag_run.conf.get('timeofftypes') else []
        )

        # Recipe steps 10-11: match each name against displayText -> {name, uri}.
        declare_timeofftypeuri = rail.SetVariableOperator(
            task_id='declare_timeofftypeuri',
            append=False,
            name='timeofftypeuri',
            value=lambda: [{
                "name": item,
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_timeoff_types'), 'displayText', item, 'uri')
            } for item in rail.get_dag_run_var('timeofftypenameslist')] if rail.get_dag_run_var('timeofftypenameslist') else null
        )

        # Recipe step 12 joins the uris with smart_join, which SKIPS blank/empty
        # values -- names that don't match an enabled time-off type resolve to no
        # uri and must be dropped (else PutTimeOffTypeAssignmentsForUser 400s on the
        # null "No URI Provided").
        final_list_of_timeoff_uris_to_be_assigned = rail.PythonOperator(
            task_id='final_list_of_timeoff_uris_to_be_assigned',
            python_callable=lambda: [item['uri'] for item in (rail.get_dag_run_var('timeofftypeuri') or []) if item.get('uri')]
        )

        # Recipe step 13: assign all matched time off types to the user in one call.
        assign_required_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_required_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('final_list_of_timeoff_uris_to_be_assigned')
            }
        )

        # Recipe step 14: per matched type, apply its default policy set schedule.
        # Same null-uri guard as the assign list: skip unmatched types so the
        # per-type default-policy call is never made with a null timeOffTypeUri.
        foreach_timeofftype_uri = rail.ForEachOperator(
            task_id='foreach_timeofftype_uri',
            items=lambda: [item for item in (rail.get_dag_run_var('timeofftypeuri') or []) if item.get('uri')],
            start_task='get_default_timeoff_type_policy_schedule_for_user',
            end_task='foreach_timeofftype_uri_end'
        )

        # Recipe step 16: default policy schedule for this user + time off type.
        get_default_timeoff_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_uri')['uri']
                }
            }
        )

        # Recipe step 17: only assign when the default policy carries an effective date.
        def default_policy_has_effective_date():
            resp = rail.result('get_default_timeoff_type_policy_schedule_for_user')
            return bool(resp and (resp[0].get('effectiveDate') or {}).get('day'))

        if_default_policy_present = rail.IfOperator(
            task_id='if_default_policy_present',
            test=default_policy_has_effective_date,
            yes_task='log_policy_to_be_assigned',
            no_task='foreach_timeofftype_uri_end'
        )

        # Recipe step 18: null -> "effective", "script" -> "scriptTarget" for the put payload.
        log_policy_to_be_assigned = rail.PythonOperator(
            task_id='log_policy_to_be_assigned',
            python_callable=lambda: json.loads(json.dumps(rail.result('get_default_timeoff_type_policy_schedule_for_user')).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        # Recipe step 19: assign the default policy set schedule.
        put_user_timeoff_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_uri')['uri']
                },
                "policySetScheduleEntries": rail.result('log_policy_to_be_assigned')
            }
        )

        foreach_timeofftype_uri_end = rail.EmptyOperator(task_id='foreach_timeofftype_uri_end')

        # Error pattern (Japan): capture any failure as a message the parent gathers
        # via dagrun_task_id='catch_error'. Skipped on success, so an empty gather == no error.
        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template("Add Timeoff for new user - Dag_Run Error - {{ get_error_message() }}")
        )

        # ---- wiring ----
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> get_enabled_timeoff_types

        get_enabled_timeoff_types >> declare_timeofftypenameslist >> declare_timeofftypeuri \
            >> final_list_of_timeoff_uris_to_be_assigned >> assign_required_timeofftypes >> foreach_timeofftype_uri

        foreach_timeofftype_uri >> get_default_timeoff_type_policy_schedule_for_user >> if_default_policy_present
        if_default_policy_present >> rail.Label('Yes') >> log_policy_to_be_assigned \
            >> put_user_timeoff_account_policy_set_schedule >> foreach_timeofftype_uri_end
        if_default_policy_present >> rail.Label('No') >> foreach_timeofftype_uri_end

        foreach_timeofftype_uri >> foreach_timeofftype_uri_end >> catch_error

    return dag


rail.for_each_instance(create_dag)
