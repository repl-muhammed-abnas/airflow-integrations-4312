# pylint: disable=line-too-long
from datetime import timedelta
from airflow.models import Variable
import rail
from momentive.common_recipes_userimport.utils import request_payload, python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_othercountries_user_sync_timeoff_new_user_child_dag_id,
        description=f'Momentive_othercountries_user_sync_timeoff_add_new_user_child_{config.instance}',
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
            no_task='get_enabled_timeofftypes'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_enabled_timeofftypes',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Recipe step 3: all enabled time-off types (empty body).
        get_enabled_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_enabled_timeofftypes',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        # Recipe steps 4-14: split conf.timeofftypes, resolve displayText->{uri,name}
        # applying the 3 insert rules (Monthly only when years-of-service < 2).
        build_assignment_list = rail.PythonOperator(
            task_id='build_assignment_list',
            python_callable=python_callable.build_assignment_list
        )

        # Recipe step 15: full-replace the user's assigned time-off types.
        put_timeoff_type_assignments = rail.RepliconServiceOperator(
            task_id='put_timeoff_type_assignments',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.put_timeoff_type_assignments_payload
        )

        # Recipe step 16: iterate the assigned {uri, name} types.
        foreach_timeofftype = rail.ForEachOperator(
            task_id='foreach_timeofftype',
            items=lambda: rail.result('build_assignment_list')['assignments'],
            start_task='if_uri_present',
            end_task='foreach_timeofftype_end'
        )

        # Recipe step 17: only process types that resolved to a uri.
        if_uri_present = rail.IfOperator(
            task_id='if_uri_present',
            test="{{ result('foreach_timeofftype').uri | is_truthy }}",
            yes_task='get_default_policy_schedule',
            no_task='foreach_timeofftype_end'
        )

        # Recipe steps 23/36/44/103: the default policy schedule for the type.
        get_default_policy_schedule = rail.RepliconServiceOperator(
            task_id='get_default_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_policy_schedule_payload
        )

        # Recipe step 95: KOR Monthly Leave needs the GetAllScripts uris.
        if_is_monthly_leave = rail.IfOperator(
            task_id='if_is_monthly_leave',
            test=f"{{{{ result('foreach_timeofftype').name == {python_callable.KOR_MONTHLY_LEAVE!r} }}}}",
            yes_task='get_all_scripts_validation',
            no_task='build_policy_entries'
        )

        # Recipe step 96: time-off validation scripts (Prevent balance overdraw).
        get_all_scripts_validation = rail.RepliconServiceOperator(
            task_id='get_all_scripts_validation',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data=request_payload.get_default_policy_schedule_payload,
            data_handler=lambda response: {
                'prevent_balance_overdraw': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Prevent balance overdraw', 'uri', '')
            }
        )

        # Recipe step 97: time-off balance event scripts (Starting Balance Set To, Yearly Accrual).
        get_all_scripts_for_monthly = rail.RepliconServiceOperator(
            task_id='get_all_scripts_for_monthly',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data=request_payload.get_default_policy_schedule_payload,
            data_handler=lambda response: {
                'starting_balance_set_to': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Starting Balance Set To', 'uri', ''),
                'yearly_accrual': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Yearly Accrual', 'uri', '')
            }
        )

        # Recipe steps 19-154: per-branch policy build (Bel / UK / KOR Annual / KOR Monthly / generic).
        build_policy_entries = rail.PythonOperator(
            task_id='build_policy_entries',
            trigger_rule='none_failed_min_one_success',
            python_callable=request_payload.build_policy_entries
        )

        # Recipe steps 38/65/120: only PUT when entries were built.
        if_entries_present = rail.IfOperator(
            task_id='if_entries_present',
            test="{{ result('build_policy_entries') | is_truthy }}",
            yes_task='put_user_timeoff_policy_schedule',
            no_task='foreach_timeofftype_end'
        )

        # Recipe Put: write the built policy-set schedule for the type.
        put_user_timeoff_policy_schedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_user_timeoff_policy_schedule_payload
        )

        foreach_timeofftype_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_end',
        )

        # Recipe steps 155-157 (operative; NOT skipped): for UAE users, (re)assign the
        # UAE_Leave time-off type. Runs once after the per-type foreach.
        if_timeofftype_startswith_UAE = rail.IfOperator(
            task_id='if_timeofftype_startswith_UAE',
            test=lambda dag_run: bool(dag_run.conf['timeofftypes'].startswith('UAE')),
            yes_task='UAE_timeoffs',
            no_task='final_response_from_dag',
        )

        UAE_timeoffs = rail.PythonOperator(
            task_id='UAE_timeoffs',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_enabled_timeofftypes'), 'displayText', 'UAE_Leave', 'uri', '')
        )

        assign_uae_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_uae_timeofftypes',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'timeOffTypeUris': ["{{ result('UAE_timeoffs') }}"]
            }
        )

        # Leaf error reply (gathered by the parent on failure).
        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Timeoff add new user - Dag_Run Error - {{ get_error_message() }}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_enabled_timeofftypes

        get_enabled_timeofftypes >> build_assignment_list >> put_timeoff_type_assignments >> foreach_timeofftype

        foreach_timeofftype >> if_uri_present

        if_uri_present >> rail.Label('Yes') >> get_default_policy_schedule >> if_is_monthly_leave
        if_uri_present >> rail.Label('No') >> foreach_timeofftype_end

        if_is_monthly_leave >> rail.Label('Yes') >> get_all_scripts_validation >> get_all_scripts_for_monthly >> build_policy_entries
        if_is_monthly_leave >> rail.Label('No') >> build_policy_entries

        build_policy_entries >> if_entries_present

        if_entries_present >> rail.Label('Yes') >> put_user_timeoff_policy_schedule >> foreach_timeofftype_end
        if_entries_present >> rail.Label('No') >> foreach_timeofftype_end

        foreach_timeofftype >> foreach_timeofftype_end >> if_timeofftype_startswith_UAE

        if_timeofftype_startswith_UAE >> rail.Label('Yes') >> UAE_timeoffs >> assign_uae_timeofftypes >> final_response_from_dag
        if_timeofftype_startswith_UAE >> rail.Label('No') >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
