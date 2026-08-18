# pylint: disable=line-too-long
from datetime import timedelta
from airflow.models import Variable
import rail
from momentive.common_recipes_userimport.utils import request_payload

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.momentive_othercountries_user_sync_zero_balance_timeoff_update_child_dag_id,
        description=f'momentive_othercountries_user_sync_0_balance_for_timeoff_update_child_{config.instance}',
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
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_timeofftype_policysummary'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_timeofftype_policysummary',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_user_timeofftype_policysummary = rail.RepliconServiceOperator(
            task_id='get_user_timeofftype_policysummary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_all_scripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        get_all_scriptsfor_time_off_validation_script_administration_service1 = rail.RepliconServiceOperator(
            task_id='get_all_scriptsfor_time_off_validation_script_administration_service1',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts"
        )

        foreach_policiesby_timeofftype = rail.ForEachOperator(
            task_id='foreach_policiesby_timeofftype',
            items=lambda: rail.result('get_user_timeofftype_policysummary')['policiesByTimeOffType'],
            start_task='is_isTimeOffAllowedAgainstThisTimeOffType_true',
            end_task='foreach_policiesby_timeofftype_end'
        )

        is_isTimeOffAllowedAgainstThisTimeOffType_true = rail.IfOperator(
            task_id='is_isTimeOffAllowedAgainstThisTimeOffType_true',
            test="{{ result('foreach_policiesby_timeofftype').isTimeOffAllowedAgainstThisTimeOffType | is_truthy and \
                result('foreach_policiesby_timeofftype').timeOffType.uri == dag_run.conf.timeoffuri }}",
            yes_task="get_balance_summary_foraccount",
            no_task="foreach_policiesby_timeofftype_end",
        )

        get_balance_summary_foraccount = rail.RepliconServiceOperator(
            task_id='get_balance_summary_foraccount',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=request_payload.get_balancesummary_foraccount,
            data_handler=lambda response: float(response['timeRemaining']) if response.get(
                'timeRemaining', '') else 0
        )

        if_description_is_present = rail.IfOperator(
            task_id='if_description_is_present',
            test="{{ result('foreach_policiesby_timeofftype').policySetSchedule | is_truthy and \
                result('foreach_policiesby_timeofftype').policySetSchedule[0].description | is_truthy}}",
            yes_task="put_remaining_balance_for_payout_as_0",
            no_task="foreach_policiesby_timeofftype_end",
        )

        put_remaining_balance_for_payout_as_0 = rail.TriggerDagRunOperator(
            task_id='put_remaining_balance_for_payout_as_0',
            trigger_dag_id=config.momentive_othercountries_user_sync_put_zero_balance_payout_child_dag_id,
            conf=request_payload.put_remaining_balance_for_payout_parameter,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_put_remaining_balance_for_payout_as_0 = rail.WaitForDagRunsSensor(
            task_id='wait_for_put_remaining_balance_for_payout_as_0',
            dag_runs='{{ result("put_remaining_balance_for_payout_as_0") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        foreach_policiesby_timeofftype_end = rail.EmptyOperator(
            task_id='foreach_policiesby_timeofftype_end'
        )

        # Leaf error reply (gathered by the parent on failure).
        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "0 balance for timeoff update - Dag_Run Error - {{ get_error_message() }}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_user_timeofftype_policysummary

        get_user_timeofftype_policysummary >> get_all_scripts >> get_all_scriptsfor_time_off_validation_script_administration_service1 >> \
            foreach_policiesby_timeofftype >> is_isTimeOffAllowedAgainstThisTimeOffType_true

        is_isTimeOffAllowedAgainstThisTimeOffType_true >> rail.Label('Yes') >> get_balance_summary_foraccount >> if_description_is_present
        is_isTimeOffAllowedAgainstThisTimeOffType_true >> rail.Label('No') >> foreach_policiesby_timeofftype_end

        if_description_is_present >> rail.Label('Yes') >> put_remaining_balance_for_payout_as_0 >> wait_for_put_remaining_balance_for_payout_as_0 >> \
            foreach_policiesby_timeofftype_end
        if_description_is_present >> rail.Label('No') >> foreach_policiesby_timeofftype_end

        foreach_policiesby_timeofftype_end >> catch_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
