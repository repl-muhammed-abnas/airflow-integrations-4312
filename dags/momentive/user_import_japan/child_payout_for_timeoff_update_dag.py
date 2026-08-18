from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_child_payout_for_timeoff_update_dag_id,
        description=f'Momentive_Japan_Child_payout_for_timeoff_update_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_child_trigger_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_child_trigger_list',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        create_child_trigger_list = rail.SetVariableOperator(
            task_id='create_child_trigger_list',
            name='childtriggeredlist',
            append=False,
            value=[]
        )

        get_balance_event_scripts = rail.RepliconServiceOperator(
            task_id='get_balance_event_scripts',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts"
        )

        get_validation_scripts = rail.RepliconServiceOperator(
            task_id='get_validation_scripts',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts"
        )

        get_user_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri']
            }
        )

        split_current_date = rail.PythonOperator(
            task_id='split_current_date',
            python_callable= lambda: python_callable.split_date_string(datetime.now().strftime("%Y-%m-%d"))
        )

        foreach_policy_type = rail.ForEachOperator(
            task_id='foreach_policy_type',
            items=lambda: rail.result("get_user_policy_summary")['policiesByTimeOffType'],
            start_task='check_policy_allowed_and_uri_match',
            end_task='foreach_policy_type_end'
        )

        check_policy_allowed_and_uri_match = rail.IfOperator(
            task_id='check_policy_allowed_and_uri_match',
            test=lambda dag_run: (
                rail.result('foreach_policy_type').get('isTimeOffAllowedAgainstThisTimeOffType', False) and
                rail.result('foreach_policy_type').get('timeOffType', {}).get('uri') == dag_run.conf['timeoffuri']
            ),
            yes_task='get_balance_summary_for_account',
            no_task='foreach_policy_type_end'
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUri": rail.result('foreach_policy_type').get('timeOffType', {}).get('uri')
                },
                "asOfDate": {
                    "day": rail.result('split_current_date')['day'],
                    "month": rail.result('split_current_date')['month'],
                    "year": rail.result('split_current_date')['year']
                }
            }
        )

        format_date = rail.PythonOperator(
            task_id='format_date',
            python_callable=lambda: f"{rail.result('split_current_date')['day']}/{rail.result('split_current_date')['month']}/{rail.result('split_current_date')['year']}"
        )

        # Recipe #9: payout only when the type's policy schedule has a dated entry
        check_policy_schedule_description_present = rail.IfOperator(
            task_id='check_policy_schedule_description_present',
            test=lambda: bool((((rail.result('foreach_policy_type') or {}).get('policySetSchedule') or [{}])[0] or {}).get('description')),
            yes_task='trigger_put_remaining_balance_child',
            no_task='foreach_policy_type_end'
        )

        trigger_put_remaining_balance_child = rail.TriggerDagRunOperator(
            task_id='trigger_put_remaining_balance_child',
            trigger_dag_id=config.momentive_japan_user_sync_child_put_remaining_balance_for_payout_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": dag_run.conf['timeoffuri'],
                "balance": int(rail.result('get_balance_summary_for_account')['timeRemaining']) if rail.result(
                    'get_balance_summary_for_account').get('timeRemaining') else 0,
                "terminationdate": rail.result('format_date'),
                "startingbalancesettouri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_balance_event_scripts'), 'displayText', 'Starting Balance Set To', 'uri')
            }
        )

        insert_childid_to_wait_list_1 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_1',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_put_remaining_balance_child')}}"
        )

        foreach_policy_type_end = rail.EmptyOperator(
            task_id='foreach_policy_type_end'
        )

        child_dag_ids = rail.PythonOperator(
            task_id='child_dag_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('childtriggeredlist')] if rail.get_dag_run_var('childtriggeredlist') else []
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('child_dag_ids') | to_json}}"
        )

        gather_responses_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_child',
            dag_runs='{{ result("child_dag_ids") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.responses_from_child_timeout),
            flatten=True
        )

        filter_error_responses = rail.PythonOperator(
            task_id='filter_error_responses',
            python_callable=lambda: [item for item in rail.result(
                'gather_responses_from_child') if item]
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Child payout for timeoff update; {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else (rail.result('filter_error_responses') or null)
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> create_child_trigger_list

        create_child_trigger_list >> get_balance_event_scripts >> get_validation_scripts >> get_user_policy_summary >> split_current_date >> foreach_policy_type >> check_policy_allowed_and_uri_match

        # ForEach loop task connections
        check_policy_allowed_and_uri_match >> rail.Label('Yes') >> get_balance_summary_for_account >> format_date >> check_policy_schedule_description_present
        check_policy_schedule_description_present >> rail.Label('Yes') >> trigger_put_remaining_balance_child >> insert_childid_to_wait_list_1 >> foreach_policy_type_end
        check_policy_schedule_description_present >> rail.Label('No') >> foreach_policy_type_end
        check_policy_allowed_and_uri_match >> rail.Label('No') >> foreach_policy_type_end

        foreach_policy_type >> foreach_policy_type_end >> child_dag_ids >> wait_for_child_dags >> gather_responses_from_child >> filter_error_responses >> catch_error >> final_response_from_dag

        return dag


rail.for_each_instance(create_dag)