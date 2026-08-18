from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_policy_assignment_rehire_update_days_dag_id,
        description=f'Momentive_Japan_Policy_Assignment_rehire_Update_days_{config.instance}',
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
            no_task='get_existing_policies'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_existing_policies',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_existing_policies = rail.RepliconServiceOperator(
            task_id='get_existing_policies',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri']
            }
        )

        extract_existing_policy_schedule = rail.PythonOperator(
            task_id='extract_existing_policy_schedule',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_existing_policies').get('policiesByTimeOffType', []),
                'timeOffType.uri',
                dag_run.conf['timeoffuri'],
                'policySetSchedule'
            )
        )

        
        filter_historical_policies = rail.PythonOperator(
            task_id='filter_historical_policies',
            python_callable=lambda dag_run: python_callable.get_relevant_historical_policies(
                rail.result('extract_existing_policy_schedule'),
                datetime.now().strftime('%Y-%m-%d'),
                '%Y-%m-%d'
            )
        )

        get_default_policy_for_user = rail.RepliconServiceOperator(
            task_id='get_default_policy_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                }
            }
        )

        # Recipe #18: append the default policy only when its first entry is dated
        if_default_policy_dated = rail.IfOperator(
            task_id='if_default_policy_dated',
            test=lambda: bool(((rail.result('get_default_policy_for_user') or [{}])[0].get('effectiveDate') or {}).get('day')),
            yes_task='extract_default_policy_set',
            no_task='append_new_policy_to_historical'
        )

        extract_default_policy_set = rail.PythonOperator(
            task_id='extract_default_policy_set',
            python_callable=lambda dag_run: (
                rail.result('get_default_policy_for_user')[0]['policySet']
                if rail.result('get_default_policy_for_user') and len(rail.result('get_default_policy_for_user')) > 0
                else {}
            )
        )

        validate_policy_set = rail.PythonOperator(
            task_id='validate_policy_set',
            python_callable=lambda dag_run: python_callable.validate_policy_structure(
            rail.result('extract_default_policy_set'),
            dag_run.conf.get('timeofftype', 'Unknown'))
        )

        convert_policy_set_with_script_target = rail.PythonOperator(
            task_id='convert_policy_set_with_script_target',
            python_callable=lambda dag_run: python_callable.convert_policy_set_with_script_target(
                rail.result('validate_policy_set')
            )
        )

        build_new_policy_entry = rail.PythonOperator(
            task_id='build_new_policy_entry',
            python_callable=lambda dag_run: {
                "effectiveDate": rail.parse_date(dag_run.conf['startdate'], '%Y-%m-%d'),
                "description": "Policy " + dag_run.conf['startdate'],
                "policySet": rail.result('convert_policy_set_with_script_target')
            }
        )

        append_new_policy_to_historical = rail.PythonOperator(
            task_id='append_new_policy_to_historical',
            python_callable=lambda dag_run: (
                rail.result('filter_historical_policies') +
                ([rail.result('build_new_policy_entry')] if rail.result('build_new_policy_entry') else [])
            )
        )

        update_user_policy_schedule = rail.RepliconServiceOperator(
            task_id='update_user_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('append_new_policy_to_historical')
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{% if 'timeOffBalanceEventScripts' in get_error_message() and 'KeyError' in get_error_message() %}"
                "Policy Assignment Error: Default accrual policies are not assigned for time off type: {{ dag_run.conf.timeofftype or 'Unknown' }}. "
                "Please assign default accrual policies in Replicon UI before running this workflow." 
                "{% else %}Error in rehire policy assignment update days flow; {{ get_error_message() }}{% endif %}"
            )
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_existing_policies

        get_existing_policies >> extract_existing_policy_schedule >> filter_historical_policies >> get_default_policy_for_user

        get_default_policy_for_user >> if_default_policy_dated >> rail.Label('Yes') >> extract_default_policy_set >> validate_policy_set >> convert_policy_set_with_script_target
        if_default_policy_dated >> rail.Label('No') >> append_new_policy_to_historical

        convert_policy_set_with_script_target >> build_new_policy_entry >> append_new_policy_to_historical >> update_user_policy_schedule >> catch_error

        return dag


rail.for_each_instance(create_dag)