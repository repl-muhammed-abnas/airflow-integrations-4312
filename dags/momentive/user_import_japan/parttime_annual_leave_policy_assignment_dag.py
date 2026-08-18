from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_parttime_assignment_dag_id,
        description=f'Momentive_user_sync_child_annual_leave_policy_parttime_assignment_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
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
            no_task='get_default_policy_for_parttime_month'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_default_policy_for_parttime_month',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        # Call GetDefaultTimeOffTypePolicyScheduleForUser (different endpoint for part-time policy)
        get_default_policy_for_parttime_month = rail.RepliconServiceOperator(
            task_id='get_default_policy_for_parttime_month',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                }
            }
        )

        # Check if policy response exists
        if_policy_response_exists = rail.IfOperator(
            task_id='if_policy_response_exists',
            test=lambda: bool(((rail.result('get_default_policy_for_parttime_month') or [{}])[0].get('effectiveDate') or {}).get('day')),
            yes_task='extract_and_convert_parttime_policy',
            no_task='catch_error'
        )

        # Extract and convert policy with date replacements (startdate + 6 months logic)
        extract_and_convert_parttime_policy = rail.PythonOperator(
            task_id='extract_and_convert_parttime_policy',
            python_callable=lambda dag_run: python_callable.build_parttime_timeoff_policy_with_date_replacement(
                rail.result('get_default_policy_for_parttime_month'),
                dag_run
            )
        )

        # Assign the converted policy to the user
        assign_annual_leave_policy_parttime = rail.RepliconServiceOperator(
            task_id='assign_annual_leave_policy_parttime',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('extract_and_convert_parttime_policy')
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Parttime Annual Leave Assignment for user ; {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_default_policy_for_parttime_month >> if_policy_response_exists

        if_policy_response_exists >> rail.Label("Yes") >> extract_and_convert_parttime_policy
        if_policy_response_exists >> rail.Label("No") >> catch_error
        
        extract_and_convert_parttime_policy >> assign_annual_leave_policy_parttime >> catch_error

    return dag


for_each_instance = rail.for_each_instance(create_dag)
