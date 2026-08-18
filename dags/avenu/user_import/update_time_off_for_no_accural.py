from datetime import timedelta
import rail
from avenu.user_import.utils import request_payload
from airflow.models import Variable

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'avenu_user_sync_update_time_off_for_no_aacural_{config.instance}_child',
        description='Avenu User Sync update_time_off_for_no_aacural',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_records,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "get_balance_summary_for_account"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_balance_summary_for_account',
            end_task="catch_and_log_errors",
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint='/services/TimeOffService2.svc/GetBalanceSummaryForAccount',
            data=request_payload.get_balance_summary_for_account,
        )

        get_all_scripts_timeOff_validation_script = rail.RepliconServiceOperator(
            task_id='get_all_scripts_timeOff_validation_script',
            endpoint='/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts',
        )

        get_all_scripts_timeOff_balance_eventscript = rail.RepliconServiceOperator(
            task_id='get_all_scripts_timeOff_balance_eventscript',
            endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts',
        )

        put_user_timeoff_policy_schedule = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: request_payload.put_user_timeoff_policy_accrual_policy(
                dag_run, config)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Error',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_balance_summary_for_account \
            >> get_all_scripts_timeOff_validation_script >> get_all_scripts_timeOff_balance_eventscript
        get_all_scripts_timeOff_balance_eventscript >> put_user_timeoff_policy_schedule >> catch_and_log_errors >> log_to_sumo
    return dag


rail.for_each_instance(create_child_dag_wbs)
