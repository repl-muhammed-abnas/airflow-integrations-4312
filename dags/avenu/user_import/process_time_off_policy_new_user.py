from datetime import timedelta
import rail
from avenu.user_import.utils import request_payload
from avenu.user_import.utils import python_callable_method
from airflow.models import Variable


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'avenu_user_sync_process_time_off_policy_new_user_{config.instance}_child',
        description='Avenu User Sync Process Time off Policy New User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_policy_new_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "get_default_time_off_policy_schedule"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_default_time_off_policy_schedule',
            end_task="catch_and_log_errors",
        )

        get_default_time_off_policy_schedule = rail.RepliconServiceOperator(
            task_id="get_default_time_off_policy_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeoff_policy_schedule_payload
        )

        policy_to_assign = rail.PythonOperator(
            task_id="policy_to_assign",
            python_callable=python_callable_method.get_policy_to_assign
        )

        is_policy_present = rail.IfOperator(
            task_id='is_policy_present',
            test=lambda: bool(rail.result('policy_to_assign')),
            yes_task='put_user_timeoff_policy',
            no_task='no_default_timeoff_policy_available'
        )

        put_user_timeoff_policy = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_user_timeoff_policy_payload
        )

        no_default_timeoff_policy_available = rail.EmptyOperator(
            task_id='no_default_timeoff_policy_available'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log_error}}",
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
        can_run_batch_task >> rail.Label("No") >> get_default_time_off_policy_schedule \
            >> policy_to_assign >> is_policy_present
        is_policy_present >> rail.Label(
            'Yes') >> put_user_timeoff_policy >> catch_and_log_errors
        is_policy_present >> rail.Label(
            'No') >> no_default_timeoff_policy_available >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
