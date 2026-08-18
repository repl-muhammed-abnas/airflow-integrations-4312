from datetime import timedelta
from airflow.models import Variable
import rail

from incyte_biosciences_international_sarl.user_import.utils import request_payload
from incyte_biosciences_international_sarl.user_import.utils.response_filter import get_filtered_time_off_types, get_policy_to_assign


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_type_assignment_new_user_dagid,
        description='IBIS - User Import - Process TIme Off Type Assignment- New User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_type_assignment_new_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_time_off_types'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_all_time_off_types',
            end_task='catch_and_log_errors',
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=get_filtered_time_off_types
        )

        put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
            task_id="put_timeoff_assignment_for_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.put_timeoff_assignment_for_user
        )

        for_each_time_off_assign_default_policy = rail.ForEachOperator(
            task_id="for_each_time_off_assign_default_policy",
            items=lambda: rail.result('get_all_time_off_types'),
            start_task='get_default_time_off_policy_schedule',
            end_task='for_each_time_off_assign_default_policy_end'
        )

        get_default_time_off_policy_schedule = rail.RepliconServiceOperator(
            task_id="get_default_time_off_policy_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=request_payload.get_default_timeoff_policy_schedule_payload,
            data_handler=get_policy_to_assign
        )

        is_policy_present = rail.IfOperator(
            task_id='is_policy_present',
            test=lambda: bool(rail.result(
                'get_default_time_off_policy_schedule')),
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

        for_each_time_off_assign_default_policy_end = rail.EmptyOperator(
            task_id='for_each_time_off_assign_default_policy_end'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.user_log}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{"User Added, Error: " if dag_run.conf.action == "add" else "" }}{{ get_error_message() }}',
            properties={
                'login_name': '{{dag_run.conf.login_name}}',
                'first_name': '{{dag_run.conf.first_name}}',
                'last_name': '{{dag_run.conf.last_name}}',
                'action': '{{"Add" if dag_run.conf.action == "add" else "Rehire" }}',
                'status': 'Error',
                'details': '{{"User Added, Error: " if dag_run.conf.action == "add" else "" }}{{ get_error_message() }}'
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_all_time_off_types

        get_all_time_off_types >> rail.Label('Yes') >> put_timeoff_assignment_for_user >> for_each_time_off_assign_default_policy
        for_each_time_off_assign_default_policy >> for_each_time_off_assign_default_policy_end

        for_each_time_off_assign_default_policy >> get_default_time_off_policy_schedule >> is_policy_present
        is_policy_present >> rail.Label('No') >> no_default_timeoff_policy_available >> for_each_time_off_assign_default_policy_end
        is_policy_present >> rail.Label( 'Yes') >> put_user_timeoff_policy >> for_each_time_off_assign_default_policy_end

        for_each_time_off_assign_default_policy_end >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
