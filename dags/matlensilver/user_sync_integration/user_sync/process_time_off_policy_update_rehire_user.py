import rail

from matlensilver.user_sync_integration.user_sync.utils import request_payload
from matlensilver.user_sync_integration.user_sync.utils import python_callable_method


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'matlen_silver_user_sync_child_process_time_off_policy_update_rehire_user_{config.instance}',
        description='Matlen_Silver User Sync Process Time off Policy For Update/Rehire user',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_policy_update_rehire_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_default_timeoff_policy_set_schedule_for_timeofftype = rail.RepliconServiceOperator(
            task_id="get_default_timeoff_policy_set_schedule_for_timeofftype",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=request_payload.get_default_timeoff_policy_set_schedule_for_timeofftype
        )

        is_policy_present = rail.IfOperator(
            task_id='is_policy_present',
            test=lambda dag_run: bool(dag_run.conf['policy']),
            yes_task='get_policy_to_assign_list',
            no_task='get_default_policy_set'
        )

        get_policy_to_assign_list = rail.PythonOperator(
            task_id='get_policy_to_assign_list',
            python_callable=python_callable_method.get_policy_to_assign_list
        )

        get_default_policy_set = rail.PythonOperator(
            task_id='get_default_policy_set',
            python_callable=python_callable_method.get_default_policy_set
        )

        get_current_policy_set_to_assign = rail.PythonOperator(
            task_id='get_current_policy_set_to_assign',
            python_callable=python_callable_method.get_current_policy_set_to_assign
        )

        get_all_policy_to_assign = rail.PythonOperator(
            task_id='get_all_policy_to_assign',
            python_callable=python_callable_method.get_all_policy_to_assign
        )

        put_user_timeoff_policy_schedule = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_user_timeoff_policy_schedule
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

        get_default_timeoff_policy_set_schedule_for_timeofftype >> is_policy_present >> rail.Label(
            'Yes') >> get_policy_to_assign_list
        is_policy_present >> rail.Label('No') >> get_default_policy_set
        get_policy_to_assign_list >> get_default_policy_set >> get_current_policy_set_to_assign
        get_current_policy_set_to_assign >> get_all_policy_to_assign >> put_user_timeoff_policy_schedule >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
