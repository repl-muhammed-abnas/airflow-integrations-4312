from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.response_filter import get_timeoffs_to_assign


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_timeoff_update_user_crv14.0_{config.instance}',
        description=f'Adtalem user import Update User - Time Off_Temp_CR14.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_timeoff_policyset'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_timeoff_policyset',
            end_task='dagrun_log_to_sumo'
        )

        get_timeoff_policyset = rail.RepliconServiceOperator(
            task_id='get_timeoff_policyset',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Time Off', 'uri', '')
        )

        is_timeoff_policy_present = rail.IfOperator(
            task_id='is_timeoff_policy_present',
            test="{{ result('get_timeoff_policyset') | is_truthy }}",
            yes_task="get_pto_fto_assigned_timeoffs",
            no_task="dagrun_log_to_sumo",
        )

        get_pto_fto_assigned_timeoffs = rail.RepliconServiceOperator(
            task_id='get_pto_fto_assigned_timeoffs',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_timeoffs_to_assign
        )

        is_timeoffs_to_assign = rail.IfOperator(
            task_id='is_timeoffs_to_assign',
            test="{{ result('get_pto_fto_assigned_timeoffs') | length > 0 }}",
            yes_task="assign_required_timeoff_types",
            no_task="dagrun_log_to_sumo",
        )

        assign_required_timeoff_types = rail.RepliconServiceOperator(
            task_id='assign_required_timeoff_types',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('get_pto_fto_assigned_timeoffs')
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_timeoff_policyset

        get_timeoff_policyset >> is_timeoff_policy_present

        is_timeoff_policy_present >> rail.Label(
            'Yes') >> get_pto_fto_assigned_timeoffs >> is_timeoffs_to_assign
        is_timeoffs_to_assign >> rail.Label(
            'Yes') >> assign_required_timeoff_types >> dagrun_log_to_sumo
        is_timeoffs_to_assign >> rail.Label(
            'No') >> dagrun_log_to_sumo
        is_timeoff_policy_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_dag)
