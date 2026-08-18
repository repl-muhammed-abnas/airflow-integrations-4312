from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.python_callable_method import get_final_timeoffs_to_assign, get_policy_schedule_entries
from adtalem.user_import.utils.response_filter import get_assigned_timeoff_policy_update


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_timeoff_updateuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_timeoff_update_user_crv2.0_{config.instance}',
        description=f'Adtalem Update User-Time Off CR2.0 {config.instance}',
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
            no_task='get_vacationbuyup_timeoff_uri'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_vacationbuyup_timeoff_uri',
            end_task='dagrun_log_to_sumo'
        )

        get_vacationbuyup_timeoff_uri = rail.RepliconServiceOperator(
            task_id='get_vacationbuyup_timeoff_uri',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Vacation Buy Up', 'uri', '')
        )

        assign_timeoff_template = rail.RepliconServiceOperator(
            task_id='assign_timeoff_template',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": config.policy_set_uri
            }
        )

        get_enabled_replicon_timeoffs = rail.RepliconServiceOperator(
            task_id='get_enabled_replicon_timeoffs',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        get_timeofftype_uris_to_assign = rail.PythonOperator(
            task_id='get_timeofftype_uris_to_assign',
            python_callable=get_final_timeoffs_to_assign
        )

        is_timeoff_uris_to_assign = rail.IfOperator(
            task_id='is_timeoff_uris_to_assign',
            test="{{ result('get_timeofftype_uris_to_assign') | length > 0 }}",
            yes_task='assign_timeoffs_user',
            no_task='get_policy_schedule'
        )

        assign_timeoffs_user = rail.RepliconServiceOperator(
            task_id='assign_timeoffs_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('get_timeofftype_uris_to_assign')
            }
        )

        get_policy_schedule = rail.PythonOperator(
            task_id='get_policy_schedule',
            python_callable=get_policy_schedule_entries,
            op_args=[config.adtalem_sicktime_timeoffpolicy_schedule_mapper_old]
        )

        get_requiredpolicy_fortimeoff_type = rail.RepliconServiceOperator(
            task_id='get_requiredpolicy_fortimeoff_type',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_assigned_timeoff_policy_update
        )

        assign_sick_timeoff_policy = rail.RepliconServiceOperator(
            task_id='assign_sick_timeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: rail.result('get_requiredpolicy_fortimeoff_type')
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_vacationbuyup_timeoff_uri
        get_vacationbuyup_timeoff_uri >> assign_timeoff_template >> get_enabled_replicon_timeoffs >> \
            get_timeofftype_uris_to_assign >> is_timeoff_uris_to_assign

        is_timeoff_uris_to_assign >> rail.Label(
            'Yes') >> assign_timeoffs_user >> get_policy_schedule

        is_timeoff_uris_to_assign >> rail.Label(
            'No') >> get_policy_schedule

        get_policy_schedule >> get_requiredpolicy_fortimeoff_type >> assign_sick_timeoff_policy >> \
            dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_timeoff_updateuser_child_dag)
