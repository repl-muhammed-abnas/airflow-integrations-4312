from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.python_callable_method import construct_policyschedule
from adtalem.user_import.utils.request_payload import get_datetime_obj, get_put_time_offpolicy_with_initial_balance_blank


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_put_balance_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_put_blank_balance_for_update_users_cr14.0_{config.instance}',
        description=f'Adtalem User Import Put blank balance for update users_CR14.0 {config.instance}',
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
            no_task='get_existingpolicy_schedule_for_timeoff'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_existingpolicy_schedule_for_timeoff',
            end_task='dagrun_log_to_sumo',
        )

        get_existingpolicy_schedule_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_existingpolicy_schedule_for_timeoff',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        get_timeoffbalance_event_script_administration_service = rail.RepliconServiceOperator(
            task_id='get_timeoffbalance_event_script_administration_service',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts"
        )

        get_preventbalanceoverdraw_script_uri = rail.RepliconServiceOperator(
            task_id='get_preventbalanceoverdraw_script_uri',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Prevent balance overdraw', 'uri', '')
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "asOfDate": get_datetime_obj(dag_run.conf['terminationdate'])
            },
            data_handler=lambda response: float(response['timeRemaining']) if response.get(
                'timeRemaining', '') else ''
        )

        past_policyset_schedule = rail.PythonOperator(
            task_id='past_policyset_schedule',
            python_callable=construct_policyschedule
        )

        if_past_policyset_schedule_present = rail.IfOperator(
            task_id='if_past_policyset_schedule_present',
            test="{{ result('past_policyset_schedule') | is_truthy }}",
            yes_task="put_time_offpolicy_with_initial_balance_blank",
            no_task="dagrun_log_to_sumo",
        )

        put_time_offpolicy_with_initial_balance_blank = rail.RepliconServiceOperator(
            task_id='put_time_offpolicy_with_initial_balance_blank',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_time_offpolicy_with_initial_balance_blank
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_existingpolicy_schedule_for_timeoff

        get_existingpolicy_schedule_for_timeoff >> get_timeoffbalance_event_script_administration_service >> \
            get_preventbalanceoverdraw_script_uri >> get_balance_summary_for_account >> \
            past_policyset_schedule >> if_past_policyset_schedule_present
        if_past_policyset_schedule_present >> rail.Label(
            'Yes') >> put_time_offpolicy_with_initial_balance_blank >> dagrun_log_to_sumo
        if_past_policyset_schedule_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_put_balance_child_dag)
