import rail

from wikwemikongboard.timeoffbalancetransfer.utils import request_payload,response_payload
from airflow.models import Variable
from datetime import timedelta


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.get_timeoff_child_dag_id,
        description=f"get timeoff child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_batch_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_timeoff_types'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_timeoff_types',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_timeoff_types = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_types",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_timeoff_types,
            data_handler=response_payload.filter_timeoff_types
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/importService1.svc/BulkGetUsers3",
            data=request_payload.get_user_details,
            data_handler=response_payload.get_template
        )

        get_timeoff_balance_details = rail.RepliconServiceOperator(
            task_id="get_timeoff_balance_details",
            endpoint="/services/TimeOffService2.svc/BulkGetBalanceSummaryForAccounts",
            data=request_payload.get_timeoff_balance
        )

        all_data = rail.PythonOperator(
            task_id="all_data",
            python_callable=request_payload.get_all_data
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >>get_user_timeoff_types >> get_user_details >> get_timeoff_balance_details >> all_data >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
