import rail
from cbrefcg.user_billing_rate_update.utils import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_billing_rates_dag_id,
        description=f'cbrefcg_process_billing_rates_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_billing_rates_active_max_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        assign_users_billing_rate = rail.RepliconServiceOperator(
            task_id='assign_users_billing_rate',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/InsertBillingRateIntoUserSpecificBillingRateSchedule",
            data=request_payload.get_users_billing_rate_payload
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        assign_users_billing_rate >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
