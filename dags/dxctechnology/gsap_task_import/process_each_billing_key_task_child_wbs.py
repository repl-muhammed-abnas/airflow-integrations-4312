import rail
from dxctechnology.gsap_task_import.tasks.process_billing_key import process_billing_key


def create_process_each_billing_key_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_each_child_wbs_billing_key_dagid,
        description=f"DXCTechnology GSAP task import process GSAP child wbs each billing key {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:

        _, finish = process_billing_key(config,
                                        project_type="child",
                                        create_task_dag_id=config.child_wbs_create_task_dagid,
                                        update_dag_task_id=config.child_wbs_update_task_dagid)

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        finish >> log_to_sumo
    return dag


rail.for_each_instance(create_process_each_billing_key_dag)
