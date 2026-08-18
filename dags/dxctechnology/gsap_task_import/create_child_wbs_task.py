import rail
from dxctechnology.gsap_task_import.tasks.create_tasks import get_create_gsap_task_group


def create_update_gsap_task_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.child_wbs_create_task_dagid,
        description=f"DXCTechnology GSAP task import create child wbs task {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_update_task_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run")

        create_task = get_create_gsap_task_group("child")

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_task >> log_to_sumo
    return dag


rail.for_each_instance(create_update_gsap_task_dag)
