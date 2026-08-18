import rail
from airflow.models import Variable

from dataaxle.user_import.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_create_job_title_dag_id,
        description=f"Dataaxle User Import - Create Job Title child DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_create_job_title_child
    ) as dag:

        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_use_batch = rail.IfOperator(
            task_id="can_use_batch",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="false"
            ).lower() == "true",
            yes_task="batch_wrapper",
            no_task="start_process",
        )

        batch_wrapper = rail.BatchTaskRunOperator(
            task_id="batch_wrapper",
            start_task="start_process",
            end_task="finish",
        )

        start_process = rail.EmptyOperator(task_id="start_process")

        create_job_titles = rail.RepliconServiceOperator(
            task_id="create_job_titles",
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data=lambda dag_run: request_payload.create_job_title_payload(dag_run),
        )

        finish = rail.EmptyOperator(task_id="finish")

        can_use_batch >> rail.Label("Yes") >> batch_wrapper >> finish
        can_use_batch >> rail.Label("No") >> start_process >> create_job_titles >> finish

    return dag

rail.for_each_instance(create_child_dag)
