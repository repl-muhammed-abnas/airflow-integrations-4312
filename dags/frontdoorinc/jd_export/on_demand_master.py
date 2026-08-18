from datetime import timedelta
import rail
from frontdoorinc.jd_export.utils.python_callable import get_ondemand_conf

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_on_demand_dag_id,
        description=f'Frontdoorinc_JDEIntegration On Demand {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        process_jd_export_dag_run = rail.TriggerDagRunOperator(
            task_id='process_jd_export_dag_run',
            trigger_dag_id=config.jd_export_child_dag_id,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            conf=lambda dag_run: get_ondemand_conf(dag_run)
        )

        wait_for_process_jd_export_dag_run = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_jd_export_dag_run',
            dag_runs='{{ result("process_jd_export_dag_run") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_jd_export_dag_run >> wait_for_process_jd_export_dag_run

        return dag

rail.for_each_instance(create_dag)
