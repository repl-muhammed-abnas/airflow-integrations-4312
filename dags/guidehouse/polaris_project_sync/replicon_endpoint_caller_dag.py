
from datetime import timedelta
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.replicon_endpoint_caller_dag_id,
        description=f'guidehouseinc_polaris_replicon_endpoint_caller_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config',
            extra_config=config,
        )

        # Calls whatever Replicon endpoint is specified in dag_run.conf.
        # Expected conf keys:
        #   endpoint  (str)  — relative Replicon service URL, e.g.
        #                       "/services/TaskService1.svc/BulkUpdateTaskResourceEstimates"
        #   data      (dict) — JSON-serialisable request body
        call_endpoint = rail.RepliconServiceOperator(
            task_id='call_endpoint',
            execution_timeout=timedelta(
                hours=getattr(config, 'endpoint_caller_execution_timeout_hours', 1),
            ),
            endpoint=lambda: rail.get_dag_run_conf()['endpoint'],
            data=lambda: rail.get_dag_run_conf()['data'],
        )
        
        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "status": "Error",
                "reason": "{{ get_error_message() }}"
            }
        )
        
        call_endpoint >> catch_error

    return dag


rail.for_each_instance(create_dag)
