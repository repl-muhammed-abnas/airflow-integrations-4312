

import rail
from airflow.operators.python import PythonOperator





def create_vp_dag(config):
    """
    Per-customer VantagePoint connection setup DAG.

    Tasks:
      1. delete_vp_conn — removes any existing {customer_id}_vp_instance_ajera_to_vp_{instance}
         connection so all fields are fully refreshed on re-setup.
      2. create_vp_conn — authenticates with the VantagePoint token endpoint and creates
         the per-customer connection (via CreateAirflowConnection / build_vp_instance_attrs).
    """
    
    def _log_results(dag_run):
        result = rail.result('get_clients')
        records = result if isinstance(result, list) else result.get('value', [])
        print(f"Connection successful. Fetched {len(records)} client record(s).")
        for rec in records[:5]:
            print(f"  ClientID={rec.get('ClientID')}  Name={rec.get('Name')}  Status={rec.get('Status')}")

    with rail.create_airflow_dag(
        dag_id=f"ajera_vantagepoint_migration_vp_conn_test_{config.instance}",
        description="Load data to vantagepoint via vantagepoint operator",
        company_key="Repliconpincstream6dev",
        integration_type="generic",
        max_active_runs=10,
        schedule_interval=None,
        catchup=False,
    ) as dag:

        # 1) GET /firm — fetches first page of clients; confirms auth + connectivity
        get_clients = rail.VantagepointFirmOperator(
            task_id='get_clients',
            vp_conn_id='{{ dag_run.conf.get("vp_conn_id", "ajera_vantagepoint_migration_vp_instance_conn") }}',
            request_method='GET',
            pagination=False,
            filters='?$top=5',
        )

        # 2) Print a summary of what came back
        log_results = PythonOperator(
            task_id='log_results',
            python_callable=_log_results,
        )

        get_clients >> log_results


        

        return dag


rail.for_each_instance(create_vp_dag)