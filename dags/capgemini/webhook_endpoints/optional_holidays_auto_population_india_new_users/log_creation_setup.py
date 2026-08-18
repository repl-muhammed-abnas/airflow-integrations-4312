import rail


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.log_creation_setup_dagid,
        description=f'Capgemini Auto Population of Optional Holidays India New Users - Log Creation Setup {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'retries': 0
        }
    ) as dag:

        for idx, log_name in enumerate(config.tenant_wide_log_list):
            # Extract bare log name from full artifact reference (artifact:<Tenant>:log:<name>)
            rail.CreateLogOperator(
                task_id=f"create_log_artifact_{idx}",
                tenant_wide_name=log_name.split(":")[-1],
                existing_log_mode="append"
            )

        return dag


rail.for_each_instance(create_dag)
