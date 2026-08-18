import rail

null = None

# pylint:disable = too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_auto_population_of_optional_holidays_india_create_log_artifacts_{config.instance}_v0',
        description=f'Capgemini Auto Population of Optional Holidays India for New Users Createb Log Artifacts v0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_new_users,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:

        for idx, log_name in enumerate(config.tenant_wide_log_list):

            rail.CreateLogOperator(
                task_id = f"get_tenant_wide_log_{idx}",
                tenant_wide_name=log_name.split(":")[-1],
                existing_log_mode="append"
            )

        return dag

rail.for_each_instance(create_child_dag)