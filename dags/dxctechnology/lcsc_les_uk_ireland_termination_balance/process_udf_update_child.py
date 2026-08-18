import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_udf_update_child_dag_id,
        description=f'DXC_LCSC_LES_UK_Ireland_termination_balance_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_update_udf_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        update_udf = rail.RepliconServiceOperator(
            task_id = 'update_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "customFieldUri": dag_run.conf["exported_udf_uri"],
                    "value": "Yes"
            }
        )

        update_udf

    return dag

rail.for_each_instance(create_child_dag)
