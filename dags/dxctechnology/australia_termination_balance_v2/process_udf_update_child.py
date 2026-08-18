import rail


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_aus_termination_balance_udf_update_child_{config.instance}_v2',
        description=f'DXC_AUS_termination_balance_udf_update_child {config.instance} V2',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
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
