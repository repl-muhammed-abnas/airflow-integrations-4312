import rail


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'crl_ermination_balance_udf_update_child_{config.instance}',
        description=f'CRL termination_balance_udf_update_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_activeleavestatus_dropdown = rail.RepliconServiceOperator(
            task_id='get_activeleavestatus_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda dag_run:{
                "customFieldUri":  dag_run.conf["exported_udf_uri"]
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Yes', 'uri', '')
        )

        update_udf = rail.RepliconServiceOperator(
            task_id='update_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "customFieldUri": dag_run.conf["exported_udf_uri"],
                "customFieldDropDownOptionUri": rail.result('get_activeleavestatus_dropdown')
            }
        )

        get_activeleavestatus_dropdown >> update_udf

    return dag


rail.for_each_instance(create_child_dag)
