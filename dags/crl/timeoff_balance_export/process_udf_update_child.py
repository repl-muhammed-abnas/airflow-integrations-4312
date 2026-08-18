import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_udf_update_child_dag,
        description=f'CRL_timeoff_balance_udf_update_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        if_update_spo_udf_is_yes = rail.IfOperator(
            task_id="if_update_spo_udf_is_yes",
            test=lambda dag_run: dag_run.conf['update_spo_udf'] == 'yes',
            yes_task="update_sick_eligible_udf",
            no_task="if_update_bpo_udf_is_yes"
        )

        update_sick_eligible_udf = rail.RepliconServiceOperator(
            task_id = 'update_sick_eligible_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "customFieldUri": dag_run.conf["sick_payout_eligible"],
                    "customFieldDropDownOptionUri": dag_run.conf["set_sick_payout"]
            }
        )

        if_update_bpo_udf_is_yes = rail.IfOperator(
            task_id="if_update_bpo_udf_is_yes",
            test=lambda dag_run: dag_run.conf['update_bpo_udf'] == 'yes',
            yes_task="update_banked_eligible_udf"
        )

        update_banked_eligible_udf = rail.RepliconServiceOperator(
            task_id = 'update_banked_eligible_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "customFieldUri": dag_run.conf["banked_payout_eligible"],
                    "customFieldDropDownOptionUri": dag_run.conf["set_banked_payout"]
            }
        )

        if_update_spo_udf_is_yes >> rail.Label("Yes") >> update_sick_eligible_udf >> if_update_bpo_udf_is_yes
        if_update_spo_udf_is_yes >> rail.Label("No") >> if_update_bpo_udf_is_yes
        if_update_bpo_udf_is_yes >> rail.Label("Yes") >> update_banked_eligible_udf

    return dag

rail.for_each_instance(create_child_dag)
