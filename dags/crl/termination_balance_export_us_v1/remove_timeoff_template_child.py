import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.remove_timeoff_templates_child_dag_id,
        description=f'CRL Termination Balance Export - Remove Time-Off Templates - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        remove_timeoff_template = rail.RepliconServiceOperator(
            task_id="remove_timeoff_template",
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "policySetUri": "{{ dag_run.conf.timeoff_policy_set_uri }}",
            }
        )

        remove_timeoff_template

    return dag


rail.for_each_instance(create_child_dag)
