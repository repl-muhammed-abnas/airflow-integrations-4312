import rail

null = None

# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_future_enddate_user_child_dagid,
        description='ViaPlus User Sync - Disable User Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.disable_user_child_dag_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint='services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            }
        )

    return dag


rail.for_each_instance(create_child_dag_wbs)
