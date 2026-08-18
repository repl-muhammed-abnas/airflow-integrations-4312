import rail

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_homepage_message_update_process_messages_child_{config.instance}',
        description=f'Capgemini Homepage Message Update process messages child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='put_homepage_message',
            endpoint='/services/OverviewPageMessageService1.svc/PutOverviewPageMessage',
            data=lambda dag_run: {
                "parameter": dag_run.conf["message_payload"],
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
