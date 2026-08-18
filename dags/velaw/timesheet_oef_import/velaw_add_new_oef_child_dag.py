import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'velaw_add_new_oef_child_{config.instance}',
        description=f'RWS send individual custom email notification for timesheets waiting for approval child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        create_new_oef_draft = rail.RepliconServiceOperator(
            task_id='create_new_oef_draft',
            endpoint='/services/ObjectExtensionTagService1.svc/CreateNewDraft',
            data=lambda dag_run: {
                "objectExtensionTagDefinitionUri": dag_run.conf['tagDefinitionUri']
            }
        )

        update_name = rail.RepliconServiceOperator(
            task_id='update_name',
            endpoint='/services/ObjectExtensionTagService1.svc/UpdateName',
            data=lambda dag_run: {
                "objectExtensionTagUri": rail.result('create_new_oef_draft'),
                "name": dag_run.conf['tagName']
            }
        )

        enable_oef = rail.RepliconServiceOperator(
            task_id='enable_oef',
            endpoint='/services/ObjectExtensionTagService1.svc/Enable',
            data=lambda : {
                "objectExtensionTagUri": rail.result('create_new_oef_draft')
            }
        )

        publish_oef_draft = rail.RepliconServiceOperator(
            task_id='publish_oef_draft',
            endpoint='/services/ObjectExtensionTagService1.svc/PublishDraft',
            data=lambda : {
                "objectExtensionTagUri": rail.result('create_new_oef_draft')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )
        create_new_oef_draft >> update_name >> enable_oef >> publish_oef_draft >> finish

    return dag

rail.for_each_instance(create_dag)
