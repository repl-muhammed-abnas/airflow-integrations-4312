import rail


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_oef_dropdown_dag_id,
        description=f'alvarezandmarsalholdingsdev_User_Import_Update_oef_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_child,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf_user_oefs")

        create_oef_draft = rail.RepliconServiceOperator(
            task_id='create_oef_draft',
            endpoint="/services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda dag_run: {
                "objectExtensionTagDefinitionUri": dag_run.conf['oefuri']
            }
        )

        update_oef_tag_name = rail.RepliconServiceOperator(
            task_id='update_oef_tag_name',
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda dag_run: {
                    "objectExtensionTagUri": rail.result("create_oef_draft"),
                    "name": dag_run.conf['name']
            }
        )

        update_oef_tag_code = rail.RepliconServiceOperator(
            task_id='update_oef_tag_code',
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda dag_run: {
                    "objectExtensionTagUri": rail.result("create_oef_draft"),
                    "code": dag_run.conf['code']
            }
        )

        enable_oef_tag = rail.RepliconServiceOperator(
            task_id='enable_oef_tag',
            endpoint="/services/ObjectExtensionTagService1.svc/Enable",
            data=lambda: {
                "objectExtensionTagUri": rail.result("create_oef_draft")
            }
        )

        publish_oef_draft = rail.RepliconServiceOperator(
            task_id='publish_oef_draft',
            endpoint="/services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                "objectExtensionTagUri": rail.result("create_oef_draft")
            }
        )

        create_oef_draft >> update_oef_tag_name >> update_oef_tag_code >> enable_oef_tag >> publish_oef_draft

    return dag


rail.for_each_instance(create_dag)
