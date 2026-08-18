import rail

def update_oef():
    with rail.TaskGroup(group_id="update_oef", prefix_group_id=False) as process_user_oefs:

        create_oef_draft= rail.RepliconServiceOperator(
            task_id='create_oef_draft',
            endpoint="/services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda dag_run: {
                "objectExtensionTagDefinitionUri": dag_run.conf['oefuri']
            }
        )

        update_oef_tag_name= rail.RepliconServiceOperator(
            task_id='update_oef_tag_name',
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda dag_run:{
                    "objectExtensionTagUri": rail.result("create_oef_draft"),
                    "name": dag_run.conf['username']
                }
        )

        enable_oef_tag= rail.RepliconServiceOperator(
            task_id='enable_oef_tag',
            endpoint="/services/ObjectExtensionTagService1.svc/Enable",
            data=lambda: {
                "objectExtensionTagUri": rail.result("create_oef_draft")
            }
        )

        publish_oef_draft= rail.RepliconServiceOperator(
            task_id='publish_oef_draft',
            endpoint="/services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                "objectExtensionTagUri": rail.result("create_oef_draft")
            }
        )

        create_oef_draft >> update_oef_tag_name >> enable_oef_tag >> publish_oef_draft

    return process_user_oefs
