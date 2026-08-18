import rail
from dxctechnology.compass_attribute_1_2_new import response_filter
from dxctechnology.compass_attribute_1_2_new import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_child_sync_attribute_system_level_{config.instance}_{config.sub_erp}_{config.attribute}',
        description=f'Sync Attributes at System Level {config.instance} {config.sub_erp} {config.attribute}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_sync_attribute_system_level,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_specific_attribute_system_level = rail.RepliconServiceOperator(
            task_id="get_specific_attribute_system_level",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data=request_payload.get_specific_attribute_system_level_payload,
            response_filter=response_filter.map_get_specific_attribute_system_level
        )

        is_attribute_present = rail.IfOperator(
            task_id="is_attribute_present",
            test="{{ result('get_specific_attribute_system_level') | length > 0 }}",
            yes_task='log_attribute_already_exist',
            no_task='create_new_draft',
        )

        log_attribute_already_exist = rail.WriteLogOperator(
            task_id='log_attribute_already_exist',
            message="Attribute Value Already Exist",
            properties={
                'Level': "System",
                'wbs': "na",
                'attributename': "{{dag_run.conf.attribute_name}}",
                'attributenumber': "{{dag_run.conf.attribute_number}}",
                'action': 'na',
                'status': "Exception",
                'recordcount': '1',
            }
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id="create_new_draft",
            endpoint="services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda dag_run: {
                "objectExtensionTagDefinitionUri": dag_run.conf['attribute_1_2_uri']
            },
        )

        update_attribute_name = rail.RepliconServiceOperator(
            task_id="update_attribute_name",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda dag_run: {
                "objectExtensionTagUri": rail.result('create_new_draft'),
                "name": dag_run.conf['attribute_name']
            },
        )

        update_attribute_code = rail.RepliconServiceOperator(
            task_id="update_attribute_code",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda dag_run: {
                "objectExtensionTagUri": rail.result('create_new_draft'),
                "code": dag_run.conf['attribute_value']
            },
        )

        enable_draft = rail.RepliconServiceOperator(
            task_id="enable_draft",
            endpoint="services/ObjectExtensionTagService1.svc/Enable",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft')
            },
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id="publish_draft",
            endpoint="services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft')
            },
        )

        log_success_adding_attribute = rail.WriteLogOperator(
            task_id='log_success_adding_attribute',
            message="Attribute Added Successfully",
            properties={
                'Level': "System",
                'wbs': "na",
                'attributename': "{{dag_run.conf.attribute_name}}",
                'attributenumber': "{{dag_run.conf.attribute_number}}",
                'action': 'Adding',
                'status': "Success",
                'recordcount': '1',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'Level': "System",
                'wbs': "na",
                'attributename': "{{dag_run.conf.attribute_name}}",
                'attributenumber': "{{dag_run.conf.attribute_number}}",
                'action': 'na',
                'status': "Error",
                'recordcount': '1',
            })

        get_specific_attribute_system_level >> is_attribute_present
        is_attribute_present >> rail.Label(
            "Yes") >> log_attribute_already_exist >> catch_and_log_errors
        is_attribute_present >> rail.Label("No") >> create_new_draft
        create_new_draft >> update_attribute_name >> update_attribute_code >> enable_draft >> publish_draft
        publish_draft >> log_success_adding_attribute
        log_success_adding_attribute >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
