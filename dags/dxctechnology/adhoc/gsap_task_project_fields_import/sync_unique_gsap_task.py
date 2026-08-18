from datetime import timedelta
import rail
from dxctechnology.adhoc.gsap_task_project_fields_import.utils import response_filter
from dxctechnology.adhoc.gsap_task_project_fields_import.utils import request_payload
from airflow.models import Variable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adhoc_dxctechnology_gsap_project_field_task_import_add_gsap_task_{config.instance}',
        description=f'Sync GSAP Task at System Level {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_sync_gsap_task_system_level,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "get_gsap_task"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_gsap_task',
            end_task="catch_and_log_errors",
        )

        get_gsap_task = rail.RepliconServiceOperator(
            task_id="get_gsap_task",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data=request_payload.get_specific_attribute_system_level_payload,
            response_filter=response_filter.map_get_specific_attribute_system_level
        )

        is_attribute_present = rail.IfOperator(
            task_id="is_attribute_present",
            test="{{ result('get_gsap_task') | length > 0 }}",
            yes_task='finish_attribute_already_exist',
            no_task='create_new_draft',
        )

        finish_attribute_already_exist = rail.EmptyOperator(
            task_id='finish_attribute_already_exist',
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id="create_new_draft",
            endpoint="services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda dag_run: {
                "objectExtensionTagDefinitionUri": dag_run.conf['gsap_task_uri']
            },
        )

        update_task_name = rail.RepliconServiceOperator(
            task_id="update_task_name",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda dag_run: {
                "objectExtensionTagUri": rail.result('create_new_draft'),
                "name": request_payload.get_task_name(dag_run)
            },
        )

        update_task_code = rail.RepliconServiceOperator(
            task_id="update_task_code",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda dag_run: {
                "objectExtensionTagUri": rail.result('create_new_draft'),
                "code": dag_run.conf['task_name']
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

        finish_created_success = rail.EmptyOperator(
            task_id = "finish"
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'Level': "System",
                'wbs': "NA",
                'task_name': "{{dag_run.conf.task_name}}",
                'task_code': "{{dag_run.conf.task_code}}",
                'action': 'NA',
                'details':'{{ get_error_message() }}',
                'status': "Error",
                'recordcount': '1',
            })

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> get_gsap_task

        get_gsap_task >> is_attribute_present
        is_attribute_present >> rail.Label(
            "Yes") >> finish_attribute_already_exist >> catch_and_log_errors
        is_attribute_present >> rail.Label("No") >> create_new_draft
        create_new_draft >> update_task_name >> update_task_code >> enable_draft >> publish_draft
        publish_draft >> finish_created_success
        finish_created_success >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
