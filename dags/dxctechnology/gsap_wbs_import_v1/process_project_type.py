from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_wbs_import_v1.utils import request_payload


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_wbs_import_child_process_project_type_{config.instance}_v1',
        description='DXC_GSAP_WBS_Automation Process Project Type',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_project_type,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_project_type_available'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_project_type_available',
            end_task='catch_and_log_errors',
        )

        is_project_type_available = rail.IfOperator(
            task_id="is_project_type_available",
            test=request_payload.is_project_type_available,
            yes_task="finish",
            no_task="create_new_draft",
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id="create_new_draft",
            endpoint="/services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda dag_run: {
                "objectExtensionTagDefinitionUri": dag_run.conf['gsapprojecttypeuri']},
        )

        update_name = rail.RepliconServiceOperator(
            task_id="update_name",
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda dag_run: {
                "objectExtensionTagUri": rail.result('create_new_draft'),
                "name": dag_run.conf['projecttype']
            },
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id="publish_draft",
            endpoint="/services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                    "objectExtensionTagUri": rail.result('create_new_draft')
            }
        )

        enable_tag = rail.RepliconServiceOperator(
            task_id="enable_tag",
            endpoint="/services/ObjectExtensionTagService1.svc/Enable",
            data=lambda: {
                    "objectExtensionTagUri": rail.result('publish_draft')['uri']
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'projectname': '',
                'status': 'Error',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> is_project_type_available

        is_project_type_available >> rail.Label('Yes') >> finish
        is_project_type_available >> rail.Label('No') >> create_new_draft
        create_new_draft >> update_name >> publish_draft >> enable_tag >> finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
