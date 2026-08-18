from datetime import timedelta
import rail
from dxctechnology.gsap_task_import_project_fields.utils import request_payload
from dxctechnology.gsap_task_import_project_fields.utils import response_filter
from airflow.models import Variable

def create_child_sync_each_attribute_project_level(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_project_field_task_import_sync_each_child_wbs_gsap_task_child_{config.instance}',
        description=f'Sync Each GSAP Task At Project Level {config.instance}  ',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_sync_gsap_task_child_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "is_end_date_prior_to_start_date"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_end_date_prior_to_start_date',
            end_task="catch_errors",
        )

        is_end_date_prior_to_start_date = rail.IfOperator(
            task_id="is_end_date_prior_to_start_date",
            test=request_payload.is_end_date_before_start_date,
            yes_task='finish_end_date_prior_to_start_date',
            no_task='get_specific_attribute_uri_system_level',
        )

        finish_end_date_prior_to_start_date = rail.EmptyOperator(
            task_id='finish_end_date_prior_to_start_date',
        )

        get_specific_attribute_uri_system_level = rail.RepliconServiceOperator(
            task_id="get_specific_attribute_uri_system_level",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data=request_payload.get_specific_attribute_system_level_payload,
            response_filter=response_filter.map_get_attribute_system_level_project
        )

        is_system_attribute_present = rail.IfOperator(
            task_id="is_system_attribute_present",
            test="{{ result('get_specific_attribute_uri_system_level') | length > 0 }}",
            yes_task='get_specific_attribute_project_level',
            no_task='finish_failure_attribute_system',
        )

        finish_failure_attribute_system = rail.EmptyOperator(
            task_id='finish_failure_attribute_system',
        )

        get_specific_attribute_project_level = rail.RepliconServiceOperator(
            task_id="get_specific_attribute_project_level",
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfProjectDependentTimeEntryObjectExtensionTags",
            data=request_payload.get_specific_attribute_project_level,
            response_filter=response_filter.map_get_specific_attribute_project_level
        )

        is_attribute_present_project = rail.IfOperator(
            task_id="is_attribute_present_project",
            test="{{ result('get_specific_attribute_project_level') | length > 0 }}",
            yes_task='update_attribute_dates_project',
            no_task='add_attribute_dates_project',
        )

        update_attribute_dates_project = rail.RepliconServiceOperator(
            task_id="update_attribute_dates_project",
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.update_attribute_dates_project
        )

        finish_update_success_record = rail.EmptyOperator(
            task_id='finish_update_success_record',
        )

        add_attribute_dates_project = rail.RepliconServiceOperator(
            task_id="add_attribute_dates_project",
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.update_attribute_dates_project
        )

        finish_add_success_record = rail.EmptyOperator(
            task_id='finish_add_success_record',
        )

        catch_errors = rail.EmptyOperator(
            task_id='catch_errors',
            trigger_rule='one_failed'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_errors >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> is_end_date_prior_to_start_date

        is_end_date_prior_to_start_date >> rail.Label(
            "YES") >> finish_end_date_prior_to_start_date >> catch_errors
        is_end_date_prior_to_start_date >> rail.Label(
            "NO") >> get_specific_attribute_uri_system_level
        get_specific_attribute_uri_system_level >> is_system_attribute_present >> rail.Label(
            "YES") >> get_specific_attribute_project_level
        is_system_attribute_present >> rail.Label(
            "NO") >> finish_failure_attribute_system >> catch_errors
        get_specific_attribute_project_level >> is_attribute_present_project
        is_attribute_present_project >> rail.Label(
            "YES") >> update_attribute_dates_project >> finish_update_success_record >> catch_errors
        is_attribute_present_project >> rail.Label(
            "NO") >> add_attribute_dates_project >> finish_add_success_record >> catch_errors

    return dag


rail.for_each_instance(create_child_sync_each_attribute_project_level)
