from datetime import timedelta
import rail
from dxctechnology.gsap_task_import_project_fields_v1.utils import request_payload
from dxctechnology.gsap_task_import_project_fields_v1.utils import response_filter
from dxctechnology.gsap_task_import_project_fields_v1.utils.python_callable_method import get_task_to_add_callable
from airflow.models import Variable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_child_wbs,
        description=f'Sync Child WBS GASP Task {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_wbs_dag_sync_gsap_task_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "query_gsap_task_records_for_wbs"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='query_gsap_task_records_for_wbs',
            end_task="finish_batch_task",
        )

        # ALL valid tasks for parent
        query_gsap_task_records_for_wbs = rail.QueryCollectionOperator(
            task_id="query_gsap_task_records_for_wbs",
            query="SELECT * FROM valid_input_records WHERE WBS = :WBS",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}"
            },
            name="feed_tasks_for_child_wbs"
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=request_payload.get_child_project_details,
            response_filter=response_filter.map_get_project_details
        )

        is_wbs_present = rail.IfOperator(
            task_id="is_wbs_present",
            test="{{ result('get_project_details') | length > 0}}",
            yes_task="get_all_assigned_gsap_task_for_project",
            no_task="finish_wbs_not_found",
        )

        get_all_assigned_gsap_task_for_project = rail.RepliconServiceOperator(
            task_id = "get_all_assigned_gsap_task_for_project",
            endpoint="/services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfEnabledProjectDependentTimeEntryObjectExtensionTags",
            data=request_payload.get_all_gsap_task_payload,
            data_handler=response_filter.get_all_assigned_gsap_task_for_project_filter
        )

        finish_wbs_not_found = rail.EmptyOperator(
            task_id='finish_wbs_not_found',
        )

        is_wbs_start_date_empty = rail.IfOperator(
            task_id="is_wbs_start_date_empty",
            test=lambda: bool(rail.result('get_project_details')[
                              0]['start_date_year']),
            yes_task="should_process_tasks",
            no_task="finish_wbs_start_date_empty",
        )

        finish_wbs_start_date_empty = rail.EmptyOperator(
            task_id='finish_wbs_start_date_empty',
        )

        should_process_tasks = rail.IfOperator(
            task_id = "should_process_tasks",
            test=lambda : (len(rail.result('get_project_details')) > 0) and (bool(rail.result('get_project_details')[
                              0]['start_date_year'])),
            yes_task= "dummy_is_wbs_in_progress",
            no_task="finish_batch_task"
        )

        dummy_is_wbs_in_progress = rail.EmptyOperator(
            task_id= "dummy_is_wbs_in_progress"
        )

        is_wbs_in_progress = rail.IfOperator(
            task_id="is_wbs_in_progress",
            test=lambda: rail.result('get_project_details')[
                0]['status'] == "In Progress",
            yes_task="get_task_add_update",
            no_task="finish_wbs_not_in_progress",
        )

        finish_wbs_not_in_progress = rail.EmptyOperator(
            task_id='finish_wbs_not_in_progress',
        )

        get_task_add_update = rail.PythonOperator(
            task_id = "get_task_add_update",
            python_callable=get_task_to_add_callable
        )

        has_task_to_update = rail.IfOperator(
            task_id = "has_task_to_update",
            test="{{ result('get_task_add_update', 'task_records_to_update') | load_json_artifact | length > 0}}",
            yes_task="update_tasks",
            no_task= "has_task_to_add"
        )

        update_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id = "update_tasks",
            items=lambda: rail.load_json_artifact(rail.result('get_task_add_update', 'task_records_to_update')),
            batch_size=config.PROJECT_DEPENDANT_OEF_ADD_LIMIT,
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.batch_update_gsap_task_payload,
            all_result_data_handler=lambda response: response_filter.combine_task_add_update_output(response, 'task_records_to_update')
        )

        has_task_to_add = rail.IfOperator(
            task_id = "has_task_to_add",
            test="{{ result('get_task_add_update') | load_json_artifact | length > 0}}",
            yes_task="disable_task_from_projects",
            no_task="finish_batch_task"
        )

        disable_task_from_projects = rail.RepliconServiceCallForEachItemOperator(
            task_id = "disable_task_from_projects",
            items=lambda: rail.load_json_artifact(rail.result('get_task_add_update', 'task_to_disable')),
            batch_size=config.PROJECT_DEPENDANT_OEF_ADD_LIMIT,
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.batch_disable_gsap_task_payload
        )

        add_task_to_project = rail.RepliconServiceCallForEachItemOperator(
            task_id = "add_task_to_project",
            items=lambda: rail.load_json_artifact(rail.result('get_task_add_update')),
            batch_size=config.PROJECT_DEPENDANT_OEF_ADD_LIMIT,
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.batch_update_gsap_task_payload,
            all_result_data_handler=response_filter.combine_task_add_update_output
        )

        finish_batch_task = rail.EmptyOperator(
            task_id = "finish_batch_task"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_fail_dag = rail.IfOperator(
            task_id = "can_fail_dag",
            test="{{ get_failed_upstream_task_ids() | is_truthy }}",
            yes_task="fail_dag"
        )

        fail_dag = rail.FailOperator(
            task_id = "fail_dag",
             message="{{ get_error_message() }}"
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> \
            finish_batch_task
        should_process_tasks >> rail.Label("Yes") >> dummy_is_wbs_in_progress >> is_wbs_in_progress
        can_run_batch_task >> rail.Label("No") >> query_gsap_task_records_for_wbs
        should_process_tasks >> rail.Label("End") >> finish_batch_task
        query_gsap_task_records_for_wbs >> get_project_details
        get_project_details >> is_wbs_present
        is_wbs_present >> rail.Label(
            "NO") >> finish_wbs_not_found >> should_process_tasks
        is_wbs_present >> rail.Label(
            "YES") >> get_all_assigned_gsap_task_for_project >> is_wbs_start_date_empty
        is_wbs_start_date_empty >> rail.Label("YES") >> should_process_tasks
        is_wbs_start_date_empty >> rail.Label(
            "NO") >> finish_wbs_start_date_empty >> should_process_tasks
        is_wbs_in_progress >> rail.Label(
            "NO") >> finish_wbs_not_in_progress >> rail.Label("End")>> finish_batch_task >> log_to_sumo >> can_fail_dag >> fail_dag
        is_wbs_in_progress >> rail.Label(
            "YES") >> get_task_add_update
        get_task_add_update  >> has_task_to_update
        has_task_to_update >> rail.Label("Yes") >> update_tasks >> has_task_to_add
        has_task_to_update >> rail.Label("No") >> has_task_to_add >> rail.Label("No") >> finish_batch_task
        has_task_to_add >> rail.Label("Yes") >> disable_task_from_projects \
            >> add_task_to_project >> finish_batch_task

    return dag


rail.for_each_instance(create_dag)
