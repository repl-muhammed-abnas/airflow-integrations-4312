from datetime import timedelta
import rail
from airflow.models import Variable
from unisys.resource_assignment.utils import request_payload, response_filter, custom_method


def create_assignment_processing_dags(config):
    """Create assignment processing child DAGs with batch execution support"""

    add_dags = []

    for idx in range(0, config.ASSIGNMENT_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.process_assignment_dag_id}{get_postfix}",
            description='Unisys Process Project Resources - Polaris Bulk API',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            # Batch task support
            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='get_assignment_data_from_query'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(days=config.execution_timeout_days),
                start_task='get_assignment_data_from_query',
                end_task='catch_and_log_errors',
            )

            # ========== Get ALL Assignment Data for Project ==========
            get_assignment_data_from_query = rail.QueryCollectionOperator(
                task_id='get_assignment_data_from_query',
                query="""SELECT * FROM valid_user_records WHERE
                    projectnumber == :project_number""",
                query_params={
                    'project_number': '{{ dag_run.conf.projectnumber }}'
                }
            )

            load_all_assignment_records = rail.PythonOperator(
                task_id="load_all_assignment_records",
                python_callable= custom_method.validate_and_consolidate_assignment_records
            )

            # ========== Get Project Details ==========
            get_project_details = rail.RepliconServiceOperator(
                task_id="get_project_details",
                endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
                data={
                    "projects": [
                        {
                            "code": "{{ dag_run.conf.projectnumber }}",
                        }
                    ]
                },
                data_handler=lambda response: response[0].get('projectDetails') if response and response[0] else None
            )

            is_project_available = rail.IfOperator(
                task_id='is_project_available',
                test='{{ result("get_project_details") is not none }}',
                yes_task='is_project_active',
                no_task='log_project_not_found'
            )

            is_project_active = rail.IfOperator(
                task_id='is_project_active',
                test='{{ result("get_project_details").status.displayText != "Completed" }}',
                yes_task='get_existing_resource_allocations',
                no_task='log_project_inactive'
            )

            log_project_not_found = rail.WriteLogOperator(
                task_id='log_project_not_found',
                log='{{ dag_run.conf.log }}',
                message='Project does not exist in Replicon',
                items= '{{ result("load_all_assignment_records") | to_json }}',
                severity='Exception',
                properties=request_payload.get_project_not_found_log_properties
            )

            log_project_inactive = rail.WriteLogOperator(
                task_id='log_project_inactive',
                log='{{ dag_run.conf.log }}',
                message='Project is not active in Replicon',
                items= '{{ result("load_all_assignment_records") | to_json }}',
                severity='Exception',
                properties=request_payload.get_project_inactive_log_properties
            )

            # ========== Get Existing Resource Allocations (GraphQL Polaris) ==========
            # Use GraphQL to get existing allocations with allocation IDs
            get_existing_resource_allocations = rail.RepliconServiceOperator(
                task_id="get_existing_resource_allocations",
                endpoint="graphql",
                app='polaris',
                data=request_payload.get_resource_allocations_graphql_query,
                data_handler=response_filter.extract_resource_allocations_from_graphql
            )

            # ========== Prepare Resource Processing List ==========
            prepare_resource_processing = rail.PythonOperator(
                task_id="prepare_resource_processing",
                python_callable=lambda: custom_method.prepare_resources_for_processing(
                    config.DATE_FORMAT_INPUT
                )
            )

            # ========== Check if there are resources to add ==========
            has_resources_to_add = rail.IfOperator(
                task_id='has_resources_to_add',
                test=lambda: rail.result('prepare_resource_processing')['resources_to_add'] and len(rail.result('prepare_resource_processing')['resources_to_add']) > 0,
                yes_task='create_new_resource_allocations',
                no_task='has_resources_to_update'
            )

            # ========== Create New Resource Allocations (GraphQL) ==========
            create_new_resource_allocations = rail.RepliconServiceCallForEachItemOperator(
                task_id="create_new_resource_allocations",
                items=lambda: rail.result('prepare_resource_processing')['resources_to_add'],
                endpoint="graphql",
                app='polaris',
                data=lambda item: request_payload.create_resource_allocation_mutation(item, config.DATE_FORMAT_INPUT)
            )

            # ========== Check if there are resources to update ==========
            has_resources_to_update = rail.IfOperator(
                task_id='has_resources_to_update',
                test=lambda: rail.result('prepare_resource_processing')['resources_to_update'] and len(rail.result('prepare_resource_processing')['resources_to_update']) > 0,
                yes_task='update_existing_resource_allocations',
                no_task='get_all_tasks_for_project'
            )

            # ========== Update Existing Resource Allocations (GraphQL) ==========
            update_existing_resource_allocations = rail.RepliconServiceCallForEachItemOperator(
                task_id="update_existing_resource_allocations",
                items=lambda: rail.result('prepare_resource_processing')['resources_to_update'],
                endpoint="graphql",
                app='polaris',
                data=lambda item: request_payload.update_resource_allocation_mutation(item, config.DATE_FORMAT_INPUT)
            )

            # ========== Get All Tasks and Assign ==========
            get_all_tasks_for_project = rail.RepliconServiceOperator(
                task_id="get_all_tasks_for_project",
                endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
                data={
                    "parentUri": "{{ result('get_project_details').uri }}"
                },
                data_handler=response_filter.format_project_task_details
            )

            has_tasks_under_project = rail.IfOperator(
                task_id='has_tasks_under_project',
                test=lambda: rail.result('get_all_tasks_for_project') and len(rail.result('get_all_tasks_for_project')) > 0,
                yes_task='assign_resources_to_all_tasks',
                no_task='log_completion'
            )

            # ========== Assign ALL Resources to ALL Tasks (True Bulk API) ==========
            # Use BulkUpdateResourceAssignments - assigns multiple resources to each task
            # More efficient than ForEach per resource
            assign_resources_to_all_tasks = rail.RepliconServiceCallForEachItemOperator(
                task_id='assign_resources_to_all_tasks',
                items=lambda: rail.result('get_all_tasks_for_project'),
                endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
                data=request_payload.bulk_assign_all_resources_to_task
            )

            log_completion = rail.WriteLogOperator(
                task_id="log_completion",
                log="{{ dag_run.conf.log }}",
                items= '{{ result("prepare_resource_processing").all_logs | to_json }}',
                message="Resource assignment completed successfully",
                severity="Success",
                properties=lambda: {
                    "workernumber": "{{ item.workernumber }}",
                    "projectnumber": "{{ item.projectnumber }}",
                    "action": "{{ item.action }}",
                    "status": "{{ item.status }}",
                    "details": "{{ item.details }}"
                }
            )

            # ========== Error Handling ==========
            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log="{{ dag_run.conf.log }}",
                items= '{{ result("load_all_assignment_records") | to_json }}',
                message='{{ get_error_message() }}',
                severity='Error',
                properties=request_payload.get_error_log_properties
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done',
            )

            # ========== Task Dependencies ==========
            can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> get_assignment_data_from_query

            # Load data and get project details
            get_assignment_data_from_query >> load_all_assignment_records >> get_project_details >> is_project_available

            # Project not found
            is_project_available >> rail.Label("No") >> log_project_not_found >> catch_and_log_errors

            # Project found, check if active
            is_project_available >> rail.Label("Yes") >> is_project_active

            # Project inactive
            is_project_active >> rail.Label("No") >> log_project_inactive >> catch_and_log_errors

            # Project active - get existing allocations and process
            is_project_active >> rail.Label("Yes") >> get_existing_resource_allocations >> prepare_resource_processing

            # Check and process resources to add
            prepare_resource_processing >> has_resources_to_add
            has_resources_to_add >> rail.Label("Yes") >> create_new_resource_allocations >> has_resources_to_update
            has_resources_to_add >> rail.Label("No") >> has_resources_to_update

            # Check and process resources to update
            has_resources_to_update >> rail.Label("Yes") >> update_existing_resource_allocations >> get_all_tasks_for_project
            has_resources_to_update >> rail.Label("No") >> get_all_tasks_for_project

            # Get tasks and assign resources
            get_all_tasks_for_project >> has_tasks_under_project

            # Assign to tasks
            has_tasks_under_project >> rail.Label("Yes") >> assign_resources_to_all_tasks >> log_completion >> catch_and_log_errors
            has_tasks_under_project >> rail.Label("No") >> log_completion

            catch_and_log_errors >> log_to_sumo

        add_dags.append(dag)

    return add_dags


rail.for_each_instance(create_assignment_processing_dags)
