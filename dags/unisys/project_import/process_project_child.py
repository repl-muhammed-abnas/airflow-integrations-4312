from datetime import timedelta
import rail
from airflow.models import Variable
from unisys.project_import.utils import request_payload, response_filter, custom_method


def create_project_processing_dags(config):
    """Create project processing child DAGs with batch execution support"""

    add_dags = []

    for idx in range(0, config.PROJECT_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.process_project_dag_id}{get_postfix}",
            description='Unisys Process Each Project Child - Phase 1',
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
                no_task='get_project_data_from_query'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(days=config.execution_timeout_days),
                start_task='get_project_data_from_query',
                end_task='catch_and_log_errors',
            )

            # ========== Get Project Data ==========
            get_project_data_from_query = rail.QueryCollectionOperator(
                task_id='get_project_data_from_query',
                query="""SELECT * FROM validdata WHERE projectnumber == :project_number""",
                query_params={
                    'project_number': '{{ dag_run.conf.projectnumber }}'
                }
            )

            load_project_data_from_query = rail.PythonOperator(
                task_id="load_project_data_from_query",
                python_callable=lambda: rail.load_all_records(rail.result("get_project_data_from_query"))[0]
            )

            # ========== Date Validation (Project Dates Only) ==========
            validate_dates = rail.PythonOperator(
                task_id="validate_dates",
                python_callable=lambda: custom_method.validate_project_dates_only(rail.result(
                    "load_project_data_from_query"), config.DATE_FORMAT_INPUT)
            )

            # Check if project dates are valid
            are_project_dates_valid = rail.IfOperator(
                task_id='are_project_dates_valid',
                test=lambda: rail.result("validate_dates")['is_valid'],
                yes_task='get_project_details',
                no_task='log_project_date_validation_error'
            )

            # Log project date validation error and skip processing
            log_project_date_validation_error = rail.WriteLogOperator(
                task_id="log_project_date_validation_error",
                log="{{ dag_run.conf.log }}",
                severity="Exception",
                message=lambda: "; ".join(rail.result("validate_dates")['errors']),
                properties=lambda dag_run: {
                    "projectnumber": dag_run.conf['projectnumber'],
                    "projectname": rail.result("load_project_data_from_query")['projectname'],
                    "taskcode": '',
                    "taskname": '',
                    "action": "Validation",
                    "status": "Exception",
                    "details": "; ".join(rail.result("validate_dates")['errors']),
                }
            )

            # ========== Get Existing Project from Replicon ==========
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
                task_id = 'is_project_available',
                test=lambda: bool(rail.result("get_project_details")),
                yes_task= 'update_project',
                no_task= 'create_project_in_replicon'
            )

            # ========== Create New Project (GraphQL Polaris) ==========
            create_project_in_replicon = rail.RepliconServiceOperator(
                task_id="create_project_in_replicon",
                endpoint="/graphql",
                app="polaris",
                data=request_payload.create_project_graphql,
                data_handler=response_filter.extract_project_uri_from_graphql
            )

            update_project_status = rail.RepliconServiceOperator(
                task_id="update_project_status",
                endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
                data=request_payload.get_project_status_payload
            )

            # ========== Remove "All Users" Timesheet Access ==========
            remove_all_users_timesheet_access = rail.RepliconServiceOperator(
                task_id="remove_all_users_timesheet_access",
                endpoint="/graphql",
                app="polaris",
                data=request_payload.remove_all_users_timesheet_access
            )

            # ========== Update Existing Project (REST API) ==========
            update_project = rail.RepliconServiceOperator(
                task_id="update_project",
                endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
                data=request_payload.create_project_with_tasks_payload
            )

            is_project_manager_is_present = rail.IfOperator(
                task_id = 'is_project_manager_is_present',
                test=lambda: bool(rail.result("load_project_data_from_query").get('projectmanager', '').strip()),
                yes_task= 'get_project_manager_in_replicon',
                no_task= 'log_project_success'
            )

            get_project_manager_in_replicon = rail.RepliconServiceOperator(
                task_id= 'get_project_manager_in_replicon',
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=lambda: {
                    "users": [
                        {
                            "employeeId": rail.result("load_project_data_from_query").get('projectmanager', ''),
                        }
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                },
                data_handler=lambda response: response[0].get('userDetails') if response and response[0].get('userDetails') else None
            )

            is_project_manager_available = rail.IfOperator(
                task_id = 'is_project_manager_available',
                test=lambda: bool(rail.result("get_project_manager_in_replicon")),
                yes_task= 'assign_missing_permissions',
                no_task= 'log_project_success'
            )

            assign_missing_permissions = rail.RepliconServiceOperator(
                task_id="assign_missing_permissions",
                endpoint="/services/ImportService1.svc/ApplyUserModifications3",
                data=request_payload.assign_pm_permissions_payload
            )

            assign_project_manager_to_project = rail.RepliconServiceOperator(
                task_id='assign_project_manager_to_project',
                endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
                data=lambda: {
                    "projectUri": request_payload.get_project_uri(),
                    "userUri": rail.result("get_project_manager_in_replicon")['uri']
                }
            )

            log_project_success = rail.WriteLogOperator(
                task_id="log_project_success",
                log="{{ dag_run.conf.log }}",
                message="Project synced successfully",
                properties=lambda dag_run: {
                    "projectnumber": dag_run.conf['projectnumber'],
                    "projectname": rail.result("load_project_data_from_query")['projectname'],
                    "taskcode": '',
                    "taskname": '',
                    "action": "Update" if request_payload.does_wbs_exist() else "Add",
                    "status": request_payload.get_log_message()['status'],
                    "details": request_payload.get_log_message()['message'],
                }
            )

            # ========== Determine if New or Existing Project ==========
            is_new_project = rail.IfOperator(
                task_id='is_new_project',
                test=lambda: not request_payload.does_wbs_exist(),
                yes_task='get_all_task_to_add_update',
                no_task='get_all_tasks_for_project'
            )

            # ========== Get Existing Tasks (for update flow) ==========
            get_all_tasks_for_project = rail.RepliconServiceOperator(
                task_id="get_all_tasks_for_project",
                endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
                data={
                    "parentUri": "{{ result('update_project').uri }}"
                },
                data_handler=response_filter.format_existing_tasks
            )

            # ========== Categorize Tasks (Add/Update/Skip) ==========
            get_all_task_to_add_update = rail.PythonOperator(
                task_id="get_all_task_to_add_update",
                python_callable=lambda: custom_method.get_task_to_add_update_skip(config.DATE_FORMAT_INPUT)
            )

            create_add_task_batches = rail.PythonOperator(
                task_id='create_add_task_batches',
                python_callable=lambda: custom_method.get_batched_tasks('add', 500)
            )

            has_tasks_to_add = rail.IfOperator(
                task_id='has_tasks_to_add',
                test='{{ result("create_add_task_batches") | is_truthy }}',
                yes_task='add_task_batches',
                no_task='create_update_task_batches'
            )

            add_task_batches = rail.RepliconServiceCallForEachItemOperator(
                task_id="add_task_batches",
                items='{{ result("create_add_task_batches") | to_json }}',
                endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
                data=request_payload.get_batched_add_task_payload
            )

            log_task_added = rail.WriteLogOperator(
                task_id="log_task_added",
                log="{{ dag_run.conf.log }}",
                message="{{ item.details }}",
                items=lambda: custom_method.map_task_success_error("add_task_batches", "add"),
                properties=lambda item: {
                    "projectnumber": item.get('projectnumber', ''),
                    "projectname": item.get('projectname', ''),
                    "taskcode": item.get('taskcode', ''),
                    "taskname": item.get('taskname', ''),
                    'action': 'Add',
                    "details": item.get('details', ''),
                    "status": item.get('status', '')
                }
            )

            create_update_task_batches = rail.PythonOperator(
                task_id='create_update_task_batches',
                python_callable=lambda: custom_method.get_batched_tasks('update', 500)
            )

            has_tasks_to_update = rail.IfOperator(
                task_id='has_tasks_to_update',
                test='{{ result("create_update_task_batches") | is_truthy }}',
                yes_task='update_task_batches',
                no_task='has_tasks_to_skip'
            )

            # Process each batch of 500 tasks sequentially
            update_task_batches = rail.RepliconServiceCallForEachItemOperator(
                task_id="update_task_batches",
                items='{{ result("create_update_task_batches") | to_json }}',
                endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
                data=request_payload.get_batched_update_task_payload
            )

            log_task_updated = rail.WriteLogOperator(
                task_id="log_task_updated",
                log="{{ dag_run.conf.log }}",
                message="{{ item.details }}",
                items=lambda: custom_method.map_task_success_error("update_task_batches", "update"),
                properties=lambda item: {
                    "projectnumber": item.get('projectnumber', ''),
                    "projectname": item.get('projectname', ''),
                    "taskcode": item.get('taskcode', ''),
                    "taskname": item.get('taskname', ''),
                    'action': 'Update',
                    "details": item.get('details', ''),
                    "status": item.get('status', '')
                }
            )

            # ========== Skip Tasks ==========
            has_tasks_to_skip = rail.IfOperator(
                task_id='has_tasks_to_skip',
                test='{{ result("get_all_task_to_add_update").skip | length > 0 }}',
                yes_task='log_task_skipped',
                no_task='catch_and_log_errors'
            )

            log_task_skipped = rail.WriteLogOperator(
                task_id="log_task_skipped",
                log="{{ dag_run.conf.log }}",
                severity="Exception",
                message="Task skipped",
                items='{{ result("get_all_task_to_add_update").skip | to_json }}',
                properties={
                    "projectnumber": '{{ item.task.projectnumber }}',
                    "projectname": '{{ item.task.projectname }}',
                    "taskcode": '{{ item.task.taskcode }}',
                    "taskname": '{{ item.task.taskname }}',
                    'action': '{{ item.action }}',
                    "details": '{{ item.message }}',
                    "status": '{{ item.status }}'
                }
            )

            # ========== Error Handling ==========
            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log="{{ dag_run.conf.log }}",
                message='{{ get_error_message() }}',
                severity= 'Error',
                properties=lambda dag_run:{
                    "projectnumber": dag_run.conf['projectnumber'],
                    "projectname": rail.result("load_project_data_from_query")['projectname'],
                    "taskcode": '',
                    "taskname": '',
                    "action": "Add",
                    "status": "Error",
                    'details': '{{ get_error_message() }}'
                }
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
            )

            # ========== Task Dependencies (Simple Flow) ==========
            can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> get_project_data_from_query

            get_project_data_from_query >> load_project_data_from_query >> validate_dates >> are_project_dates_valid

            # If project dates are valid, check if project exists
            are_project_dates_valid >> rail.Label('Yes') >> get_project_details >> is_project_available

            # If project dates are invalid, log error and skip to catch_and_log_errors
            are_project_dates_valid >> rail.Label('No') >> log_project_date_validation_error >> catch_and_log_errors

            # If project exists, update it (REST API)
            is_project_available >> rail.Label('Yes') >> update_project >> is_project_manager_is_present

            # If project doesn't exist, create it (GraphQL)
            is_project_available >> rail.Label('No') >> create_project_in_replicon >> update_project_status >> remove_all_users_timesheet_access >> is_project_manager_is_present

            # Project manager flow
            is_project_manager_is_present >> rail.Label('Yes') >> get_project_manager_in_replicon
            is_project_manager_is_present >> rail.Label('No') >> log_project_success

            get_project_manager_in_replicon >> is_project_manager_available
            is_project_manager_available >> rail.Label('Yes') >>  assign_missing_permissions >> assign_project_manager_to_project >> log_project_success
            is_project_manager_available >> rail.Label('No') >> log_project_success >> is_new_project

            # New project: categorize all tasks as "add"
            is_new_project >> rail.Label("Yes") >> get_all_task_to_add_update >> create_add_task_batches

            # Existing project: get existing tasks, then categorize
            is_new_project >> rail.Label("No") >> get_all_tasks_for_project >> get_all_task_to_add_update >> create_add_task_batches

            create_add_task_batches >> has_tasks_to_add
            has_tasks_to_add >> rail.Label("Yes") >> add_task_batches >> log_task_added >> create_update_task_batches
            has_tasks_to_add >> rail.Label("No") >> create_update_task_batches
            create_update_task_batches >> has_tasks_to_update

            has_tasks_to_update >> rail.Label("Yes") >> update_task_batches >> log_task_updated >> has_tasks_to_skip
            has_tasks_to_update >> rail.Label("No") >> has_tasks_to_skip

            has_tasks_to_skip >> rail.Label("Yes") >> log_task_skipped >> catch_and_log_errors
            has_tasks_to_skip >> rail.Label("No") >> catch_and_log_errors

            catch_and_log_errors >> log_to_sumo

        add_dags.append(dag)

    return add_dags


rail.for_each_instance(create_project_processing_dags)
