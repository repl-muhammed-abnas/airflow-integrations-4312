from datetime import timedelta
import rail
from crl.project_import_v3.utils import request_payload,response_filter,custom_method
from airflow.models import Variable

def create_child_dag_wbs(config):

    add_dags = []

    for idx in range(0, config.PROJECT_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.process_project_dag_id}{get_postfix}",
            description='CRL Process Each Project Child',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='create_project_log'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='create_project_log',
                end_task='catch_and_log_errors',
            )

            create_project_log = rail.CreateLogOperator(
                task_id="create_project_log"
            )

            log_project_and_exception_log = rail.PythonOperator(
                task_id="log_project_and_exception_log",
                python_callable=lambda dag_run: {
                    "exception_log": dag_run.conf['exception_log'],
                    "project_log": rail.result("create_project_log")
                }
            )

            get_project_data_from_query = rail.QueryCollectionOperator(
                task_id='get_project_data_from_query',
                query="""SELECT * from validwbsdata WHERE projectname == :program_name""",
                query_params = {
                    'program_name': '{{ dag_run.conf.projectname }}'
                }
            )

            get_project_by_name = rail.RepliconServiceOperator(
                task_id="get_project_by_name",
                endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
                data={
                    "projects": [
                        {
                            "name": "{{ dag_run.conf.projectname }}",
                        }
                    ]
                },
                data_handler=lambda response: response[0].get('projectDetails') if response and response[0].get('projectDetails') else None
            )

            get_all_projects_with_same_code = rail.RepliconServiceOperator(
                task_id="get_all_projects_with_same_code",
                endpoint="/services/ProjectListService1.svc/GetData",
                data=request_payload.get_project_by_code,
                data_handler=lambda response, dag_run: list(filter(lambda item: item['projectcode'] == request_payload.get_project_data()[
                    'projectcode'] and item['projectname'] != dag_run.conf['projectname'],map(lambda i: {
                        "projectname": i["cells"][2]["textValue"],
                        "projecturi": i["cells"][0]["uri"],
                        "projectcode": i["cells"][1]["textValue"]
                }, response["rows"]))) if response.get("rows") else []
            )

            has_duplicate_projects_to_process = rail.IfOperator(
                task_id='has_duplicate_projects_to_process',
                test=lambda: len(rail.result('get_all_projects_with_same_code')) > 0,
                yes_task='trigger_duplicate_projects_processing',
                no_task='get_service_center_details_for_project'
            )

            trigger_duplicate_projects_processing = rail.TriggerDagRunForEachItemOperator(
                task_id='trigger_duplicate_projects_processing',
                trigger_dag_id=f'{config.process_duplicate_projects_dag_id}',
                items=lambda: rail.result('get_all_projects_with_same_code'),
                conf=lambda dag_run, item: {
                    'projecturi': item['projecturi'],
                    'projectname': item['projectname'],
                    'projectcode': item['projectcode'],
                    'project_data': request_payload.get_project_data(),
                    'project_log': rail.result('create_project_log'),
                    'exception_log': dag_run.conf['exception_log'],
                    'project_custom_fields': dag_run.conf['project_custom_fields'],
                    'task_custom_fields': dag_run.conf['task_custom_fields']
                }
            )

            get_service_center_details_for_project = rail.RepliconServiceOperator(
                task_id="get_service_center_details_for_project",
                endpoint="/services/ServiceCenterListService1.svc/GetData",
                data= request_payload.get_service_center_details_for_project,
                data_handler=response_filter.get_project_data_from_list_service
            )

            get_service_center_details_for_task = rail.RepliconServiceCallForEachItemOperator(
                task_id="get_service_center_details_for_task",
                endpoint="/services/ServiceCenterListService1.svc/GetData",
                items= '{{ result("get_project_data_from_query") }}',
                data= request_payload.get_service_center_details_for_task,
                data_handler=lambda response,item: response_filter.get_data_from_list_service(response, item)
            )

            get_company_code_uris = rail.PythonOperator(
                task_id = 'get_company_code_uris',
                python_callable= custom_method.get_company_code_details
            )

            create_project = rail.RepliconServiceOperator(
                task_id="create_project",
                endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
                data=request_payload.create_projectorapply_modifications
            )

            is_groups_available_in_replicon = rail.IfOperator(
                task_id = 'is_groups_available_in_replicon',
                test=lambda: not bool(request_payload.get_exception_log_message()),
                yes_task= 'log_project_success',
                no_task= 'log_project_exception'
            )

            log_project_exception = rail.WriteLogOperator(
                task_id="log_project_exception",
                log="{{result('create_project_log')}}",
                message="Project created successfully with Exception",
                properties=lambda dag_run: {
                    "projectcode": request_payload.get_project_data()['projectcode'],
                    "projectname": request_payload.get_project_data()['projectname'],
                    "clientcode": request_payload.get_project_data()['clientcode'],
                    "taskcode": '',
                    "taskname": '',
                    "action": "Update" if request_payload.does_wbs_exist() else "Add",
                    "Status": "Exception",
                    "details": "Project Updated Successfully," + str(request_payload.get_exception_log_message()) if
                    request_payload.does_wbs_exist() else "Project Added Successfully," + str(request_payload.get_exception_log_message()),
                }
            )

            log_project_success = rail.WriteLogOperator(
                task_id="log_project_success",
                log="{{result('create_project_log')}}",
                message="Project created successfully",
                properties=lambda dag_run: {
                    "projectcode": request_payload.get_project_data()['projectcode'],
                    "projectname": request_payload.get_project_data()['projectname'],
                    "clientcode": request_payload.get_project_data()['clientcode'],
                    "taskcode": '',
                    "taskname": '',
                    "action": "Update" if request_payload.does_wbs_exist() else "Add",
                    "Status": "Success",
                    "details": "Project Updated Successfully" if request_payload.does_wbs_exist() else "Project Added Successfully",
                }
            )

            is_business_area_present = rail.IfOperator(
                task_id = 'is_business_area_present',
                test=lambda: bool(rail.result("get_company_code_uris")),
                yes_task= 'bulk_update_project_team_members',
                no_task= 'is_new_project'
            )

            bulk_update_project_team_members = rail.RepliconServiceOperator(
                task_id='bulk_update_project_team_members',
                endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
                data=lambda: {
                    'projectUri': rail.result('create_project')['uri'],
                    'resourceUri': [item['uri'] for item in rail.result("get_company_code_uris")] if rail.result(
                        "get_company_code_uris") else [],
                    'projectTeamMemberAssignmentOptionUri': 'urn:replicon:project-team-member-assignment-option:assign'
                }
            )

            is_new_project = rail.IfOperator(
                task_id = 'is_new_project',
                test=lambda: not request_payload.does_wbs_exist(),
                yes_task= 'get_all_task_to_add_update',
                no_task= 'get_all_tasks_for_project'
            )

            get_all_tasks_for_project = rail.RepliconServiceOperator(
                task_id="get_all_tasks_for_project",
                endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
                data={
                    "parentUri": "{{result('create_project').uri}}"
                },
                data_handler=response_filter.format_project_task_details
            )

            get_all_task_to_add_update = rail.PythonOperator(
                task_id="get_all_task_to_add_update",
                python_callable=custom_method.get_task_to_add_update_skip
            )

            has_tasks_to_add = rail.IfOperator(
                task_id = 'has_tasks_to_add',
                test= '{{ result("get_all_task_to_add_update").tasks_to_add | is_truthy }}',
                yes_task= 'add_task',
                no_task= 'has_tasks_to_update'
            )

            add_task = rail.RepliconServiceOperator(
                task_id="add_task",
                endpoint="services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
                data=request_payload.get_batch_put_task_payload
            )

            log_task_added_success_error = rail.WriteLogOperator(
                task_id="log_task_added_success_error",
                log="{{result('create_project_log')}}",
                message="{{ item.details }}",
                items=lambda: custom_method.map_task_success_error(
                    "add_task", "add","tasks_to_add"),
                properties=lambda item: {
                    "projectcode": item['projectcode'],
                    "projectname": request_payload.get_project_data()['projectname'],
                    "clientcode": request_payload.get_project_data()['clientcode'],
                    "taskcode": item['taskcode'],
                    "taskname": item['taskname'],
                    'action': 'Add',
                    "details": item['details'],
                    "Status": item['status']
                }
            )

            has_tasks_to_update = rail.IfOperator(
                task_id = 'has_tasks_to_update',
                test= '{{ result("get_all_task_to_add_update").tasks_to_update | is_truthy }}',
                yes_task= 'update_task',
                no_task= 'has_tasks_to_skip'
            )

            update_task = rail.RepliconServiceOperator(
                task_id="update_task",
                endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
                data=request_payload.get_update_task_payload
            )

            log_task_updated_success_error = rail.WriteLogOperator(
                task_id="log_task_updated_success_error",
                log="{{result('create_project_log')}}",
                message="{{ item.details }}",
                items=lambda: custom_method.map_task_success_error(
                    "update_task", "update","tasks_to_update"),
                properties=lambda dag_run, item: {
                    "projectcode": request_payload.get_project_data()['projectcode'],
                    "projectname": request_payload.get_project_data()['projectname'],
                    "clientcode": request_payload.get_project_data()['clientcode'],
                    "taskcode": item['taskcode'],
                    "taskname": item['taskname'],
                    'action': 'Update',
                    "details": item['details'],
                    "Status": item['status']
                }
            )

            has_tasks_to_skip = rail.IfOperator(
                task_id = 'has_tasks_to_skip',
                test= '{{ result("get_all_task_to_add_update").task_to_skip | is_truthy }}',
                yes_task= 'log_task_skipped',
                no_task= 'catch_and_log_errors'
            )

            log_task_skipped = rail.WriteLogOperator(
                task_id="log_task_skipped",
                log="{{result('create_project_log')}}",
                severity="Exception",
                message="Skipped",
                items='{{ result("get_all_task_to_add_update").task_to_skip | to_json }}',
                properties={
                    "projectcode": '{{ item.task.projectcode }}',
                    "projectname": '{{ item.task.projectname }}',
                    "clientcode": '{{ item.task.clientcode }}',
                    "taskcode": '{{ item.task.taskcode }}',
                    "taskname": '{{ item.task.taskname }}',
                    'action': 'Update',
                    "details": '{{ item.message }}',
                    "Status": 'Skipped'
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log="{{result('create_project_log')}}",
                message='{{ get_error_message() }}',
                severity= 'Error',
                properties=lambda dag_run:{
                    "projectcode": request_payload.get_project_data()['projectcode'],
                    "projectname": request_payload.get_project_data()['projectname'],
                    "clientcode": request_payload.get_project_data()['clientcode'],
                    "taskcode": '',
                    "taskname": '',
                    "action": "Add",
                    "Status": "Error",
                    'details': '{{ get_error_message() }}'
                }
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done',
                extra_info= lambda dag_run:{
                    "projectcode": request_payload.get_project_data()['projectcode'],
                    "projectname": request_payload.get_project_data()['projectname'],
                    "clientcode": request_payload.get_project_data()['clientcode'],
                    'details': 'Project and Tasks are synced successfully.'
                }
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> create_project_log

            create_project_log >> log_project_and_exception_log >> get_project_data_from_query >> get_project_by_name >> get_all_projects_with_same_code
            
            get_all_projects_with_same_code >> has_duplicate_projects_to_process >> rail.Label(
                "Yes") >> trigger_duplicate_projects_processing >> get_service_center_details_for_project
            
            has_duplicate_projects_to_process >> rail.Label(
                "No") >> get_service_center_details_for_project
            
            get_service_center_details_for_project >> get_service_center_details_for_task >> get_company_code_uris >>\
                    create_project >> is_groups_available_in_replicon

            is_groups_available_in_replicon >> rail.Label(
                "Yes") >> log_project_success >> is_business_area_present

            is_groups_available_in_replicon >> rail.Label(
                "No") >> log_project_exception >> is_business_area_present

            is_business_area_present >> rail.Label(
                "Yes") >> bulk_update_project_team_members >> is_new_project

            is_business_area_present >> rail.Label(
                "No") >> is_new_project

            is_new_project >> rail.Label(
                "Yes") >> get_all_task_to_add_update

            is_new_project >> rail.Label(
                "No") >> get_all_tasks_for_project >> get_all_task_to_add_update >> has_tasks_to_add

            has_tasks_to_add >> rail.Label(
                "Yes") >> add_task >> log_task_added_success_error >> has_tasks_to_update

            has_tasks_to_add >> rail.Label(
                "No") >> has_tasks_to_update

            has_tasks_to_update >> rail.Label(
                "Yes") >> update_task >> log_task_updated_success_error >> has_tasks_to_skip

            has_tasks_to_update >> rail.Label(
                "No") >> has_tasks_to_skip

            has_tasks_to_skip >> rail.Label(
                "Yes") >> log_task_skipped >> catch_and_log_errors

            has_tasks_to_skip >> rail.Label(
                "No") >> catch_and_log_errors >> log_to_sumo

        add_dags.append(dag)

    return add_dags

rail.for_each_instance(create_child_dag_wbs)
