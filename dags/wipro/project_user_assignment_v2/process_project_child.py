from datetime import timedelta
import rail
from wipro.project_user_assignment_v2.utils import request_payload, response_filter, custom_methods

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_project_dag_id,
        description='Wipro Process Each Project Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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

        can_process_project = rail.IfOperator(
            task_id = 'can_process_project',
            test= '{{ dag_run.conf.can_process_project == "No" }}',
            yes_task= 'finish',
            no_task= 'get_project_data_from_query'
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        get_project_data_from_query = rail.QueryCollectionOperator(
            task_id='get_project_data_from_query',
            query="""SELECT * from final_collection WHERE projectcode == :program_code""",
            query_params = {
                'program_code': '{{ dag_run.conf.projectcode }}'
            }
        )

        load_project_data_from_query = rail.PythonOperator(
            task_id="load_project_data_from_query",
            python_callable=custom_methods.load_project_data_from_query_callable
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                        "code": "{{ dag_run.conf.projectcode }}"
                    }
                ]
            },
            data_handler=lambda response: response[0].get('projectDetails')
        )

        create_project = rail.RepliconServiceOperator(
            task_id="create_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.create_projectorapply_modifications
        )

        is_project_manager_is_present = rail.IfOperator(
            task_id = 'is_project_manager_is_present',
            test=lambda: bool(rail.result("load_project_data_from_query")['pm_empid']),
            yes_task= 'get_project_manager_in_replicon',
            no_task= 'log_project_success'
        )

        get_project_manager_in_replicon = rail.RepliconServiceOperator(
            task_id= 'get_project_manager_in_replicon',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "loginName": '{{ result("load_project_data_from_query").pm_loginname }}' + '@wipro.com',
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        is_project_manager_available = rail.IfOperator(
            task_id = 'is_project_manager_available',
            test=lambda: bool(rail.result("get_project_manager_in_replicon")),
            yes_task= 'get_project_manager_permission_set',
            no_task= 'check_user_details_from_payload'
        )

        def check_user_fields():
            user_data = rail.result("load_project_data_from_query")
            if not user_data['pm_name'] or not user_data['pm_loginname'] or not user_data['pm_email'] or not user_data['pm_empid']:
                return True
            return False

        check_user_details_from_payload = rail.IfOperator(
            task_id = 'check_user_details_from_payload',
            test= check_user_fields,
            yes_task= 'log_user_skipped',
            no_task= 'create_user'
        )

        log_user_skipped = rail.EmptyOperator(
            task_id = 'log_user_skipped'
        )

        create_user = rail.RepliconServiceOperator(
            task_id= "create_user",
            endpoint="/services/importService1.svc/PutUser2",
            data=request_payload.get_create_user_payload
        )

        put_product_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "productUris": config.license_uris
            }
        )

        get_project_manager_permission_set = rail.RepliconServiceOperator(
            task_id= "get_project_manager_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler= lambda resp: rail.find_first_by_attr_and_get_attr(
                resp,'displayText','Project Manager','uri')
        )

        assign_permission_set = rail.RepliconServiceOperator(
            task_id= "assign_permission_set",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: {
                "userUri": rail.result("get_project_manager_in_replicon")[0]['userDetails']['uri'],
                "permissionSetUri": rail.result("get_project_manager_permission_set")
            }
        )

        assign_project_manager_to_project = rail.RepliconServiceOperator(
            task_id='assign_project_manager_to_project',
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: {
                "projectUri": rail.result('create_project')['uri'],
                "userUri": rail.result('create_user')['uri'] if rail.result("create_user") else rail.result(
                    "get_project_manager_in_replicon")[0]['userDetails']['uri']
            }
        )

        allocate_users_to_project = rail.RepliconServiceCallForEachItemOperator(
            task_id='allocate_users_to_project',
            endpoint="/services/ProjectService1.svc/AssignResourceToProject",
            items= '{{ result("get_project_data_from_query") }}',
            data=lambda item: {
                "projectUri": rail.result('create_project')['uri'],
                "resourceUri": item['user_uri'],
            }
        )

        log_project_success = rail.WriteLogOperator(
            task_id="log_project_success",
            log="{{result('create_project_log')}}",
            message="Project synced successfully",
            properties=lambda dag_run: {
                "employee_id" : rail.result("load_project_data_from_query")['empid'],
                "projectcode": dag_run.conf['projectcode'],
                "projectname": rail.result("load_project_data_from_query")['projectname'],
                "taskcode": '',
                "taskname": '',
                "action": "Update" if request_payload.does_wbs_exist() else "Add",
                "Status": "Success",
                "details": request_payload.get_log_message()
            }
        )

        is_new_project = rail.IfOperator(
            task_id = 'is_new_project',
            test=lambda: not request_payload.does_wbs_exist(),
            yes_task= 'get_distinct_users',
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

        get_distinct_users = rail.QueryCollectionOperator(
            task_id='get_distinct_users',
            query="""SELECT DISTINCT user_uri from get_project_data_from_query """,
        )

        get_resource_start_dates = rail.RepliconServiceCallForEachItemOperator(
            task_id = 'get_resource_start_dates',
            items= '{{ result("get_distinct_users") }}',
            endpoint="/services/ResourceService1.svc/GetResourceTaskAllocationDetailsForProjects",
            data={
                "resourceUri": "{{ item.user_uri }}",
                "projectUris": [
                    "{{ result('create_project').uri }}"
                ]
            },
            data_handler=response_filter.get_resource_start_date,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_all_task_to_add_update = rail.PythonOperator(
            task_id="get_all_task_to_add_update",
            python_callable=custom_methods.get_task_to_add_update_skip
        )

        has_tasks_to_disable = rail.IfOperator(
            task_id = 'has_tasks_to_disable',
            test= '{{ result("get_all_task_to_add_update").tasks_to_disable | is_truthy }}',
            yes_task= 'disable_task',
            no_task= 'has_tasks_to_add'
        )

        disable_task = rail.RepliconServiceOperator(
            task_id="disable_task",
            endpoint="services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            data=request_payload.get_batch_disable_task_payload
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
            items=lambda: custom_methods.map_task_success_error(
                add_task.task_id, "add"),
            properties=lambda item: {
                "employee_id" : item['empid'],
                "projectcode": item['projectcode'],
                "projectname": rail.result("load_project_data_from_query")['projectname'],
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
            yes_task= 'update_task'
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
            items=lambda: custom_methods.map_task_success_error(
                update_task.task_id, "update"),
            properties=lambda  item: {
                "employee_id" : item['empid'],
                "projectcode": item['projectcode'],
                "projectname": rail.result("load_project_data_from_query")['projectname'],
                "taskcode": item['taskcode'],
                "taskname": item['taskname'],
                'action': 'Update',
                "details": item['details'],
                "Status": item['status']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{result('create_project_log')}}",
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties=lambda dag_run:{
                "employee_id" : rail.result("load_project_data_from_query")['empid'],
                "projectcode": dag_run.conf['projectcode'],
                "projectname": rail.result("load_project_data_from_query")['projectname'],
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
        )

        create_project_log >> log_project_and_exception_log >> can_process_project

        can_process_project >> rail.Label(
            "Yes") >> finish

        can_process_project >> rail.Label(
            "No") >> get_project_data_from_query

        get_project_data_from_query >> load_project_data_from_query >>\
            get_project_details >> create_project >> is_project_manager_is_present

        is_project_manager_is_present >> rail.Label(
            "Yes") >> get_project_manager_in_replicon >> is_project_manager_available

        is_project_manager_is_present >> rail.Label(
            "No") >> log_project_success

        is_project_manager_available >> rail.Label(
            "Yes") >> get_project_manager_permission_set >> assign_permission_set >> assign_project_manager_to_project >> \
                log_project_success

        is_project_manager_available >> rail.Label(
            "No") >> check_user_details_from_payload

        check_user_details_from_payload >> rail.Label(
            "Yes") >> log_user_skipped >> log_project_success

        check_user_details_from_payload >> rail.Label(
            "No") >> create_user >> put_product_assignments_for_user >> assign_project_manager_to_project

        log_project_success >> allocate_users_to_project >> is_new_project >> rail.Label(
            "Yes") >> get_distinct_users >> get_resource_start_dates >> get_all_task_to_add_update >> has_tasks_to_disable

        is_new_project >> rail.Label(
            "No") >> get_all_tasks_for_project >> get_distinct_users

        has_tasks_to_disable >> rail.Label(
            "Yes") >> disable_task >> has_tasks_to_add

        has_tasks_to_disable >> rail.Label(
            "No") >> has_tasks_to_add

        has_tasks_to_add >> rail.Label(
            "Yes") >> add_task >> log_task_added_success_error >> has_tasks_to_update

        has_tasks_to_add >> rail.Label(
            "No") >> has_tasks_to_update

        has_tasks_to_update >> rail.Label(
            "Yes") >> update_task >> log_task_updated_success_error >> catch_and_log_errors

        catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag_wbs)
