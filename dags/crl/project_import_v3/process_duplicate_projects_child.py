from datetime import timedelta
import rail
from crl.project_import_v3.utils import request_payload, response_filter, custom_method
from airflow.models import Variable

def create_child_dag_duplicate_projects(config):

    with rail.create_airflow_dag(
        dag_id=config.process_duplicate_projects_dag_id,
        description='CRL Process Duplicate Projects with Same Code',
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
            no_task='get_project_data_from_query'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_project_data_from_query',
            end_task='catch_and_log_errors',
        )

        get_project_data_from_query = rail.QueryCollectionOperator(
            task_id='get_project_data_from_query',
            query="""SELECT * from validwbsdata WHERE projectname == :program_name""",
            query_params = {
                'program_name': '{{ dag_run.conf.project_data.projectname }}'
            },
            name='duplicateprojectdata'
        )

        get_service_center_details_for_project = rail.RepliconServiceOperator(
            task_id="get_service_center_details_for_project",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data= request_payload.get_service_center_details_for_project_duplicate,
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

        update_duplicate_project = rail.RepliconServiceOperator(
            task_id="update_duplicate_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.update_duplicate_project_full_payload
        )

        is_business_area_present = rail.IfOperator(
            task_id = 'is_business_area_present',
            test=lambda: bool(rail.result("get_company_code_uris")),
            yes_task= 'bulk_update_project_team_members',
            no_task= 'get_all_tasks_for_duplicate_project'
        )

        bulk_update_project_team_members = rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data=lambda: {
                'projectUri': rail.result('update_duplicate_project')['uri'],
                'resourceUri': [item['uri'] for item in rail.result("get_company_code_uris")] if rail.result(
                    "get_company_code_uris") else [],
                'projectTeamMemberAssignmentOptionUri': 'urn:replicon:project-team-member-assignment-option:assign'
            }
        )

        get_all_tasks_for_duplicate_project = rail.RepliconServiceOperator(
            task_id="get_all_tasks_for_duplicate_project",
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data=lambda dag_run: {
                "parentUri": dag_run.conf['projecturi']
            },
            data_handler=response_filter.format_project_task_details
        )

        get_all_task_to_add_update = rail.PythonOperator(
            task_id="get_all_task_to_add_update",
            python_callable=custom_method.get_task_to_add_update_skip_for_duplicate
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
            data=request_payload.get_batch_put_task_payload_for_duplicate
        )

        has_tasks_to_update = rail.IfOperator(
            task_id = 'has_tasks_to_update',
            test= '{{ result("get_all_task_to_add_update").tasks_to_update | is_truthy }}',
            yes_task= 'update_task',
            no_task= 'catch_and_log_errors'
        )

        update_task = rail.RepliconServiceOperator(
            task_id="update_task",
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            data=request_payload.get_update_task_payload_for_duplicate
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.project_log}}",
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties=lambda dag_run:{
                "projectcode": dag_run.conf['project_data']['projectcode'],
                "projectname": dag_run.conf['projectname'],
                "clientcode": dag_run.conf['project_data']['clientcode'],
                "taskcode": '',
                "taskname": '',
                "action": "Update",
                "Status": "Error",
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_project_data_from_query

        get_project_data_from_query >> get_service_center_details_for_project >> get_service_center_details_for_task >>\
            get_company_code_uris >> update_duplicate_project

        update_duplicate_project >> is_business_area_present >> rail.Label(
            "Yes") >> bulk_update_project_team_members >> get_all_tasks_for_duplicate_project
        
        is_business_area_present >> rail.Label(
            "No") >> get_all_tasks_for_duplicate_project
        
        get_all_tasks_for_duplicate_project >> get_all_task_to_add_update >> has_tasks_to_add

        has_tasks_to_add >> rail.Label(
            "Yes") >> add_task >> has_tasks_to_update

        has_tasks_to_add >> rail.Label(
            "No") >> has_tasks_to_update

        has_tasks_to_update >> rail.Label(
            "Yes") >> update_task >> catch_and_log_errors

        has_tasks_to_update >> rail.Label(
            "No") >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag_duplicate_projects)
