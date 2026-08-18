from datetime import timedelta
import rail
from pwcglobal.project_import_ury_and_arg.utils import request_payload,response_filter,python_callable
from pwcglobal.project_import_ury_and_arg.task.process_project_manager import process_project_manager_task_group
from pwcglobal.project_import_ury_and_arg.task.process_co_project_manager import process_co_project_manager_task_group
from airflow.models import Variable

def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_projects,
        description='Pwc Process Each Project Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_projects,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
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

        get_project_data_from_query = rail.QueryCollectionOperator(
            task_id='get_project_data_from_query',
            query="""SELECT * from inputdata WHERE projectcode == :project_code AND NULLIF(taskname, '') IS NOT NULL""",
            query_params = {
                'project_code': '{{ dag_run.conf.projectcode }}'
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                        "code": "{{ dag_run.conf.projectcode }}",
                    }
                ]
            },
            data_handler=lambda response: response[0].get('projectDetails')
        )

        is_projectmanager_partyid_and_legalentity_present = rail.IfOperator(
            task_id = 'is_projectmanager_partyid_and_legalentity_present',
            test=lambda dag_run: bool(dag_run.conf['projectmanager_partyid'] and dag_run.conf['projectmanager_legalentityid']),
            yes_task= 'search_projectmanager_by_partyid_and_legal_entity',
            no_task= 'is_project_available_in_replicon'
        )

        process_project_manager_entry,  process_project_manager_exit= process_project_manager_task_group()

        is_project_available_in_replicon = rail.IfOperator(
            task_id = 'is_project_available_in_replicon',
            test=lambda: not request_payload.does_project_code_exist(),
            yes_task= 'create_project',
            no_task= 'update_project'
        )

        create_project = rail.RepliconServiceOperator(
            task_id="create_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_payload.create_or_update_project_payload(
                dag_run, config.project_belongs_to
            )
        )

        update_project = rail.RepliconServiceOperator(
            task_id="update_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_payload.create_or_update_project_payload(
                dag_run, config.project_belongs_to
            )
        )

        is_engagementpartner_partyid_and_legalentity_present = rail.IfOperator(
            task_id = 'is_engagementpartner_partyid_and_legalentity_present',
            test=lambda dag_run: bool(dag_run.conf['engagementpartner_partyid'] and dag_run.conf['engagementpartner_legalentity']),
            yes_task= 'search_engagementpartner_by_partyid_and_legal_entity',
            no_task= 'log_project_success'
        )

        process_co_project_manager_entry,  process_co_project_manager_exit= process_co_project_manager_task_group()

        log_project_success = rail.WriteLogOperator(
            task_id="log_project_success",
            log="{{result('create_project_log')}}",
            message="Project created/updated successfully",
            properties=lambda dag_run: {
                "projectcode": dag_run.conf['projectcode'],
                "projectname": dag_run.conf['projectname'],
                "clientcode": dag_run.conf['clientcode'],
                "taskcode": '',
                "taskname": '',
                "action": "Update" if request_payload.does_project_code_exist() else "Add",
                "status": request_payload.get_project_log_status(dag_run),
                "details": request_payload.get_project_log_details(dag_run),
            }
        )

        update_project_team_members = rail.RepliconServiceOperator(
            task_id='update_project_team_members',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data=lambda dag_run: {
                'projectUri': rail.result('update_project')['uri'] if request_payload.does_project_code_exist() else \
                    rail.result('create_project')['uri'],
                'resourceUri': [dag_run.conf['project_belongs_to']] if dag_run.conf['project_belongs_to'] else [],
                'projectTeamMemberAssignmentOptionUri': 'urn:replicon:project-team-member-assignment-option:assign'
            }
        )

        is_new_project = rail.IfOperator(
            task_id = 'is_new_project',
            test=lambda: not request_payload.does_project_code_exist(),
            yes_task= 'get_all_task_to_add_update',
            no_task= 'get_all_tasks_for_project'
        )

        get_all_tasks_for_project = rail.RepliconServiceOperator(
            task_id="get_all_tasks_for_project",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data={
                "parentUri": "{{result('update_project').uri}}"
            },
            data_handler=response_filter.format_project_task_details
        )

        get_all_task_to_add_update = rail.PythonOperator(
            task_id="get_all_task_to_add_update",
            python_callable=python_callable.get_task_to_add_update_skip
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
            data=lambda: request_payload.get_add_task_payload(config.project_belongs_to)
        )

        log_task_added_success_error = rail.WriteLogOperator(
            task_id="log_task_added_success_error",
            log="{{result('create_project_log')}}",
            message="{{ item.details }}",
            items=lambda: python_callable.map_task_success_error(
                "add_task", "add","tasks_to_add"),
            properties=lambda item: {
                "projectcode": item['projectcode'],
                "projectname": item['projectname'],
                "clientcode": item['clientcode'],
                "taskcode": item['taskcode'],
                "taskname": item['taskname'],
                'action': 'Add',
                "status": item['status'],
                "details": item['details'],
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
            items=lambda: python_callable.map_task_success_error(
                "update_task", "update","tasks_to_update"),
            properties=lambda dag_run, item: {
                "projectcode": dag_run.conf['projectcode'],
                "projectname": dag_run.conf['projectname'],
                "clientcode": dag_run.conf['clientcode'],
                "taskcode": item['taskcode'],
                "taskname": item['taskname'],
                'action': 'Update',
                "status": item['status'],
                "details": item['details'],
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
                "status": 'Skipped',
                "details": '{{ item.message }}',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{result('create_project_log')}}",
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties=lambda dag_run:{
                "projectcode": dag_run.conf['projectcode'],
                "projectname": dag_run.conf['projectname'],
                "clientcode": dag_run.conf['clientcode'],
                "taskcode": '',
                "taskname": '',
                "action": "Add",
                "status": "Error",
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_project_log

        create_project_log >> get_project_data_from_query >> get_project_details >> \
        is_projectmanager_partyid_and_legalentity_present >> rail.Label(
            "Yes") >> process_project_manager_entry
        process_project_manager_exit >> is_project_available_in_replicon
        is_projectmanager_partyid_and_legalentity_present >> rail.Label(
            "No") >> is_project_available_in_replicon >> rail.Label(
            'Yes') >> create_project >> is_engagementpartner_partyid_and_legalentity_present
        is_project_available_in_replicon >> rail.Label('No') >> update_project >> is_engagementpartner_partyid_and_legalentity_present
        is_engagementpartner_partyid_and_legalentity_present >> rail.Label(
            "Yes") >> process_co_project_manager_entry
        process_co_project_manager_exit >> log_project_success
        is_engagementpartner_partyid_and_legalentity_present >> rail.Label(
            "No") >> log_project_success >> update_project_team_members >> is_new_project >> rail.Label(
            "Yes") >> get_all_task_to_add_update >> has_tasks_to_add
        is_new_project >> rail.Label(
            "No") >> get_all_tasks_for_project >> get_all_task_to_add_update >> has_tasks_to_add >> rail.Label(
            "Yes") >> add_task >> log_task_added_success_error >> has_tasks_to_update
        has_tasks_to_add >> rail.Label(
            "No") >> has_tasks_to_update >> rail.Label(
            "Yes") >> update_task >> log_task_updated_success_error >> has_tasks_to_skip
        has_tasks_to_update >> rail.Label(
            "No") >> has_tasks_to_skip >> rail.Label(
            "Yes") >> log_task_skipped >> catch_and_log_errors
        has_tasks_to_skip >> rail.Label(
            "No") >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
