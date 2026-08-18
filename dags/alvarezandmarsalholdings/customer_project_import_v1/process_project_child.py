from datetime import timedelta
import rail
from alvarezandmarsalholdings.customer_project_import_v1.utils import request_payload,response_filter,python_callable
from alvarezandmarsalholdings.customer_project_import_v1.task.process_project_manager import process_project_manager_task_group
from alvarezandmarsalholdings.customer_project_import_v1.task.add_update_tasks import get_task_added_or_updated
from alvarezandmarsalholdings.customer_project_import_v1.task.assign_unassign_resource import assign_unassign_resource
from airflow.models import Variable

def create_child_dag(config):
    add_dags = []

    for idx in range(0, config.PROJECT_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.process_projects}{get_postfix}",
            description=f'{config.company_key} Customer Project Import - Process Each Project Child',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_projects,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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

            has_mandatory_fields = rail.IfOperator(
                task_id='has_mandatory_fields',
                test=request_payload.get_all_mandatory_check_projects,
                yes_task="get_project_details",
                no_task="log_mandatory_project_fields_not_present"
            )

            log_mandatory_project_fields_not_present = rail.WriteLogOperator(
                task_id='log_mandatory_project_fields_not_present',
                log='{{ result("create_project_log") }}',
                message="Missing mandatory fields",
                severity='Exception',
                properties=lambda dag_run: {
                    'projectcode': dag_run.conf['ProjectID'],
                    'projectname': dag_run.conf['ProjectName'],
                    'taskcode': '',
                    'taskname': '',
                    'action': 'Validation',
                    'status': 'Exception',
                    "details": request_payload.get_exception_message(dag_run, request_payload.MANDATORY_FIELDS['project_fields']),
                }
            )

            get_project_details = rail.RepliconServiceOperator(
                task_id="get_project_details",
                endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
                data={
                    "projects": [
                        {
                            "code": "{{ dag_run.conf.ProjectID }}",
                        }
                    ]
                },
                data_handler=lambda response: response[0].get('projectDetails')
            )

            process_project_manager = rail.EmptyOperator(
                task_id='process_project_manager'
            )

            process_project_manager_entry,  process_project_manager_exit= process_project_manager_task_group(config)

            is_project_not_available_in_replicon = rail.IfOperator(
                task_id = 'is_project_not_available_in_replicon',
                test=lambda: not request_payload.does_project_code_exist(),
                yes_task= 'create_project',
                no_task= 'update_project'
            )

            create_project = rail.RepliconServiceOperator(
                task_id="create_project",
                endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
                data=lambda dag_run: request_payload.create_or_update_project_payload(
                    dag_run, config
                )
            )

            update_project = rail.RepliconServiceOperator(
                task_id="update_project",
                endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
                data=lambda dag_run: request_payload.create_or_update_project_payload(
                    dag_run, config
                )
            )

            log_project_success = rail.WriteLogOperator(
                task_id="log_project_success",
                log="{{result('create_project_log')}}",
                message="Project created/updated successfully",
                properties=lambda dag_run: {
                    'projectcode': dag_run.conf['ProjectID'],
                    'projectname': dag_run.conf['ProjectName'],
                    'taskcode': '',
                    'taskname': '',
                    "action": "Update" if request_payload.does_project_code_exist() else "Add",
                    "status": request_payload.get_project_log_details(dag_run)['status'],
                    "details": request_payload.get_project_log_details(dag_run)['message'],
                }
            )

            format_payload_tasks = rail.PythonOperator(
                task_id="format_payload_tasks",
                python_callable=lambda dag_run: python_callable.get_formatted_payload_tasks(dag_run, config.instance)
            )
            
            has_billing_resp_users = rail.IfOperator(
                task_id='has_billing_resp_users',
                test=lambda: bool(rail.result('format_payload_tasks')['billing_responsibles']),
                yes_task="get_billing_responsible_users_data",
                no_task="has_task_mandatory_fields"
            )

            get_billing_responsible_users_data = rail.RepliconServiceOperator(
                task_id='get_billing_responsible_users_data',
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=request_payload.get_billing_responsible_users_payload,
                data_handler=response_filter.get_user_data_from_list
            )
            
            has_task_mandatory_fields = rail.IfOperator(
                task_id='has_task_mandatory_fields',
                test=lambda: not bool(rail.result('format_payload_tasks')['missing_mandatory_fields']),
                yes_task="process_task_level1",
                no_task="log_mandatory_task_fields_not_present"
            )

            log_mandatory_task_fields_not_present = rail.WriteLogOperator(
                task_id='log_mandatory_task_fields_not_present',
                log='{{ result("create_project_log") }}',
                items= "{{ result('format_payload_tasks').missing_mandatory_fields }}",
                message="Missing Mandatory Task Fields",
                severity='Exception',
                properties=lambda item, dag_run: {
                    'projectcode': dag_run.conf['ProjectID'],
                    'projectname': dag_run.conf['ProjectName'],
                    'taskcode': item['taskcode'],
                    'taskname': item['taskname'],
                    'action': 'Validation',
                    'status': 'Exception',
                    "details": item['message'],
                }
            )

            process_task_level1 = rail.EmptyOperator(
                task_id='process_task_level1'
            )

            process_task_level1_entry,  process_task_level1_exit= get_task_added_or_updated(
                'group_task_level1', 1, 
            )

            process_task_level2 = rail.EmptyOperator(
                task_id='process_task_level2'
            )

            process_task_level2_entry,  process_task_level2_exit= get_task_added_or_updated(
                'group_task_level2', 2, 
            )

            project_team_member_uris = rail.PythonOperator(
                task_id="project_team_member_uris",
                python_callable=python_callable.get_project_team_members_uris
            )

            update_project_team_members = rail.RepliconServiceOperator(
                task_id='update_project_team_members',
                endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
                data=lambda: {
                    'projectUri': rail.result('update_project')['uri'] if request_payload.does_project_code_exist() else \
                        rail.result('create_project')['uri'],
                    'resourceUri': rail.result('project_team_member_uris')['resource_uris'],
                    'projectTeamMemberAssignmentOptionUri': 'urn:replicon:project-team-member-assignment-option:assign'
                }
            )
            
            has_billing_responsible_users = rail.IfOperator(
                task_id='has_billing_responsible_users',
                test=lambda: bool(rail.result('format_payload_tasks')['billing_responsibles']),
                yes_task="get_permissions_to_assign",
                no_task="has_invalid_team_member_uris"
            )

            get_permissions_to_assign = rail.PythonOperator(
                task_id="get_permissions_to_assign",
                python_callable=lambda dag_run: python_callable.get_permissions_to_assign(dag_run, config)
            )

            add_missing_permissions = rail.RepliconServiceCallForEachItemOperator(
                task_id='add_missing_permissions',
                endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
                items=lambda: rail.result('get_permissions_to_assign')['permissions_to_add'],
                execution_timeout=timedelta(days=config.execution_timeout_days),
                data=lambda item: item
            )

            has_invalid_records_for_permission = rail.IfOperator(
                task_id='has_invalid_records_for_permission',
                test=lambda: bool(rail.result('get_permissions_to_assign')['log_details']),
                yes_task="log_invalid_records_for_permission",
                no_task="has_invalid_team_member_uris"
            )

            log_invalid_records_for_permission = rail.WriteLogOperator(
                task_id='log_invalid_records_for_permission',
                log='{{ result("create_project_log") }}',
                items=lambda: rail.result('get_permissions_to_assign')['log_details'],
                message="Missing billing responsible users",
                severity='Exception',
                properties=lambda dag_run, item: {
                    'projectcode': dag_run.conf['ProjectID'],
                    'projectname': dag_run.conf['ProjectName'],
                    'taskcode': item['task_code'],
                    'taskname': item['task_name'],
                    'action': 'Validation',
                    'status': 'Exception',
                    "details": item['message'],
                }
            )

            has_invalid_team_member_uris = rail.IfOperator(
                task_id='has_invalid_team_member_uris',
                test=lambda: bool(rail.result('project_team_member_uris')['log_messages']),
                yes_task="log_invalid_resources",
                no_task="process_tasks_resource"
            )

            log_invalid_resources = rail.WriteLogOperator(
                task_id='log_invalid_resources',
                log='{{ result("create_project_log") }}',
                items=lambda: rail.result('project_team_member_uris')['log_messages'],
                message="Missing Mandatory Task Fields",
                severity='Exception',
                properties=lambda dag_run, item: {
                    'projectcode': dag_run.conf['ProjectID'],
                    'projectname': dag_run.conf['ProjectName'],
                    'taskcode': item['task_code'],
                    'taskname': item['task_name'],
                    'action': 'Validation',
                    'status': 'Exception',
                    "details": item['message'],
                }
            )

            process_tasks_resource = rail.EmptyOperator(
                task_id='process_tasks_resource'
            )

            process_tasks_resource_entry,  process_tasks_resource_exit= assign_unassign_resource()

            finish = rail.EmptyOperator(
                task_id="finish"
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log="{{result('create_project_log')}}",
                message='{{ get_error_message() }}',
                severity= 'Error',
                properties=lambda dag_run:{
                    'projectcode': dag_run.conf['ProjectID'],
                    'projectname': dag_run.conf['ProjectName'],
                    'taskcode': '',
                    'taskname': '',
                    "action": "Update" if request_payload.does_project_code_exist() else "Add",
                    "status": "Error",
                    'details': '{{ get_error_message() }}'
                }
            )

            batch_task >> create_project_log
            batch_task >> catch_and_log_errors

            create_project_log >> \
            has_mandatory_fields >> rail.Label(
                "Yes") >> get_project_details >> process_project_manager
            has_mandatory_fields >> rail.Label(
                "No") >> log_mandatory_project_fields_not_present >> finish
            process_project_manager >> process_project_manager_entry
            process_project_manager_exit >> is_project_not_available_in_replicon
            is_project_not_available_in_replicon >> rail.Label(
                'Yes') >> create_project >> log_project_success
            is_project_not_available_in_replicon >> rail.Label('No') >> update_project >> log_project_success
            log_project_success >> format_payload_tasks >> has_billing_resp_users >> rail.Label(
                "Yes") >> get_billing_responsible_users_data >> has_task_mandatory_fields
            has_billing_resp_users >> rail.Label(
                "No") >> has_task_mandatory_fields
            has_task_mandatory_fields >> rail.Label(
                "Yes") >> process_task_level1
            has_task_mandatory_fields >> rail.Label("No") >> log_mandatory_task_fields_not_present >> process_task_level1
            process_task_level1 >> process_task_level1_entry
            process_task_level1_exit >> process_task_level2 >> process_task_level2_entry
            process_task_level2_exit >> project_team_member_uris >> update_project_team_members >> has_billing_responsible_users >> rail.Label(
                "Yes") >> get_permissions_to_assign >> add_missing_permissions >> \
                has_invalid_records_for_permission >> rail.Label("Yes") >> log_invalid_records_for_permission >> has_invalid_team_member_uris
            has_invalid_records_for_permission >> rail.Label("No") >> has_invalid_team_member_uris
            has_billing_responsible_users >> rail.Label(
                "No") >> has_invalid_team_member_uris
            has_invalid_team_member_uris >> rail.Label(
                "Yes") >> log_invalid_resources >> process_tasks_resource
            has_invalid_team_member_uris >> rail.Label("No") >> process_tasks_resource >> process_tasks_resource_entry
            process_tasks_resource_exit >> finish >> catch_and_log_errors

        add_dags.append(dag)
    return add_dags

rail.for_each_instance(create_child_dag)
