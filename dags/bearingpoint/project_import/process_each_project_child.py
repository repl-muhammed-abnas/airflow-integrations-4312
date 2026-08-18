from datetime import timedelta
import rail
from bearingpoint.project_import.utils import request_payload,response_filter,custom_method
from airflow.models import Variable

# pylint:disable = too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_project_dag_id,
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
            no_task='get_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_project_details',
            end_task='catch_and_log_errors',
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                        "code": "{{ dag_run.conf.WorkPackageID }}",
                    }
                ]
            },
            data_handler=lambda response: response[0].get('projectDetails')
        )

        is_manager_has_not_valid_permission = rail.IfOperator(
            task_id = 'is_manager_has_not_valid_permission',
            test=lambda dag_run: dag_run.conf['manager_uri'] and config.permission_sets['project_manager'] not in dag_run.conf[
                'manager_permission_set'].split(','),
            yes_task= 'assign_manager_permission_set',
            no_task= 'create_or_update_project'
        )

        assign_manager_permission_set = rail.RepliconServiceOperator(
            task_id= "assign_manager_permission_set",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['manager_uri'],
                "permissionSetUri": dag_run.conf['manager_permission_uri']
            }
        )

        create_or_update_project = rail.RepliconServiceOperator(
            task_id="create_or_update_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.create_projectorapply_modifications
        )

        is_comanager_available_in_replicon = rail.IfOperator(
            task_id = 'is_comanager_available_in_replicon',
            test= '{{ dag_run.conf.co_manager_uri | is_truthy }}',
            yes_task= 'is_comanager_has_valid_permission',
            no_task= 'log_project_success'
        )

        is_comanager_has_valid_permission = rail.IfOperator(
            task_id = 'is_comanager_has_valid_permission',
            test=lambda dag_run: config.permission_sets['project_comanager'] in dag_run.conf[
                'co_manager_permission_set'].split(','),
            yes_task= 'assign_comanager_to_project',
            no_task= 'assign_co_manager_permission_set'
        )

        assign_co_manager_permission_set = rail.RepliconServiceOperator(
            task_id= "assign_co_manager_permission_set",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['co_manager_uri'],
                "permissionSetUri": dag_run.conf['co_manager_permission_uri']
            }
        )

        assign_comanager_to_project = rail.RepliconServiceOperator(
            task_id="assign_comanager_to_project",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda dag_run: {
                "projectUri": rail.result("create_or_update_project")['uri'],
                "sharedUris": [dag_run.conf['co_manager_uri']]
            }
        )

        log_project_success = rail.WriteLogOperator(
            task_id="log_project_success",
            log= "{{ dag_run.conf.log }}",
            message="Project created successfully",
            properties=custom_method.get_project_log_message
        )

        def get_billing_rates_to_process(dag_run):
            return list(filter(lambda item: item['role_name'] not in [role['name'] for role in dag_run.conf[
                'billing_rates']],dag_run.conf['all_resources_list'])) if dag_run.conf['all_resources_list'] else []

        has_billing_rates_to_create = rail.IfOperator(
            task_id = 'has_billing_rates_to_create',
            test=lambda dag_run: bool(get_billing_rates_to_process(dag_run)),
            yes_task= 'create_resource_billing_rates',
            no_task= 'is_restrict_time_posting_set_to_yes'
        )

        create_resource_billing_rates = rail.RepliconServiceCallForEachItemOperator(
            task_id = 'create_resource_billing_rates',
            endpoint= "/services/BillingRateService1.svc/PutCompanyBillingRate",
            items= get_billing_rates_to_process,
            data= {
                "billingRate": {
                    "target": {
                        "name": "{{ item.role_name }}"
                    },
                    "name": "{{ item.role_name }}",
                    "description": "{{ item.role }}",
                    "isEnabled": "1",
                }
            }
        )

        is_restrict_time_posting_set_to_yes = rail.IfOperator(
            task_id = 'is_restrict_time_posting_set_to_yes',
            test= lambda dag_run: dag_run.conf['RestrictTimePosting'].lower() == 'y',
            yes_task= 'assign_restricted_resources_to_project',
            no_task= 'assign_all_users_in_project_team'
        )

        assign_restricted_resources_to_project = rail.RepliconServiceOperator(
            task_id="assign_restricted_resources_to_project",
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data=lambda dag_run: {
                'projectUri': rail.result("create_or_update_project")['uri'],
                'resourceUri': [item['resource_uri'] for item in dag_run.conf['task_resource_list']] if dag_run.conf['task_resource_list'] else [],
                'projectTeamMemberAssignmentOptionUri': 'urn:replicon:project-team-member-assignment-option:assign'
            }
        )

        assign_billing_rates_to_restricted_resources = rail.RepliconServiceCallForEachItemOperator(
            task_id="assign_billing_rates_to_restricted_resources",
            endpoint='/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3',
            items= '{{ dag_run.conf.task_resource_list | to_json }}',
            data= request_payload.get_restricted_users_billing_rates_payload
        )

        assign_all_users_in_project_team = rail.RepliconServiceOperator(
            task_id = 'assign_all_users_in_project_team',
            endpoint= '/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data =lambda: {
                "projectUri": rail.result("create_or_update_project")['uri'],
                "resourceUri":[
                    "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1"
                ],
                "projectTeamMemberAssignmentOptionUri":"urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        assign_billing_rates_to_all_resources = rail.RepliconServiceOperator(
            task_id="assign_billing_rates_to_all_resources",
            endpoint='/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3',
            data= request_payload.get_all_users_billing_rates_payload
        )

        is_new_project = rail.IfOperator(
            task_id = 'is_new_project',
            test=lambda: not request_payload.does_wbs_exist(),
            yes_task= 'get_all_task_to_add_update',
            no_task= 'get_all_tasks_for_project'
        )

        get_all_tasks_for_project = rail.RepliconServiceOperator(
            task_id="get_all_tasks_for_project",
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails",
            data=lambda: {
                "pageIndex": "1",
                "pageSize": "100000",
                "projectUris": [
                    rail.result("create_or_update_project")['uri']
                ]
            },
            data_handler=response_filter.format_project_task_details
        )

        get_all_task_to_add_update = rail.PythonOperator(
            task_id="get_all_task_to_add_update",
            python_callable=custom_method.get_tasks_to_process_data
        )

        has_tasks_to_add = rail.IfOperator(
            task_id = 'has_tasks_to_add',
            test= '{{ result("get_all_task_to_add_update").tasks_to_add | is_truthy }}',
            yes_task= 'add_task',
            no_task= 'has_tasks_to_update'
        )

        add_task = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_task',
            endpoint='/services/ProjectService1.svc/PutTask',
            items= '{{ result("get_all_task_to_add_update").tasks_to_add | to_json }}',
            data= request_payload.get_put_task_data
        )

        log_task_added_success_error = rail.WriteLogOperator(
            task_id="log_task_added_success_error",
            log= "{{ dag_run.conf.log }}",
            message="{{ item.details }}",
            items=lambda: custom_method.map_task_success_error(
                "add_task", "added","tasks_to_add"),
            properties=lambda item,dag_run: {
                "projectcode": dag_run.conf['WorkPackageID'],
                "projectname": dag_run.conf['WorkPackagename'],
                "clientcode": dag_run.conf['Customer'],
                "taskname": item['task_name'],
                "parenttaskname": item['parent_task_name'],
                'action': 'Add',
                "details": item['details'],
                "status": item['status']
            }
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
            data=request_payload.get_update_task_payload
        )

        log_task_updated_success_error = rail.WriteLogOperator(
            task_id="log_task_updated_success_error",
            log= "{{ dag_run.conf.log }}",
            message="{{ item.details }}",
            items=lambda: custom_method.map_task_success_error(
                "update_task", "updated","tasks_to_update"),
            properties=lambda dag_run, item: {
                "projectcode": dag_run.conf['WorkPackageID'],
                "projectname": dag_run.conf['WorkPackagename'],
                "clientcode": dag_run.conf['Customer'],
                "taskname": item['task_name'],
                "parenttaskname": item['parent_task_name'],
                'action': 'Update',
                "details": item['details'],
                "status": item['status']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log= "{{ dag_run.conf.log }}",
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties=lambda dag_run:{
                "projectcode": dag_run.conf['WorkPackageID'],
                "projectname": dag_run.conf['WorkPackagename'],
                "clientcode": dag_run.conf['Customer'],
                "taskcode": '',
                "taskname": '',
                "parenttaskname": '',
                "action": "Add",
                "status": "Error",
                'details': '{{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info= lambda dag_run:{
                "projectcode": dag_run.conf['WorkPackageID'],
                "projectname": dag_run.conf['WorkPackagename'],
                "clientcode": dag_run.conf['Customer'],
                'details': 'Project and Tasks are synced successfully.'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> get_project_details >> is_manager_has_not_valid_permission

        is_manager_has_not_valid_permission >> rail.Label(
            "Yes") >> assign_manager_permission_set >> create_or_update_project

        is_manager_has_not_valid_permission >> rail.Label(
            "No") >> create_or_update_project

        create_or_update_project >> is_comanager_available_in_replicon

        is_comanager_available_in_replicon >> rail.Label(
            "Yes") >> is_comanager_has_valid_permission

        is_comanager_has_valid_permission >> rail.Label(
            "Yes") >> assign_comanager_to_project >> log_project_success

        is_comanager_has_valid_permission >> rail.Label(
            "No") >> assign_co_manager_permission_set >> assign_comanager_to_project

        is_comanager_available_in_replicon >> rail.Label(
            "No") >> log_project_success >> has_billing_rates_to_create

        has_billing_rates_to_create >> rail.Label(
            "Yes") >> create_resource_billing_rates >> is_restrict_time_posting_set_to_yes

        has_billing_rates_to_create >> rail.Label(
            "No") >> is_restrict_time_posting_set_to_yes

        is_restrict_time_posting_set_to_yes >> rail.Label(
            "Yes") >> assign_restricted_resources_to_project >> assign_billing_rates_to_restricted_resources >> is_new_project

        is_restrict_time_posting_set_to_yes >> rail.Label(
            "No") >> assign_all_users_in_project_team >> assign_billing_rates_to_all_resources >> assign_restricted_resources_to_project

        is_new_project >> rail.Label(
            "Yes") >> get_all_task_to_add_update

        is_new_project >> rail.Label(
            "No") >> get_all_tasks_for_project >> get_all_task_to_add_update >> has_tasks_to_add

        has_tasks_to_add >> rail.Label(
            "Yes") >> add_task >> log_task_added_success_error >> has_tasks_to_update

        has_tasks_to_add >> rail.Label(
            "No") >> has_tasks_to_update

        has_tasks_to_update >> rail.Label(
            "No") >> catch_and_log_errors

        has_tasks_to_update >> rail.Label(
            "Yes") >> update_task >> log_task_updated_success_error >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag_wbs)
