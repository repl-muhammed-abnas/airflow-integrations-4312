from datetime import timedelta
import rail
from airflow.models import Variable
from eisner_amper.project_import_customer_update_api_v1.utils import request_payload, response_filter, python_callable_methods

null = None

# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_project,
        description='Eisner Amper Project Data Import - Customer Records Process Each Project',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_projects,
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
            task_id='create_project_log'
        )

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.get_all_mandatory_check_projects,
            yes_task="is_project_manager_in_payload",
            no_task="log_mandatory_project_fields_not_present"
        )

        log_mandatory_project_fields_not_present = rail.WriteLogOperator(
            task_id='log_mandatory_project_fields_not_present',
            log='{{ result("create_project_log") }}',
            message=lambda dag_run :request_payload.get_exception_message(dag_run, request_payload.MANDATORY_FIELDS['project_fields']),
            severity='Exception',
            properties=lambda dag_run: {
                'clientcode': dag_run.conf['clientcode'],
                'projectcode': dag_run.conf['item']['ProjectCode'],
                'taskname': '',
                'taskcode': '',
                'action': 'Validation',
                'status': 'Exception',
            }
        )

        is_project_manager_in_payload = rail.IfOperator(
            task_id='is_project_manager_in_payload',
            test=lambda dag_run: bool(dag_run.conf['item']['ProjectManager']),
            yes_task="get_user_info_on_empid",
            no_task="get_all_user_uri"
        )

        get_user_info_on_empid = rail.RepliconServiceOperator(
            task_id="get_user_info_on_empid",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_info_on_empid,
            data_handler=response_filter.get_filtered_user_info
        )

        is_project_manager_available = rail.IfOperator(
            task_id='is_project_manager_available',
            test=lambda: bool(rail.result('get_user_info_on_empid')),
            yes_task="is_project_manager_disabled",
            no_task="log_project_manager_not_available"
        )

        is_project_manager_disabled = rail.IfOperator(
            task_id='is_project_manager_disabled',
            test=lambda: rail.result('get_user_info_on_empid')[0]['status']!='True',
            yes_task="log_project_manager_disabled",
            no_task="get_permission_sets_assigned_to_user"
        )

        log_project_manager_disabled = rail.WriteLogOperator(
            task_id='log_project_manager_disabled',
            log='{{ result("create_project_log") }}',
            message="Project Manager disabled in Replicon",
            severity='Exception',
            properties=lambda dag_run: {
                'clientcode': dag_run.conf['clientcode'],
                'projectcode': dag_run.conf['item']['ProjectCode'],
                'taskname': '',
                'taskcode': '',
                'action': 'Validation',
                'status': 'Exception',
            }
        )

        log_project_manager_not_available = rail.WriteLogOperator(
            task_id='log_project_manager_not_available',
            log='{{ result("create_project_log") }}',
            message="Project Manager not available in Replicon",
            severity='Exception',
            properties=lambda dag_run: {
                'clientcode': dag_run.conf['clientcode'],
                'projectcode': dag_run.conf['item']['ProjectCode'],
                'taskname': '',
                'taskcode': '',
                'action': 'Validation',
                'status': 'Exception',
            }
        )

        get_permission_sets_assigned_to_user = rail.RepliconServiceOperator(
            task_id='get_permission_sets_assigned_to_user',
            endpoint='/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers',
            data=lambda:{
                "userUris": [ rail.result('get_user_info_on_empid')[0]['uri'] ]
            }
        )

        is_project_manager_permission_assigned = rail.IfOperator(
            task_id='is_project_manager_permission_assigned',
            test=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_permission_sets_assigned_to_user'),
                "policyUri", 'urn:replicon:policy:project-management', 'permissionSet.uri') != dag_run.conf['projectmanagerpermissionuri'],
            yes_task="assign_project_manager_permission_set",
            no_task="get_all_user_uri"
        )

        assign_project_manager_permission_set = rail.RepliconServiceOperator(
            task_id='assign_project_manager_permission_set',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=request_payload.assign_permission_set
        )

        get_all_user_uri = rail.RepliconServiceOperator(
            task_id='get_all_user_uri',
            endpoint='/services/ProjectService1.svc/GetAllUserTeamMemberUri'
        )

        get_project_details_based_on_wbs = rail.RepliconServiceOperator(
            task_id='get_project_details_based_on_wbs',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": '{{ dag_run.conf.item.ProjectCode }}',
                        "parameterCorrelationId": null
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": null}])[0]['projectDetails']
        )

        create_projectorapply_modifications = rail.RepliconServiceOperator(
            task_id='create_projectorapply_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.create_projectorapply_modifications
        )

        is_cost_center_present_in_payload = rail.IfOperator(
            task_id='is_cost_center_present_in_payload',
            test=lambda dag_run: bool(dag_run.conf['item']['ProjectCostCenterCode']),
            yes_task="search_cost_center_code",
            no_task="is_task_available_in_payload"
        )

        search_cost_center_code = rail.RepliconServiceOperator(
            task_id='search_cost_center_code',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.get_cost_center_code_payload,
            data_handler=response_filter.filter_cost_center_code
        )

        is_cost_center_available = rail.IfOperator(
            task_id='is_cost_center_available',
            test=lambda: bool(rail.result('search_cost_center_code')),
            yes_task="update_project_cost_center",
            no_task="log_cost_center_not_available"
        )

        update_project_cost_center = rail.RepliconServiceOperator(
            task_id='update_project_cost_center',
            endpoint='/services/ProjectService1.svc/UpdateCostCenter2',
            data=request_payload.update_project_cost_center_payload
        )

        log_cost_center_not_available = rail.WriteLogOperator(
            task_id='log_cost_center_not_available',
            log='{{ result("create_project_log") }}',
            message='Cost center is not available in Replicon',
            severity='Exception',
            properties=lambda dag_run: {
                'clientcode': dag_run.conf['clientcode'],
                'projectcode': dag_run.conf['item']['ProjectCode'],
                'taskname': '',
                'taskcode': '',
                'action': 'Validation',
                'status': 'Exception',
            }
        )

        is_task_available_in_payload = rail.IfOperator(
            task_id='is_task_available_in_payload',
            test=lambda dag_run: bool(dag_run.conf['item']['WorkItemSet']),
            yes_task="get_children_task_details",
            no_task="log_project_success"
        )

        get_children_task_details = rail.RepliconServiceOperator(
            task_id='get_children_task_details',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ result("create_projectorapply_modifications").uri }}'
            },
        )

        get_tasks_to_add_update = rail.PythonOperator(
            task_id="get_tasks_to_add_update",
            python_callable=python_callable_methods.get_tasks_to_add_update
        )

        has_task_validation_errors = rail.IfOperator(
            task_id='has_task_validation_errors',
            test='{{ result("get_tasks_to_add_update").tasks_with_validation_errors | is_truthy }}',
            yes_task='log_task_validation_errors',
            no_task='has_tasks_to_add'
        )

        log_task_validation_errors = rail.WriteLogOperator(
            task_id="log_task_validation_errors",
            log='{{ result("create_project_log") }}',
            items=lambda: python_callable_methods.map_task_validation_errors(),
            message=lambda item: item['message'],
            severity=lambda item: item['severity'],
            properties=lambda item: {
                'clientcode': item['clientcode'],
                'projectcode': item['projectcode'],
                'taskname': item['taskname'],
                'taskcode': item['taskcode'],
                'action': item['action'],
                'status': item['status'],
            }
        )

        has_tasks_to_add = rail.IfOperator(
            task_id='has_tasks_to_add',
            test='{{ result("get_tasks_to_add_update").tasks_to_add | is_truthy }}',
            yes_task='add_tasks',
            no_task='has_tasks_to_update'
        )

        add_tasks = rail.RepliconServiceOperator(
            task_id="add_tasks",
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            data=request_payload.get_add_tasks_payload
        )

        log_tasks_added = rail.WriteLogOperator(
            task_id="log_tasks_added",
            log='{{ result("create_project_log") }}',
            items=lambda: python_callable_methods.map_add_task_results(),
            message=lambda item: item['message'],
            severity=lambda item: item['severity'],
            properties=lambda item: {
                'clientcode': item['clientcode'],
                'projectcode': item['projectcode'],
                'taskname': item['taskname'],
                'taskcode': item['taskcode'],
                'action': item['action'],
                'status': item['status'],
            }
        )

        has_tasks_to_update = rail.IfOperator(
            task_id='has_tasks_to_update',
            test='{{ result("get_tasks_to_add_update").tasks_to_update | is_truthy }}',
            yes_task='update_tasks',
            no_task='log_project_success'
        )

        update_tasks = rail.RepliconServiceOperator(
            task_id="update_tasks",
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            data=request_payload.get_update_tasks_payload
        )

        log_tasks_updated = rail.WriteLogOperator(
            task_id="log_tasks_updated",
            log='{{ result("create_project_log") }}',
            items=lambda: python_callable_methods.map_update_task_results(),
            message=lambda item: item['message'],
            severity=lambda item: item['severity'],
            properties=lambda item: {
                'clientcode': item['clientcode'],
                'projectcode': item['projectcode'],
                'taskname': item['taskname'],
                'taskcode': item['taskcode'],
                'action': item['action'],
                'status': item['status'],
            }
        )

        log_project_success = rail.WriteLogOperator(
            task_id='log_project_success',
            log='{{ result("create_project_log") }}',
            message=lambda: "Project Updated Successfully" if request_payload.does_wbs_exist() else "Project Added Successfully",
            severity='Success',
            properties=lambda dag_run: {
                'clientcode': dag_run.conf['clientcode'],
                'projectcode': dag_run.conf['item']['ProjectCode'],
                'taskname': '',
                'taskcode': '',
                'action': 'Update' if request_payload.does_wbs_exist() else "Add",
                'status': 'Success',
            }
        )

        get_required_project_details_for_sorting= rail.PythonOperator(
            task_id="get_required_project_details_for_sorting",
            python_callable=lambda dag_run:{
                'client_code':  dag_run.conf['clientcode'],
                'project_code': dag_run.conf['item']['ProjectCode'],
                'project_uri': rail.result("create_projectorapply_modifications")["uri"]
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_project_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'clientcode': dag_run.conf['clientcode'],
                'projectcode': dag_run.conf['item']['ProjectCode'],
                'taskname': '',
                'taskcode': '',
                'action': 'Sync',
                'status': 'Error',
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_project_log

        create_project_log >> has_mandatory_fields >> rail.Label('No') >> log_mandatory_project_fields_not_present >> catch_and_log_errors
        has_mandatory_fields >> rail.Label('Yes') >> is_project_manager_in_payload

        is_project_manager_in_payload >> rail.Label('Yes') >>  get_user_info_on_empid >> is_project_manager_available
        is_project_manager_available >> rail.Label('Yes') >> is_project_manager_disabled
        is_project_manager_disabled >> rail.Label('Yes') >> log_project_manager_disabled >> get_all_user_uri
        is_project_manager_disabled >> rail.Label('No') >> get_permission_sets_assigned_to_user

        get_permission_sets_assigned_to_user >> is_project_manager_permission_assigned

        is_project_manager_permission_assigned >> rail.Label('Yes') >> get_all_user_uri
        is_project_manager_permission_assigned >> rail.Label('No') >> assign_project_manager_permission_set >> get_all_user_uri


        is_project_manager_available >> rail.Label('No') >> log_project_manager_not_available >> get_all_user_uri
        is_project_manager_in_payload >> rail.Label('No') >> get_all_user_uri

        get_all_user_uri >> get_project_details_based_on_wbs >> create_projectorapply_modifications >> is_cost_center_present_in_payload

        is_cost_center_present_in_payload >> rail.Label('Yes') >> search_cost_center_code >> is_cost_center_available
        is_cost_center_available >> rail.Label('Yes') >> update_project_cost_center >> is_task_available_in_payload
        is_cost_center_available >> rail.Label('No') >> log_cost_center_not_available >> is_task_available_in_payload

        is_cost_center_present_in_payload >> rail.Label('No') >> is_task_available_in_payload

        is_task_available_in_payload >> rail.Label('Yes') >> get_children_task_details
        is_task_available_in_payload >> rail.Label('No') >> log_project_success

        get_children_task_details >> get_tasks_to_add_update >> has_task_validation_errors

        has_task_validation_errors >> rail.Label('Yes') >> log_task_validation_errors >> has_tasks_to_add
        has_task_validation_errors >> rail.Label('No') >> has_tasks_to_add

        has_tasks_to_add >> rail.Label('Yes') >> add_tasks >> log_tasks_added >> has_tasks_to_update
        has_tasks_to_add >> rail.Label('No') >> has_tasks_to_update

        has_tasks_to_update >> rail.Label('Yes') >> update_tasks >> log_tasks_updated >> log_project_success
        has_tasks_to_update >> rail.Label('No') >> log_project_success

        log_project_success >> get_required_project_details_for_sorting >> catch_and_log_errors


    return dag

rail.for_each_instance(create_child_dag_wbs)
