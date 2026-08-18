import json
import rail
from rosterfy.hubspot_polaris_psa_integration.utils import request_payload
from rosterfy.hubspot_polaris_psa_integration.utils import python_callable

def process_project(config, caller):
    with rail.TaskGroup(group_id='project_proccess', prefix_group_id=False) as project_proccess:

        get_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_project_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data = {
                "objectUri": "urn:replicon:object-type:project"
            }
        )

        get_initiate_project_status_level = rail.RepliconServiceOperator(
            task_id='get_initiate_project_status_level',
            endpoint="/services/ProjectStatusService1.svc/GetEnabledProjectStatusLabels",
            data_handler = lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Tentative', 'uri')
        )

        add_project = rail.RepliconServiceOperator(
            task_id='add_project',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data = lambda: request_payload.add_project_payload(caller,config.instance)
        )

        if_solutionconsultant_present_in_hubspot = rail.IfOperator(
            task_id="if_solutionconsultant_present_in_hubspot",
            test=lambda : bool((json.loads(rail.result('get_details_of_deal')).get('associations')) and (
                json.loads(rail.result('get_details_of_deal'))['associations'].get('companies')) and (
                    rail.result('get_required_id_from_company').get('solution_consultant_id')) and rail.result(
                        'get_existing_client_data_based_on_code')),
            yes_task="get_solutionconsultant_details_from_hubspot",
            no_task="projectprocess_end"
        )

        get_solutionconsultant_details_from_hubspot = rail.SimpleHttpOperator(
            task_id='get_solutionconsultant_details_from_hubspot',
            method='GET',
            endpoint=config.owner_endpoint + "{{ result('get_required_id_from_company').solution_consultant_id }}",
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            extra_options={
                'verify': False
            },
        )

        search_for_solutionconsultant_in_replicon = rail.RepliconServiceOperator(
            task_id='search_for_solutionconsultant_in_replicon',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_solution_consultant_data,
            data_handler=python_callable.get_existing_user_detail
        )

        if_solutionconsultant_present_and_enabled = rail.IfOperator(
            task_id='if_solutionconsultant_present_and_enabled',
            test=lambda: bool(rail.result('search_for_solutionconsultant_in_replicon') and rail.result(
                'search_for_solutionconsultant_in_replicon').get('uri') and (
                    rail.result('search_for_solutionconsultant_in_replicon').get('enabled').lower() == 'true')),
            yes_task="get_assigned_permissionset_foruser",
            no_task="log_projectmanager_not_presentordisabled",
        )

        log_projectmanager_not_presentordisabled = rail.PythonOperator(
            task_id = "log_projectmanager_not_presentordisabled",
            python_callable=lambda : "Project Manager to be assigned is not present or is disabled"
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ result('search_for_solutionconsultant_in_replicon')['uri'] }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'permissionSet.name', 'Project Manager', 'permissionSet.uri', '')
        )

        if_projectmanager_permission_present = rail.IfOperator(
            task_id='if_projectmanager_permission_present',
            test='''{{ result('get_assigned_permissionset_foruser') | is_truthy }}''',
            yes_task="assign_projectmanager",
            no_task="log_project_manager_permission_not_available",
        )

        log_project_manager_permission_not_available = rail.PythonOperator(
            task_id = "log_project_manager_permission_not_available",
            python_callable=lambda : "Solution Consultant does not have Project Manager Permission"
        )

        assign_projectmanager = rail.RepliconServiceOperator(
            task_id='assign_projectmanager',
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data = {
                "userUri": "{{ result('search_for_solutionconsultant_in_replicon')['uri'] }}",
                "projectUri":"{{ result('add_project').uri }}"
            }
        )

        projectprocess_end = rail.EmptyOperator(
            task_id = "projectprocess_end"
        )

        get_project_custom_fields >> get_initiate_project_status_level >> add_project >> if_solutionconsultant_present_in_hubspot

        if_solutionconsultant_present_in_hubspot >> rail.Label('Yes') >> get_solutionconsultant_details_from_hubspot
        if_solutionconsultant_present_in_hubspot >> rail.Label('Yes') >> projectprocess_end

        get_solutionconsultant_details_from_hubspot >> search_for_solutionconsultant_in_replicon >> if_solutionconsultant_present_and_enabled

        if_solutionconsultant_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser >> if_projectmanager_permission_present
        if_solutionconsultant_present_and_enabled >> rail.Label('No') >> log_projectmanager_not_presentordisabled >> projectprocess_end

        if_projectmanager_permission_present >> rail.Label('Yes') >> assign_projectmanager >> projectprocess_end
        if_projectmanager_permission_present >> rail.Label('No') >> log_project_manager_permission_not_available >> projectprocess_end

        return project_proccess
