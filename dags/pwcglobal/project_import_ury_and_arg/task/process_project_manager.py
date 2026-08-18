import rail
from pwcglobal.project_import_ury_and_arg.utils import request_payload, response_filter

null = None

def process_project_manager_task_group():
    with rail.TaskGroup(group_id='process_project_manager_task', prefix_group_id=False):

        search_projectmanager_by_partyid_and_legal_entity = rail.RepliconServiceOperator(
            task_id='search_projectmanager_by_partyid_and_legal_entity',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_projectmanager_by_partyid_and_legal_entity_uri_payload,
            data_handler=lambda response, dag_run: response_filter.search_projectmanager_response_filter(
                dag_run, response)
        )

        is_projectmanager_present_and_enabled = rail.IfOperator(
            task_id='is_projectmanager_present_and_enabled',
            test="{{ result('search_projectmanager_by_partyid_and_legal_entity') | is_truthy and \
                result('search_projectmanager_by_partyid_and_legal_entity')[0].projectmanager_uri | is_truthy and \
                result('search_projectmanager_by_partyid_and_legal_entity')[0].is_enable == 'True' }}",
            yes_task="log_project_manager_present_and_enabled",
            no_task="log_project_manager_not_present_or_disabled"
        )

        log_project_manager_present_and_enabled = rail.EmptyOperator(
            task_id="log_project_manager_present_and_enabled",
        )

        log_project_manager_not_present_or_disabled = rail.EmptyOperator(
            task_id="log_project_manager_not_present_or_disabled",
        )

        def is_projectmanager_permission(response):
            project_manager_permission = True
            if response:
                if rail.find_first_by_attr_and_get_attr(
                        response, 'policyUri', 'urn:replicon:policy:project-management', 'permissionSet'):
                    project_manager_permission = False
            return project_manager_permission

        get_assigned_projectmanager_permission = rail.RepliconServiceOperator(
            task_id='get_assigned_projectmanager_permission',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_projectmanager_by_partyid_and_legal_entity')[0].projectmanager_uri }}"
            },
            data_handler=is_projectmanager_permission
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_assigned_projectmanager_permission') }}",
            yes_task='add_missing_project_manager_permission',
            no_task='finish_process_project_manager'
        )

        add_missing_project_manager_permission = rail.RepliconServiceOperator(
            task_id='add_missing_project_manager_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('search_projectmanager_by_partyid_and_legal_entity')[0].projectmanager_uri }}",
                'permissionSetUri': '{{ dag_run.conf.project_manager_permission_uri }}'
            }
        )

        finish_process_project_manager = rail.EmptyOperator(
            task_id="finish_process_project_manager",
        )

        search_projectmanager_by_partyid_and_legal_entity >> is_projectmanager_present_and_enabled >> rail.Label(
            "Yes") >> log_project_manager_present_and_enabled >> get_assigned_projectmanager_permission >> \
        should_add_missing_permissions >> rail.Label("Yes") >> add_missing_project_manager_permission >> finish_process_project_manager
        should_add_missing_permissions >> rail.Label("No") >> finish_process_project_manager
        is_projectmanager_present_and_enabled >> rail.Label(
            "No") >> log_project_manager_not_present_or_disabled >> finish_process_project_manager

    return search_projectmanager_by_partyid_and_legal_entity, finish_process_project_manager
