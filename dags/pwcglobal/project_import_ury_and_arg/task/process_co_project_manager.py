import rail
from pwcglobal.project_import_ury_and_arg.utils import request_payload, response_filter

null = None

def process_co_project_manager_task_group():
    with rail.TaskGroup(group_id='process_co_project_manager_task', prefix_group_id=False):

        search_engagementpartner_by_partyid_and_legal_entity = rail.RepliconServiceOperator(
            task_id='search_engagementpartner_by_partyid_and_legal_entity',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_engagementpartner_by_partyid_and_legal_entity_uri_payload,
            data_handler=lambda response, dag_run: response_filter.search_engagementpartner_response_filter(
                dag_run, response)
        )

        is_engagementpartner_present_and_enabled = rail.IfOperator(
            task_id='is_engagementpartner_present_and_enabled',
            test="{{ result('search_engagementpartner_by_partyid_and_legal_entity') | is_truthy and \
                result('search_engagementpartner_by_partyid_and_legal_entity')[0].engagementpartner_uri | is_truthy and \
                result('search_engagementpartner_by_partyid_and_legal_entity')[0].is_enable == 'True' }}",
            yes_task="log_engagementpartner_present_and_enabled",
            no_task="log_engagementpartner_not_present_or_disabled"
        )

        log_engagementpartner_present_and_enabled = rail.EmptyOperator(
            task_id="log_engagementpartner_present_and_enabled",
        )

        log_engagementpartner_not_present_or_disabled = rail.EmptyOperator(
            task_id="log_engagementpartner_not_present_or_disabled",
        )

        def is_engagementpartner_permission(response):
            engagementpartner_permission = True
            if response:
                if rail.find_first_by_attr_and_get_attr(
                        response, 'policyUri', 'urn:replicon:policy:project-management', 'permissionSet'):
                    engagementpartner_permission = False
            return engagementpartner_permission

        get_assigned_engagementpartner_permission = rail.RepliconServiceOperator(
            task_id='get_assigned_engagementpartner_permission',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_engagementpartner_by_partyid_and_legal_entity')[0].engagementpartner_uri }}"
            },
            data_handler=is_engagementpartner_permission
        )

        should_add_engagementpartner_missing_permissions = rail.IfOperator(
            task_id='should_add_engagementpartner_missing_permissions',
            test="{{ result('get_assigned_engagementpartner_permission') }}",
            yes_task='add_missing_engagementpartner_permission',
            no_task='add_update_project_comanager'
        )

        add_missing_engagementpartner_permission = rail.RepliconServiceOperator(
            task_id='add_missing_engagementpartner_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('search_engagementpartner_by_partyid_and_legal_entity')[0].engagementpartner_uri }}",
                'permissionSetUri': '{{ dag_run.conf.project_co_manager_permission_uri }}'
            }
        )

        add_update_project_comanager = rail.RepliconServiceOperator(
            task_id = 'add_update_project_comanager',
            endpoint= '/services/ProjectService1.svc/PutExplicitSharingAssignments',
            data=lambda: {
                "projectUri": rail.result("update_project")['uri'] if rail.result('get_project_details') else rail.result("create_project")['uri'],
                "sharedUris": [rail.result('search_engagementpartner_by_partyid_and_legal_entity')[0]['engagementpartner_uri']]
            }
        )

        finish_process_co_project_manager = rail.EmptyOperator(
            task_id="finish_process_co_project_manager",
        )

        search_engagementpartner_by_partyid_and_legal_entity >> is_engagementpartner_present_and_enabled >> rail.Label(
            "Yes") >> log_engagementpartner_present_and_enabled >> get_assigned_engagementpartner_permission >> \
        should_add_engagementpartner_missing_permissions >> rail.Label(
            "Yes") >> add_missing_engagementpartner_permission >> add_update_project_comanager >> finish_process_co_project_manager
        should_add_engagementpartner_missing_permissions >> rail.Label(
            "No") >> add_update_project_comanager >> finish_process_co_project_manager
        is_engagementpartner_present_and_enabled >> rail.Label(
            "No") >> log_engagementpartner_not_present_or_disabled >> finish_process_co_project_manager

    return search_engagementpartner_by_partyid_and_legal_entity, finish_process_co_project_manager
