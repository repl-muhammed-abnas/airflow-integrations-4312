from datetime import timedelta
import rail
from pwcglobal.project_import_api_v1 import request_payload, custom_method


def get_add_permission_policy(caller, action_type='create'):
    with rail.TaskGroup(group_id=f'add_permission_policy_{caller}', prefix_group_id=False) as add_permission_policy:

        is_team_member_to_assign_present = rail.IfOperator(
            task_id=f'is_team_member_to_assign_present_{caller}',
            test=lambda: bool(rail.result(
                f'get_permissionuri_useruri_{caller}')),
            yes_task=f'get_assigned_permissions_for_user_{caller}',
            no_task=f'add_permission_policy_process_complete_{caller}'
        )

        get_assigned_permissions_for_user = rail.RepliconServiceOperator(
            task_id=f'get_assigned_permissions_for_user_{caller}',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: {
                "userUri": rail.result(f'get_permissionuri_useruri_{caller}')['user_uri']
            },
            data_handler=lambda response: list(filter(
                lambda x: x['policyUri'] == 'urn:replicon:policy:project-management', response))
        )

        if action_type == 'create' and caller == 'project_manager':

            is_required_permissions_not_present = rail.IfOperator(
                task_id=f'is_required_permissions_not_present_{caller}',
                test=lambda: not bool(rail.result(
                    f'get_assigned_permissions_for_user_{caller}')),
                yes_task=f'assign_required_permission_{caller}',
                no_task=f'add_permission_policy_process_complete_{caller}'
            )

        else:

            is_required_permissions_not_present = rail.IfOperator(
                task_id=f'is_required_permissions_not_present_{caller}',
                test=lambda: not bool(rail.result(
                    f'get_assigned_permissions_for_user_{caller}')),
                yes_task=f'assign_required_permission_{caller}',
                no_task=f'impersonate_and_create_interactive_session_{caller}'
            )

        assign_required_permission = rail.RepliconServiceOperator(
            task_id=f'assign_required_permission_{caller}',
            endpoint="services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: {
                "userUri": rail.result(f'get_permissionuri_useruri_{caller}')['user_uri'],
                "permissionSetUri": rail.result(f'get_permissionuri_useruri_{caller}')['permission']
            }
        )

        put_policy_data_access_scopes = rail.RepliconServiceOperator(
            task_id=f'put_policy_data_access_scopes_{caller}',
            endpoint="services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=lambda: request_payload.get_put_policy_data_access_scopes_payload(
                caller)
        )

        if action_type == 'update' and caller == 'project_manager':

            update_project_manager = rail.RepliconServiceOperator(
                task_id="update_project_manager",
                endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
                data={
                    "projectUri": "{{ dag_run.conf.project_uri }}",
                    "userUri": "{{ result('get_permissionuri_useruri_project_manager').user_uri }}"
                }
            )

        if caller == 'project_comanager':

            assign_comanager_to_project = rail.RepliconServiceOperator(
                task_id="assign_comanager_to_project",
                endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
                data=lambda dag_run: {
                    "projectUri": rail.result('create_project_with_payload') if action_type == 'create' else dag_run.conf['project_uri'],
                    "sharedUris": [rail.result('get_permissionuri_useruri_project_comanager')['user_uri']]
                }
            )

            if action_type == 'update':

                update_engagement_party_udf_value = rail.RepliconServiceOperator(
                    task_id="update_engagement_party_udf_value",
                    endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
                    data={
                        "objectUri": "{{ dag_run.conf.project_uri }}",
                        "customFieldUri": "{{ dag_run.conf.engagement_manager_party_uri }}",
                        "value": "{{ result('get_project_co_manager_to_assign') | map_to_attr('user_name') | first_or_default }}"
                    }
                )

        if not (action_type == 'create' and caller == 'project_manager'):

            impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
                task_id=f'impersonate_and_create_interactive_session_{caller}',
                endpoint='/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession',
                data=lambda: {
                    "impersonatedUserUri": rail.result(f'get_permissionuri_useruri_{caller}')['user_uri']
                },
                response_filter=custom_method.map_impersonate_and_create_interactive_session
            )

            put_column_view_settings_for_user = rail.RepliconServiceCallForEachItemOperator(
                task_id=f'put_column_view_settings_for_user_{caller}',
                endpoint='/services/ListSettingsService1.svc/PutColumnSettingsForUser',
                items=lambda: request_payload.all_column_setting_payloads(
                    caller),
                execution_timeout=timedelta(days=14),
                data=lambda item: item,
                headers=lambda: rail.result(
                    f'impersonate_and_create_interactive_session_{caller}'),
            )

        add_permission_policy_process_complete = rail.EmptyOperator(
            task_id=f'add_permission_policy_process_complete_{caller}'
        )

        is_team_member_to_assign_present >> rail.Label(
            "Yes") >> get_assigned_permissions_for_user >> \
            is_required_permissions_not_present

        if action_type == 'create' and caller == 'project_manager':
            is_required_permissions_not_present >> rail.Label(
                "Yes") >> assign_required_permission >> \
                put_policy_data_access_scopes >> add_permission_policy_process_complete

            is_required_permissions_not_present >> rail.Label(
                "No") >> add_permission_policy_process_complete

        else:
            is_required_permissions_not_present >> rail.Label(
                "Yes") >> assign_required_permission >> \
                put_policy_data_access_scopes >> impersonate_and_create_interactive_session

            is_required_permissions_not_present >> rail.Label(
                "No") >> impersonate_and_create_interactive_session

        if action_type == 'update' and caller == 'project_manager':
            impersonate_and_create_interactive_session >> update_project_manager >> \
                put_column_view_settings_for_user >> add_permission_policy_process_complete

        if caller == 'project_comanager':
            impersonate_and_create_interactive_session >> assign_comanager_to_project >> \
                put_column_view_settings_for_user

            if action_type == 'update':
                put_column_view_settings_for_user >> update_engagement_party_udf_value >> add_permission_policy_process_complete

            else:
                put_column_view_settings_for_user >> add_permission_policy_process_complete

        is_team_member_to_assign_present >> rail.Label(
            "No") >> add_permission_policy_process_complete

        return add_permission_policy
