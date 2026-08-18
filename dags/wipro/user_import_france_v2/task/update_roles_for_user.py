from wipro.user_import_france_v2.utils import request_payload, custom_methods
from wipro.user_import_france_v2.task import put_supervisor_table_settings
import rail

null = None


def update_user_roles(config):
    with rail.TaskGroup(
        group_id="update_france_user_roles",
        prefix_group_id=False
    ) as user_roles:

        create_manager_permission_variable = rail.SetVariableOperator(
            task_id="create_manager_permission_variable",
            name="manager_permission",
            value=[],
            append=False
        )

        if_hr_manager_permission_update = rail.IfOperator(
            task_id="if_hr_manager_permission_update",
            test=lambda dag_run: dag_run.conf["hr_manager_flg"] and
            dag_run.conf["hr_manager_flg"] == "Y" and not rail.find_first_by_attr_and_get_attr(
                rail.result('get_update_user_details')["permissionSets"],
                "displayText",
                config.GENERAL_MAPPER["hr_manager"],
                "uri"),
            yes_task="assign_hr_manager_permission",
            no_task="if_primary_manager_permission_update"
        )

        assign_hr_manager_permission = rail.SetVariableOperator(
            task_id="assign_hr_manager_permission",
            name='{{result("create_manager_permission_variable").name}}',
            value='{{dag_run.conf.hr_manager_uri}}',
            append=True
        )

        if_primary_manager_permission_update = rail.IfOperator(
            task_id="if_primary_manager_permission_update",
            test=lambda dag_run: dag_run.conf["primary_manager_flg"] and
            dag_run.conf["primary_manager_flg"] == "Y" and not (rail.find_first_by_attr_and_get_attr(
                    rail.result('get_update_user_details')["permissionSets"],
                    "displayText",
                    config.GENERAL_MAPPER["l1_manager"],
                    "uri") and rail.find_first_by_attr_and_get_attr(
                rail.result('get_update_user_details')["permissionSets"],
                "displayText",
                config.GENERAL_MAPPER["end_user_manager"],
                "uri")),
            yes_task="assign_primary_manager_permission",
            no_task="if_project_manager_permission_update"
        )

        assign_primary_manager_permission = rail.SetVariableOperator(
            task_id="assign_primary_manager_permission",
            name='{{result("create_manager_permission_variable").name}}',
            value=lambda dag_run: [
                dag_run.conf["l1_manager_uri"], dag_run.conf["end_user_manager_uri"]],
            append=True
        )

        user_uri = '{{result("get_update_user_details").userDetails.uri}}'
        put_table_view_setting_for_user_update = put_supervisor_table_settings.get_put_table_view_setting_supervisor(
            user_uri, "supervisor_user"
        )

        if_project_manager_permission_update = rail.IfOperator(
            task_id="if_project_manager_permission_update",
            test=lambda dag_run: dag_run.conf["project_manager_flg"] and
            dag_run.conf["project_manager_flg"] == "Y" and not rail.find_first_by_attr_and_get_attr(
                rail.result('get_update_user_details')["permissionSets"],
                "displayText",
                config.GENERAL_MAPPER["project_manager"],
                "uri"),
            yes_task="assign_project_manager_permission",
            no_task="get_manager_permission"
        )

        assign_project_manager_permission = rail.SetVariableOperator(
            task_id="assign_project_manager_permission",
            name='{{result("create_manager_permission_variable").name}}',
            value='{{dag_run.conf.france_project_manager_uri}}',
            append=True
        )

        get_manager_permission = rail.GetVariableOperator(
            task_id="get_manager_permission",
            name="manager_permission"
        )
        if_any_manager_update = rail.IfOperator(
            task_id="if_any_manager_update",
            test='{{result("get_manager_permission").value|is_truthy}}',
            yes_task="assign_manager_permissions",
            no_task="user_role_update_end"
        )

        assign_manager_permissions = rail.RepliconServiceOperator(
            task_id="assign_manager_permissions",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run:{
                            "user": {
                                "uri": rail.result('get_update_user_details')["userDetails"]["uri"]
                            },
                            "modifications": {
                                "permissionSetsToApply": {
                                "permissionSetUrisToAssign": custom_methods.get_permission_details(dag_run),
                                "policyUrisToRemovePermissionSet": []
                                }
                            },
                            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                        }
        )

        if_hr_manager = rail.IfOperator(
            task_id="if_hr_manager",
            test='{{get_task_state("assign_hr_manager_permission") == "success"}}',
            yes_task="restrict_hr_manager_policy_access",
            no_task="user_role_update_end"
        )

        restrict_hr_manager_policy_access = rail.RepliconServiceOperator(
            task_id="restrict_hr_manager_policy_access",
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=request_payload.get_restrict_hr_manager_payload
        )

        user_role_update_end = rail.EmptyOperator(
            task_id="user_role_update_end"
        )
        create_manager_permission_variable >>\
            if_hr_manager_permission_update >> rail.Label("Yes") >>\
            assign_hr_manager_permission >> if_primary_manager_permission_update
        if_hr_manager_permission_update >> rail.Label("No") >>\
            if_primary_manager_permission_update >> rail.Label(
                "No") >> if_project_manager_permission_update
        if_primary_manager_permission_update >> rail.Label("Yes") >>\
            assign_primary_manager_permission >> put_table_view_setting_for_user_update >>\
            if_project_manager_permission_update >> rail.Label(
            "No") >> get_manager_permission >> if_any_manager_update
        if_project_manager_permission_update >> rail.Label("Yes") >>\
            assign_project_manager_permission >> get_manager_permission >>\
            if_any_manager_update >> rail.Label("Yes") >>\
            assign_manager_permissions >>\
            if_hr_manager >> rail.Label(
                "Yes") >> restrict_hr_manager_policy_access >> user_role_update_end
        if_hr_manager >> rail.Label("No") >> user_role_update_end
        if_any_manager_update >> rail.Label("No") >>\
            user_role_update_end

        return user_roles
