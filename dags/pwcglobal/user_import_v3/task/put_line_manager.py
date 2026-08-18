import rail
from pwcglobal.user_import_v3 import request_payload
from pwcglobal.user_import_v3 import custom_method


def put_line_manager_udf(status):

    with rail.TaskGroup(group_id='put_line_manager', prefix_group_id=False) as put_line_manager:


        has_valid_linemanager = rail.IfOperator(
            task_id='has_valid_linemanager',
            test=lambda: request_payload.get_conf()["linemanagerpartyid"] != request_payload.get_conf()['employeeid'],
            yes_task='get_user_with_line_manager_partyid',
            no_task="line_manager_complete"
        )

        get_user_with_line_manager_partyid = rail.RepliconServiceOperator(
            task_id="get_user_with_line_manager_partyid",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.line_manager_request,
            data_handler=custom_method.get_user_data
        )

        if_line_manager_in_replicon = rail.IfOperator(
            task_id="if_line_manager_in_replicon",
            test=lambda: bool(rail.result("get_user_with_line_manager_partyid")),
            yes_task="get_user_with_legal_entity_id",
            no_task="line_manager_complete"
        )

        get_user_with_legal_entity_id = rail.PythonOperator(
            task_id="get_user_with_legal_entity_id",
            python_callable=custom_method.user_with_legal_entity_id
        )

        if_line_manager_with_legal_entity_id = rail.IfOperator(
            task_id="if_line_manager_with_legal_entity_id",
            test=lambda: bool(rail.result("get_user_with_legal_entity_id")),
            yes_task="if_add_user",
            no_task="line_manager_complete"
        )

        if_add_user = rail.IfOperator(
            task_id="if_add_user",
            test=lambda:bool(status=="add"),
            yes_task="get_user_permission",
            no_task="is_linemanager_changed"
        )

        is_linemanager_changed = rail.IfOperator(
            task_id="is_linemanager_changed",
            test=lambda: request_payload.get_conf()["customfielduri"]['linemanager'] and
                    rail.result("get_user_with_legal_entity_id")["linemanagerloginname"] !=
                    rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                    'customField.displayText',
                    'Line Manager',
                    'text'),
            yes_task="get_user_permission",
            no_task="line_manager_complete"
        )

        get_user_permission = rail.RepliconServiceOperator(
            task_id="get_user_permission",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: {
                "userUri": rail.result("get_user_with_legal_entity_id")["linemanageruri"]
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response[0]["permissionSet"],
                "displayText",
                "Supervisor",
                "uri"
            )
        )

        if_supervisor_permission_for_line_manager = rail.IfOperator(
            task_id="if_supervisor_permission_for_line_manager",
            test='{{result("get_user_permission")|is_truthy}}',
            yes_task="update_line_manager",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: {
                    "userUri": rail.result("get_user_with_legal_entity_id")["linemanageruri"],
                    "permissionSetUri": request_payload.get_conf()["supervisorpermissionseturi"]
            }
        )

        update_line_manager = rail.RepliconServiceOperator(
            task_id="update_line_manager",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                    "objectUri":  rail.result("create_user")["uri"] if status == "add" else dag_run.conf["useruri"],
                    "customFieldUri": request_payload.get_conf()['customfielduri']["linemanager"],
                    "value": rail.result("get_user_with_legal_entity_id")["linemanagerloginname"]
            }
        )

        line_manager_complete = rail.EmptyOperator(task_id="line_manager_complete")

        has_valid_linemanager >> rail.Label("Yes") >>\
        get_user_with_line_manager_partyid >>\
        if_line_manager_in_replicon>> rail.Label("Yes") >> get_user_with_legal_entity_id >>\
        if_line_manager_with_legal_entity_id >> rail.Label("Yes") >>\
        if_add_user >> rail.Label("Yes") >> get_user_permission
        if_add_user >> rail.Label("No") >>\
        is_linemanager_changed >> rail.Label("Yes") >>\
        get_user_permission >> if_supervisor_permission_for_line_manager >> rail.Label(
        "Yes") >> update_line_manager
        if_supervisor_permission_for_line_manager >> rail.Label(
            "No") >> assign_supervisor_permission >> update_line_manager >> line_manager_complete
        is_linemanager_changed >> rail.Label("No") >> line_manager_complete
        if_line_manager_in_replicon >> rail.Label("No") >> line_manager_complete
        if_line_manager_with_legal_entity_id >> rail.Label("No") >>\
        line_manager_complete
        has_valid_linemanager >> rail.Label("No") >> line_manager_complete
        return put_line_manager
