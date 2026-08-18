import json
from step.project_task_import import request_payload
import rail

null = None


def step_create_parent_task(task_name):
    with rail.TaskGroup(
        group_id=f"task_for_{task_name}",
        prefix_group_id=False
    ) as task_group:

        create_parent_task = rail.RepliconServiceOperator(
            task_id="create_parent_task",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_create_task_request,
            data_handler=lambda response: response["uri"] if "uri" in response else null
        )

        if_resource_is_allusers = rail.IfOperator(
            task_id="if_resource_is_allusers",
            test=lambda dag_run: bool(
                dag_run.conf["User Name"].lower() == "all"),
            yes_task="all_users_uri",
            no_task="create_user_list"
        )

        all_users_uri = rail.PythonOperator(
            task_id="all_users_uri",
            python_callable=lambda: "urn:replicon-tenant:" +
            rail.get_tenant_slug()+":department:1"
        )

        assign_all_users_to_project = rail.RepliconServiceOperator(
            task_id="assign_all_users_to_project",
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data=lambda: json.dumps({
                "projectUri": rail.result("get_project_details"),
                "resourceUri": [rail.result("all_users_uri")],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            })
        )

        assign_all_users_to_task = rail.RepliconServiceOperator(
            task_id="assign_all_users_to_task",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: json.dumps({
                    "taskUri": rail.result("create_parent_task"),
                    "resourceUris": [rail.result("all_users_uri")],
                    "isAssigned": "true"
            })
        )

        create_user_list = rail.PythonOperator(
            task_id="create_user_list",
            python_callable=lambda dag_run: dag_run.conf["User Name"].split(
                '|') if "|" in dag_run.conf["User Name"] else dag_run.conf["User Name"]
        )

        add_users_to_task = rail.ForEachOperator(
            task_id="add_users_to_task",
            items=lambda: rail.result("create_user_list") if len(rail.result(
                "create_user_list")) > 1 else [rail.result("create_user_list")],
            start_task="search_for_user_in_replicon",
            end_task="end_for"
        )

        search_for_user_in_replicon = rail.RepliconServiceOperator(
            task_id="search_for_user_in_replicon",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_request,
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                list(map(lambda i: i["cells"][0], response["rows"])),
                "textValue",
                rail.result("add_users_to_task"), "uri")
        )

        if_user_exists_in_replicon = rail.IfOperator(
            task_id="if_user_exists_in_replicon",
            test=lambda: bool(rail.result("search_for_user_in_replicon")),
            yes_task="assign_users_to_project",
            no_task="insert_userdata"
        )

        insert_userdata = rail.SetVariableOperator(
            task_id='insert_userdata',
            append=True,
            name='userdata',
            value=lambda: rail.result("add_users_to_task")
        )

        assign_users_to_project = rail.RepliconServiceOperator(
            task_id="assign_users_to_project",
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data=lambda: json.dumps({
                "projectUri": rail.result("get_project_details"),
                "resourceUri": [rail.result("search_for_user_in_replicon")],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            })
        )

        assign_users_to_task = rail.RepliconServiceOperator(
            task_id="assign_users_to_task",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: json.dumps({
                    "taskUri": rail.result("create_parent_task"),
                    "resourceUris": [
                        rail.result("search_for_user_in_replicon")
                    ],
                "isAssigned": "true"
            })
        )

        remove_all_user_assignment = rail.RepliconServiceOperator(
            task_id="remove_all_user_assignment",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: json.dumps({
                    "taskUri": rail.result("create_parent_task"),
                    "resourceUris": [
                        "urn:replicon-tenant:" + rail.get_tenant_slug()+":department:1"
                    ],
                "isAssigned": "false"
            })
        )

        end_for = rail.EmptyOperator(task_id="end_for")

        if_user_not_available = rail.IfOperator(
            task_id="if_user_not_available",
            test=lambda: bool(rail.result("insert_userdata")["value"]),
            yes_task="step_log_unassigned_users",
            no_task="step_log_success"
        )
        step_log_unassigned_users = rail.WriteLogOperator(
            task_id="step_log_unassigned_users",
            log='{{dag_run.conf.lookuptable}}',
            severity="success with exceptions",
            message="User(s): not present in Replicon ",
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "success with exceptions",
                "details": "User(s):" + ",".join(rail.result("insert_userdata")["value"])+" not present in Replicon ",
                "childjobid": rail.render_template('{{ecid()}}'),
                # pylint:disable=line-too-long
                "Taskname": dag_run.conf["Task Name Level 1"] + "|" + dag_run.conf["Task Name Level 2"] if dag_run.conf["Task Name Level 2"] else dag_run.conf["Task Name Level 1"]
            }
        )

        step_log_success = rail.WriteLogOperator(
            task_id="step_log_success",
            log='{{dag_run.conf.lookuptable}}',
            severity="success",
            message="success",
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "success",
                "details": "NA",
                "childjobid": rail.render_template('{{ecid()}}'),
                # pylint:disable=line-too-long
                "Taskname": dag_run.conf["Task Name Level 1"] + "|" + dag_run.conf["Task Name Level 2"] if dag_run.conf["Task Name Level 2"] else dag_run.conf["Task Name Level 1"]
            }
        )

        create_parent_task >>\
            if_resource_is_allusers >> rail.Label("Yes") >> all_users_uri >>\
            assign_all_users_to_project >> assign_all_users_to_task
        if_resource_is_allusers >> rail.Label("No") >>\
            create_user_list >> add_users_to_task >> end_for
        add_users_to_task >> search_for_user_in_replicon >>\
            if_user_exists_in_replicon >> rail.Label("Yes") >>\
            assign_users_to_project >> assign_users_to_task >> remove_all_user_assignment >> end_for
        if_user_exists_in_replicon >> rail.Label(
            "No") >> insert_userdata >> end_for
        end_for >> if_user_not_available >> rail.Label(
            "Yes") >> step_log_unassigned_users
        if_user_not_available >> rail.Label("No") >> step_log_success

        return task_group
