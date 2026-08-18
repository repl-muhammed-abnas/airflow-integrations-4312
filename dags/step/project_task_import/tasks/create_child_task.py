import json
from step.project_task_import import request_payload
import rail

null = None


def step_create_child_task(task_name):
    with rail.TaskGroup(
        group_id=f"task_for_{task_name}",
        prefix_group_id=False
    ) as task_group:

        create_child_task = rail.RepliconServiceOperator(
            task_id=f"create_child_task_{task_name}",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_create_child_task_request,
            data_handler=lambda response: response["uri"] if "uri" in response else null
        )

        move_task_under_parent = rail.RepliconServiceOperator(
            task_id=f"move_task_under_parent_{task_name}",
            endpoint="/services/TaskService1.svc/MoveTask",
            data=lambda: json.dumps({
                "taskUri": rail.result(f"create_child_task_{task_name}"),
                "targetUri": rail.result("create_parent_task") or rail.result("get_task_data")["taskuri"],
                "moveTaskMethodUri": "urn:replicon:move-task-method:child-of-target"
            })
        )

        update_child_task_name = rail.RepliconServiceOperator(
            task_id=f"update_child_task_name_{task_name}",
            endpoint="/services/TaskService1.svc/UpdateName",
            data={
                "taskUri": '{{result("create_child_task_'+task_name+'")}}',
                "name": '{{dag_run.conf["Task Name Level 2"]}}'
            }
        )

        if_resource_is_allusers = rail.IfOperator(
            task_id=f"if_resource_is_allusers_{task_name}",
            test=lambda dag_run: bool(
                dag_run.conf["User Name"].lower() == "all"),
            yes_task=f"all_users_uri_{task_name}",
            no_task=f"create_user_list_{task_name}"
        )

        all_users_uri = rail.PythonOperator(
            task_id=f"all_users_uri_{task_name}",
            python_callable=lambda: "urn:replicon-tenant:" +
            rail.get_tenant_slug()+":department:1"
        )

        assign_all_users_to_project = rail.RepliconServiceOperator(
            task_id=f"assign_all_users_to_project_{task_name}",
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data=lambda: json.dumps({
                "projectUri": rail.result("get_project_details"),
                "resourceUri": [rail.result(f"all_users_uri_{task_name}")],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            })
        )

        assign_all_users_to_task = rail.RepliconServiceOperator(
            task_id=f"assign_all_users_to_task_{task_name}",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: json.dumps({
                    "taskUri": rail.result(f"create_child_task_{task_name}"),
                    "resourceUris": [
                        rail.result(f"all_users_uri_{task_name}")
                    ],
                "isAssigned": "true"
            }),
        )

        create_user_list = rail.PythonOperator(
            task_id=f"create_user_list_{task_name}",
            python_callable=lambda dag_run: dag_run.conf["User Name"].split(
                '|') if "|" in dag_run.conf["User Name"] else dag_run.conf["User Name"]
        )

        add_users_to_task = rail.ForEachOperator(
            task_id=f"add_users_to_task_{task_name}",
            items=lambda: [rail.result(f"create_user_list_{task_name}")] if isinstance(rail.result(
                f"create_user_list_{task_name}"), str) else rail.result(f"create_user_list_{task_name}"),
            start_task=f"search_for_user_in_replicon_{task_name}",
            end_task=f"end_for_{task_name}"
        )

        search_for_user_in_replicon = rail.RepliconServiceOperator(
            task_id=f"search_for_user_in_replicon_{task_name}",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: json.dumps({
                "page": "1",
                "pagesize": "10",
                "columnUris": [
                    "urn:replicon:user-list-column:user"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": rail.result(f"add_users_to_task_{task_name}").split(",")[-1],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }),
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                list(map(lambda i: i["cells"][0], response["rows"])),
                "textValue",
                rail.result(f"add_users_to_task_{task_name}"), "uri")
        )

        if_user_exists_in_replicon = rail.IfOperator(
            task_id=f"if_user_exists_in_replicon_{task_name}",
            test=lambda: bool(rail.result(
                f"search_for_user_in_replicon_{task_name}")),
            yes_task=f"assign_users_to_project_{task_name}",
            no_task=f'insert_userdata_{task_name}'
        )

        insert_userdata = rail.SetVariableOperator(
            task_id=f'insert_userdata_{task_name}',
            append=True,
            name='userdata',
            value=lambda: rail.result(f"add_users_to_task_{task_name}")
        )

        assign_users_to_project = rail.RepliconServiceOperator(
            task_id=f"assign_users_to_project_{task_name}",
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data=lambda: json.dumps({
                "projectUri": rail.result("get_project_details"),
                "resourceUri": [
                    rail.result(f"search_for_user_in_replicon_{task_name}")
                ],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            })
        )

        assign_users_to_task = rail.RepliconServiceOperator(
            task_id=f"assign_users_to_task_{task_name}",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: json.dumps({
                    "taskUri": rail.result(f"create_child_task_{task_name}"),
                    "resourceUris": [
                        rail.result(f"search_for_user_in_replicon_{task_name}")
                    ],
                "isAssigned": "true"
            })
        )

        remove_all_user_assignment = rail.RepliconServiceOperator(
            task_id=f"remove_all_user_assignment_{task_name}",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: json.dumps({
                    "taskUri": rail.result(f"create_child_task_{task_name}"),
                    "resourceUris": [
                        "urn:replicon-tenant:" + rail.get_tenant_slug()+":department:1"
                    ],
                "isAssigned": "false"
            })
        )

        end_for = rail.EmptyOperator(task_id=f"end_for_{task_name}")

        if_user_not_available = rail.IfOperator(
            task_id=f"if_user_not_available_{task_name}",
            test=lambda: bool(rail.result(f"insert_userdata_{task_name}")),
            yes_task=f"step_log_unassigned_users_{task_name}",
            no_task=f"step_log_success_{task_name}"
        )
        step_log_unassigned_users = rail.WriteLogOperator(
            task_id=f"step_log_unassigned_users_{task_name}",
            log='{{dag_run.conf.lookuptable}}',
            severity="success with exceptions",
            message="""User(s): not present in Replicon""",
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "success with exceptions",
                "details": "User(s):" + str(rail.result(f"insert_userdata_{task_name}")["value"])+" not present in Replicon ",
                "childjobid": rail.render_template('{{ecid()}}'),
                # pylint:disable=line-too-long
                "Taskname": dag_run.conf["Task Name Level 1"] + "|" + dag_run.conf["Task Name Level 2"] if dag_run.conf["Task Name Level 2"] else dag_run.conf["Task Name Level 1"]
            }
        )

        step_log_success = rail.WriteLogOperator(
            task_id=f"step_log_success_{task_name}",
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

        create_child_task >>\
            move_task_under_parent >> update_child_task_name >>\
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
