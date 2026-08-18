from americanintegrated.project_import.custom_methods import get_project_update_status
import rail
null = None

# pylint: disable=too-many-statements


def update_existing_project(config):
    with rail.TaskGroup(
        group_id=f"americanintegreated_update_project_{config.instance}",
        prefix_group_id=False
    ) as task_group:

        create_status_variable = rail.SetVariableOperator(
            task_id="create_status_variable",
            name="update_var",
            value="no",
            append=False
        )

        if_project_leader_code_present = rail.IfOperator(
            task_id="if_project_leader_code_present",
            test='{{dag_run.conf["projectleadercode"]| is_truthy}}',
            yes_task="search_project_leader_in_replicon",
            no_task="if_project_co_managercode_present"
        )

        search_project_leader_in_replicon = rail.RepliconServiceOperator(
            task_id="search_project_leader_in_replicon",
            endpoint="/services/UserService1.svc/GetUser2",
            data=lambda dag_run: {
                    "user": {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["projectleadercode"],
                        "parameterCorrelationId": null
                    }
            },
            data_handler=lambda response: response["uri"] if "uri" in response else null
        )

        if_project_co_managercode_present = rail.IfOperator(
            task_id="if_project_co_managercode_present",
            test='{{dag_run.conf["projectcomanagercode"]| is_truthy}}',
            yes_task="search_project_co_managerin_replicon",
            no_task="get_project_details"
        )

        search_project_co_managerin_replicon = rail.RepliconServiceOperator(
            task_id="search_project_co_managerin_replicon",
            endpoint="/services/UserService1.svc/GetUser2",
            data=lambda dag_run: {
                    "user": {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["projectcomanagercode"],
                        "parameterCorrelationId": null
                    }
            },
            data_handler=lambda response: response["uri"] if "uri" in response else null
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: {
                    "projects": [
                        {
                            "uri": null,
                            "name": null,
                            "code": dag_run.conf["projectcode"],
                            "parameterCorrelationId": null
                        }
                    ]
            },
            data_handler=lambda response: {
                "name": response[0]["projectDetails"]["name"],
                "status": response[0]["projectDetails"]["status"]["displayText"],
                "uri": response[0]["projectDetails"]["uri"],
                "prevailingwage": rail.find_first_by_attr_and_get_attr(
                    list(
                        map(lambda i: i["customField"], response[0]["projectDetails"]["customFields"])),
                    "displayText",
                    "Prevailing Wage",
                    "uri"),
                "projectleaderuri": response[0]["projectDetails"]["projectLeader"]["user"]["uri"] if response[0]["projectDetails"]["projectLeader"] else None
            } if response[0]["projectDetails"] else null
        )

        check_if_project_name_updated = rail.IfOperator(
            task_id="check_if_project_name_updated",
            test=lambda dag_run: (rail.result("get_project_details") and
                                  rail.result("get_project_details")["name"] != dag_run.conf["projectcode"] + " - " + dag_run.conf["projectname"]),
            yes_task="update_project_name",
            no_task="if_project_status_present"
        )

        update_project_name = rail.RepliconServiceOperator(
            task_id="update_project_name",
            endpoint="/services/ProjectService1.svc/UpdateName",
            data={
                    "projectUri": '{{result("get_project_details").uri}}',
                    "name": '{{dag_run.conf["projectcode"] + " - " +dag_run.conf["projectname"]}}'
            }
        )

        if_project_status_present = rail.IfOperator(
            task_id="if_project_status_present",
            test='{{dag_run.conf.projectstatus| is_truthy}}',
            yes_task="if_project_status_progress",
            no_task="empty_prevailing_wages_present"
        )

        empty_prevailing_wages_present = rail.EmptyOperator(
            task_id="empty_prevailing_wages_present")

        if_project_status_progress = rail.IfOperator(
            task_id="if_project_status_progress",
            test=lambda dag_run: ("progress" in dag_run.conf["projectstatus"].lower() and rail.result("get_project_details")
                                  and "progress" not in rail.result("get_project_details")["status"].lower()),
            yes_task="update_project_status_progress",
            no_task="if_project_status_completed"
        )

        update_project_status_progress = rail.RepliconServiceOperator(
            task_id="update_project_status_progress",
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data={
                    "projectUri": '{{result("get_project_details").uri}}',
                    "projectStatusUri": "urn:replicon:project-status-type:in-progress"
            }
        )

        if_project_status_completed = rail.IfOperator(
            task_id="if_project_status_completed",
            test=lambda dag_run: ("close" in dag_run.conf["projectstatus"].lower() and rail.result("get_project_details")
                                  and "completed" not in rail.result("get_project_details")["status"].lower()),
            yes_task="update_project_status_closed",
            no_task="empty_prevailing_wages_present"
        )

        update_project_status_closed = rail.RepliconServiceOperator(
            task_id="update_project_status_closed",
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data={
                    "projectUri": '{{result("get_project_details").uri}}',
                    "projectStatusUri": "urn:replicon:project-status-type:completed"
            }
        )

        if_prevailing_wages_present = rail.IfOperator(
            task_id="if_prevailing_wages_present",
            test='{{dag_run.conf.prevailingwages | is_truthy}}' and
                    '{{result("get_project_details")["prevailingwage"] != dag_run.conf.prevailingwages}}',
            yes_task="get_prevailing_wage_drop_down_options",
            no_task="if_project_leader_updated"
        )

        get_prevailing_wage_drop_down_options = rail.RepliconServiceOperator(
            task_id="get_prevailing_wage_drop_down_options",
            endpoint="services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                    "customFieldUri": '{{result("get_project_details")["prevailingwage"]}}'
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                dag_run.conf["prevailingwages"],
                "uri")
        )

        update_prevailing_wages = rail.RepliconServiceOperator(
            task_id="update_prevailing_wages",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                    "objectUri": rail.result("get_project_details")["uri"],
                    "customFieldUri": dag_run.conf["prevailingwageuri"],
                    "customFieldDropDownOptionUri": rail.result("get_prevailing_wage_drop_down_options")
            }
        )

        if_project_leader_updated = rail.IfOperator(
            task_id="if_project_leader_updated",
            test=lambda: (rail.result("get_project_details")[
                "projectleaderuri"] != rail.result("search_project_leader_in_replicon")) if rail.result(
                    "search_project_leader_in_replicon") else None,
            no_task="if_comanager_present_in_replicon",
            yes_task="update_project_leader"
        )

        update_project_leader = rail.RepliconServiceOperator(
            task_id="update_project_leader",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data={
                    "projectUri": '{{result("get_project_details").uri}}',
                    "userUri": '{{result("search_project_leader_in_replicon")}}'
            }
        )

        if_comanager_present_in_replicon = rail.IfOperator(
            task_id="if_comanager_present_in_replicon",
            test='{{result("search_project_co_managerin_replicon")|is_truthy}}',
            yes_task="get_explicit_sharing_assignments",
            no_task="if_project_updated"
        )

        get_explicit_sharing_assignments = rail.RepliconServiceOperator(
            task_id="get_explicit_sharing_assignments",
            endpoint="/services/ProjectService1.svc/GetExplicitSharingAssignments",
            data={
                    "projectUri": '{{result("get_project_details")["uri"]}}'
            },
            data_handler=lambda response: list(
                map(lambda i: i["user"]["uri"], response))
        )

        if_no_sharing_assignments_for_comanager = rail.IfOperator(
            task_id="if_no_sharing_assignments_for_comanager",
            test=lambda: (rail.result("search_project_co_managerin_replicon")
                          not in rail.result("get_explicit_sharing_assignments")),
            yes_task="put_explicit_sharing_assignments",
            no_task="if_project_updated"
        )

        put_explicit_sharing_assignments = rail.RepliconServiceOperator(
            task_id="put_explicit_sharing_assignments",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: {
                "projectUri": rail.result("get_project_details")["uri"],
                "sharedUris": [rail.result("search_project_co_managerin_replicon")] +
                rail.result("get_explicit_sharing_assignments")
            }
        )

        if_project_updated = rail.IfOperator(
            task_id="if_project_updated",
            test=bool(get_project_update_status),
            yes_task="write_project_updated_log",
            no_task="write_no_update_to_project_log"
        )

        write_project_updated_log = rail.WriteLogOperator(
            task_id="write_project_updated_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Project Updated",
            severity="Success",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Project Name": dag_run.conf["projectname"],
                "Project Code": dag_run.conf["projectcode"],
                "Status": "Success",
                "Reason": "Project Updated",
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        write_no_update_to_project_log = rail.WriteLogOperator(
            task_id="write_no_update_to_project_log",
            log='{{dag_run.conf.lookuptable}}',
            message="No Change in Project data",
            severity="Skipped",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Project Name": dag_run.conf["projectname"],
                "Project Code": dag_run.conf["projectcode"],
                "Status": "Skipped",
                "Reason": "No Change in Project data",
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        create_status_variable >>\
            if_project_leader_code_present >> rail.Label("Yes") >>\
            search_project_leader_in_replicon >> if_project_co_managercode_present
        if_project_leader_code_present >> rail.Label("No") >>\
            if_project_co_managercode_present >> rail.Label("Yes") >>\
            search_project_co_managerin_replicon >> get_project_details
        if_project_co_managercode_present >> rail.Label("No") >>\
            get_project_details >>\
            check_if_project_name_updated >> rail.Label(
                "No") >> if_project_status_present
        check_if_project_name_updated >> rail.Label("Yes") >>\
            update_project_name >>\
            if_project_status_present >> rail.Label("Yes") >>\
            if_project_status_progress >> rail.Label("Yes") >>\
            update_project_status_progress >> empty_prevailing_wages_present
        if_project_status_progress >> rail.Label("No") >>\
            if_project_status_completed >> rail.Label("Yes") >>\
            update_project_status_closed >> empty_prevailing_wages_present
        if_project_status_present >> rail.Label(
            "No") >> empty_prevailing_wages_present
        if_project_status_completed >> rail.Label("No") >> empty_prevailing_wages_present >>\
            if_prevailing_wages_present >> rail.Label("Yes") >>\
            get_prevailing_wage_drop_down_options >> update_prevailing_wages >>\
            if_project_leader_updated
        if_prevailing_wages_present >> rail.Label("No") >>\
            if_project_leader_updated >> rail.Label("Yes") >>\
            update_project_leader >> if_comanager_present_in_replicon
        if_project_leader_updated >> rail.Label("No") >>\
            if_comanager_present_in_replicon >> rail.Label("Yes") >>\
            get_explicit_sharing_assignments >>\
            if_no_sharing_assignments_for_comanager >> rail.Label("Yes") >>\
            put_explicit_sharing_assignments >> if_project_updated
        if_no_sharing_assignments_for_comanager >> rail.Label(
            "No") >> if_project_updated
        if_comanager_present_in_replicon >> rail.Label("No") >>\
            if_project_updated >> rail.Label("Yes") >>\
            write_project_updated_log
        if_project_updated >> rail.Label("No") >>\
            write_no_update_to_project_log

        return task_group
