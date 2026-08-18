from americanintegrated.project_import import request_payload
import rail
null = None


def create_new_project(config):
    with rail.TaskGroup(
        group_id=f"americanintegreated_create_project_{config.instance}",
        prefix_group_id=False
    ) as task_group:

        if_project_leader_code_present_create = rail.IfOperator(
            task_id="if_project_leader_code_present_create",
            test='{{dag_run.conf["projectleadercode"]| is_truthy}}',
            yes_task="search_project_leader_in_replicon_create",
            no_task="if_project_co_managercode_present_create"
        )

        search_project_leader_in_replicon_create = rail.RepliconServiceOperator(
            task_id="search_project_leader_in_replicon_create",
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

        if_project_co_managercode_present_create = rail.IfOperator(
            task_id="if_project_co_managercode_present_create",
            test='{{dag_run.conf["projectcomanagercode"]| is_truthy}}',
            yes_task="search_project_co_managerin_replicon_create",
            no_task="if_project_status_present_create"
        )

        search_project_co_managerin_replicon_create = rail.RepliconServiceOperator(
            task_id="search_project_co_managerin_replicon_create",
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

        if_project_status_present_create = rail.IfOperator(
            task_id="if_project_status_present_create",
            test='{{dag_run.conf.projectstatus| is_truthy}}',
            yes_task="if_project_status_completed_create",
            no_task="create_project"
        )

        if_project_status_completed_create = rail.IfOperator(
            task_id="if_project_status_completed_create",
            test=lambda dag_run: (
                "close" in dag_run.conf["projectstatus"].lower()),
            yes_task="write_project_closed_log",
            no_task="create_project"
        )

        write_project_closed_log = rail.WriteLogOperator(
            task_id="write_project_closed_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Status received as Close",
            severity="Skipped",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Project Name": dag_run.conf["projectname"],
                "Project Code": dag_run.conf["projectcode"],
                "Status": "Skipped",
                "Reason": "Status received as Close",
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        create_project = rail.RepliconServiceOperator(
            task_id="create_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.create_project_request,
            data_handler=lambda response: response["uri"] if response else null
        )

        update_time_entry_date_range = rail.RepliconServiceOperator(
            task_id="update_time_entry_date_range",
            endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data={
                "projectUri": '{{result("create_project")}}',
                "dateRange": null
            }
        )

        if_project_leader_in_replicon_create = rail.IfOperator(
            task_id="if_project_leader_in_replicon_create",
            test='{{result("search_project_leader_in_replicon_create")| is_truthy}}',
            no_task="if_comanager_present_in_replicon_create",
            yes_task="update_project_leader_create"
        )

        update_project_leader_create = rail.RepliconServiceOperator(
            task_id="update_project_leader_create",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data={
                    "projectUri": '{{result("create_project")}}',
                    "userUri": '{{result("search_project_leader_in_replicon_create")}}'
            }
        )

        if_comanager_present_in_replicon_create = rail.IfOperator(
            task_id="if_comanager_present_in_replicon_create",
            test='{{result("search_project_co_managerin_replicon_create")|is_truthy}}',
            yes_task="put_explicit_sharing_assignments_create",
            no_task="create_task"
        )

        put_explicit_sharing_assignments_create = rail.RepliconServiceOperator(
            task_id="put_explicit_sharing_assignments_create",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: {
                "projectUri": rail.result("create_project"),
                "sharedUris": [rail.result("search_project_co_managerin_replicon_create")]
            }
        )

        create_task = rail.TriggerDagRunOperator(
            task_id="create_task",
            trigger_dag_id= config.process_task_dag_id,
            wait_for_completion=True,
            conf={
                    "clientcode": '{{dag_run.conf.clientcode}}',
                    "clientname": '{{dag_run.conf.clientname}}',
                    "projectstatus": '{{dag_run.conf.projectstatus}}',
                    "projectcode": '{{dag_run.conf.projectcode}}',
                    "projectname": '{{dag_run.conf.projectname}}',
                    "projectleadercode": '{{dag_run.conf.projectleadercode}}',
                    "projectleadername": '{{dag_run.conf.projectleadername}}',
                    "prevailingwages": '{{dag_run.conf.prevailingwages}}',
                    "md5_reference": '{{dag_run.conf.md5_reference}}',
                    "projectcomanagercode": '{{dag_run.conf.projectcomanagercode}}',
                    "lookuptable": '{{dag_run.conf.lookuptable}}',
                    "parent_ecid": '{{dag_run.conf.parent_ecid}}',
                    "Prevailing wages DT uri": '{{dag_run.conf["Prevailing wages DT uri"]}}',
                    "Prevailing wages OT uri": '{{dag_run.conf["Prevailing wages OT uri"]}}',
                    "Prevailing wages RT uri": '{{dag_run.conf["Prevailing wages RT uri"]}}',
                    "projecturi": '{{result("create_project")}}',
                    "prevailing_wage_artifact": '{{dag_run.conf["prevailing_wage_artifact"]}}',
                    "basic_task_artifact": '{{dag_run.conf["basic_task_artifact"]}}'
            }
        )

        gather_failure_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_failure_logs",
            dag_runs="{{result('create_task')}}",
            dagrun_task_id="write_task_creation_error_log",
            flatten=True
        )

        if_no_failure_logs_present = rail.IfOperator(
            task_id="if_no_failure_logs_present",
            test='{{ result("gather_failure_logs") | is_falsy }}',
            yes_task="write_project_added_log"
        )

        write_project_added_log = rail.WriteLogOperator(
            task_id="write_project_added_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Project Updated",
            severity="Success",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Project Name": dag_run.conf["projectname"],
                "Project Code": dag_run.conf["projectcode"],
                "Status": "Success",
                "Reason": "Project Added",
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        if_project_leader_code_present_create >> rail.Label("Yes") >>\
                search_project_leader_in_replicon_create >> if_project_co_managercode_present_create
        if_project_leader_code_present_create >> rail.Label("No") >>\
            if_project_co_managercode_present_create >> rail.Label("Yes") >>\
                search_project_co_managerin_replicon_create >> if_project_status_present_create
        if_project_co_managercode_present_create >> rail.Label("No") >>\
            if_project_status_present_create >> rail.Label("Yes") >>\
                if_project_status_completed_create >> rail.Label("Yes") >>\
                    write_project_closed_log
        if_project_status_completed_create >> rail.Label("No") >>\
            create_project
        if_project_status_present_create >> rail.Label("No") >>\
            create_project >> update_time_entry_date_range >>\
                if_project_leader_in_replicon_create >> rail.Label("No") >>\
                    update_project_leader_create >> if_comanager_present_in_replicon_create
        if_project_leader_in_replicon_create >> rail.Label("No") >>\
            if_comanager_present_in_replicon_create >> rail.Label("Yes") >>\
                put_explicit_sharing_assignments_create >> create_task
        if_comanager_present_in_replicon_create >> rail.Label("No") >>\
            create_task >> gather_failure_logs >> if_no_failure_logs_present
        if_no_failure_logs_present >> rail.Label(
            "Yes") >> write_project_added_log

        return task_group
