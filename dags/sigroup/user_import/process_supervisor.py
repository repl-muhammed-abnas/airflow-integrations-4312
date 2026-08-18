import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_process_supervisor_dagid,
       description="sigroup user import supervisor check child",
        max_active_runs=config.child_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        search_user_in_replicon = rail.RepliconServiceOperator(
            task_id="search_user_in_replicon",
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": "{{ dag_run.conf.initialsupervisorloginname }}",
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: {
                'uri': response[0]['userDetails']['uri'],
                'login_name': response[0]['securityConfiguration']['loginName'],
                'status': response[0]['userDetails']['isEnabled'],
            } if response and response[0] and 'userDetails' in response[0] else []
        )

        if_user_and_supervisor_different = rail.IfOperator(
            task_id="if_user_and_supervisor_different",
            test=lambda dag_run: bool(
                dag_run.conf["initialsupervisorloginname"] != dag_run.conf["loginname"] or \
                    not rail.result("search_user_in_replicon") or \
                    rail.result("search_user_in_replicon")["status"] == "false"),
            yes_task="if_supervisor_uri_present",
            no_task="write_log_supervisor_not_valid"
        )

        if_supervisor_uri_present = rail.IfOperator(
            task_id="if_supervisor_uri_present",
            test=lambda : bool(
                "uri" in rail.result("search_user_in_replicon")),
            yes_task="get_permission_set_for_user",
            no_task="write_log_supervisor_not_available"
        )

        get_permission_set_for_user = rail.RepliconServiceOperator(
            task_id="get_permission_set_for_user",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": '{{result("search_user_in_replicon")["uri"]}}'
            }
        )

        if_permission_set_assigned = rail.IfOperator(
            task_id="if_permission_set_assigned",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                rail.result("get_permission_set_for_user")[0]["permissionSet"],
                "displayText",
                "Manager",
                "uri")),
            yes_task="if_add_action",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": '{{result("search_user_in_replicon").uri}}',
                "permissionSetUri": '{{dag_run.conf.supervisorpermissionuri}}'
            }
        )

        if_add_action = rail.IfOperator(
            task_id="if_add_action",
            test=lambda dag_run: bool(dag_run.conf["action"].lower() != "data change"),
            yes_task="update_initial_supervisor",
            no_task="if_update_action"
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id="update_initial_supervisor",
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": '{{dag_run.conf.useruri}}',
                "initialSupervisorUri": '{{result("search_user_in_replicon").uri}}',
                "scheduleEntries": []
            }
        )

        if_update_action = rail.IfOperator(
            task_id="if_update_action",
            test=lambda dag_run: bool(
                dag_run.conf["action"].lower() == "data change"),
            yes_task="update_supervisor_schedule",
            no_task="write_log_supervisor_failed"
        )

        update_supervisor_schedule = rail.RepliconServiceOperator(
            task_id="update_supervisor_schedule",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "supervisorUri": rail.result("search_user_in_replicon")["uri"],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["actioneffectivedate"], "%m/%d/%Y"),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        write_log_supervisor_not_valid = rail.WriteLogOperator(
            task_id="write_log_supervisor_not_valid",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor is not updated",
            severity="Exception",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Add",
                "Status": "Exception",
                "Details": "Supervisor is not updated as the Login name for user and supervisor is same on the input file",
                
            }
        )

        write_log_supervisor_not_available = rail.WriteLogOperator(
            task_id="write_log_supervisor_not_available",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor is not updated",
            severity="Exception",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Add",
                "Status": "Exception",
                "Details": "Supervisor is not updated as the supervisor with login name " +\
                    dag_run.conf["initialsupervisorloginname"] + " is not available",
                
            }
        )

        write_log_supervisor_failed = rail.WriteLogOperator(
            task_id="write_log_supervisor_failed",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor is not updated",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Supervisor",
                "Status": "Error",
                "Details": "Supervisor assignmen failed "+rail.render_template('{{get_error_message()}}'),
                
            }
        )

        search_user_in_replicon >>\
            if_user_and_supervisor_different >> rail.Label("No") >>\
            write_log_supervisor_not_valid >> write_log_supervisor_failed
        if_user_and_supervisor_different >> rail.Label("Yes") >>\
            if_supervisor_uri_present >> rail.Label("No") >>\
            write_log_supervisor_not_available >> write_log_supervisor_failed
        if_supervisor_uri_present >> rail.Label("Yes") >> get_permission_set_for_user >>\
            if_permission_set_assigned >> rail.Label("No") >>\
            assign_supervisor_permission >> if_add_action
        if_permission_set_assigned >> rail.Label("Yes") >>\
            if_add_action >> rail.Label("Yes") >> update_initial_supervisor >>\
            write_log_supervisor_failed
        if_add_action >> rail.Label("No") >>\
        if_update_action >>\
        rail.Label("No") >> write_log_supervisor_failed
        if_update_action >>\
        rail.Label("Yes") >> update_supervisor_schedule >>\
        write_log_supervisor_failed

        return dag


rail.for_each_instance(create_airflow_dag)
