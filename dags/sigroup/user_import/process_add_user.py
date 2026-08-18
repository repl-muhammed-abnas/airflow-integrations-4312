import json
from sigroup.user_import.utils import request_payload, custom_methods
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_add_user_dag_id,
       description="sigroup user import add user child",
        max_active_runs=config.child_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        create_user = rail.RepliconServiceOperator(
            task_id="create_user",
            endpoint="/services/importservice1.svc/PutUser3",
            data=request_payload.get_add_user_payload,
        )

        unassign_all_defualt_timeoff = rail.RepliconServiceOperator(
            task_id="unassign_all_defualt_timeoff",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": '{{result("create_user").uri}}',
                "timeOffTypeUris": []
            }
        )

        if_initial_supervisor_loginname = rail.IfOperator(
            task_id="if_initial_supervisor_loginname",
            test=lambda dag_run: bool(
                dag_run.conf["initialsupervisorloginname"]),
            yes_task="bulk_get_supervisor_details",
            no_task="if_timeoff_type_present"
        )

        bulk_get_supervisor_details = rail.RepliconServiceOperator(
            task_id="bulk_get_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": '{{dag_run.conf.initialsupervisorloginname}}',
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: {"uri": response[0]["userDetails"]["uri"],
                                           "isenabled": response[0]["userDetails"]["isEnabled"],
                                           "permissionsets": response[0]["permissionSets"]}
            if response and response[0] and "userDetails" in response[0]
            and "permissionSets" in response[0] else null
        )

        if_user_enabled = rail.IfOperator(
            task_id="if_user_enabled",
            test=lambda: bool(rail.result(
                "bulk_get_supervisor_details") and rail.result(
                "bulk_get_supervisor_details")["isenabled"]),
            yes_task="if_supervisor_permission_assigned",
            no_task="write_pending_supervisor_log"
        )

        if_supervisor_permission_assigned = rail.IfOperator(
            task_id="if_supervisor_permission_assigned",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                rail.result("bulk_get_supervisor_details")["permissionsets"],
                "displayText",
                "Supervisor",
                "uri"
            )),
            yes_task="assign_supervisor_for_user",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                    "userUri": '{{result("bulk_get_supervisor_details").uri}}',
                    "permissionSetUri": '{{dag_run.conf.supervisorpermissionuri}}'
            }
        )

        assign_supervisor_for_user = rail.RepliconServiceOperator(
            task_id="assign_supervisor_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                    "userUri": '{{result("create_user").uri}}',
                    "supervisorUri": '{{result("bulk_get_supervisor_details").uri}}',
                    "dateRange": null
            }
        )

        write_pending_supervisor_log = rail.WriteLogOperator(
            task_id="write_pending_supervisor_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Pending supervisor logs",
            severity="Pending",
            properties=lambda dag_run: {
                **dag_run.conf,
                "useruri": rail.result("create_user")["uri"]
            }
        )

        if_timeoff_type_present = rail.IfOperator(
            task_id="if_timeoff_type_present",
            test=lambda dag_run: bool(dag_run.conf["timeofftypes"]),
            yes_task="start_timeoff_process",
            no_task="if_exceptions"
        )

        start_timeoff_process = rail.EmptyOperator(
            task_id="start_timeoff_process")
        
        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id="get_enabled_timeoff_types",
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        get_timeoff_types_data = rail.PythonOperator(
            task_id="get_timeoff_types_data",
            python_callable=lambda dag_run: list(filter(lambda i : i is not None, map(lambda i:
                rail.find_first_by_attr_and_get_attr(
                rail.result("get_enabled_timeoff_types"),
                "displayText",
                i,
                "uri"), list(dag_run.conf["timeofftypes"].split("|")))))
        )

        assign_required_timeoff_types = rail.RepliconServiceOperator(
            task_id="assign_required_timeoff_types",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda :{
                "userUri": rail.result("create_user")["uri"],
                "timeOffTypeUris": rail.result("get_timeoff_types_data")
            }
        )

        get_default_timeoff_policies_for_user = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_default_timeoff_policies_for_user",
            items='{{result("get_timeoff_types_data")|to_json}}',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": '{{result("create_user").uri}}',
                    "timeOffTypeUri": '{{item}}'
                }
            }
        )

        assign_default_timeoff_policies = rail.RepliconServiceCallForEachItemOperator(
            task_id="assign_default_timeoff_policies",
            items='{{result("get_timeoff_types_data")|to_json}}',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda item,dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("create_user")["uri"],
                    "timeOffTypeUri": item
                },
                "policySetScheduleEntries": json.loads(json.dumps(rail.result('get_default_timeoff_policies_for_user')
                                                                  [rail.result("get_timeoff_types_data").index(item)])
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
            }
        )


        if_exceptions = rail.IfOperator(
            task_id="if_exceptions",
            test=lambda dag_run: bool(custom_methods.get_excpetion_logs(dag_run)),
            yes_task="write_log_exceptions_log",
            no_task="write_log_user_created_successfully"
        )

        write_log_exceptions_log = rail.WriteLogOperator(
            task_id="write_log_exceptions_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Exceptions for user attributes",
            severity="Exception",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Add",
                "Status": "Exception",
                "Details": "User created partially - " + custom_methods.get_excpetion_logs(dag_run),
                
            }
        )

        write_log_user_created_successfully = rail.WriteLogOperator(
            task_id="write_log_user_created_successfully",
            log='{{dag_run.conf.lookuptable}}',
            message="User created successfully",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Add",
                "Status": "Success",
                "Details": "User created successfully",
                
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="User created failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Add",
                "Status": "Error",
                "Details": rail.render_template('{{get_error_message()}}'),
                
            }
        )

        get_all_permission_sets >> create_user >> unassign_all_defualt_timeoff >>\
        if_initial_supervisor_loginname >> rail.Label("Yes") >>\
        bulk_get_supervisor_details >>\
        if_user_enabled >> rail.Label("Yes") >>\
        if_supervisor_permission_assigned >> rail.Label("Yes") >>\
        assign_supervisor_for_user
        if_supervisor_permission_assigned >> rail.Label("No") >>\
            assign_supervisor_permission >> assign_supervisor_for_user >>\
            if_timeoff_type_present
        if_user_enabled >> rail.Label("No") >> write_pending_supervisor_log >> if_timeoff_type_present
        if_initial_supervisor_loginname >> rail.Label("No") >>\
            if_timeoff_type_present >> rail.Label("Yes") >> start_timeoff_process >>\
        get_enabled_timeoff_types >>\
        get_timeoff_types_data >>\
        assign_required_timeoff_types >>\
        get_default_timeoff_policies_for_user >> assign_default_timeoff_policies >> if_exceptions
        if_timeoff_type_present >> rail.Label("No") >>\
            if_exceptions >> rail.Label("Yes") >>\
            write_log_exceptions_log >> catch_and_log_errors
        if_exceptions >> rail.Label("No") >>\
            write_log_user_created_successfully >>\
            catch_and_log_errors

        return dag


rail.for_each_instance(create_airflow_dag)
