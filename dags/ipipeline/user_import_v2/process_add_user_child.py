from datetime import timedelta
import json
from ipipeline.user_import_v2.utils import request_payload, custom_methods
from airflow.models import Variable
import rail

null = None


def create_add_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.add_user_child_dag_id,
        description="iPipeline Add User Child DAG - Creates new users in Replicon",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.add_user_child_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="is_supervisor_present"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="is_supervisor_present",
            end_task="catch_and_log_errors"
        )

        is_supervisor_present = rail.IfOperator(
            task_id='is_supervisor_present',
            test='{{ dag_run.conf.supervisor | is_truthy }}',
            yes_task='if_user_and_supervisor_same',
            no_task='create_new_user'
        )

        if_user_and_supervisor_same = rail.IfOperator(
            task_id='if_user_and_supervisor_same',
            test='{{ dag_run.conf.email == dag_run.conf.supervisor }}',
            yes_task='create_new_user',
            no_task='get_supervisor_details'
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": dag_run.conf.get("supervisor"),
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        if_supervisor_exists = rail.IfOperator(
            task_id="if_supervisor_exists",
            test='{{ result("get_supervisor_details") | is_truthy }}',
            yes_task="if_supervisor_permission_exists",
            no_task="if_supervisor_present_as_user_in_feed"
        )

        if_supervisor_present_as_user_in_feed = rail.IfOperator(
            task_id='if_supervisor_present_as_user_in_feed',
            test=lambda dag_run: dag_run.conf.get(
                "supervisor") in custom_methods.get_all_user_login_names_from_feed(dag_run),
            yes_task='pending_supervisor_flag',
            no_task='create_new_user'
        )

        pending_supervisor_flag = rail.SetVariableOperator(
            task_id='pending_supervisor_flag',
            name='pending_supervisor_flag',
            value='true'
        )

        if_supervisor_permission_exists = rail.IfOperator(
            task_id="if_supervisor_permission_exists",
            test=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_supervisor_details")["permissionSets"],
                                                              "displayText", config.defaults_mapper_data["supervisor_permission"], "uri"),
            yes_task="create_new_user",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: request_payload.get_assign_supervisor_permission_payload(
                config.defaults_mapper_data["supervisor_permission"])
        )

        create_new_user = rail.RepliconServiceOperator(
            task_id="create_new_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_user_creation_payload(
                dag_run, config)
        )

        if_orgrole_code_exists = rail.IfOperator(
            task_id='if_orgrole_code_exists',
            test='{{ dag_run.conf.calculated_orgrole_code | is_truthy }}',
            yes_task='get_resource_pool_from_replicon',
            no_task='if_timeoff_types_with_logic_to_be_assigned'
        )

        get_resource_pool_from_replicon = rail.RepliconServiceOperator(
            task_id='get_resource_pool_from_replicon',
            endpoint='/services/ResourcePoolService1.svc/GetPageOfAvailableResourcePoolFilteredBySearchParameter',
            data=request_payload.get_resource_pools_payload,
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, "displayText", dag_run.conf["calculated_orgrole_code"])
        )

        is_resource_pool_present_in_replicon = rail.IfOperator(
            task_id='is_resource_pool_present_in_replicon',
            test='{{ result("get_resource_pool_from_replicon") | is_truthy }}',
            yes_task='assign_resource_pool_to_user',
            no_task='if_timeoff_types_with_logic_to_be_assigned'
        )

        assign_resource_pool_to_user = rail.RepliconServiceOperator(
            task_id='assign_resource_pool_to_user',
            endpoint='/services/ResourcePoolService1.svc/UpdateUserResourcePoolAssignment',
            data=lambda: request_payload.get_assign_resource_pool_payload(
                rail.result("create_new_user")["user"]["uri"])
        )

        # if timeoff types with accrual logic need to be assigned
        if_timeoff_types_with_logic_to_be_assigned = rail.IfOperator(
            task_id='if_timeoff_types_with_logic_to_be_assigned',
            test=lambda dag_run: bool([timeoff_config.get("uri") for timeoff_config in dag_run.conf.get(
                "calculated_time_off_types", {}).values() if timeoff_config.get(
                    "reference_logic_type") in config.timeoffs_with_accrual_logic] if dag_run.conf.get("calculated_time_off_types") else []),
            yes_task='get_default_timeoff_policies',
            no_task='process_user_logging'
        )

        # Get default policies for all timeoff types before user creation
        get_default_timeoff_policies = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_default_timeoff_policies",
            items=lambda dag_run: [timeoff_config.get("uri") for timeoff_config in dag_run.conf.get("calculated_time_off_types", {}).values() if timeoff_config.get(
                "reference_logic_type") in config.timeoffs_with_accrual_logic] if dag_run.conf.get("calculated_time_off_types") else [],
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda item: {"timeOffTypeUri": item},
            data_handler=lambda response, item: {
                "timeOffUri": item,
                "defaultPolicy": response
            }
        )

        trigger_timeoff_with_logic_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_timeoff_with_logic_assignment",
            items=lambda dag_run: [timeoff_config for timeoff_config in dag_run.conf.get("calculated_time_off_types", {}).values() if timeoff_config.get(
                "reference_logic_type") in config.timeoffs_with_accrual_logic] if dag_run.conf.get("calculated_time_off_types") else [],
            trigger_dag_id=config.timeoff_with_logic_assignment_dag_id,
            conf=lambda dag_run, item: {
                "user_uri": rail.result("create_new_user")["user"]["uri"],
                "user_start_date": dag_run.conf.get("start_date"),
                "proration_effective_date": rail.parse_date(dag_run.conf.get('start_date'), config.REP_DATE_FORMAT),
                "current_date": dag_run.conf.get("current_date"),
                "default_policyset_schedule_for_timeoff": rail.find_first_by_attr_and_get_attr(rail.result(
                    "get_default_timeoff_policies"), "timeOffUri", item['uri'], 'defaultPolicy'),
                "timeoff_uri": item['uri'],
                "timeoff_type_name": item['timeoff_type_name'],
                "timeoff_reference_logic_type": item['reference_logic_type'],
                "employee_id": dag_run.conf.get("employee_id"),
                "seniority_level": dag_run.conf.get("seniority_level"),
                "schedule_hours": dag_run.conf.get("scheduled_hours"),
                "fte": dag_run.conf.get("fte"),
                "uksick": dag_run.conf.get("uksick"),
                "previous_existing_schedule_for_user": "",
                "previous_seniority_level": "",
                "is_only_seniority_level_changed": "",
                "existing_timeoff_policyset_schedule_for_timeoff": "",
                "action": "Add"
            },
        )

        wait_for_timeoff_with_logic_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_with_logic_assignment',
            dag_runs='{{ result("trigger_timeoff_with_logic_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_responses_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_child',
            dag_runs='{{ result("trigger_timeoff_with_logic_assignment") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        if_error_in_gather_reponse_from_timeoff_assignment_dag_runs = rail.IfOperator(
            task_id='if_error_in_gather_reponse_from_timeoff_assignment_dag_runs',
            test=lambda: bool(rail.result("gather_responses_from_child")) and "Error" in json.dumps(rail.result(
                "gather_responses_from_child")[0]),
            yes_task="fail_with_error_in_process_timeoffs_with_logic_assignment_dag",
            no_task="get_pending_supervisor_flag",
        )

        fail_with_error_in_process_timeoffs_with_logic_assignment_dag = rail.FailOperator(
            task_id='fail_with_error_in_process_timeoffs_with_logic_assignment_dag',
            message="Error in workflow for assignning timeoff types with logic"
        )

        get_pending_supervisor_flag = rail.GetVariableOperator(
            task_id='get_pending_supervisor_flag',
            name='pending_supervisor_flag'
        )

        if_pending_supervisor_flag = rail.IfOperator(
            task_id='if_pending_supervisor_flag',
            test=lambda: rail.result("get_pending_supervisor_flag")[
                "value"] == 'true',
            yes_task='write_supervisor_pending_logs',
            no_task='process_user_logging'
        )

        write_supervisor_pending_logs = rail.WriteLogOperator(
            task_id="write_supervisor_pending_logs",
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor",
            severity="Pending",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "supervisor": dag_run.conf["supervisor"],
                "action": "Add",
                "user_uri": rail.result("create_new_user")["user"]["uri"],
            }
        )

        process_user_logging = rail.EmptyOperator(
            task_id='process_user_logging'
        )

        if_user_created_with_errors = rail.IfOperator(
            task_id='if_user_created_with_errors',
            test=lambda: bool(rail.result("create_new_user")[
                              "errors"][0]["notifications"]) if rail.result("create_new_user")["errors"] else False,
            yes_task='write_added_user_with_exceptions_logs',
            no_task='write_added_user_logs'
        )

        write_added_user_with_exceptions_logs = rail.WriteLogOperator(
            task_id="write_added_user_with_exceptions_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda: "User partially created with errors - " +
                " | ".join([details["displayText"] for details in rail.result(
                    "create_new_user")["errors"][0]["notifications"]]),
            severity="Error",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": "Add",
                "status": "Error",
                "details": "User partially created with errors - " + " | ".join(
                    [details["displayText"] for details in rail.result("create_new_user")["errors"][0]["notifications"]])
                }
        )

        write_added_user_logs = rail.WriteLogOperator(
            task_id="write_added_user_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: "User created successfully" if not request_payload.get_exception_logs(dag_run, config) else
                ("User partially created - " +
                 " | ".join(request_payload.get_exception_logs(dag_run, config))),
            severity=lambda dag_run: "Success" if not request_payload.get_exception_logs(
                dag_run, config) else "Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": "Add",
                "status": "Success" if not request_payload.get_exception_logs(dag_run, config) else "Exception",
                "details": "User created successfully" if not request_payload.get_exception_logs(dag_run, config) else ("User partially created - " + " | ".join(
                    request_payload.get_exception_logs(dag_run, config)))
            }
        )

        finish_user_creation = rail.EmptyOperator(
            task_id='finish_user_creation'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.log_artifact }}',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule="one_failed",
            properties={
                "employeeid": '{{ dag_run.conf.employee_id }}',
                "action": "Add",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> is_supervisor_present
        is_supervisor_present >> rail.Label("No") >> create_new_user
        is_supervisor_present >> rail.Label(
            "Yes") >> if_user_and_supervisor_same
        if_user_and_supervisor_same >> rail.Label(
            "No") >> get_supervisor_details >> if_supervisor_exists
        if_user_and_supervisor_same >> rail.Label("Yes") >> create_new_user
        if_supervisor_exists >> rail.Label(
            "Yes") >> if_supervisor_permission_exists
        if_supervisor_exists >> rail.Label(
            "No") >> if_supervisor_present_as_user_in_feed

        if_supervisor_present_as_user_in_feed >> rail.Label(
            "Yes") >> pending_supervisor_flag >> create_new_user
        if_supervisor_present_as_user_in_feed >> rail.Label(
            "No") >> create_new_user

        if_supervisor_permission_exists >> rail.Label("Yes") >> create_new_user
        if_supervisor_permission_exists >> rail.Label(
            "No") >> assign_supervisor_permission >> create_new_user

        # After user creation - resource pool and timeoff processing
        create_new_user >> if_orgrole_code_exists

        # Org role code and resource pool flow
        if_orgrole_code_exists >> rail.Label(
            "Yes") >> get_resource_pool_from_replicon >> is_resource_pool_present_in_replicon
        if_orgrole_code_exists >> rail.Label(
            "No") >> if_timeoff_types_with_logic_to_be_assigned

        is_resource_pool_present_in_replicon >> rail.Label(
            "Yes") >> assign_resource_pool_to_user >> if_timeoff_types_with_logic_to_be_assigned
        is_resource_pool_present_in_replicon >> rail.Label(
            "No") >> if_timeoff_types_with_logic_to_be_assigned

        # Timeoff processing after user creation and resource pool assignment
        if_timeoff_types_with_logic_to_be_assigned >> rail.Label(
            "Yes") >> get_default_timeoff_policies
        if_timeoff_types_with_logic_to_be_assigned >> rail.Label(
            "No") >> process_user_logging

        get_default_timeoff_policies >> trigger_timeoff_with_logic_assignment >> wait_for_timeoff_with_logic_assignment >> gather_responses_from_child \
            >> if_error_in_gather_reponse_from_timeoff_assignment_dag_runs

        if_error_in_gather_reponse_from_timeoff_assignment_dag_runs >> rail.Label(
            "Yes") >> fail_with_error_in_process_timeoffs_with_logic_assignment_dag >> get_pending_supervisor_flag
        if_error_in_gather_reponse_from_timeoff_assignment_dag_runs >> rail.Label(
            "No") >> get_pending_supervisor_flag

        get_pending_supervisor_flag >> if_pending_supervisor_flag
        if_pending_supervisor_flag >> rail.Label(
            "Yes") >> write_supervisor_pending_logs >> process_user_logging
        if_pending_supervisor_flag >> rail.Label("No") >> process_user_logging

        process_user_logging >> if_user_created_with_errors

        # User creation logging flow
        if_user_created_with_errors >> rail.Label(
            "Yes") >> write_added_user_with_exceptions_logs >> finish_user_creation
        if_user_created_with_errors >> rail.Label(
            "No") >> write_added_user_logs >> finish_user_creation

        finish_user_creation >> catch_and_log_errors

        return dag


# Create child DAG for each instance
rail.for_each_instance(create_add_user_child_dag)
