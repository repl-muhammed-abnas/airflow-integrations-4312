from datetime import timedelta
from dxctechnology.fieldglass_workorder_import.utils import custom_methods, request_payload
from airflow.models import Variable
import rail
null = None

# pylint: disable=too-many-statements

def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_gsap_user_details_dag_id,
        description=" update user details",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_process_child_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "get_bulk_user_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout),
            start_task='get_bulk_user_details',
            end_task="write_update_user_fail_log",
        )

        get_bulk_user_details = rail.RepliconServiceOperator(
            task_id="get_bulk_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                    "users": [
                        {
                            "uri": '{{dag_run.conf.useruri}}',
                            "loginName": null,
                            "parameterCorrelationId": null
                        }
                    ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            data_handler=custom_methods.get_gsap_user_details
        )

        if_user_enabled = rail.IfOperator(
            task_id="if_user_enabled",
            test=lambda:bool(rail.result("get_bulk_user_details")["isenabled"] == "True"),
            yes_task="if_permission_assigned",
            no_task="enable_user_in_replicon"
        )

        enable_user_in_replicon = rail.RepliconServiceOperator(
            task_id="enable_user_in_replicon",
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                    "userUri": '{{dag_run.conf.useruri}}'
            }
        )

        if_permission_assigned = rail.IfOperator(
            task_id="if_permission_assigned",
            test=lambda:bool(rail.result("get_bulk_user_details")["permission_sets"]),
            yes_task="if_perner_exists",
            no_task="assign_permission_set"
        )

        assign_permission_set = rail.RepliconServiceOperator(
            task_id="assign_permission_set",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data={
                    "user": {
                        "uri": '{{dag_run.conf.useruri}}'
                    },
                "modifications": {
                        "permissionSetsToApply": {
                            "permissionSetUrisToAssign": [
                                '{{dag_run.conf.contingentworkercontractorpermission}}'
                            ]
                        }
                        },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_perner_exists = rail.IfOperator(
            task_id="if_perner_exists",
            test=lambda dag_run:bool(rail.result("get_bulk_user_details")["perner"] == dag_run.conf["GHR_personnel_number"]),
            yes_task="if_workorder_id_exists",
            no_task="update_perner"
        )

        update_perner = rail.RepliconServiceOperator(
            task_id="update_perner",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_perner_update_gsap
        )

        if_workerorder_id_exists = rail.IfOperator(
            task_id="if_workorder_id_exists",
            test=lambda dag_run:bool(rail.result("get_bulk_user_details")["workorderid"] == dag_run.conf["WorkOrderID"]),
            yes_task="if_timesheet_value_present",
            no_task="update_workorderid"
        )

        update_workorderid = rail.RepliconServiceOperator(
            task_id="update_workorderid",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_workorderid_update_gsap
        )

        if_timesheet_value_present = rail.IfOperator(
            task_id="if_timesheet_value_present",
            test=lambda dag_run:bool(dag_run.conf["timesheettemplatetoassign"] and dag_run.conf["Timesheeettemplateuri"]),
            yes_task="if_timesheet_template_updated",
            no_task="if_timesheet_approval_path_updated"
        )

        if_timesheet_template_updated = rail.IfOperator(
            task_id="if_timesheet_template_updated",
            test=lambda dag_run:bool(dag_run.conf["timesheettemplatetoassign"] != dag_run.conf["Timesheeettemplateuri"]),
            yes_task="assign_policy_set_to_user",
            no_task="if_timesheet_approval_path_updated"
        )

        assign_policy_set_to_user = rail.RepliconServiceOperator(
            task_id="assign_policy_set_to_user",
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                    "userUri": '{{dag_run.conf.useruri}}',
                    "policySetUri": '{{dag_run.conf.Timesheeettemplateuri}}'
            }
        )

        if_timesheet_approval_path_updated = rail.IfOperator(
            task_id="if_timesheet_approval_path_updated",
            test=lambda dag_run:bool(dag_run.conf["timesheetapprovalpath"] != "GSAP Aus Contractor"),
            yes_task="update_timesheet_approval_path",
            no_task="if_workweek_updated"
        )

        update_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id="update_timesheet_approval_path",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_timesheet_approval_gsap
        )

        if_workweek_updated = rail.IfOperator(
            task_id="if_workweek_updated",
            test=lambda dag_run:bool(dag_run.conf["workweek"] != dag_run.conf["workweektoassign"]),
            yes_task="update_user_workweek",
            no_task="get_effective_user_membership"
        )

        update_user_workweek = rail.RepliconServiceOperator(
            task_id="update_user_workweek",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_gsap_workweek_request
        )

        get_effective_user_membership = rail.RepliconServiceOperator(
            task_id="get_effective_user_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                    "userUri": '{{dag_run.conf.useruri}}',
                    "dateRange": null
            },
            data_handler=lambda response: response.get("divisions",[{}])[0].get("division",{}).get(
                "division", {}).get("displayText") if "divisions" in response and response["divisions"] else null
        )

        if_company_code_update = rail.IfOperator(
            task_id="if_company_code_update",
            test=lambda dag_run: bool(rail.result("get_effective_user_membership") != dag_run.conf["actual_company_code_value"]
                                      and dag_run.conf["companycodeuri"]),
            yes_task="apply_user_modification_division",
            no_task="if_company_code_present"
        )

        apply_user_modification_division = rail.RepliconServiceOperator(
            task_id="apply_user_modification_division",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_division_update_request_gsap
        )

        if_company_code_present = rail.IfOperator(
            task_id="if_company_code_present",
            test='{{dag_run.conf.companycodeuri | is_truthy}}',
            yes_task="put_division_schedule",
            no_task="if_costcenter_update"
        )

        put_division_schedule = rail.RepliconServiceOperator(
            task_id="put_division_schedule",
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=request_payload.get_division_assignment_gsap
        )

        if_costcenter_update = rail.IfOperator(
            task_id="if_costcenter_update",
            test=lambda dag_run:bool(rail.result("get_effective_user_membership") != dag_run.conf["CostCenterCode"]
                                     and dag_run.conf["costcenteruri"]),
            yes_task="apply_user_modification_costcenter",
            no_task="if_costcenter_present"
        )

        apply_user_modification_costcenter = rail.RepliconServiceOperator(
            task_id="apply_user_modification_costcenter",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_costcenter_update_request_gsap
        )

        if_costcenter_present = rail.IfOperator(
            task_id="if_costcenter_present",
            test='{{dag_run.conf.costcenteruri | is_truthy}}',
            yes_task="put_costcenter_schedule",
            no_task="if_timesheet_period_updated"
        )

        put_costcenter_schedule = rail.RepliconServiceOperator(
            task_id="put_costcenter_schedule",
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=request_payload.get_costcenter_assignment_gsap
        )

        if_timesheet_period_updated = rail.IfOperator(
            task_id="if_timesheet_period_updated",
            test=lambda dag_run:bool(dag_run.conf["timesheetperiod"] !=
                                     "Weekly - Starting Saturday - CSC Contractors, US and Canada employees"),
            yes_task="update_timesheet_period",
            no_task="update_user_activities"
        )

        update_timesheet_period = rail.RepliconServiceOperator(
            task_id="update_timesheet_period",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_timesheet_period_value_gsap
        )

        update_user_activities = rail.RepliconServiceOperator(
            task_id="update_user_activities",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run:request_payload.get_activities_request_gsap(dag_run, config)
        )

        write_update_user_fail_log = rail.WriteLogOperator(
            task_id="write_update_user_fail_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Error",
            message='{{get_error_message()}}',
            trigger_rule="one_failed",
            properties={
                "workorderid": '{{dag_run.conf.WorkOrderID}}',
                "ContingentWorkerID": '{{dag_run.conf.ContingentWorkerID}}',
                "WO_GHRPersonnelNumber": '{{dag_run.conf.WO_GHRPersonnelNumber}}',
                "status": "Error",
                "details": '{{get_error_message()}}',
                "Action": "User_attributes_update"
            }
        )

        write_update_user_success_log = rail.WriteLogOperator(
            task_id="write_update_user_success_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="success",
            message="Updated user attributes related to Work order in Replicon",
            properties={
                "workorderid": '{{dag_run.conf.WorkOrderID}}',
                "ContingentWorkerID": '{{dag_run.conf.ContingentWorkerID}}',
                "WO_GHRPersonnelNumber": '{{dag_run.conf.WO_GHRPersonnelNumber}}',
                "status": "success",
                "details": 'Updated user attributes related to Work order in Replicon',
                "Action": "User_attributes_update"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            trigger_rule="all_done",
            sumo_conn_id="sumologic-dagrunlogger"
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> write_update_user_fail_log >>log_to_sumo
        can_run_batch_task >> rail.Label("No") >>\
        get_bulk_user_details >> if_user_enabled >> rail.Label("No") >>\
        enable_user_in_replicon >> if_permission_assigned
        if_user_enabled >> rail.Label("Yes") >>\
        if_permission_assigned >> rail.Label("Yes") >> if_perner_exists
        if_permission_assigned >> rail.Label("No") >>\
        assign_permission_set >> if_perner_exists >> rail.Label("No") >>\
        update_perner >> if_workerorder_id_exists
        if_perner_exists >> rail.Label("Yes") >> \
        if_workerorder_id_exists >> rail.Label("No") >>\
        update_workorderid >> if_timesheet_value_present
        if_workerorder_id_exists >> rail.Label("Yes") >>\
        if_timesheet_value_present >> rail.Label("Yes") >>\
        if_timesheet_template_updated >> rail.Label("Yes") >>\
        assign_policy_set_to_user >> if_timesheet_approval_path_updated
        if_timesheet_template_updated >> rail.Label("No")>>\
        if_timesheet_approval_path_updated
        if_timesheet_value_present >> rail.Label("No") >>\
        if_timesheet_approval_path_updated >> rail.Label("Yes") >>\
        update_timesheet_approval_path >> if_workweek_updated
        if_timesheet_approval_path_updated >> rail.Label("No") >>\
        if_workweek_updated >> rail.Label("Yes") >> update_user_workweek >>\
        get_effective_user_membership
        if_workweek_updated >> rail.Label("No") >>\
        get_effective_user_membership >>\
        if_company_code_update >> rail.Label("Yes") >>\
        apply_user_modification_division >> if_costcenter_update
        if_company_code_update >> rail.Label("No") >>\
        if_company_code_present >> rail.Label("No") >> if_costcenter_update
        if_company_code_present >> rail.Label("Yes") >>\
        put_division_schedule >>\
        if_costcenter_update >> rail.Label("Yes") >>\
        apply_user_modification_costcenter >> if_timesheet_period_updated
        if_costcenter_update >> rail.Label("No") >>\
        if_costcenter_present >> rail.Label("No") >> if_timesheet_period_updated
        if_costcenter_present >> rail.Label("Yes") >>\
        put_costcenter_schedule >>\
        if_timesheet_period_updated >> rail.Label("Yes") >>\
        update_timesheet_period >> update_user_activities
        if_timesheet_period_updated >> rail.Label("No") >>\
        update_user_activities >>\
        write_update_user_success_log >> write_update_user_fail_log >> log_to_sumo

        return dag

rail.for_each_instance(create_airflow_child_dag)
