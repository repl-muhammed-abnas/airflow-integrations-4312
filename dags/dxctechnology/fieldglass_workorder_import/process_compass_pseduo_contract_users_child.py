
from datetime import timedelta
from dxctechnology.fieldglass_workorder_import.utils import request_payload
from airflow.models import Variable
import rail
null = None


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.compass_pseudo_constractor_update_dag_id,
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
            no_task= "if_user_present"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout),
            start_task='if_user_present',
            end_task="write_update_user_success_log",
        )

        if_user_present = rail.IfOperator(
            task_id="if_user_present",
            test=lambda dag_run: bool(dag_run.conf["useruri"]),
            yes_task="if_timesheet_template_value_present",
            no_task="write_update_user_ignored_log"
        )

        write_update_user_ignored_log = rail.WriteLogOperator(
            task_id="write_update_user_ignored_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Ignored",
            message="Employee is not present in Replicon",
            properties={
                "workorderid": '{{dag_run.conf.WorkOrderID}}',
                "ContingentWorkerID": '{{dag_run.conf.ContingentWorkerID}}',
                "status": "Ignored",
                "details": 'Employee is not present in Replicon',
                "Action": "User_attributes_update"
            }
        )

        if_timesheet_template_value_present = rail.IfOperator(
            task_id="if_timesheet_template_value_present",
            test=lambda dag_run:bool(dag_run.conf["timesheettemplatetoassign"] and dag_run.conf["Timesheeettemplateuri"]),
            yes_task="if_timesheet_template_updated",
            no_task="if_timeentry_approval_path_updated"
        )

        if_timesheet_template_updated = rail.IfOperator(
            task_id="if_timesheet_template_updated",
            test=lambda dag_run:bool(dag_run.conf["timesheettemplatetoassign"] != dag_run.conf["Timesheeettemplateuri"]),
            yes_task="assign_policy_set_to_user",
            no_task="if_timeentry_approval_path_updated"
        )

        assign_policy_set_to_user = rail.RepliconServiceOperator(
            task_id="assign_policy_set_to_user",
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                    "userUri": '{{dag_run.conf.useruri}}',
                    "policySetUri": '{{dag_run.conf.Timesheeettemplateuri}}'
            }
        )

        if_timeentry_approval_path_updated = rail.IfOperator(
            task_id="if_timeentry_approval_path_updated",
            test=lambda dag_run:bool(dag_run.conf["Timeentryapprovalpath"] != "Time Entry Approval Workflow (Russia)"),
            yes_task="update_timesheet_approval_path",
            no_task="if_timesheet_approval_path_updated"
        )

        update_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id="update_timesheet_approval_path",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data={
                    "user": {
                        "uri": '{{dag_run.conf.useruri}}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "modifications": {
                        "timeEntryRevisionGroupApprovalPathToApply": {
                        "uri": null,
                        "name": "Time Entry Approval Workflow (Russia)"
                        }
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
        )

        if_timesheet_approval_path_updated = rail.IfOperator(
            task_id="if_timesheet_approval_path_updated",
            test=lambda dag_run:bool(dag_run.conf["Timesheetapprovalpath"] != "System"),
            yes_task="update_timesheet_approval_path_system",
            no_task="if_rateunit_present"
        )

        update_timesheet_approval_path_system = rail.RepliconServiceOperator(
            task_id="update_timesheet_approval_path_system",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data={
                    "user": {
                        "uri": '{{dag_run.conf.useruri}}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                "modifications": {
                        "timesheetApprovalPathToApply": {
                            "uri": null,
                            "name": "System"
                        }
                        },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_rateunit_present = rail.IfOperator(
            task_id="if_rateunit_present",
            test='{{dag_run.conf.RateUnit|is_truthy}}',
            yes_task="if_rate_unit_updated",
            no_task="get_effective_user_membership"
        )

        if_rate_unit_updated = rail.IfOperator(
            task_id="if_rate_unit_updated",
            test=lambda dag_run:bool(dag_run.conf["Rateunitudfcurrentvalue"] != dag_run.conf["RateUnit"]),
            yes_task="update_rate_unit_udf",
            no_task="get_effective_user_membership"
        )

        update_rate_unit_udf = rail.RepliconServiceOperator(
            task_id="update_rate_unit_udf",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                    "objectUri": '{{dag_run.conf.useruri}}',
                    "customFieldUri": '{{dag_run.conf.RateunitcustomfieldURI}}',
                    "value": '{{dag_run.conf.RateUnit}}'
            }
        )

        get_effective_user_membership = rail.RepliconServiceOperator(
            task_id="get_effective_user_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                    "userUri": '{{dag_run.conf.useruri}}',
                    "dateRange": null
            },
            data_handler=lambda response: response.get("costCenters",[{}])[0].get("costCenter",{}).get(
                "costCenter", {}).get("displayText") if "costCenters" in response and response["costCenters"] else null
        )

        if_costcenter_update = rail.IfOperator(
            task_id="if_costcenter_update",
            test=lambda dag_run:bool(
                dag_run.conf["EmployeetypegroupURI"] and
                rail.result("get_effective_user_membership") and
                rail.result("get_effective_user_membership") != "Leveraged Non-Hrly AC"),
            yes_task="apply_user_modification_division",
            no_task="if_costcenter_present"

        )

        apply_user_modification_division = rail.RepliconServiceOperator(
            task_id="apply_user_modification_division",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_division_update_request_compass_po_bolb
        )

        if_costcenter_present = rail.IfOperator(
            task_id="if_costcenter_present",
            test=lambda dag_run:bool(
                dag_run.conf["EmployeetypegroupURI"]),
            yes_task="put_employee_type_group_schedule_for_user",
            no_task="write_update_user_success_log"

        )

        put_employee_type_group_schedule_for_user = rail.RepliconServiceOperator(
            task_id="put_employee_type_group_schedule_for_user",
            endpoint="/services/EmployeeTypeGroupService1.svc/PutEmployeeTypeGroupScheduleForUser",
            data=lambda dag_run:{
                    "userUri": dag_run.conf["useruri"],
                    "scheduleEntries": [
                        {
                        "employeeTypeGroup": {
                            "uri": dag_run.conf["EmployeetypegroupURI"],
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                        }
                    ]
                }
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

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> write_update_user_success_log>>log_to_sumo
        can_run_batch_task >> rail.Label("No") >>\
        if_user_present >> rail.Label("Yes") >> if_timesheet_template_value_present
        if_user_present >> rail.Label("No") >>\
        write_update_user_ignored_log >> log_to_sumo
        if_timesheet_template_value_present >> rail.Label("Yes") >>\
        if_timesheet_template_updated >> rail.Label("Yes") >>\
        assign_policy_set_to_user >> if_timeentry_approval_path_updated
        if_timesheet_template_value_present >> rail.Label("No") >>\
        if_timeentry_approval_path_updated
        if_timesheet_template_updated >> rail.Label("No") >>\
        if_timeentry_approval_path_updated >> rail.Label("Yes") >>\
        update_timesheet_approval_path >> if_timesheet_approval_path_updated
        if_timeentry_approval_path_updated >> rail.Label("No")>>\
        if_timesheet_approval_path_updated >> rail.Label("Yes") >>\
        update_timesheet_approval_path_system >> if_rateunit_present
        if_timesheet_approval_path_updated >> rail.Label("No") >>\
        if_rateunit_present >> rail.Label("Yes") >> if_rate_unit_updated >>\
        rail.Label("Yes") >> update_rate_unit_udf>>\
        get_effective_user_membership
        if_rate_unit_updated >> rail.Label("No") >> get_effective_user_membership
        if_rateunit_present >> rail.Label("No") >> get_effective_user_membership>>\
        if_costcenter_update >> rail.Label("Yes") >> apply_user_modification_division>>\
        write_update_user_success_log
        if_costcenter_update >> rail.Label("No") >>\
        if_costcenter_present >> rail.Label("No") >> write_update_user_success_log
        if_costcenter_present >> rail.Label("Yes") >>\
        put_employee_type_group_schedule_for_user >>\
        write_update_user_success_log >>  write_update_user_fail_log >> log_to_sumo


        return dag


rail.for_each_instance(create_airflow_child_dag)
