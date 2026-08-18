from datetime import timedelta
from dxctechnology.fieldglass_workorder_import.utils import request_payload
from airflow.models import Variable
import rail
null = None

# pylint: disable=too-many-statements
def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_compass_user_details_dag_id,
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
            no_task= "if_rate_unit_hr"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout),
            start_task='if_rate_unit_hr',
            end_task="write_update_user_fail_log",
        )

        if_rate_unit_hr = rail.IfOperator(
            task_id="if_rate_unit_hr",
            test='{{dag_run.conf.RateUnit == "Hr"}}',
            yes_task="if_user_is_not_enabled",
            no_task="log_to_sumo"
        )

        if_user_is_not_enabled = rail.IfOperator(
            task_id="if_user_is_not_enabled",
            test='{{dag_run.conf.userstatus != "Enabled"}}',
            yes_task="get_bulk_user_details",
            no_task="get_bulk_project_details"
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
            }
        )

        enable_user_in_replicon = rail.RepliconServiceOperator(
            task_id="enable_user_in_replicon",
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                    "userUri": '{{dag_run.conf.useruri}}'
            }
        )

        get_bulk_project_details = rail.RepliconServiceOperator(
            task_id="get_bulk_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                    "projects": [
                        {
                            "uri": null,
                            "name": '{{dag_run.conf.CostCenterCode}}',
                            "code": null,
                            "parameterCorrelationId": null
                        }
                    ]
            },
            data_handler=lambda response: response[0]["projectDetails"][
                "uri"] if response and response[0].get("projectDetails") else null
        )

        if_project_exists = rail.IfOperator(
            task_id="if_project_exists",
            test=lambda: bool(rail.result("get_bulk_project_details")),
            yes_task="update_workorder_id_oef",
            no_task="if_timesheet_tempalte_value_present"
        )

        update_workorder_id_oef = rail.RepliconServiceOperator(
            task_id="update_workorder_id_oef",
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data={
                "objectUri": '{{result("get_bulk_project_details")}}',
                "value": {
                    "definition": {
                        "uri": '{{dag_run.conf.WorkOrderID_projectoef_uri}}',
                        "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": '{{dag_run.conf.WorkOrderID}}',
                    "fileValue": null,
                    "jsonValue": null
                }
            }
        )

        update_remaining_spend_oef = rail.RepliconServiceOperator(
            task_id="update_remaining_spend_oef",
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data={
                "objectUri": '{{result("get_bulk_project_details")}}',
                "value": {
                    "definition": {
                        "uri": '{{dag_run.conf.remainingspend_projectoef_uri}}',
                        "name": null
                    },
                    "tag": null,
                    "numericValue": '{{dag_run.conf.RemainingSpend}}',
                    "textValue": null,
                    "fileValue": null,
                    "jsonValue": null
                }
            }
        )

        assign_resource_to_project = rail.RepliconServiceOperator(
            task_id="assign_resource_to_project",
            endpoint="/services/ProjectService1.svc/AssignResourceToProject",
            data={
                "projectUri":  '{{result("get_bulk_project_details")}}',
                "resourceUri": '{{dag_run.conf.useruri}}',
                "resourceToReplaceUri": null
            }
        )

        if_timesheet_tempalte_value_present = rail.IfOperator(
            task_id="if_timesheet_tempalte_value_present",
            test=lambda dag_run:bool(dag_run.conf["timesheettemplatetoassign"] and dag_run.conf["Timesheeettemplateuri"]),
            yes_task="if_timesheet_template_updated",
            no_task="if_timesheet_approval_path_updated"
        )

        if_timesheet_template_updated = rail.IfOperator(
            task_id="if_timesheet_template_updated",
            test='{{dag_run.conf.timesheettemplatetoassign != dag_run.conf.Timesheeettemplateuri}}',
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
            test=lambda dag_run:bool(dag_run.conf["timesheetapprovalpath"] != "Project Manager Compass AC"),
            yes_task="update_timesheet_approval_path",
            no_task="if_workweek_updated"
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
                        "timesheetApprovalPathToApply": {
                            "uri": null,
                            "name": "Project Manager Compass AC"
                        }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_workweek_updated = rail.IfOperator(
            task_id="if_workweek_updated",
            test=lambda dag_run:bool(dag_run.conf["workweek"] != dag_run.conf["workweektoassign"]),
            yes_task="if_c1_or_compass_workweek",
            no_task="get_effective_user_membership"
        )

        if_c1_or_compass_workweek = rail.IfOperator(
            task_id="if_c1_or_compass_workweek",
            test=lambda dag_run:bool(dag_run.conf["FinanceSystem"].lower() in ["c1", "es"]),
            yes_task="update_user_workweek",
            no_task="get_effective_user_membership"
        )

        update_user_workweek = rail.RepliconServiceOperator(
            task_id="update_user_workweek",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_compass_workweek_request
        )

        get_effective_user_membership = rail.RepliconServiceOperator(
            task_id="get_effective_user_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                    "userUri": '{{dag_run.conf.useruri}}',
                    "dateRange": null
            },
            data_handler=lambda response: response.get("costCenters",[{}])[0].get("costCenter",{}).get(
                "costCenter", {}).get("displayText") if "costCenters" in response  and response["costCenters"] else null
        )

        if_costcenter_update = rail.IfOperator(
            task_id="if_costcenter_update",
            test=lambda dag_run:bool(rail.result("get_effective_user_membership") != dag_run.conf["actual_costcenter_value"]
                                     and dag_run.conf["costcenteruri"]),
            yes_task="apply_user_modification_division",
            no_task="if_costcenter_present"

        )

        apply_user_modification_division = rail.RepliconServiceOperator(
            task_id="apply_user_modification_division",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_division_update_request_compass
        )

        if_costcenter_present = rail.IfOperator(
            task_id="if_costcenter_present",
            test='{{dag_run.conf.costcenteruri | is_truthy}}',
            yes_task="put_division_schedule",
            no_task="if_workorderid_present"
        )

        put_division_schedule = rail.RepliconServiceOperator(
            task_id="put_division_schedule",
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                    "userUri": '{{dag_run.conf.useruri}}',
                    "scheduleEntries": [
                        {
                            "division": {
                                "uri": '{{dag_run.conf.costcenteruri}}',
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ]
            }
        )

        if_workorderid_present = rail.IfOperator(
            task_id="if_workorderid_present",
            test=lambda dag_run:bool(dag_run.conf["WorkOrderID_Customfielduri"]),
            yes_task="if_workerid_updated",
            no_task="if_cwf_agency_wbs_updated"
        )

        if_workerid_updated = rail.IfOperator(
            task_id="if_workerid_updated",
            test=lambda dag_run:bool(dag_run.conf["WorkOrderID_assigned"] != dag_run.conf["WorkOrderID"]),
            yes_task="update_workorderid",
            no_task="if_cwf_agency_wbs_updated"
        )

        update_workorderid = rail.RepliconServiceOperator(
            task_id="update_workorderid",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                    "objectUri": '{{dag_run.conf.useruri}}',
                    "customFieldUri": '{{dag_run.conf.WorkOrderID_Customfielduri}}',
                    "value": '{{dag_run.conf.WorkOrderID}}'
            }
        )

        if_cwf_agency_wbs_updated = rail.IfOperator(
            task_id="if_cwf_agency_wbs_updated",
            test=lambda dag_run:dag_run.conf["CostCenterCode"] and \
                    dag_run.conf["cwf_agency_wbs_customfielduri"] and \
                    "agency contractor" in dag_run.conf["employeetype"].lower(),
            yes_task="update_cwf_agency_wbs",
            no_task="if_timesheet_period_updated"
        )

        update_cwf_agency_wbs = rail.RepliconServiceOperator(
            task_id="update_cwf_agency_wbs",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                    "objectUri": '{{dag_run.conf.useruri}}',
                    "customFieldUri": '{{dag_run.conf.cwf_agency_wbs_customfielduri}}',
                    "value": '{{dag_run.conf.CostCenterCode}}'
            }
        )

        if_timesheet_period_updated = rail.IfOperator(
            task_id="if_timesheet_period_updated",
            test=lambda dag_run:bool(dag_run.conf["timesheetperiod"] != "Weekly - Starting Monday - ES employees and Contractors"),
            yes_task="update_timesheet_period",
            no_task="update_user_activities"
        )

        update_timesheet_period = rail.RepliconServiceOperator(
            task_id="update_timesheet_period",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_timesheet_period_value_compass
        )

        update_user_activities = rail.RepliconServiceOperator(
            task_id="update_user_activities",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run:request_payload.get_activities_request_compass(dag_run, config)
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

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> write_update_user_fail_log >>log_to_sumo
        can_run_batch_task >> rail.Label("No") >>\
        if_rate_unit_hr >> rail.Label("No") >> log_to_sumo
        if_rate_unit_hr >> rail.Label("Yes") >>\
        if_user_is_not_enabled >> rail.Label("Yes") >>\
        get_bulk_user_details >> enable_user_in_replicon >>\
        get_bulk_project_details >> if_project_exists >> rail.Label("Yes") >>\
        update_workorder_id_oef >> update_remaining_spend_oef >> assign_resource_to_project >>\
        if_timesheet_tempalte_value_present
        if_project_exists >> rail.Label("No") >> if_timesheet_tempalte_value_present
        if_user_is_not_enabled >> rail.Label("No") >> get_bulk_project_details
        if_timesheet_tempalte_value_present >> rail.Label("Yes") >>\
        if_timesheet_template_updated >> rail.Label("Yes") >>\
        assign_policy_set_to_user >> if_timesheet_approval_path_updated
        if_timesheet_template_updated >> rail.Label("No") >>\
        if_timesheet_approval_path_updated
        if_timesheet_tempalte_value_present >> rail.Label("No") >>\
        if_timesheet_approval_path_updated >> rail.Label("Yes") >>\
        update_timesheet_approval_path >> if_workweek_updated
        if_timesheet_approval_path_updated >> rail.Label("No") >>\
        if_workweek_updated >> rail.Label("Yes") >> \
        if_c1_or_compass_workweek >> rail.Label("Yes") >>\
        update_user_workweek >>\
        get_effective_user_membership
        if_c1_or_compass_workweek >> rail.Label("No") >>\
        get_effective_user_membership
        if_workweek_updated >> rail.Label("No") >>\
        get_effective_user_membership >>\
        if_costcenter_update >> rail.Label("Yes") >>\
        apply_user_modification_division >> if_workorderid_present
        if_costcenter_update >> rail.Label("No") >>\
        if_costcenter_present >> rail.Label("No") >> if_workorderid_present
        if_costcenter_present >> rail.Label("Yes") >>\
        put_division_schedule >>\
        if_workorderid_present >> rail.Label("No") >> if_cwf_agency_wbs_updated
        if_workorderid_present >> rail.Label("Yes") >>\
        if_workerid_updated >> rail.Label("Yes") >>\
        update_workorderid >> if_cwf_agency_wbs_updated
        if_workerid_updated >> rail.Label("No") >>\
        if_cwf_agency_wbs_updated >> rail.Label("Yes") >> update_cwf_agency_wbs >> if_timesheet_period_updated
        if_cwf_agency_wbs_updated >> rail.Label("No") >>\
        if_timesheet_period_updated >> rail.Label("Yes") >>\
        update_timesheet_period >> update_user_activities
        if_timesheet_period_updated >> rail.Label("No") >>\
        update_user_activities >>\
        write_update_user_success_log >> write_update_user_fail_log >> log_to_sumo

        return dag


rail.for_each_instance(create_airflow_child_dag)
