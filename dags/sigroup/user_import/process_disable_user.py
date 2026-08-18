from datetime import timedelta
from sigroup.user_import.utils import custom_methods
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_disable_user_dag_id,
       description="sigroup user import disable user child",
        max_active_runs=config.child_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        if_termination_date_present = rail.IfOperator(
            task_id="if_termination_date_present",
            test=lambda dag_run: bool(dag_run.conf["enddate"]),
            yes_task="if_valid_termination_date",
            no_task="write_log_no_termination_date"
        )

        if_valid_termination_date = rail.IfOperator(
            task_id="if_valid_termination_date",
            test=lambda dag_run: bool(not custom_methods.is_future_termination(dag_run)),
            yes_task="disable_login",
            no_task="update_employment_date_range"
        )

        disable_login = rail.RepliconServiceOperator(
            task_id="disable_login",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.useruri}}"
            }
        )

        update_employment_date_range = rail.RepliconServiceOperator(
            task_id="update_employment_date_range",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["startdate"], "%m/%d/%Y"),
                    "endDate": rail.parse_date(dag_run.conf["enddate"], "%m/%d/%Y"),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }

        )

        update_timesheet_period = rail.RepliconServiceOperator(
            task_id="update_timesheet_period",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf["useruri"],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "uri": null,
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(dag_run.conf["enddate"], "%m/%d/%Y")
                                }
                            ]
                        },
                        "projectRolesToApply": null
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
            }
        )

        get_user_timeoff_type_policy = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_type_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                    "userUri": '{{dag_run.conf.useruri}}'
            }
        )

        put_blank_line_policy = rail.trigger_parallel_dagrun(
            task_id="put_blank_line_policy",
            items='{{result("get_user_timeoff_type_policy").policiesByTimeOffType|to_json}}',
            trigger_dag_id=config.sigroup_user_import_disable_user_blank_timeoff_policy,
            parallel_count=config.time_off_policy_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item,dag_run: {
                **dag_run.conf,
                "timeoffuri": item["timeOffType"]["uri"],
                "useruri": dag_run.conf["useruri"],
                "enddate": dag_run.conf["enddate"].split("/")[1] + "/" +
                dag_run.conf["enddate"].split(
                    "/")[0] + "/" + dag_run.conf["enddate"].split("/")[-1],
                "policyset": item["policySetSchedule"],
                'isTimeOffAllowedAgainstThisTimeOffType': item.get("isTimeOffAllowedAgainstThisTimeOffType","")
            }
        )

        end_timeoff = rail.EmptyOperator(task_id="end_timeoff")

        update_action = rail.RepliconServiceOperator(
            task_id="update_action",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                    "objectUri": '{{dag_run.conf.useruri}}',
                    "customFieldUri": '{{dag_run.conf.actionuri}}',
                    "value": '{{dag_run.conf.action}}'
            }
        )

        update_status = rail.RepliconServiceOperator(
            task_id="update_status",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                    "objectUri": '{{dag_run.conf.useruri}}',
                    "customFieldUri": '{{dag_run.conf.statusuri}}',
                    "value": '{{dag_run.conf.status}}'
            }
        )

        update_action_effective_date = rail.RepliconServiceOperator(
            task_id="update_action_effective_date",
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                    "objectUri": dag_run.conf["useruri"],
                    "customFieldUri": dag_run.conf["actioneffectivedateuri"],
                    "value": rail.parse_date(dag_run.conf["actioneffectivedate"], "%m/%d/%Y")
            }
        )

        update_admin_modified = rail.RepliconServiceOperator(
            task_id="update_admin_modified",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                    "objectUri": dag_run.conf["useruri"],
                    "customFieldUri": dag_run.conf["adminmodifieduri"],
                    "customFieldDropDownOptionUri": dag_run.conf["adminmodified"]
            }
        )

        if_future_termination = rail.IfOperator(
            task_id="if_future_termination",
            test=lambda dag_run: bool(custom_methods.is_future_termination(dag_run)),
            yes_task="write_log_invalid_termination_date",
            no_task="write_log_disable_user_success"
        )

        write_log_disable_user_success = rail.WriteLogOperator(
            task_id="write_log_disable_user_success",
            log='{{dag_run.conf.lookuptable}}',
            message="User disabled",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Disable user",
                "Status": "Success",
                "Details": "User profile disabled successfully wiith end date",
                
            }
        )

        write_log_disable_user_failed = rail.WriteLogOperator(
            task_id="write_log_disable_user_failed",
            log='{{dag_run.conf.lookuptable}}',
            message="User disabled",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Disable user",
                "Status": "Error",
                "Details": "User profile disable failed",
                
            }
        )

        write_log_no_termination_date = rail.WriteLogOperator(
            task_id="write_log_no_termination_date",
            log='{{dag_run.conf.lookuptable}}',
            message="User disabled",
            severity="Exception",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Disable user",
                "Status": "Exception",
                "Details": "User not disbaled as end date was not provided",
                
            }
        )

        write_log_invalid_termination_date = rail.WriteLogOperator(
            task_id="write_log_invalid_termination_date",
            log='{{dag_run.conf.lookuptable}}',
            message="User disabled",
            severity="Exception",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Disable user",
                "Status": "Exception",
                "Details": "User not disabled as the end date received in the future",
                
            }
        )

        if_termination_date_present >> rail.Label("Yes") >>\
            if_valid_termination_date >> rail.Label("No") >>\
            update_employment_date_range
        if_valid_termination_date >> rail.Label("Yes") >>\
        disable_login >> update_employment_date_range >>\
        update_timesheet_period >> get_user_timeoff_type_policy >>\
        put_blank_line_policy >> end_timeoff>>\
        update_action >> update_status >> update_action_effective_date >>\
        update_admin_modified >> if_future_termination >> rail.Label("No") >>\
        write_log_disable_user_success >> write_log_disable_user_failed
        if_future_termination >> rail.Label("Yes") >>\
            write_log_invalid_termination_date >> write_log_disable_user_failed
        if_termination_date_present >> rail.Label("No") >>\
            write_log_no_termination_date >> write_log_disable_user_failed
        return dag


rail.for_each_instance(create_airflow_dag)
