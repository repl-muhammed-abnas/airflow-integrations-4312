import pendulum
from bearingpoint.user_import_v1.utils import request_payload, custom_methods
from bearingpoint.user_import_v1.tasks.process_user_groups_data import get_all_groups_data
from airflow.models import Variable
import rail
null = None


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_child_dag_id,
        description=f"BearingPoint User Import Update User Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.update_user_child_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="get_user_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_user_details",
            end_task="catch_and_log_errors"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["employee_id"],
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        get_current_group_membership = rail.RepliconServiceOperator(
            task_id="get_current_group_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda dag_run: {
                    "userUri": rail.result("get_user_details")["userDetails"]['uri'],
                    "dateRange": {
                        "startDate": dag_run.conf["process_start_time"],
                        "endDate": dag_run.conf["process_start_time"]
                    }
            },
            data_handler=lambda response: {
                "existinglocationuri": response.get('locations', [{}])[0].get('location', {}).get('location', {}).get('uri')
                    if response.get('locations') else null,
                "existinglocationname": response.get('locations', [{}])[0].get('location', {}).get('location', {}).get('displayText') 
                    if response.get('locations') else null,
                "existingservicecenteruri": response.get('serviceCenters', [{}])[0].get('serviceCenter', {}).get('serviceCenter', {}).get('uri')
                    if response.get('serviceCenters') else null,
                "existingservicecentername": response.get('serviceCenters', [{}])[0].get('serviceCenter', {}).get('serviceCenter', {}).get('displayText')
                    if response.get('serviceCenters') else null,
                "existingdepartmenturi": response.get('departments', [{}])[0].get('department', {}).get('department', {}).get('uri')
                    if response.get('departments') else null,
                "existingdepartmentname": response.get('departments', [{}])[0].get('department', {}).get('department', {}).get('displayText')
                    if response.get('departments') else null,
                "existingcostcenteruri": response.get('costCenters', [{}])[0].get('costCenter', {}).get('costCenter', {}).get('uri')
                    if response.get('costCenters') else null,
                "existingcostcentername": response.get('costCenters', [{}])[0].get('costCenter', {}).get('costCenter', {}).get('displayText')
                    if response.get('costCenters') else null,
                "existingemployeetypeuri": response.get('employeeTypes', [{}])[0].get('employeeType', {}).get('employeeType', {}).get('uri')
                    if response.get('employeeTypes') else null,
                "existingemployeetypename": response.get('employeeTypes', [{}])[0].get('employeeType', {}).get('employeeType', {}).get('displayText')
                    if response.get('employeeTypes') else null
            }
        )

        get_user_holiday_calendar = rail.RepliconServiceOperator(
            task_id='get_user_holiday_calendar',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidayCalendarAssignmentScheduleForUserAndDateRange",
            data=request_payload.get_user_holiday_cal_payload,
            data_handler=custom_methods.get_user_current_holiday_calendar
        )

        groups_data_start = rail.EmptyOperator(
            task_id='groups_data_start'
        )

        process_get_groups_data = get_all_groups_data()

        if_user_and_supervisor_same = rail.IfOperator(
            task_id='if_user_and_supervisor_same',
            test='{{ dag_run.conf.employee_id == dag_run.conf.supervisor }}',
            yes_task='update_user_details',
            no_task='get_supervisor_details'
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["supervisor"],
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
            no_task="update_user_details"
        )

        if_supervisor_permission_exists = rail.IfOperator(
            task_id="if_supervisor_permission_exists",
            test=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_supervisor_details")["permissionSets"],
                                                              "displayText", config.SUPERVISOR_PERMISSION, "uri"),
            yes_task="get_supervisor_assignment_details",
            no_task="assign_supervisor_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permission",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: request_payload.get_assign_supervisor_permission_payload(config.SUPERVISOR_PERMISSION)
        )

        get_supervisor_assignment_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_assignment_details",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": rail.result("get_user_details")["userDetails"]["uri"],
                "asOfDate": dag_run.conf["process_start_time"]
            },
            data_handler=lambda response: rail.set_result(key="supervisor", val=response["supervisor"] if response else {})
        )

        update_user_details = rail.RepliconServiceOperator(
            task_id="update_user_details",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_update_user_req(
                dag_run, config.time_zone, config.location_wise_data_mapper)
        )

        if_not_rehired = rail.IfOperator(
            task_id='if_not_rehired',
            test=lambda dag_run: request_payload.is_rehired_user(dag_run) != "rehired",
            yes_task='if_location_updated',
            no_task='if_user_updated_with_exceptions'
        )

        if_location_updated = rail.IfOperator(
            task_id='if_location_updated',
            test=request_payload.get_updated_location,
            yes_task='put_timeoff_assignment_for_user',
            no_task='if_user_updated_with_exceptions'
        )

        put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
            task_id='put_timeoff_assignment_for_user',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=request_payload.put_timeoff_assignment_payload
        )

        if_user_updated_with_exceptions = rail.IfOperator(
            task_id='if_user_updated_with_exceptions',
            test=lambda: bool(rail.result("update_user_details")[
                              "errors"][0]["notifications"]) if rail.result("update_user_details")["errors"] else False,
            yes_task='write_updated_user_with_exceptions_logs',
            no_task='write_updated_user_logs'
        )

        write_updated_user_with_exceptions_logs = rail.WriteLogOperator(
            task_id="write_updated_user_with_exceptions_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: "User partially updated with errors - " + " | ".join(
                request_payload.get_updated_logs(dag_run, config) + [details["displayText"]
                    for details in rail.result("update_user_details")["errors"][0]["notifications"]]
                        + request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper)),
            severity="Error",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": "Update",
                "status": "Error",
                "details": "User partially updated with errors - " + " | ".join(request_payload.get_updated_logs(dag_run, config) + 
                    [details["displayText"] for details in rail.result("update_user_details")["errors"][0]["notifications"]]
                        + request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper))
            }
        )

        write_updated_user_logs = rail.WriteLogOperator(
            task_id="write_updated_user_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: "User updated successfully" if not request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) and
                    request_payload.get_updated_logs(dag_run, config) else
                        ("User partially updated - " + " | ".join(request_payload.get_updated_logs(dag_run, config)
                        + request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper))) if request_payload.get_updated_logs(dag_run, config)
                            else ("User not updated - " + " | ".join(request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper))
                                if request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) else "User not updated"),
            severity=lambda dag_run: ("Success" if not request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) and
                    request_payload.get_updated_logs(dag_run, config) else ("Exception" if request_payload.get_exception_logs(
                        dag_run, config.location_wise_data_mapper) else "Skipped")),
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employee_id"],
                "action": "Update",
                "status": ("Success" if not request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) and
                    request_payload.get_updated_logs(dag_run, config) else ("Exception" if request_payload.get_exception_logs(
                        dag_run, config.location_wise_data_mapper) else "Skipped")),
                "details": "User updated successfully" if not request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) and
                    request_payload.get_updated_logs(dag_run, config) else
                        ("User partially updated - " + " | ".join(request_payload.get_updated_logs(dag_run, config)
                        + request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper))) if request_payload.get_updated_logs(dag_run, config)
                            else ("User not updated - " + " | ".join(request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper))
                                if request_payload.get_exception_logs(dag_run, config.location_wise_data_mapper) else "User not updated"),
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.log_artifact }}',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule="one_failed",
            properties={
                "employeeid": '{{ dag_run.conf.employee_id }}',
                "action": "Update",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_user_details >> get_current_group_membership \
            >> get_user_holiday_calendar >> groups_data_start >> process_get_groups_data \
                >> if_user_and_supervisor_same >> rail.Label("No") >> get_supervisor_details \
                    >> if_supervisor_exists >> rail.Label("Yes") >> if_supervisor_permission_exists
        if_user_and_supervisor_same >> rail.Label("Yes") >> update_user_details
        if_supervisor_exists >> rail.Label("No") >> update_user_details
        if_supervisor_permission_exists >> rail.Label(
            "No") >> assign_supervisor_permission >> get_supervisor_assignment_details
        if_supervisor_permission_exists >> rail.Label("Yes") >> get_supervisor_assignment_details \
            >> update_user_details >> if_not_rehired
        if_not_rehired >> rail.Label("Yes") >> if_location_updated
        if_not_rehired >> rail.Label("No") >> if_user_updated_with_exceptions
        if_location_updated >> rail.Label("Yes") >> put_timeoff_assignment_for_user >> if_user_updated_with_exceptions
        if_location_updated >> rail.Label("No") >> if_user_updated_with_exceptions
        if_user_updated_with_exceptions >> rail.Label(
            "No") >> write_updated_user_logs >> catch_and_log_errors
        if_user_updated_with_exceptions >> rail.Label(
            "Yes") >> write_updated_user_with_exceptions_logs >> catch_and_log_errors

        return dag


rail.for_each_instance(create_airflow_child)
