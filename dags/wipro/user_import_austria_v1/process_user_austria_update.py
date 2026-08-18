from wipro.user_import_austria_v1.task import update_roles_for_user
from wipro.user_import_austria_v1.utils import custom_methods, request_payload
from airflow.models import Variable
import rail
null = None


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_dag_id,
        description="wipro User import process record",
        company_key=config.company_key,
        max_active_runs=config.max_active_run_sub_child,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_process_batch_task, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="get_update_user_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_update_user_details",
            end_task="catch_and_log_errors"
        )

        get_update_user_details = rail.RepliconServiceOperator(
            task_id="get_update_user_details",
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
            data_handler=lambda response: response[0]
        )

        get_extension_field_values = rail.PythonOperator(
            task_id="get_extension_field_values",
            python_callable=lambda: list(map(lambda i: {
                "displayText": i["definition"]["displayText"],
                "uri": i["definition"]["uri"],
                "textValue": i["textValue"]
            }, rail.result('get_update_user_details')["userDetails"]["extensionFieldValues"]))
        )

        get_user_custom_field_values = rail.PythonOperator(
            task_id="get_user_custom_field_values",
            python_callable=lambda: list(map(lambda i: {
                "displayText": i["customField"]["displayText"],
                "uri": i["customField"]["uri"],
                "textValue": i["text"]
            }, rail.result('get_update_user_details')["userDetails"]["customFieldValues"]))
        )

        get_assigned_policy_set_for_user = rail.RepliconServiceOperator(
            task_id="get_assigned_policy_set_for_user",
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data=lambda: {
                    "userUri": rail.result('get_update_user_details')["userDetails"]['uri']
            },
            data_handler=lambda response: {
                "timesheet_template": rail.find_first_by_attr_and_get_attr(
                    response, "policyUri", "urn:replicon:policy:timesheet", "policySet"
                ),
                "punch_policy": rail.find_first_by_attr_and_get_attr(
                    response, "policyUri", "urn:replicon:policy:time-punch", "policySet"
                )
            }
        )

        get_current_location_for_the_user = rail.RepliconServiceOperator(
            task_id="get_current_location_for_the_user",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda: {
                    "userUri": rail.result('get_update_user_details')["userDetails"]['uri'],
                    "dateRange": null
            },
            data_handler=lambda response: {
                "existinglocationuri": response.get('locations', [{}])[0].get('location', {}).get('location', {}).get('uri')
                if response["locations"] else "",
                "existingemployeetypeuri": response.get("employeeTypes", [{}])[0].get("employeeType", {}).get("employeeType", {}).get('uri')
                if response["employeeTypes"] else "",

            }
        )

        update_user_details = rail.RepliconServiceOperator(
            task_id="update_user_details",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.get_update_user_req
        )

        if_supervisor_details_in_feed = rail.IfOperator(
            task_id="if_supervisor_details_in_feed",
            test=lambda dag_run: bool(dag_run.conf["primary_supervisor_id"]),
            yes_task="get_effective_supervisor_of_user",
            no_task="if_reversal_date_update"
        )

        get_effective_supervisor_of_user = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data={
                "userUri": "{{ result('get_update_user_details')['userDetails']['uri']}}",
                "asOfDate": custom_methods.get_today_date()
            }
        )

        write_supervisor_pending_logs = rail.WriteLogOperator(
            task_id="write_supervisor_pending_logs",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor",
            severity="Pending",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "primary_supervisor_id": dag_run.conf["primary_supervisor_id"],
                "primary_supervisor_adid": dag_run.conf["primary_supervisor_adid"],
                "primary_supervisor_mailid": dag_run.conf["primary_supervisor_mailid"],
                "Add_Update": "Update",
                "useruri": rail.result('get_update_user_details')["userDetails"]["uri"],
                "supervisor_uri": rail.result('get_effective_supervisor_of_user')['supervisor']["uri"]
                if rail.result('get_effective_supervisor_of_user') else ""
            }
        )

        if_reversal_date_update = rail.IfOperator(
            task_id="if_reversal_date_update",
            test=lambda dag_run: bool(custom_methods.check_if_custom_field_date_udapte(
                dag_run.conf["reversal_date"], "Reversal Date")),
            yes_task="if_user_enabled",
            no_task="if_end_date_update"
        )

        if_user_enabled = rail.IfOperator(
            task_id="if_user_enabled",
            test=lambda: rail.result('get_update_user_details')[
                "securityConfiguration"]["isLoginEnabled"],
            yes_task="if_user_has_end_date",
            no_task="enable_user"
        )

        enable_user = rail.RepliconServiceOperator(
            task_id="enable_user",
            endpoint="/services/securityservice1.svc/EnableLogin",
            data={
                "userUri": "{{ result('get_update_user_details').userDetails.uri }}"
            }
        )

        if_user_has_end_date = rail.IfOperator(
            task_id="if_user_has_end_date",
            test=lambda: rail.result('get_update_user_details')[
                "userDetails"]["employmentDateRange"]["endDate"],
            yes_task="remove_end_date",
            no_task="update_the_reversal_date"
        )

        remove_end_date = rail.RepliconServiceOperator(
            task_id='remove_end_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda: {
                "userUri": rail.result('get_update_user_details')["userDetails"]["uri"],
                "dateRange": {
                    "startDate": rail.result('get_update_user_details')["userDetails"]["employmentDateRange"]["startDate"],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        update_the_reversal_date = rail.RepliconServiceOperator(
            task_id="update_the_reversal_date",
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                    "objectUri": rail.result('get_update_user_details')["userDetails"]["uri"],
                    "customFieldUri": dag_run.conf["reversal_dateuri"],
                    "value": rail.parse_date(dag_run.conf["reversal_date"], "%Y-%m-%d")
            }
        )

        if_end_date_update = rail.IfOperator(
            task_id="if_end_date_update",
            test=lambda dag_run: bool(custom_methods.check_if_end_date(dag_run)),
            yes_task="update_end_date",
            no_task="user_role_update_start"
        )

        update_end_date = rail.RepliconServiceOperator(
            task_id='update_end_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": rail.result('get_update_user_details')["userDetails"]["uri"],
                "dateRange": {
                    "startDate": rail.result('get_update_user_details')["userDetails"]["employmentDateRange"]["startDate"],
                    "endDate": custom_methods.check_if_end_date(dag_run),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        user_role_update_start = rail.EmptyOperator(
            task_id="user_role_update_start")
        user_role_update = update_roles_for_user.update_user_roles(config)

        get_all_update_logs = rail.PythonOperator(
            task_id="get_all_update_logs",
            python_callable=custom_methods.get_updated_logs
        )

        write_updated_user_logs = rail.WriteLogOperator(
            task_id="write_updated_user_logs",
            log='{{dag_run.conf.lookuptable}}',
            message="User updated",
            severity="Success",
            trigger_rule="all_success",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Success",
                "action": "Update",
                "details": rail.result("get_all_update_logs"),


            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="User partially updated",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Failed",
                "action": "Update",
                "details": "User partially updated " + custom_methods.get_error_message(),


            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            get_update_user_details >> get_user_custom_field_values >>\
            get_extension_field_values >> get_assigned_policy_set_for_user >>\
            get_current_location_for_the_user >> update_user_details >>\
            if_supervisor_details_in_feed >> rail.Label(
                "No") >> if_reversal_date_update
        if_supervisor_details_in_feed >> rail.Label("Yes") >>\
            get_effective_supervisor_of_user >>\
            write_supervisor_pending_logs >>\
            if_reversal_date_update >> rail.Label("Yes") >>\
            if_user_enabled >> rail.Label("Yes") >> if_user_has_end_date
        if_user_enabled >> rail.Label("No") >>\
            enable_user >>\
            if_user_has_end_date >> rail.Label(
                "No") >> update_the_reversal_date
        if_user_has_end_date >> rail.Label("Yes") >>\
            remove_end_date >> update_the_reversal_date >> if_end_date_update
        if_reversal_date_update >> rail.Label("No") >>\
            if_end_date_update >> rail.Label("No") >> user_role_update_start
        if_end_date_update >> rail.Label("Yes") >>\
            update_end_date >>\
            user_role_update_start >> user_role_update >>\
            get_all_update_logs >>\
            write_updated_user_logs >> catch_and_log_errors

        return dag


rail.for_each_instance(create_airflow_child)
