from wipro.user_import_saudi_arabia_v2.task import put_supervisor_table_settings
from wipro.user_import_saudi_arabia_v2.utils import request_payload, custom_methods
from airflow.models import Variable
import rail
null = None


def create_aiflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.create_supervisor_dag_id,
        description="wipro User import process record",
        company_key=config.company_key,
        max_active_runs=config.max_active_run_child,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_location_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda:Variable.get(config.can_process_batch_task,default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="validate_supervisor_employee_id_unequal"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="validate_supervisor_employee_id_unequal",
            end_task="catch_and_log_errors"
        )

        validate_supervisor_employee_id_unequal = rail.IfOperator(
            task_id="validate_supervisor_employee_id_unequal",
            test=lambda dag_run:(
                dag_run.conf["primary_supervisor_id"] and\
                  dag_run.conf["primary_supervisor_id"] != dag_run.conf["employee_id"]),
            yes_task="get_supervisor_user_details",
            no_task="write_supervisor_employee_id_match_log"
        )

        write_supervisor_employee_id_match_log = rail.WriteLogOperator(
            task_id="write_supervisor_employee_id_match_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor not assigned",
            severity="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Exception",
                "details": (
                    "Supervisor not assigned as employee id same as supervisor id"
                    if dag_run.conf["primary_supervisor_id"]
                    else "Supervisor not assigned as supervisor id is not available in the feed"
                )
            }
        )

        get_supervisor_user_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["primary_supervisor_id"],
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_supervisor_in_replicon = rail.IfOperator(
            task_id="if_supervisor_in_replicon",
            test='{{result("get_supervisor_user_details")| is_truthy}}',
            yes_task="if_supervisor_has_permissions",
            no_task="if_mailid_and_loginname_in_feed"
        )

        if_supervisor_has_permissions = rail.IfOperator(
            task_id="if_supervisor_has_permissions",
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_supervisor_user_details")[
                    0]["permissionSets"],
                "displayText",
                config.GENERAL_MAPPER["l1_manager"],
                "uri") and rail.find_first_by_attr_and_get_attr(
                rail.result("get_supervisor_user_details")[
                    0]["permissionSets"],
                "displayText",
                config.GENERAL_MAPPER["end_user_manager"],
                "uri"),
            yes_task="if_initial_supervisor",
            no_task="update_supervisor_permission"
        )

        if_mailid_and_loginname_in_feed = rail.IfOperator(
            task_id="if_mailid_and_loginname_in_feed",
            test=lambda dag_run: dag_run.conf["primary_supervisor_mailid"] and dag_run.conf["primary_supervisor_adid"],
            yes_task="create_supervisor_in_replicon",
            no_task="write_log_supervisor_data_not_available"
        )

        create_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id="create_supervisor_in_replicon",
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.get_supervisor_create_payload,
            data_handler=lambda response: response["uri"] if response else null
        )

        remove_all_time_off_types = rail.RepliconServiceOperator(
            task_id="remove_all_time_off_types",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                    "userUri": rail.render_template('{{result("create_supervisor_in_replicon")}}'),
                    "timeOffTypeUris": []
            }
        )

        write_log_supervisor_created = rail.WriteLogOperator(
            task_id="write_log_supervisor_created",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor created",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Success",
                "details": "Supervisor created successfully",


            }
        )

        unassign_products = rail.RepliconServiceOperator(
            task_id='unassign_products',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_unassign_products_payload
        )

        update_supervisor_permission = rail.RepliconServiceOperator(
            task_id="update_supervisor_permission",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_update_supervisor_permission_payload
        )

        write_log_supervisor_updated = rail.WriteLogOperator(
            task_id="write_log_supervisor_updated",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor permission updated",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Success",
                "details": "Supervisor permission updated ",


            }
        )

        if_user_is_enabled = rail.IfOperator(
            task_id="if_user_is_enabled",
            test=lambda: bool(rail.result("get_supervisor_user_details")[
                              0]["userDetails"]["isEnabled"]),
            yes_task="start_supervisor_table_settings",
            no_task="write_log_supervisor_disabled"
        )

        write_log_supervisor_disabled = rail.WriteLogOperator(
            task_id="write_log_supervisor_disabled",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor table settings not updated",
            severity="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Exception",
                "details": "Supervisor table settings not updated as user is disabled",


            }
        )

        start_supervisor_table_settings = rail.EmptyOperator(
            task_id="start_supervisor_table_settings")

        user_uri = '{{result("create_supervisor_in_replicon") or result("get_supervisor_user_details")[0]["userDetails"]["uri"]}}'

        put_supervisor_table_view = put_supervisor_table_settings.get_put_table_view_setting_supervisor(
            user_uri, "supervisor")

        if_initial_supervisor = rail.IfOperator(
            task_id="if_initial_supervisor",
            test=lambda dag_run:(dag_run.conf["Add_Update"] == "Add" or not dag_run.conf.get("supervisor_uri")),
            yes_task="assign_initial_supervisor_to_employee",
            no_task="check_supervisor_update"
        )

        assign_initial_supervisor_to_employee = rail.RepliconServiceOperator(
            task_id="assign_initial_supervisor_to_employee",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri":dag_run.conf['useruri'],
                "supervisorUri": rail.render_template(user_uri),
                "dateRange": {}
            }
        )

        check_supervisor_update = rail.IfOperator(
            task_id="check_supervisor_update",
            test=custom_methods.check_supervisor_update,
            yes_task="assign_supervisor_to_employee",
            no_task="end_supervisor"
        )

        assign_supervisor_to_employee = rail.RepliconServiceOperator(
            task_id="assign_supervisor_to_employee",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri":dag_run.conf['useruri'],
                "supervisorUri": rail.render_template(user_uri),
                "dateRange": {
                    "startDate": custom_methods.get_today_date()
                }
            }
        )

        write_log_supervisor_assignment_updated = rail.WriteLogOperator(
            task_id="write_log_supervisor_assignment_updated",
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
                "details": "Supervisor updated",


            }
        )

        write_log_supervisor_data_not_available = rail.WriteLogOperator(
            task_id="write_log_supervisor_data_not_available",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor not created",
            severity="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Exception",
                "details": "Supervisor not present hence the same is not assigned",


            }
        )

        end_supervisor = rail.EmptyOperator(task_id="end_supervisor")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="Supervisor not processed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Failed",
                "details": "Supervisor not processed due to following reason/s" + custom_methods.get_error_message(),


            }
        )

        can_run_batch_task >>rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
        validate_supervisor_employee_id_unequal >>rail.Label("Yes") >>\
        write_supervisor_employee_id_match_log >> catch_and_log_errors
        validate_supervisor_employee_id_unequal >>  rail.Label("No")>>\
        get_supervisor_user_details >>\
        if_supervisor_in_replicon >> rail.Label("No") >>\
        if_mailid_and_loginname_in_feed >> rail.Label("No") >>\
        write_log_supervisor_data_not_available >> catch_and_log_errors
        if_mailid_and_loginname_in_feed >> rail.Label("Yes") >>\
        create_supervisor_in_replicon >> unassign_products>>\
        remove_all_time_off_types >> write_log_supervisor_created >>\
        start_supervisor_table_settings >> put_supervisor_table_view
        if_supervisor_in_replicon >> rail.Label("Yes") >>\
        if_supervisor_has_permissions >> rail.Label("Yes") >>\
        if_initial_supervisor
        if_supervisor_has_permissions >> rail.Label("No") >>\
        update_supervisor_permission >>\
        write_log_supervisor_updated >>\
        if_user_is_enabled >> rail.Label("Yes") >>\
        start_supervisor_table_settings >> put_supervisor_table_view >> if_initial_supervisor
        if_user_is_enabled >> rail.Label("No") >>\
        write_log_supervisor_disabled >>\
        if_initial_supervisor >> rail.Label("Yes") >> assign_initial_supervisor_to_employee >> catch_and_log_errors
        if_initial_supervisor >> rail.Label("No") >> check_supervisor_update >>\
        rail.Label("Yes") >> assign_supervisor_to_employee >>\
        write_log_supervisor_assignment_updated >> end_supervisor
        check_supervisor_update >> rail.Label("No") >> end_supervisor >>\
        catch_and_log_errors
        return dag


rail.for_each_instance(create_aiflow_child)
