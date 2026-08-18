from conduent.user_import.utils import custom_methods, request_payload
from airflow.models import Variable
import rail
null = None


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.conduent_user_import_update_users_child,
        description="Conduent user import process update users",
        max_active_runs=config.max_active_run_child,
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_user_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_user_log",
            end_task="catch_and_log_errors"
        )

        create_user_log = rail.CreateLogOperator(task_id="create_user_log")

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": dag_run.conf["useruri"],
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        if_manager_win = rail.IfOperator(
            task_id="if_manager_win",
            test=lambda dag_run: dag_run.conf["manager_win"],
            yes_task="get_supervisor_details",
            no_task="get_current_group_membership"
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_supervisor_details,
            data_handler=custom_methods.get_supervisor_uri
        )

        get_current_group_membership = rail.RepliconServiceOperator(
            task_id="get_current_group_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda: {
                    "userUri": rail.result('get_user_details')["userDetails"]['uri'],
                    "dateRange": null
            },
            data_handler=lambda response: {
                "existingbusinessgroupuri": response['divisions'][0].get('division', {}).get('division', {}).get('uri')
                if response['divisions'] else null,
                "existingbusinessgroupname": response['divisions'][0].get('division', {}).get('division', {}).get('displayText')
                if response['divisions'] else null,
                "existinglocationuri": response['locations'][0].get('location', {}).get('location', {}).get('uri')
                if response["locations"] else null,
                "existinglocationname": response['locations'][0].get('location', {}).get('location', {}).get('displayText')
                if response["locations"] else null,
                "existingcostcentersuri": response['costCenters'][0].get('costCenter', {}).get('costCenter', {}).get('uri')
                if response['costCenters'] else null,
                "existingcostcentersname": response['costCenters'][0].get('costCenter', {}).get('costCenter', {}).get('displayText')
                if response['costCenters'] else null,
            } if response else []
        )

        get_supervisor_assignment_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_assignment_details",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda: {
                    "userUri": rail.result('get_user_details')["userDetails"]['uri'],
                    "asOfDate": null
            },
            data_handler=lambda response: response["supervisor"]["uri"] if response else null
        )

        update_user = rail.RepliconServiceOperator(
            task_id="update_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.get_update_user_payload(
                dag_run, config)
        )

        if_supervisor_updated = rail.IfOperator(
            task_id="if_supervisor_updated",
            test=lambda dag_run: rail.result("get_supervisor_details") and
            (not rail.result("get_supervisor_assignment_details") or (
                rail.result("get_supervisor_details") != rail.result("get_supervisor_assignment_details"))),
            yes_task="update_supervisor",
            no_task="get_update_logs",
        )

        update_supervisor = rail.RepliconServiceOperator(
            task_id='update_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": rail.result("get_user_details")["userDetails"]["uri"],
                "supervisorUri": rail.result("get_supervisor_details"),
                "dateRange": {
                    "startDate": custom_methods.get_effective_date(config, dag_run)
                }
            }
        )

        get_update_logs = rail.PythonOperator(
            task_id="get_update_logs",
            python_callable=lambda dag_run: custom_methods.get_update_user_logs(
                config, dag_run)
        )

        get_exception_logs = rail.PythonOperator(
            task_id="get_exception_logs",
            python_callable=custom_methods.get_user_exception_logs
        )

        write_user_updated_log = rail.WriteLogOperator(
            task_id="write_user_updated_log",
            log='{{result("create_user_log")}}',
            message="User create failed",
            properties=lambda dag_run: {
                "win_id": dag_run.conf["win_id"],
                "first_name": dag_run.conf["first_name"],
                "last_name": dag_run.conf["last_name"],
                "email": dag_run.conf["email"],
                "action": "Update",
                "assignment_status": dag_run.conf["assignment_status"],
                "date_active": dag_run.conf["date_active"],
                "status": "Exception" if rail.result("get_exception_logs") else "Success",
                "details": rail.result("get_update_logs")
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{result("create_user_log")}}',
            message="User create failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "win_id": dag_run.conf["win_id"],
                "first_name": dag_run.conf["first_name"],
                "last_name": dag_run.conf["last_name"],
                "email": dag_run.conf["email"],
                "action": "Update",
                "assignment_status": dag_run.conf["assignment_status"],
                "date_active": dag_run.conf["date_active"],
                "details": rail.render_template('{{get_error_message()}}'),
                "status": "Error"
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            create_user_log >>\
            get_user_details >> if_manager_win >> rail.Label("Yes") >> get_supervisor_details >>\
            get_current_group_membership
        if_manager_win >> rail.Label("No") >>\
            get_current_group_membership >>\
            get_supervisor_assignment_details >>\
            update_user >>\
            if_supervisor_updated >> rail.Label("No") >> get_update_logs
        if_supervisor_updated >> rail.Label("Yes") >>\
            update_supervisor >>\
            get_update_logs >>\
            get_exception_logs >> write_user_updated_log >> catch_and_log_errors

    return dag


rail.for_each_instance(create_airflow_child)
