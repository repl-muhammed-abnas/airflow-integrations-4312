from uuid import uuid4
from pendulum import now
from conduent.user_import.utils import custom_methods, request_payload
from airflow.models import Variable
import rail

null = None


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.conduent_user_import_create_users_child,
        description="Conduent user import create users",
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
            end_task="if_user_already_exists"
        )

        create_user_log = rail.CreateLogOperator(task_id="create_user_log")

        if_manager_win = rail.IfOperator(
            task_id="if_manager_win",
            test=lambda dag_run: dag_run.conf["manager_win"],
            yes_task="get_supervisor_details",
            no_task="if_rehire_user"
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_supervisor_details,
            data_handler=custom_methods.get_supervisor_uri
        )

        if_rehire_user = rail.IfOperator(
            task_id="if_rehire_user",
            test=lambda dag_run: dag_run.conf["user_type"] == "rehire_user",
            yes_task="get_user_details",
            no_task="create_user_in_replicon"
        )

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

        update_user_loginname = rail.RepliconServiceOperator(
            task_id="update_user_loginname",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: {
                    "target": {
                        "uri": rail.result("get_user_details")["userDetails"]["uri"],
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    },
                "template": null,
                "modifications": {
                        "loginName": {
                            "value": rail.result("get_user_details")["securityConfiguration"]["loginName"] + "_old"
                        },
                        "employmentDateRange": {
                            "value": {
                                "startDate": rail.result("get_user_details")["userDetails"]["employmentDateRange"]["startDate"],
                                "endDate": rail.parse_date(dag_run.conf["date_termed"], "%m/%d/%Y") if dag_run.conf["date_termed"] else
                                rail.parse_date(now(tz=config.time_zone).strftime(
                                    "%m/%d/%Y"), "%m/%d/%Y"),
                                "relativeDateRangeUri": null,
                                "relativeDateRangeAsOfDate": null
                            }
                        },
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )

        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id="create_user_in_replicon",
            endpoint="/services/ImportService1.svc/PutUser3",
            data=lambda dag_run: request_payload.get_create_user_payload(
                dag_run, config)
        )

        enable_notifications_for_user = rail.RepliconServiceOperator(
            task_id="enable_notifications",
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data=request_payload.put_notifications_payload
        )

        get_exception_logs = rail.PythonOperator(
            task_id="get_exception_logs",
            python_callable=custom_methods.get_user_exception_logs
        )

        write_user_created_log = rail.WriteLogOperator(
            task_id="write_user_created_log",
            log='{{result("create_user_log")}}',
            message="User created",
            properties=lambda dag_run: {
                "win_id": dag_run.conf["win_id"],
                "first_name": dag_run.conf["first_name"],
                "last_name": dag_run.conf["last_name"],
                "email": dag_run.conf["email"],
                "action": "Rehire" if dag_run.conf["user_type"] == "rehire_user" else "Add",
                "assignment_status": dag_run.conf["assignment_status"],
                "date_active": dag_run.conf["date_active"],
                "status": "Exception" if rail.result("get_exception_logs") else "Success",
                "details": rail.result("get_exception_logs") + "User created partially"
                    if rail.result("get_exception_logs") else "User created succesfully."
            }
        )

        if_user_already_exists = rail.IfOperator(
            task_id = 'if_user_already_exists',
            trigger_rule='one_failed',
            test=custom_methods.if_loginname_already_exists,
            yes_task='log_user_already_exists_exception',
            no_task='catch_and_log_errors'
        )

        log_user_already_exists_exception = rail.WriteLogOperator(
            task_id="log_user_already_exists_exception",
            log='{{result("create_user_log")}}',
            message="User create failed",
            severity="Exception",
            properties=lambda dag_run: {
                "win_id": dag_run.conf["win_id"],
                "first_name": dag_run.conf["first_name"],
                "last_name": dag_run.conf["last_name"],
                "email": dag_run.conf["email"],
                "action": "Rehire" if dag_run.conf["user_type"] == "rehire_user" else "Add",
                "assignment_status": dag_run.conf["assignment_status"],
                "date_active": dag_run.conf["date_active"],
                "details": "The specified user already exists.",
                "status": "Exception"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{result("create_user_log")}}',
            message="User create failed",
            severity="Error",
            properties=lambda dag_run: {
                "win_id": dag_run.conf["win_id"],
                "first_name": dag_run.conf["first_name"],
                "last_name": dag_run.conf["last_name"],
                "email": dag_run.conf["email"],
                "action": "Rehire" if dag_run.conf["user_type"] == "rehire_user" else "Add",
                "assignment_status": dag_run.conf["assignment_status"],
                "date_active": dag_run.conf["date_active"],
                "details": rail.render_template('{{get_error_message()}}'),
                "status": "Error"
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> if_user_already_exists
        can_run_batch_task >> rail.Label("No") >>\
            create_user_log >>\
            if_manager_win >> rail.Label("No") >>\
            if_rehire_user
        if_manager_win >> rail.Label("Yes") >>\
            get_supervisor_details >> if_rehire_user >> rail.Label(
                "No") >> create_user_in_replicon
        if_rehire_user >> rail.Label("Yes") >> get_user_details >>\
            update_user_loginname >>\
            create_user_in_replicon >> enable_notifications_for_user >>\
            get_exception_logs >> write_user_created_log >> if_user_already_exists
        if_user_already_exists >> rail.Label("Yes") >> log_user_already_exists_exception
        if_user_already_exists >> rail.Label("No") >> catch_and_log_errors
    return dag


rail.for_each_instance(create_airflow_child)
