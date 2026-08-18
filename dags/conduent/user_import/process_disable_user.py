from datetime import datetime, timedelta
from pendulum import now, datetime as pdt
from conduent.user_import.utils import request_payload
from airflow.models import Variable
import rail
null = None


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.conduent_user_import_disable_users_child,
        description="Conduent user import process disable users",
        max_active_runs=config.max_active_run_child,
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=pdt(
            year=2024, month=8, day=1, tz=config.time_zone),
        schedule_interval=config.disable_user_schedule_interval
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_disable_user_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_disable_user_log",
            end_task="end_disable_user"
        )

        create_disable_user_log = rail.CreateLogOperator(
            task_id="create_disable_user_log"
        )

        get_all_user_with_enddate = rail.RepliconServiceOperator(
            task_id="get_all_user_with_enddate",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_user_payload(
                config, dag_run),
            data_handler=lambda response: list(
                map(lambda cell: {
                    "useruri": cell["cells"][0]["uri"],
                    "enddate": cell["cells"][1]["textValue"],
                    "win_id": cell["cells"][2]["textValue"],
                    "login_name": cell["cells"][3]["textValue"]
                }, filter(lambda ele: datetime.strptime(ele["cells"][1]["textValue"], "%m/%d/%Y") <
                          datetime.strptime(datetime.strftime(now(tz=config.time_zone), "%d/%m/%Y"),
                                            "%d/%m/%Y") and "textValue" in ele["cells"][2], response["rows"]))
            )
        )

        for_each_user = rail.ForEachOperator(
            task_id="for_each_user",
            items='{{result("get_all_user_with_enddate")|to_json}}',
            start_task="get_user_details",
            end_task="end_disable_user"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "uri": rail.result("for_each_user")["useruri"],
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        if_employee_is_project_manager = rail.IfOperator(
            task_id="if_employee_is_project_manager",
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_details")["permissionSets"],
                "displayText",
                "Project Manager",
                "uri"),
            yes_task="end_disable_user",
            no_task="disable_user_in_replicon"
        )

        disable_user_in_replicon = rail.RepliconServiceOperator(
            task_id='disable_user_in_replicon',
            endpoint='services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ result("for_each_user").useruri }}'
            }
        )

        write_user_disable_logs = rail.WriteLogOperator(
            task_id="write_user_disable_logs",
            log='{{result("create_disable_user_log")}}',
            message="User disabled",
            properties=lambda: {
                "win_id": rail.result("for_each_user")["win_id"],
                "first_name": rail.result("get_user_details")["userDetails"]["firstName"],
                "last_name": rail.result("get_user_details")["userDetails"]["lastName"],
                "email": rail.result("for_each_user")["login_name"],
                "assignment_status": rail.result("get_user_details")["userDetails"]["customFieldValues"][0]["text"],
                "date_active": rail.result("get_user_details")["userDetails"]["employmentDateRange"]["startDate"],
                "details": "User disabled",
                "status": "Success",
                "action": "Disable"
            }
        )

        end_disable_user = rail.EmptyOperator(
            task_id="end_disable_user")

        process_logs = rail.TriggerDagRunOperator(
            task_id="process_logs",
            trigger_dag_id=config.conduent_user_import_process_logs_child,
            wait_for_completion=True,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda dag_run: {
                "parent_run_id": dag_run.id,
                "disable_user": True
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> end_disable_user
        can_run_batch_task >> rail.Label("No") >> \
            create_disable_user_log >>\
            get_all_user_with_enddate >> for_each_user >> end_disable_user
        for_each_user >>\
            get_user_details >>\
            if_employee_is_project_manager >> rail.Label(
                "Yes") >> end_disable_user
        if_employee_is_project_manager >> rail.Label("No") >>\
            disable_user_in_replicon >> write_user_disable_logs >> end_disable_user >> process_logs

    return dag


rail.for_each_instance(create_airflow_child)
