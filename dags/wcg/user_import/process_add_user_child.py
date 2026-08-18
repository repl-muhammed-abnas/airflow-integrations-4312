from airflow.models import Variable
import rail
from wcg.user_import.utils import request_payload


def create_add_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_add_user_child_dag_id,
        description="WCG User Import - Add New User",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.add_user_child_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_user",
            end_task="catch_and_log_errors"
        )

        create_user = rail.RepliconServiceOperator(
            task_id="create_user",
            endpoint="/services/ImportService1.svc/PutUser3",
            data=lambda dag_run: request_payload.get_create_user_payload(dag_run, config)
        )

        check_if_labor_cost_present = rail.IfOperator(
            task_id="check_if_labor_cost_present",
            test='{{ dag_run.conf.get("labor_cost") is not none and dag_run.conf.labor_cost != "" }}',
            yes_task="set_user_cost_rate",
            no_task="if_supervisor_present"
        )

        set_user_cost_rate = rail.RepliconServiceOperator(
            task_id="set_user_cost_rate",
            endpoint="/services/ResourceService1.svc/PutUserCostRateSchedule",
            data=lambda dag_run: {
                "userUri": rail.result("create_user")["uri"],
                "schedule": {
                    "initialHourlyRate": {
                        "amount": str(dag_run.conf.get("labor_cost", "0")),
                        "currency": {
                            "uri": 'urn:replicon-tenant:'+ rail.get_tenant_slug() + ':currency:1',
                            "name": None,
                            "symbol": None
                        }
                    },
                    "scheduleEntries": []
                }
            }
        )

        if_supervisor_present = rail.IfOperator(
            task_id="if_supervisor_present",
            test='{{ dag_run.conf.get("supervisorempid") is not none and dag_run.conf.supervisorempid != "" }}',
            yes_task="check_if_user_and_supervisor_same",
            no_task="write_added_user_logs"
        )

        check_if_user_and_supervisor_same = rail.IfOperator(
            task_id='check_if_user_and_supervisor_same',
            test='{{ dag_run.conf.employeeid == dag_run.conf.supervisorempid }}',
            yes_task='write_added_user_logs',
            no_task='log_supervisor_for_later_processing'
        )

        log_supervisor_for_later_processing = rail.WriteLogOperator(
            task_id='log_supervisor_for_later_processing',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor assignment queued for processing after user report refresh",
            severity='Pending',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "firstname": dag_run.conf.get("firstname", ""),
                "lastname": dag_run.conf.get("lastname", ""),
                "useruri": rail.result("create_user")["uri"],
                "supervisorempid": dag_run.conf.get("supervisorempid", ""),
                "hire_date": dag_run.conf.get("hire_date", ""),
                "action": "Add",
                "status": "Pending",
                "details": f"User created successfully. Supervisor (NetSuite Internal ID: {dag_run.conf.get('supervisorempid', 'N/A')}) assignment queued for processing"
            }
        )

        write_added_user_logs = rail.WriteLogOperator(
            task_id="write_added_user_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: (
                "User added partially - Supervisor ID same as Employee ID"
                if dag_run.conf.get("employeeid") == dag_run.conf.get("supervisorempid")
                else "User added successfully"
            ),
            severity=lambda dag_run: (
                "Exception"
                if dag_run.conf.get("employeeid") == dag_run.conf.get("supervisorempid")
                else "Success"
            ),
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "firstname": dag_run.conf.get("firstname", ""),
                "lastname": dag_run.conf.get("lastname", ""),
                "action": "Add",
                "status": (
                    "Exception"
                    if dag_run.conf.get("employeeid") == dag_run.conf.get("supervisorempid")
                    else "Success"
                ),
                "details": (
                    f"Supervisor Employee ID ({dag_run.conf.get('supervisorempid')}) is same as Employee ID ({dag_run.conf.get('employeeid')})"
                    if dag_run.conf.get("employeeid") == dag_run.conf.get("supervisorempid")
                    else "User added successfully"
                )
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.log_artifact }}',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule="one_failed",
            properties={
                "employeeid": '{{ dag_run.conf.employeeid }}',
                "firstname": '{{ dag_run.conf.get("firstname", "") }}',
                "lastname": '{{ dag_run.conf.get("lastname", "") }}',
                "action": "Add",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_user >> check_if_labor_cost_present

        check_if_labor_cost_present >> rail.Label("Yes") >> set_user_cost_rate >> if_supervisor_present
        check_if_labor_cost_present >> rail.Label("No") >> if_supervisor_present

        if_supervisor_present >> rail.Label("Yes") >> check_if_user_and_supervisor_same
        if_supervisor_present >> rail.Label("No") >> write_added_user_logs

        check_if_user_and_supervisor_same >> rail.Label("Yes") >> write_added_user_logs
        check_if_user_and_supervisor_same >> rail.Label("No") >> log_supervisor_for_later_processing >> write_added_user_logs

        write_added_user_logs >> catch_and_log_errors

    return dag

rail.for_each_instance(create_add_user_child_dag)
