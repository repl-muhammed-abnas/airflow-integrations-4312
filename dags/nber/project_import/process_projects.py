import rail
from nber.project_import.utils import custom_methods, request_methods
from datetime import timedelta
from airflow.models import Variable

null = None

def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_project_dagid,
        description=f"Grant Import v1 child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        schedule_interval=None
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='catch_and_log_errors',
        )
        
        create_log = rail.CreateLogOperator(task_id="create_log")

        
        if_project_manager_in_payload = rail.IfOperator(
            task_id="if_project_manager_in_payload",
            test=lambda dag_run: dag_run.conf.get("grant_manager"),
            yes_task = "get_project_manager_details",
            no_task ="get_project_details"
        )

        get_project_manager_details = rail.RepliconServiceOperator(
            task_id="get_project_manager_details",
            endpoint = "/services/UserListService1.svc/GetData",
            data=request_methods.get_project_manager,
            data_handler = lambda response, dag_run: list(filter(lambda x:
                x["cells"][0]["textValue"] == dag_run.conf["grant_manager"], response["rows"]))
        )

        pm_assignment_details = rail.PythonOperator(
            task_id="pm_assignment_details",
            python_callable = lambda: rail.result("get_project_manager_details")[0]["cells"][0]["uri"] if 
            rail.result("get_project_manager_details") and \
                len(rail.result("get_project_manager_details")) == 1 else None
        )

        if_unique_pm_profile = rail.IfOperator(
            task_id="if_unique_pm_profile",
            test=lambda: rail.result("pm_assignment_details"),
            yes_task="add_project_manager_permissions",
            no_task ="get_project_details"
        )

        add_project_manager_permissions = rail.RepliconServiceOperator(
            task_id="add_project_manager_permissions",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_methods.add_pm_permissions
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: {
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": dag_run.conf["grant_code"],
                        "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda response: null if not response or not response[0].get("projectDetails") \
                else response[0]["projectDetails"]
        )

        if_project_exists = rail.IfOperator(
            task_id="if_project_exists",
            test=lambda: bool(rail.result("get_project_details")),
            yes_task="update_project",
            no_task="create_project"
        )

        create_project = rail.RepliconServiceOperator(
            task_id="create_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.build_create_payload(dag_run)
        )

        def get_log_details(dag_run, action="Created"):
            msg = ""

            if not dag_run.conf.get("program"):
                msg +=  f"Grant {action} but Program Not Found"
            if dag_run.conf.get("grant_manager") and not rail.result("get_project_manager_details"):
                msg += ";Project Manager profile not found in Replicon" \
                    if msg else f"Grant {action} Project Manager not assigned"
            elif dag_run.conf.get("grant_manager") and rail.result("get_project_manager_details") and \
                len(rail.result("get_project_manager_details")) > 1:
                msg += ";Project Manager not assigned as multiple active profiles are found in Replicon" \
                    if msg else f"Grant {action} Project Manager not assigned"
            return msg
        
        write_create_success = rail.WriteLogOperator(
            task_id="write_create_success",
            log='{{ result("create_log") }}',
            message="Grant Created Successfully",
            properties=lambda dag_run: {
                "grant_name": dag_run.conf["grant_name"],
                "grant_code": dag_run.conf["grant_code"],
                "status": "Exception" if get_log_details(dag_run) else "Success",
                "action": "Add",
                "details":  get_log_details(dag_run) if get_log_details(dag_run) else "Grant Created Successfully"
            }
        )

        update_project = rail.RepliconServiceOperator(
            task_id="update_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.build_update_payload(
                dag_run,
                rail.result("get_project_details")
            )
        )

        write_update_success = rail.WriteLogOperator(
            task_id="write_update_success",
            log='{{ result("create_log") }}',
            message="Grant Updated Successfully",
            severity="Success",
            properties=lambda dag_run: {
                "grant_name": dag_run.conf["grant_name"],
                "grant_code": dag_run.conf["grant_code"],
                "status": "Exception" if get_log_details(dag_run) else "Success",
                "action": "Update",
                "details": get_log_details(dag_run, "Updated") if get_log_details(dag_run, "Updated")\
                      else "Grant Updated Successfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ result("create_log") }}',
            trigger_rule="one_failed",
            message="Project was not processed",
            severity="Error",
            properties=lambda dag_run: {
                "grant_name": dag_run.conf.get("grant_name", "N/A"),
                "grant_code": dag_run.conf.get("grant_code", "N/A"),
                "status": "Error",
                "action": "Sync",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )
        can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
                'No') >> \
        create_log >> if_project_manager_in_payload >> rail.Label("No")>> get_project_details
        if_project_manager_in_payload >> rail.Label("Yes")>>\
        get_project_manager_details >> pm_assignment_details >>\
        if_unique_pm_profile >> rail.Label("Yes") >>\
        add_project_manager_permissions >> get_project_details
        if_unique_pm_profile >> rail.Label("No") >>\
        get_project_details >> if_project_exists >> rail.Label("Yes") >> update_project >> write_update_success

        (
            if_project_exists
            >> rail.Label("No")
            >> create_project
            >> write_create_success
        )

        write_update_success >> catch_and_log_errors
        write_create_success >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)