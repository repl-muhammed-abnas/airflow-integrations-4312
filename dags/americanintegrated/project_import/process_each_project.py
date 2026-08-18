from americanintegrated.project_import import request_payload
from americanintegrated.project_import.tasks.update_projects import update_existing_project
from americanintegrated.project_import.tasks.create_projects import create_new_project
import rail
null = None


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.process_project_dag_id,
        description="americanintegrated project client",
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        company_key=config.company_key,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        if_project_code_present = rail.IfOperator(
            task_id="if_project_code_present",
            test='{{dag_run.conf.projectcode| is_truthy}}',
            yes_task="get_project_details_by_code",
            no_task="write_no_project_code_log"
        )

        write_no_project_code_log = rail.WriteLogOperator(
            task_id="write_no_project_code_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Project Skipped",
            severity="Skipped",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Project Name": dag_run.conf["projectname"],
                "Project Code": dag_run.conf["projectcode"],
                "Status": "Skipped",
                "Reason": "Project name not present",
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        get_project_details_by_code = rail.RepliconServiceOperator(
            task_id="get_project_details_by_code",
            endpoint="/services/ProjectListService1.svc/GetData",
            data=request_payload.get_project_by_code,
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(list(map(lambda i: {
                    "projectname": i["cells"][0]["textValue"],
                    "projecturi": i["cells"][0]["uri"],
                    "projectcode": i["cells"][1]["textValue"]
            }, response["rows"])),
                "projectcode",
                dag_run.conf["projectcode"],
                "projecturi") if response["rows"] else null
        )

        if_project_exists = rail.IfOperator(
            task_id="if_project_exists",
            test='{{result("get_project_details_by_code")| is_truthy}}',
            yes_task="start_update",
            no_task="start_create"
        )

        start_update = rail.EmptyOperator(task_id="start_update")

        update_project = update_existing_project(config)

        finish_update = rail.EmptyOperator(task_id="finish_update")

        start_create = rail.EmptyOperator(task_id="start_create")

        add_new_project = create_new_project(config)

        finish_create = rail.EmptyOperator(task_id="finish_create")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message='{{get_error_message()}}',
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Project Name": dag_run.conf["projectname"],
                "Project Code": dag_run.conf["projectcode"],
                "Status": "Error",
                "Reason": rail.render_template('{{get_error_message()}}'),
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        if_project_code_present >> rail.Label(
            "No") >> write_no_project_code_log >> catch_and_log_errors
        if_project_code_present >> rail.Label("Yes") >> get_project_details_by_code >>\
            if_project_exists >> rail.Label("Yes") >>\
            start_update >> update_project >> finish_update >> catch_and_log_errors
        if_project_exists >> rail.Label("No") >> \
            start_create >> add_new_project >> finish_create >> catch_and_log_errors

        return dag


rail.for_each_instance(create_airflow_child_dag)
