from americanintegrated.task_import.utils import request_methods
from americanintegrated.task_import.utils import custom_methods
import rail


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"amerincanintergrated_task_import_for_basic_tasks_child_{config.instance}",
        description="americanintegrated task import child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_child_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

        if_no_code_and_name_for_project = rail.IfOperator(
            task_id="if_no_code_and_name_for_project",
            test=custom_methods.check_code_and_name_in_replicon,
            yes_task="create_basic_task",
            no_task="if_task_code_exists"
        )

        create_basic_task = rail.RepliconServiceOperator(
            task_id="create_basic_task",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_methods.get_basic_task_request
        )

        write_basic_task_success_log = rail.WriteLogOperator(
            task_id="write_basic_task_success_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Success",
            message="Task added",
            properties={
                "jobid": '{{dag_run.conf.parent_ecid}}',
                "child_jobid": '{{dag_run_ecid()}}',
                "project_name": '{{dag_run.conf.projectname}}',
                "tasks": '{{dag_run.conf.taskname}}',
                "status": "Success",
                "details": "Task added"
            }
        )

        if_task_code_exists = rail.IfOperator(
            task_id="if_task_code_exists",
            test=custom_methods.check_for_code,
            yes_task="update_task_name",
            no_task="if_task_name_exists"
        )

        update_task_name = rail.RepliconServiceOperator(
            task_id="update_task_name",
            endpoint="/services/TaskService1.svc/UpdateName",
            data=lambda dag_run: {
                    "taskUri": rail.find_first_by_attr_and_get_attr(
                        dag_run.conf["get_all_tasks"],
                        "taskcode",
                        dag_run.conf["taskcode"],
                        "taskuri"),
                    "name": dag_run.conf["taskname"]
            }
        )

        write_basic_task_update_log = rail.WriteLogOperator(
            task_id="write_basic_task_update_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Success",
            message="Task name updated",
            properties={
                "jobid": '{{dag_run.conf.parent_ecid}}',
                "child_jobid": '{{dag_run_ecid()}}',
                "project_name": '{{dag_run.conf.projectname}}',
                "tasks": '{{dag_run.conf.taskname}}',
                "status": "Success",
                "details": "Task name updated"
            }
        )

        if_task_name_exists = rail.IfOperator(
            task_id="if_task_name_exists",
            test=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                dag_run.conf["get_all_tasks"],
                "displayText",
                dag_run.conf["taskname"],
                "taskcode") == dag_run.conf["taskcode"],
            no_task="write_basic_task_skipped_log",
            yes_task="write_basic_task_exists_log"

        )

        write_basic_task_skipped_log = rail.WriteLogOperator(
            task_id="write_basic_task_skipped_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Skipped",
            message="Basic Task name not updated, another task with same name already exists with different code.",
            properties={
                "jobid": '{{dag_run.conf.parent_ecid}}',
                "child_jobid": '{{dag_run_ecid()}}',
                "project_name": '{{dag_run.conf.projectname}}',
                "tasks": '{{dag_run.conf.taskname}}',
                "status": "Skipped",
                "details": "Basic Task name not updated, another task with same name already exists with different code."
            }
        )

        write_basic_task_exists_log = rail.WriteLogOperator(
            task_id="write_basic_task_exists_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Skipped",
            message="No change received.",
            properties={
                "jobid": '{{dag_run.conf.parent_ecid}}',
                "child_jobid": '{{dag_run_ecid()}}',
                "project_name": '{{dag_run.conf.projectname}}',
                "tasks": '{{dag_run.conf.taskname}}',
                "status": "Skipped",
                "details": "No change received."
            }
        )

        write_task_error_log = rail.WriteLogOperator(
            task_id="write_task_error_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Error",
            message="{{get_error_message()}}",
            trigger_rule="one_failed",
            properties={
                "jobid": '{{dag_run.conf.parent_ecid}}',
                "child_jobid": '{{dag_run_ecid()}}',
                "project_name": '{{dag_run.conf.projectname}}',
                "tasks": '{{dag_run.conf.taskname}}',
                "status": "Error",
                "details": "{{get_error_message()}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            trigger_rule="all_done",
            sumo_conn_id="sumologic-dagrunlogger"
        )

        if_no_code_and_name_for_project >> rail.Label("Yes") >> create_basic_task >>\
            write_basic_task_success_log >> write_task_error_log
        if_no_code_and_name_for_project >> rail.Label("No") >>\
            if_task_code_exists >> rail.Label("Yes") >> update_task_name >>\
            write_basic_task_update_log >> write_task_error_log
        if_task_code_exists >> rail.Label("No") >> if_task_name_exists >>\
            rail.Label(
                "Yes") >> write_basic_task_exists_log >> write_task_error_log
        if_task_name_exists >>\
            rail.Label("No") >> write_basic_task_skipped_log >>\
            write_task_error_log >> log_to_sumo
        return dag


rail.for_each_instance(create_airflow_child_dag)
