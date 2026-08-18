from americanintegrated.task_import.utils import request_methods
from americanintegrated.task_import.utils import custom_methods
import rail


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"amerincanintergrated_task_import_for_prevailing_wage_tasks_child_{config.instance}",
        description="americanintegrated task import child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_child_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

        if_no_code_and_name_for_project = rail.IfOperator(
            task_id="if_no_code_and_name_for_project",
            test=custom_methods.check_code_and_name_in_replicon_for_wage,
            yes_task="create_prevailing_wage_task",
            no_task="if_task_code_present_in_project"
        )

        create_prevailing_wage_task = rail.RepliconServiceOperator(
            task_id="create_prevailing_wage_task",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_methods.get_prevailing_wage_task_request
        )

        write_prevailing_wage_success_log = rail.WriteLogOperator(
            task_id="write_prevailing_wage_success_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="success",
            message="Prevailing wage task added",
            properties={
                "jobid": '{{dag_run.conf.parent_ecid}}',
                "child_jobid": '{{dag_run_ecid()}}',
                "project_name": '{{dag_run.conf.projectname}}',
                "tasks": '{{dag_run.conf.taskname}}',
                "status": "success",
                "details": "Task added"
            }
        )

        if_task_code_present_in_project = rail.IfOperator(
            task_id="if_task_code_present_in_project",
            test=lambda dag_run: not (rail.find_first_by_attr_and_get_attr(
                dag_run.conf["get_all_tasks"],
                "taskcode",
                dag_run.conf["taskcode"],
                "taskuri")),
            yes_task="write_prevailing_wage_code_log",
            no_task="if_task_name_present_in_project"
        )

        write_prevailing_wage_code_log = rail.WriteLogOperator(
            task_id="write_prevailing_wage_code_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Skipped",
            message="Another task with same code but different name found. Hence task not added.",
            properties={
                "jobid": '{{dag_run.conf.parent_ecid}}',
                "child_jobid": '{{dag_run_ecid()}}',
                "project_name": '{{dag_run.conf.projectname}}',
                "tasks": '{{dag_run.conf.taskname}}',
                "status": "Skipped",
                "details": "Another task with same code but different name found. Hence task not added."
            }
        )

        if_task_name_present_in_project = rail.IfOperator(
            task_id="if_task_name_present_in_project",
            test=lambda dag_run: (rail.find_first_by_attr_and_get_attr(
                dag_run.conf["get_all_tasks"],
                "displayText",
                dag_run.conf["taskname"],
                "taskuri"
            )),
            yes_task="write_prevailing_wage_name_log",
            no_task="write_task_error_log"
        )

        write_prevailing_wage_name_log = rail.WriteLogOperator(
            task_id="write_prevailing_wage_name_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Skipped",
            message="Task already exists",
            properties={
                "jobid": '{{dag_run.conf.parent_ecid}}',
                "child_jobid": '{{dag_run_ecid()}}',
                "project_name": '{{dag_run.conf.projectname}}',
                "tasks": '{{dag_run.conf.taskname}}',
                "status": "Skipped",
                "details": "Task already exists"
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

        if_no_code_and_name_for_project >> rail.Label("Yes") >> create_prevailing_wage_task >>\
            write_prevailing_wage_success_log >> write_task_error_log
        if_no_code_and_name_for_project >> rail.Label("No") >> if_task_code_present_in_project >>\
            rail.Label(
                "Yes") >> write_prevailing_wage_code_log >> write_task_error_log
        if_task_code_present_in_project >>\
            rail.Label("No") >> if_task_name_present_in_project >> rail.Label("Yes") >>\
            write_prevailing_wage_name_log >> write_task_error_log >> log_to_sumo
        if_task_name_present_in_project >> rail.Label("No") >>\
        write_task_error_log >> log_to_sumo
        return dag


rail.for_each_instance(create_airflow_child_dag)
