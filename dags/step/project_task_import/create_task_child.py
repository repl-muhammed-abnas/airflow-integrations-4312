import json
from step.project_task_import import request_payload
from step.project_task_import.tasks.create_child_task import step_create_child_task
from step.project_task_import.tasks.create_parent_task import step_create_parent_task
import rail
null = None


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"step_project_task_import_to_replicon_create_task_child_{config.instance}",
        description="step task creation dag",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_run,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        if_childtask_without_parent = rail.IfOperator(
            task_id="if_childtask_without_parent",
            test=lambda dag_run: bool(
                not dag_run.conf["Task Name Level 1"] and dag_run.conf["Task Name Level 2"]),
            yes_task="step_log_no_parent_task",
            no_task="get_project_details"
        )

        step_log_no_parent_task = rail.WriteLogOperator(
            task_id="step_log_no_parent_task",
            log='{{dag_run.conf.lookuptable}}',
            severity="ignored",
            message="Project created",
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "ignored",
                "details": "Task Level 1 not present ",
                "childjobid": rail.render_template('{{ecid()}}'),
                "Taskname": dag_run.conf["Task Name Level 2"]
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: json.dumps({
                    "projects": [
                        {
                            "uri": null,
                            "name": dag_run.conf["Project Name"],
                            "code": null,
                            "parameterCorrelationId": null
                        }
                    ]
            }),
            data_handler=lambda response: response[0]["projectDetails"][
                "uri"] if response[0]["projectDetails"] else None
        )

        if_project_does_not_exist = rail.IfOperator(
            task_id="if_project_does_not_exist",
            test="{{result('get_project_details')| is_falsy}}",
            yes_task="step_log_project_does_not_exists",
            no_task="if_task_level1_in_record"
        )

        step_log_project_does_not_exists = rail.WriteLogOperator(
            task_id="step_log_project_does_not_exists",
            log='{{dag_run.conf.lookuptable}}',
            severity="ignored",
            message="Project is not present in Replicon",
            # pylint:disable=line-too-long
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "ignored",
                "details": "Project is not present in Replicon",
                "childjobid": rail.render_template('{{ecid()}}'),
                "Taskname": dag_run.conf["Task Name Level 1"] + "|" + dag_run.conf["Task Name Level 2"] if dag_run.conf["Task Name Level 2"] else dag_run.conf["Task Name Level 1"]
            }
        )

        if_task_level1_in_record = rail.IfOperator(
            task_id="if_task_level1_in_record",
            test=lambda dag_run: bool(dag_run.conf["Task Name Level 1"]),
            yes_task="get_task_details",
            no_task="end_task_import"
        )

        get_task_details = rail.RepliconServiceOperator(
            task_id="get_task_details",
            endpoint="/services/TaskListService1.svc/GetData",
            data=request_payload.get_task_details,
            data_handler=lambda response: response["rows"]
        )

        if_data_type_in_task_details = rail.IfOperator(
            task_id="if_data_type_in_task_details",
            test=lambda: bool(rail.result("get_task_details") and "cells" in rail.result("get_task_details")[0] and
                              "dataType" in rail.result("get_task_details")[0]["cells"][0]),
            yes_task="get_task_data",
            no_task="run_create_parent_task_group1"
        )

        get_task_data = rail.PythonOperator(
            task_id="get_task_data",
            # pylint:disable=line-too-long
            python_callable=lambda: {
                    "taskname": rail.result("get_task_details")[0]["cells"][0]["textValue"],
                    "taskuri": rail.result("get_task_details")[0]["cells"][0]["uri"],
                    "parenttask": rail.result("get_task_details")[0]["cells"][2]["textValue"] if "textValue" in rail.result("get_task_details")[0]["cells"][2] else null,
                    "parenttaskuri": rail.result("get_task_details")[0]["cells"][2]["uri"] if "uri" in rail.result("get_task_details")[0]["cells"][2] else null,
            }
        )

        if_level1_task_in_project = rail.IfOperator(
            task_id="if_level1_task_in_project",
            test=lambda: bool(rail.result("get_task_data")["parenttask"] is null and
                              rail.result("get_task_data")["taskname"] is not null),
            yes_task="if_task_level2_in_record",
            no_task="run_create_parent_task_group1"
        )

        if_task_level2_in_record = rail.IfOperator(
            task_id="if_task_level2_in_record",
            test='{{dag_run.conf["Task Name Level 2"]|is_truthy}}',
            yes_task="get_all_children_tasks",
            no_task="end_task_import"
        )

        get_all_children_tasks = rail.RepliconServiceOperator(
            task_id="get_all_children_tasks",
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data=lambda: json.dumps({
                    "parentUri": rail.result("get_task_data")["taskuri"]
            }),
            data_handler=lambda response: list(
                map(lambda i: i["name"], response)) if response else null
        )

        if_child_task_exists = rail.IfOperator(
            task_id="if_child_task_exists",
            test=lambda dag_run: bool(dag_run.conf["Task Name Level 2"] in rail.result(
                "get_all_children_tasks") if rail.result("get_all_children_tasks") else null),
            yes_task="step_log_child_task_exists",
            no_task="run_child_task_group1"
        )

        run_child_task_group1 = rail.EmptyOperator(
            task_id="run_child_task_group1")

        declare_userdata_list = rail.SetVariableOperator(
            task_id="declare_userdata_list",
            append=True,
            name='userdata',
            value=[]
        )
        create_child_task_1 = step_create_child_task(
            task_name="create_child_task_1")

        step_log_child_task_exists = rail.WriteLogOperator(
            task_id="step_log_child_task_exists",
            log='{{dag_run.conf.lookuptable}}',
            severity="ignored",
            message="Task(s) is already present in Replicon",
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "ignored",
                "details": "Task(s) is already present in Replicon",
                "childjobid": rail.render_template('{{ecid()}}'),
                # pylint:disable=line-too-long
                "Taskname": dag_run.conf["Task Name Level 1"] + "|" + dag_run.conf["Task Name Level 2"] if dag_run.conf["Task Name Level 2"] else dag_run.conf["Task Name Level 1"]
            }
        )

        run_create_parent_task_group1 = rail.EmptyOperator(
            task_id="run_create_parent_task_group1")

        create_parent_task_1 = step_create_parent_task(
            task_name="create_parent_task_1")

        if_task_level2_in_record1 = rail.IfOperator(
            task_id="if_task_level2_in_record1",
            test='{{dag_run.conf["Task Name Level 2"]|is_truthy}}',
            yes_task="run_child_task_group2",
            no_task="end_task_import"
        )
        run_child_task_group2 = rail.EmptyOperator(
            task_id="run_child_task_group2")

        create_child_task_2 = step_create_child_task(
            task_name="create_child_task_2")

        end_task_import = rail.EmptyOperator(task_id="end_task_import")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            trigger_rule="one_failed",
            log='{{dag_run.conf.lookuptable}}',
            severity="Error",
            message="{{get_error_message()}}",
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}"),
                "childjobid": rail.render_template('{{ecid()}}'),
                # pylint:disable=line-too-long
                "Taskname": dag_run.conf["Task Name Level 1"] + "|" + dag_run.conf["Task Name Level 2"] if dag_run.conf["Task Name Level 2"] else dag_run.conf["Task Name Level 1"]
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger"
        )

        if_childtask_without_parent >> rail.Label("Yes") >>\
            step_log_no_parent_task >> end_task_import
        if_childtask_without_parent >> rail.Label("No") >> get_project_details >>\
            if_project_does_not_exist >> rail.Label("Yes") >>\
            step_log_project_does_not_exists >> end_task_import
        if_project_does_not_exist >> rail.Label("No") >>\
            if_task_level1_in_record >> rail.Label("Yes") >> get_task_details >>\
            if_data_type_in_task_details >> rail.Label("Yes") >> get_task_data >>\
            if_level1_task_in_project >> rail.Label("Yes") >>\
            if_task_level2_in_record >> rail.Label("Yes") >> get_all_children_tasks >>\
            if_child_task_exists >> rail.Label("No") >>\
            run_child_task_group1 >> declare_userdata_list >> create_child_task_1 >> end_task_import
        if_child_task_exists >> rail.Label(
            "Yes") >> step_log_child_task_exists >> end_task_import
        if_data_type_in_task_details >> rail.Label("No") >>\
            run_create_parent_task_group1 >> create_parent_task_1 >>\
            if_task_level2_in_record1 >> rail.Label("Yes") >>\
            run_child_task_group2 >> create_child_task_2 >> end_task_import
        if_task_level2_in_record1 >> rail.Label("No") >> end_task_import
        if_level1_task_in_project >> rail.Label("No") >>\
            run_create_parent_task_group1 >> create_parent_task_1 >>\
            if_task_level2_in_record1 >> rail.Label("Yes") >>\
            run_child_task_group2 >> create_child_task_2 >> end_task_import
        if_task_level1_in_record >> rail.Label("No") >> end_task_import
        if_task_level2_in_record >> rail.Label("No") >> end_task_import
        end_task_import >> catch_and_log_errors >> log_to_sumo

        return dag


rail.for_each_instance(create_airflow_child_dag)
