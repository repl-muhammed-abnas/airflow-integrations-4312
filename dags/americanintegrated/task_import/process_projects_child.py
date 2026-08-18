from datetime import timedelta
import rail


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"amerincanintergrated_task_import_for_existing_projects_child_{config.instance}",
        description="americanintegrated task import child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_child_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

        get_all_tasks = rail.RepliconServiceOperator(
            task_id="get_all_tasks",
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails2",
            data={
                    "pageIndex": "1",
                    "pageSize": "1000",
                    "projectUris": [
                        "{{dag_run.conf.projecturi}}"
                    ],
                "taskDataInclusionOptionUris": []
            },
            data_handler=lambda response: list(map(lambda i: {
                "displayText": i["displayText"],
                "name": i["name"],
                "taskuri": i["uri"],
                "taskcode": i["code"]
            }, response))
        )

        if_prevailing_wage_task = rail.IfOperator(
            task_id="if_prevailing_wage_task",
            test=lambda dag_run: (dag_run.conf["prevailing_wage"] == "Yes" and 
                dag_run.conf["prevailing_wage_artifact_length"] > 0),
            yes_task="start_prevailing_wage_task",
            no_task="if_basic_task"
        )

        start_prevailing_wage_task = rail.EmptyOperator(
            task_id="start_prevailing_wage_task")
        for_each_prevailing_wage_task = rail.trigger_parallel_dagrun(
            task_id="for_each_prevailing_wage_task",
            items=lambda dag_run: rail.load_all_records(
                dag_run.conf["prevailing_wage_artifact"]),
            trigger_dag_id=f"amerincanintergrated_task_import_for_prevailing_wage_tasks_child_{config.instance}",
            parallel_count=config.max_active_child_runs,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda item, dag_run: {
                **item,
                "get_all_tasks": rail.result("get_all_tasks"),
                "projectname": dag_run.conf["projectname"],
                "projecturi": dag_run.conf["projecturi"],
                "prevailing_wage": dag_run.conf["prevailing_wage"],
                "Prevailing wages RT uri": dag_run.conf["Prevailing wages RT uri"],
                "Prevailing wages OT uri": dag_run.conf["Prevailing wages OT uri"],
                "Prevailing wages DT uri": dag_run.conf["Prevailing wages DT uri"],
                "lookuptable": dag_run.conf["lookuptable"],
                "parent_ecid": dag_run.conf["parent_ecid"],
            }
        )

        end_prevailing_wage_task = rail.EmptyOperator(
            task_id="end_prevailing_wage_task")

        if_basic_task = rail.IfOperator(
            task_id="if_basic_task",
            test=lambda dag_run: ((dag_run.conf["prevailing_wage"] == "No" and 
                dag_run.conf["basic_tasks_artifact_length"] > 0) or (
                dag_run.conf["prevailing_wage"] == "Yes" and 
                dag_run.conf["basic_tasks_artifact_length"] > 0)),
            yes_task="start_basic_task",
            no_task="end_basic_task"
        )
        start_basic_task = rail.EmptyOperator(task_id="start_basic_task")
        for_each_basic_task = rail.trigger_parallel_dagrun(
            task_id="for_each_basic_task",
            items=lambda dag_run: rail.load_all_records(
                dag_run.conf["basic_tasks_artifact"]),
            trigger_dag_id=f"amerincanintergrated_task_import_for_basic_tasks_child_{config.instance}",
            parallel_count=config.max_active_child_runs,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda item, dag_run: {
                **item,
                "get_all_tasks": rail.result("get_all_tasks"),
                "projectname": dag_run.conf["projectname"],
                "projecturi": dag_run.conf["projecturi"],
                "prevailing_wage": dag_run.conf["prevailing_wage"],
                "Prevailing wages RT uri": dag_run.conf["Prevailing wages RT uri"],
                "Prevailing wages OT uri": dag_run.conf["Prevailing wages OT uri"],
                "Prevailing wages DT uri": dag_run.conf["Prevailing wages DT uri"],
                "lookuptable": dag_run.conf["lookuptable"],
                "parent_ecid": dag_run.conf["parent_ecid"],
            }
        )

        end_basic_task = rail.EmptyOperator(task_id="end_basic_task")
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
                "tasks": '',
                "status": "Error",
                "details": "{{get_error_message()}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            trigger_rule="all_done",
            sumo_conn_id="sumologic-dagrunlogger"
        )

        get_all_tasks >>\
            if_prevailing_wage_task >> rail.Label(
                "No") >> if_basic_task
        if_prevailing_wage_task >> rail.Label(
            "Yes") >> start_prevailing_wage_task >>\
            for_each_prevailing_wage_task >> end_prevailing_wage_task >>\
            if_basic_task >> rail.Label("Yes") >> start_basic_task >>\
            for_each_basic_task >> end_basic_task
        if_basic_task >> rail.Label(
            "No") >> end_basic_task >> write_task_error_log >> log_to_sumo
    return dag


rail.for_each_instance(create_airflow_child_dag)
