import json
from step.project_task_import import request_payload
import rail
null = None


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"step_project_task_import_to_replicon_create_project_child_{config.instance}",
        description="create project child",
        max_active_runs=config.max_active_child_run,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
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
            yes_task="create_step_project",
            no_task="step_log_project_exists"
        )

        step_log_project_exists = rail.WriteLogOperator(
            task_id="step_log_project_exists",
            log='{{dag_run.conf.lookuptable}}',
            severity="Ignored",
            message="Project already present in Replicon",
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "Ignored",
                "details": "Project already present in Replicon",
                "childjobid": rail.render_template('{{ecid()}}'),
                "Taskname": ""
            }
        )

        create_step_project = rail.RepliconServiceOperator(
            task_id="create_step_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.get_create_project_request,
            data_handler=lambda response: response["uri"] if "uri" in response else null
        )

        remove_team_member_assignment = rail.RepliconServiceOperator(
            task_id="remove_team_member_assignment",
            endpoint="/services/ProjectService1.svc/PutProjectTeamMemberAssignments",
            data=lambda: json.dumps({
                    "projectUri": rail.result("create_step_project"),
                    "resourceUris": []
            })
        )

        step_log_project_creation_success = rail.WriteLogOperator(
            task_id="step_log_project_creation_success",
            log='{{dag_run.conf.lookuptable}}',
            severity="success",
            message="Project created",
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "success",
                "details": "NA",
                "childjobid": rail.render_template('{{ecid()}}'),
                "Taskname": ""
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            trigger_rule="one_failed",
            log='{{dag_run.conf.lookuptable}}',
            severity="Error",
            message="Project creation failed",
            properties=lambda dag_run: {
                "jobid": dag_run.conf["parent_ecid"],
                "projectname": dag_run.conf["Project Name"],
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}"),
                "childjobid": rail.render_template('{{ecid()}}'),
                "Taskname": ""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger"
        )

        get_project_details >>\
        if_project_does_not_exist >> rail.Label("Yes") >> create_step_project >>\
        remove_team_member_assignment >> step_log_project_creation_success >> catch_and_log_errors
        if_project_does_not_exist >> rail.Label(
            "No") >> step_log_project_exists >> catch_and_log_errors >> log_to_sumo

        return dag


rail.for_each_instance(create_airflow_child_dag)
