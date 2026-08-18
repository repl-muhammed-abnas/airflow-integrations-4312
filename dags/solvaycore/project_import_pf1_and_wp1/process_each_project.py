import rail
null=None
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"solvaycore_project_import_to_replicon_process_each_project_child_{config.instance}",
        description="solvaycore project sync to replicon process projects",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_child,
        default_args={
            "sftp_conn_id":config.sftp_conn_id
        }
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_bulk_project_details = rail.RepliconServiceOperator(
            task_id="get_bulk_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run:{
                    "projects": [
                        {
                        "uri": null,
                        "name": null,
                        "code": dag_run.conf["accoladeprojectid"]
                                    if dag_run.conf["accoladeprojectid"]
                                    else dag_run.conf["projectcode"] ,
                        "parameterCorrelationId": null
                        }
                    ]
                },
            data_handler=lambda response:
                        response[0]["projectDetails"]["uri"]
                        if response[0]["projectDetails"] is not None and
                        "uri" in response[0]["projectDetails"]
                        else None
        )

        if_project_uri_not_present = rail.IfOperator(
            task_id="if_project_uri_not_present",
            test='{{result("get_bulk_project_details") | is_falsy}}',
            yes_task="process_each_project_record_create",
            no_task="process_each_project_record_update"

        )

        process_each_project_record_create = rail.TriggerDagRunOperator(
            task_id="process_each_project_record_create",
            trigger_dag_id=f"solvaycore_project_import_to_replicon_process_create_project_child_{config.instance}",
            conf=lambda dag_run: {
                **dag_run.conf,
            },
            wait_for_completion=True
        )

        process_each_project_record_update = rail.TriggerDagRunOperator(
            task_id="process_each_project_record_update",
            trigger_dag_id=f"solvaycore_project_import_to_replicon_process_update_project_child_{config.instance}",
            conf=lambda dag_run:{
                **dag_run.conf,
            },
            wait_for_completion=True
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.lookuptable}}",
            severity="Failed",
            message='{{ get_error_message() }}',
            properties=lambda dag_run:{
                "projectcode" : dag_run.conf["projectcode"],
                "projectdescription": dag_run.conf["projectdescription"],
                "JobID":dag_run.conf["parent_ecid"],
                "Task Code": dag_run.conf["wbscode"],
                "Status": "Failed",
                "Reason":rail.render_template('{{ get_error_message() }}'),
                "Child jobid": rail.render_template('{{ecid()}}')
            }
        )
  
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        get_bulk_project_details >>\
        if_project_uri_not_present >> rail.Label("Yes") >> process_each_project_record_create >> catch_and_log_errors
        if_project_uri_not_present >> rail.Label("No") >> process_each_project_record_update >> catch_and_log_errors >> log_to_sumo
    return dag
rail.for_each_instance(create_child_dag)
