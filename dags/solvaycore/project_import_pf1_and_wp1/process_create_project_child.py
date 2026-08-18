import json
import rail
from solvaycore.project_import_pf1_and_wp1 import request_payload
from solvaycore.project_import_pf1_and_wp1 import custom_methods

null=None
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"solvaycore_project_import_to_replicon_process_create_project_child_{config.instance}",
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

        if_data_fields_are_not_present = rail.IfOperator(
            task_id="if_data_fields_are_not_present",
            test= lambda dag_run:bool(not(dag_run.conf["projectcode"] and
                                        dag_run.conf["projectdescription"] and
                                        dag_run.conf["companycode"] and
                                        dag_run.conf["projectstatus"] and
                                        dag_run.conf["controllingarea"] and
                                        dag_run.conf["wbscode"] and
                                        dag_run.conf["wbsdescription"] and
                                        dag_run.conf["wbsstatus"] and
                                        dag_run.conf["costtype"] and
                                        dag_run.conf["objectclass"] and
                                        dag_run.conf["bu"])),
            yes_task="data_fields_are_not_present_errlog",
            no_task="if_data_fields_values_are_not_present"
        )

        data_fields_are_not_present_errlog = rail.WriteLogOperator(
            task_id="data_fields_are_not_present_errlog",
            log="{{dag_run.conf.lookuptable}}",
            message="{{get_error_message()}}",
            severity="Exception",
            properties=custom_methods.data_field_errors
        )

        if_data_fields_values_are_not_present = rail.IfOperator(
            task_id="if_data_fields_values_are_not_present",
            test=lambda dag_run:bool(dag_run.conf["projectstatus"] not in  ["AVAILABLE", "CLOSED"] or
                        dag_run.conf["wbsstatus"] not in  ["AVAILABLE","CLOSED"] or
                        dag_run.conf["costtype"] not in ["OPEX","CAPEX"] or
                        dag_run.conf["originsystem"] not in (["PF1", "WP1"] if config.instance == "production" else ["PF2", "WP2"])),
            yes_task="data_fields_values_are_not_present_errlog",
            no_task="if_cost_type_and_object_class_are_present"
        )

        data_fields_values_are_not_present_errlog = rail.WriteLogOperator(
            task_id="data_fields_values_are_not_present_errlog",
            log="{{dag_run.conf.lookuptable}}",
            message="{{get_error_message()}}",
            severity="Exception",
            properties=lambda dag_run,config:custom_methods.data_field_value_errors(dag_run,config)
        )

        if_cost_type_and_object_class_are_present = rail.IfOperator(
            task_id="if_cost_type_and_object_class_are_present",
            test=lambda dag_run:bool((dag_run.conf["costtype"].upper() == "OPEX" and
                                    dag_run.conf["objectclass"].upper() == "OVERHEAD") or
                                    (dag_run.conf["costtype"].upper() == "CAPEX" and
                                    dag_run.conf["objectclass"].upper() == "INVESTMENT")),
            yes_task="if_accolade_project_and_projecturi_not_present",
            no_task="cost_type_and_object_class_are_present_errlog"
        )

        cost_type_and_object_class_are_present_errlog = rail.WriteLogOperator(
            task_id="cost_type_and_object_class_are_present_errlog",
            log="{{dag_run.conf.lookuptable}}",
            message="{{get_error_message()}}",
            severity="Exception",
            properties=lambda dag_run:{
                "projectcode" : dag_run.conf["projectcode"],
                "projectdescription": dag_run.conf["projectdescription"],
                "JobID":dag_run.conf["parent_ecid"],
                "Task Code": dag_run.conf["wbscode"],
                "Status": "Exception",
                "Reason":"Invalid Object class/Cost type.",
                "Child jobid": rail.render_template('{{ecid()}}')
            }
        )


        if_accolade_project_and_projecturi_not_present = rail.IfOperator(
            task_id="if_accolade_project_and_projecturi_not_present",
            test=lambda dag_run:bool(not rail.result("get_bulk_project_details") and
                            dag_run.conf["accoladeprojectid"]),
            yes_task="accolade_project_or_noproject_uri_errlog",
            no_task="if_not_accolade_project_or_noproject_uri"
        )

        accolade_project_or_noproject_uri_errlog = rail.WriteLogOperator(
            task_id="accolade_project_or_noproject_uri_errlog",
            log="{{dag_run.conf.lookuptable}}",
            message="{{get_error_message()}}",
            severity="Exception",
            properties=lambda dag_run:{
                "projectcode" : dag_run.conf["projectcode"],
                "projectdescription": dag_run.conf["projectdescription"],
                "JobID":dag_run.conf["parent_ecid"],
                "Task Code": dag_run.conf["wbscode"],
                "Status": "Exception",
                "Reason":"Accolade Project is not available in replicon.",
                "Child jobid": rail.render_template('{{ecid()}}')
            }
        )

        if_not_accolade_project_or_noproject_uri = rail.IfOperator(
            task_id="if_not_accolade_project_or_noproject_uri",
            test=lambda dag_run:bool(not rail.result("get_bulk_project_details") or
                            not dag_run.conf["accoladeprojectid"]),
            yes_task="get_project_leader_uri_if_present",
            no_task="not_accolade_project_or_noproject_uri_errlog"
        )

        not_accolade_project_or_noproject_uri_errlog = rail.WriteLogOperator(
            task_id="not_accolade_project_or_noproject_uri_errlog",
            log="{{dag_run.conf.lookuptable}}",
            message="{{get_error_message()}}",
            severity="Exception",
            properties=lambda dag_run:{
                "projectcode" : dag_run.conf["projectcode"],
                "projectdescription": dag_run.conf["projectdescription"],
                "JobID":dag_run.conf["parent_ecid"],
                "Task Code": dag_run.conf["wbscode"],
                "Status": "Exception",
                "Reason":"Accolade Project is not available in replicon.",
                "Child jobid": rail.render_template('{{ecid()}}')
            }
        )

        get_project_leader_uri_if_present = rail.RepliconServiceOperator(
            task_id="get_project_leader_uri_if_present",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_payload,
            data_handler=lambda response, dag_run:(response["rows"][0]["cells"][0]["uri"] if "rows" in response and
                        len(response["rows"]) > 0 and
                        response["rows"][0]["cells"] and
                        "uri" in response["rows"][0]["cells"][0] and
                        response["rows"][0]["cells"][0]["textValue"] == dag_run.conf["projectleader"] else null)
        )

        get_object_extension_definitions_for_project = rail.RepliconServiceOperator(
            task_id="get_object_extension_definitions_for_project",
            endpoint="/services/ObjectExtensionService1.svc/GetPageOfObjectExtensionDefinitionsFilteredBySearch",
            data={
                    "page": "1",
                    "pageSize": "500",
                    "bindingContextUri": "urn:replicon:object-type:project",
                    "textSearch": null
                }
        )

        get_object_extension_tags_for_gbu = rail.RepliconServiceOperator(
            task_id="get_object_extension_tags_for_gbu",
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda:json.dumps({
                    "page": "1",
                    "pageSize": "500",
                    "objectExtensionTagDefinitionUri":rail.find_first_by_attr_and_get_attr(
                                                        rail.result("get_object_extension_definitions_for_project"),
                                                        "displayText", "BFC Global Business Unit", "uri"),
                    "textSearch": null
                })
        )

        get_object_extension_tags_for_source_system = rail.RepliconServiceOperator(
            task_id="get_object_extension_tags_for_source_system",
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda: json.dumps({
                    "page": "1",
                    "pageSize": "500",
                    "objectExtensionTagDefinitionUri":rail.find_first_by_attr_and_get_attr(
                                                        rail.result("get_object_extension_definitions_for_project"),
                                                        "displayText", "Source System", "uri"),
                    "textSearch": null
                })
        )

        get_object_extension_tags_for_psfamily = rail.RepliconServiceOperator(
            task_id="get_object_extension_tags_for_psfamily",
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda: json.dumps({
                    "page": "1",
                    "pageSize": "500",
                    "objectExtensionTagDefinitionUri":rail.find_first_by_attr_and_get_attr(
                                                        rail.result("get_object_extension_definitions_for_project"),
                                                        "displayText", "PS Family", "uri"),
                    "textSearch": null
                })
        )

        get_service_center_data = rail.RepliconServiceOperator(
            task_id="get_service_center_data",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "1000",
                    "columnUris": [
                        "urn:replicon:service-center-list-column:code",
                        "urn:replicon:service-center-list-column:name"
                    ],
                    "sort": [],
                    "filterExpression": null
                },
            data_handler=lambda response:(list(map(lambda item:{
                                "code":item["cells"][0]["textValue"],
                                "name": item["cells"][1]["textValue"]
                                }, response["rows"])))
        )

        create_service_center_collection = rail.CreateCollectionOperator(
            task_id="create_service_center_collection",
            source="{{result('get_service_center_data')|to_json}}",
            name="servicecenterlistcreateproject",
        )

        query_service_center_collection = rail.QueryCollectionOperator(
            task_id="query_service_center_collection",
            query="""select * from servicecenterlistcreateproject where code='{{dag_run.conf["companycode"]}}'"""
        )

        if_object_extension_tags_for_psfamily = rail.IfOperator(
            task_id="if_object_extension_tags_for_psfamily",
            test=lambda dag_run:rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_tags_for_psfamily"),
                                "displayText", dag_run.conf["psfamily"], "uri") is not None,
            yes_task="if_object_extension_tags_for_gbu",
            no_task="object_extension_tags_for_psfamily_errlog"
        )

        object_extension_tags_for_psfamily_errlog = rail.WriteLogOperator(
            task_id="object_extension_tags_for_psfamily_errlog",
            log="{{dag_run.conf.lookuptable}}",
            message="{{get_error_message()}}",
            severity="Exception",
            properties=lambda dag_run:{
                "projectcode" : dag_run.conf["projectcode"],
                "projectdescription": dag_run.conf["projectdescription"],
                "JobID":dag_run.conf["parent_ecid"],
                "Task Code": dag_run.conf["wbscode"],
                "Status": "Exception",
                "Reason":"Source System is not available",
                "Child jobid": rail.render_template('{{ecid()}}')
            }
        )

        if_object_extension_tags_for_gbu = rail.IfOperator(
            task_id="if_object_extension_tags_for_gbu",
            test=lambda dag_run:rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_tags_for_gbu"),
                                            "displayText", dag_run.conf["gbu"], "uri") is not None,
            yes_task="create_project",
            no_task="object_extension_tags_for_gbu_errlog"
        )

        object_extension_tags_for_gbu_errlog = rail.WriteLogOperator(
            task_id="object_extension_tags_for_gbu_errlog",
            log="{{dag_run.conf.lookuptable}}",
            message="{{get_error_message()}}",
            severity="Exception",
            properties=lambda dag_run:{
                "projectcode" : dag_run.conf["projectcode"],
                "projectdescription": dag_run.conf["projectdescription"],
                "JobID":dag_run.conf["parent_ecid"],
                "Task Code": dag_run.conf["wbscode"],
                "Status": "Exception",
                "Reason":"Global Business unit not available in Replicon.",
                "Child jobid": rail.render_template('{{ecid()}}')
            }
        )
        create_project = rail.RepliconServiceOperator(
            task_id="create_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.create_project_request,
            data_handler=lambda response:response["uri"]
        )

        get_object_extension_definitions_for_task = rail.RepliconServiceOperator(
            task_id="get_object_extension_definitions_for_task",
            endpoint="services/ObjectExtensionService1.svc/GetPageOfObjectExtensionDefinitionsFilteredBySearch",
            data={
                    "page": "1",
                    "pageSize": "500",
                    "bindingContextUri": "urn:replicon:object-type:task",
                    "textSearch": null
                }
        )

        get_object_extension_tags_for_log_system = rail.RepliconServiceOperator(
            task_id="get_object_extension_tags_for_log_system",
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda:json.dumps({
                    "page": "1",
                    "pageSize": "500",
                    "objectExtensionTagDefinitionUri":rail.find_first_by_attr_and_get_attr(
                                                        rail.result("get_object_extension_definitions_for_task"),
                                                        "displayText", "Log System", "uri"),
                    "textSearch": null
                })
        )

        create_project_task = rail.RepliconServiceOperator(
            task_id="create_project_task",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.create_task_request
        )

        write_sucess_project_import_log = rail.WriteLogOperator(
            task_id="write_sucess_project_import_log",
            log="{{dag_run.conf.lookuptable}}",
            message="Success",
            properties=lambda dag_run:{
                "projectcode" : dag_run.conf["projectcode"],
                "projectdescription": dag_run.conf["projectdescription"],
                "JobID":dag_run.conf["parent_ecid"],
                "Task Code": dag_run.conf["wbscode"],
                "Status": "Success",
                "Reason":"",
                "Child jobid": rail.render_template('{{ecid()}}')
            }
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
        if_data_fields_are_not_present >> \
        rail.Label("Yes") >> data_fields_are_not_present_errlog >> catch_and_log_errors
        if_data_fields_are_not_present >> \
        rail.Label("No") >>\
        if_data_fields_values_are_not_present >>\
        rail.Label("Yes") >> data_fields_values_are_not_present_errlog >> catch_and_log_errors
        if_data_fields_values_are_not_present >>\
        rail.Label("No") >>\
        if_cost_type_and_object_class_are_present>>\
        rail.Label("No") >> cost_type_and_object_class_are_present_errlog >> catch_and_log_errors
        if_cost_type_and_object_class_are_present>>\
        rail.Label("Yes") >>\
        if_accolade_project_and_projecturi_not_present>>\
        rail.Label("Yes") >> accolade_project_or_noproject_uri_errlog >> catch_and_log_errors
        if_accolade_project_and_projecturi_not_present>>\
        rail.Label("No") >> \
        if_not_accolade_project_or_noproject_uri >>\
        rail.Label("Yes") >> get_project_leader_uri_if_present
        if_not_accolade_project_or_noproject_uri >>\
        rail.Label("No") >> not_accolade_project_or_noproject_uri_errlog >> catch_and_log_errors
        get_project_leader_uri_if_present >>\
        get_object_extension_definitions_for_project >>\
        [get_object_extension_tags_for_gbu,get_object_extension_tags_for_source_system,get_object_extension_tags_for_psfamily]>>\
        get_service_center_data >>\
        create_service_center_collection >> query_service_center_collection >>\
        if_object_extension_tags_for_psfamily >>\
        rail.Label("No") >> object_extension_tags_for_psfamily_errlog >> catch_and_log_errors
        if_object_extension_tags_for_psfamily >>\
        rail.Label("Yes") >>\
        if_object_extension_tags_for_gbu >>\
        rail.Label("No") >> object_extension_tags_for_gbu_errlog >> catch_and_log_errors
        if_object_extension_tags_for_gbu >>\
        rail.Label("Yes") >>\
        create_project >> \
        get_object_extension_definitions_for_task >> get_object_extension_tags_for_log_system >>\
        create_project_task >> write_sucess_project_import_log >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
