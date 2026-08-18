
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'siliconvalleycleanwater_workorder_sync_svc_create_workorder_child_v1_0_{config.instance}',
        description=f'SVC_create_workorder_Child - V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_dataclientdata_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_dataclientdata_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_dataclientdata_3 = rail.RepliconServiceOperator(
            task_id='get_dataclientdata_3',
            endpoint="/services/ClientListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:client-list-column:client",
                    "urn:replicon:client-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:client-list-filter:code"
                        },
                        "operatorUri": "urn:replicon:filter-operator:text-search",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": "18",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "operatorUri": "urn:replicon:filter-operator:or",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:client-list-filter:code"
                        },
                        "operatorUri": "urn:replicon:filter-operator:text-search",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": "19",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        invoke_custom_ruby_code_4 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_4',
            python_callable=lambda: {"clientlistoutput": list(map(lambda item: {
                "name": item['cells'][0].get('textValue'),
                "uri":  item['cells'][0].get('uri'),
                "code":  item['cells'][1].get('textValue'),
            }, rail.result('get_dataclientdata_3')['rows']))
            }
        )

        invoke_custom_ruby_code_5 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_5',
            python_callable=lambda: {
                "client1": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_4')['clientlistoutput'], 'code', "18", ('uri')),
                "client2": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_4')['clientlistoutput'], 'code', "19", ('uri'))
            }
        )

        declare_variable_6 = rail.SetVariableOperator(
            task_id='declare_variable_6',
            append=False,
            name='status',
            value='In Progress'
        )

        if_status_downcase_equals_to_canceled_7 = rail.IfOperator(
            task_id='if_status_downcase_equals_to_canceled_7',
            test='''{{ dag_run.conf.Status.lower()=='canceled' }}''',
            yes_task="update_variable_8",
            no_task="if_status_downcase_equals_to_closed_9",
        )

        update_variable_8 = rail.SetVariableOperator(
            task_id='update_variable_8',
            append=False,
            name='status',
            value='Cancelled'
        )

        if_status_downcase_equals_to_closed_9 = rail.IfOperator(
            task_id='if_status_downcase_equals_to_closed_9',
            test='''{{ dag_run.conf.Status.lower()=='closed' }}''',
            yes_task="update_variable_10",
            no_task="if_status_downcase_equals_to_completed_11",
        )

        update_variable_10 = rail.SetVariableOperator(
            task_id='update_variable_10',
            append=False,
            name='status',
            value='Completed'
        )

        if_status_downcase_equals_to_completed_11 = rail.IfOperator(
            task_id='if_status_downcase_equals_to_completed_11',
            test='''{{ dag_run.conf.Status.lower()=='completed' }}''',
            yes_task="update_variable_12",
            no_task="declare_variable_13",
        )

        update_variable_12 = rail.SetVariableOperator(
            task_id='update_variable_12',
            append=False,
            name='status',
            value='Completed'
        )

        declare_variable_13 = rail.SetVariableOperator(
            task_id='declare_variable_13',
            append=False,
            name='startdate',
            value=None
        )

        if_request_startdateday_present_14 = rail.IfOperator(
            task_id='if_request_startdateday_present_14',
            test='''{{ dag_run.conf.startdate | is_truthy }}''',
            yes_task="update_variable_15",
            no_task="declare_variable_16",
        )

        update_variable_15 = rail.SetVariableOperator(
            task_id='update_variable_15',
            append=False,
            name='startdate',
            value=lambda: {
                "date": rail.parse_date(rail.get_dag_run_conf()['startdate'], "%Y-%m-%d")
            }
        )

        declare_variable_16 = rail.SetVariableOperator(
            task_id='declare_variable_16',
            append=False,
            name='enddate',
            value=None
        )

        if_request_enddateday_present_17 = rail.IfOperator(
            task_id='if_request_enddateday_present_17',
            test='''{{ dag_run.conf.enddate | is_truthy }}''',
            yes_task="update_variable_18",
            no_task="declare_list_19",
        )

        update_variable_18 = rail.SetVariableOperator(
            task_id='update_variable_18',
            append=False,
            name='enddate',
            value=lambda: {
                "date": rail.parse_date(rail.get_dag_run_conf()['enddate'], "%Y-%m-%d")
            }
        )

        declare_list_19 = rail.SetVariableOperator(
            task_id='declare_list_19',
            append=False,
            name='oef',
            value=[]
        )

        if_request_requestiduri_present_20 = rail.IfOperator(
            task_id='if_request_requestiduri_present_20',
            test='''{{ dag_run.conf.requestiduri | is_truthy  and dag_run.conf.requestidvalue | is_truthy }}''',
            yes_task="insert_to_list_21",
            no_task="if_request_physicallocationuri_present_22",
        )

        insert_to_list_21 = rail.SetVariableOperator(
            task_id='insert_to_list_21',
            append=True,
            name='{{ result("declare_list_19").name }}',
            value={
                "definition": {
                    "uri": "{{ dag_run.conf.requestiduri }}",
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": "{{ dag_run.conf.requestidvalue | sn }}",
                "fileValue": null,
                "jsonValue": null
            }
        )

        if_request_physicallocationuri_present_22 = rail.IfOperator(
            task_id='if_request_physicallocationuri_present_22',
            test='''{{ dag_run.conf.physicallocationuri | is_truthy  and dag_run.conf.physicallocationvalue | is_truthy }}''',
            yes_task="insert_to_list_23",
            no_task="if_request_equipmenturi_present_24",
        )

        insert_to_list_23 = rail.SetVariableOperator(
            task_id='insert_to_list_23',
            append=True,
            name='{{ result("declare_list_19").name }}',
            value={
                "definition": {
                    "uri": "{{ dag_run.conf.physicallocationuri }}",
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": "{{ dag_run.conf.physicallocationvalue | sn }}",
                "fileValue": null,
                "jsonValue": null
            }
        )

        if_request_equipmenturi_present_24 = rail.IfOperator(
            task_id='if_request_equipmenturi_present_24',
            test='''{{ dag_run.conf.equipmenturi | is_truthy  and dag_run.conf.euquipmentvalue | is_truthy }}''',
            yes_task="insert_to_list_25",
            no_task="log_customoeftoassign_26",
        )

        insert_to_list_25 = rail.SetVariableOperator(
            task_id='insert_to_list_25',
            append=True,
            name='{{ result("declare_list_19").name }}',
            value={
                "definition": {
                    "uri": "{{ dag_run.conf.equipmenturi }}",
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": "{{ dag_run.conf.euquipmentvalue | sn }}",
                "fileValue": null,
                "jsonValue": null
            }
        )

        log_customoeftoassign_26 = rail.PythonOperator(
            task_id='log_customoeftoassign_26',
            python_callable=lambda: json.loads(json.dumps(rail.get_dag_run_var(rail.result('declare_list_19')[
                                               'name'])).replace('{"tagName":{}}', "null").replace('tagName": {}}', 'tagName":null}'))
        )

        create_project_or_apply_modifications_28 = rail.RepliconServiceOperator(
            task_id='create_project_or_apply_modifications_28',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": rail.render_template("{{ dag_run.conf.projectname }}")
                    },
                    "codeToApply": {
                        "value": rail.render_template("{{ dag_run.conf.projectnumber }}")
                    },
                    "startDateToApply": rail.get_dag_run_var('startdate'),
                    "endDateToApply": rail.get_dag_run_var('enddate'),
                    "clientBillingAllocationMethodToApply": "urn:replicon:client-billing-allocation-method:user-specified",
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "uri":   rail.render_template("{{ result('invoke_custom_ruby_code_5').client1 }}"),
                                    "name": null,
                                    "code": null,
                                    "parameterCorrelationId": null
                                },
                                "costAllocationPercentage": null
                            },
                            {
                                "client": {
                                    "uri": rail.render_template("{{ result('invoke_custom_ruby_code_5').client2 }}"),
                                    "name": null,
                                    "code": null,
                                    "parameterCorrelationId": null
                                },
                                "costAllocationPercentage": null
                            }
                        ],
                        "effectiveDate": null
                    },
                    "statusToApply": {
                        "uri": null,
                        "name": rail.get_dag_run_var('status')
                    },
                    "programToApply": {
                        "program": {
                            "uri": null,
                            "name": "Work Orders"
                        }
                    },
                    "objectExtensionFieldsToApply": rail.result('log_customoeftoassign_26')
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": rail.render_template("{{ current_time() }}")
            }
        )

        update_allow_time_entry_against_tasks_only_29 = rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only_29',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data={
                "projectUri": "{{ result('create_project_or_apply_modifications_28').uri }}",
                "allowTimeEntryAgainstTasksOnly": "false"
            }
        )

        foreach_request_33 = rail.ForEachOperator(
            task_id='foreach_request_33',
            items="{{ dag_run.conf.projectdata | to_json }}",
            start_task='if_foreach_request_33_resourceuri_present_34',
            end_task='foreach_request_33_end'
        )

        if_foreach_request_33_resourceuri_present_34 = rail.IfOperator(
            task_id='if_foreach_request_33_resourceuri_present_34',
            test='''{{ result('foreach_request_33').resourceuri | is_truthy }}''',
            yes_task="assign_user_to_project_35",
            no_task="svc_workorder_logs_add_entry_36",
        )

        assign_user_to_project_35 = rail.RepliconServiceOperator(
            task_id="assign_user_to_project_35",
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment2",
            data={
                "projectUri": "{{ result('create_project_or_apply_modifications_28').uri }}",
                "userUris":  ["{{ result('foreach_request_33').resourceuri }}"],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        svc_workorder_logs_add_entry_36 = rail.WriteLogOperator(
            task_id='svc_workorder_logs_add_entry_36',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Success",
            properties={
                "soserviceordernumber": "{{ dag_run.conf.projectnumber }}",
                "soserviceorderdescription": "{{ dag_run.conf.projectname }}",
                "soserviceorderstatus": "{{ dag_run.conf.Status }}",
                "umrequestedbyid30": "{{ result('foreach_request_33').resourcename if result('foreach_request_33').resourceuri  else '' }}",
                "sorequestedd_and_sodatecompletedate": "{{ dag_run.conf.startdate }} and {{ dag_run.conf.enddate }}",
                "project_level_oef's": '''umPhysicalLocation   -  "{{dag_run.conf.physicallocationvalue|sn}}", soRequestID - "{{dag_run.conf.requestidvalue|sn}}", Equipment Position - "{{dag_run.conf.euquipmentvalue|sn}}"''',
                "status": "Success",
                "message": "{{ get_error_message() |sn }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        foreach_request_33_end = rail.EmptyOperator(
            task_id='foreach_request_33_end',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message="na",
            severity="Exception",
            properties={
                "soserviceordernumber": "{{ dag_run.conf.projectnumber }}",
                "soserviceorderdescription": "{{ dag_run.conf.projectname }}",
                "soserviceorderstatus": "{{ dag_run.conf.Status }}",
                "umrequestedbyid30": "",
                "sorequestedd_and_sodatecompletedate": "{{ dag_run.conf.startdate }} and {{ dag_run.conf.enddate }}",
                "project_level_oef's": '''umPhysicalLocation   -  "{{dag_run.conf.physicallocationvalue|sn}}", soRequestID - "{{dag_run.conf.requestidvalue|sn}}", Equipment Position - "{{dag_run.conf.euquipmentvalue|sn}}"''',
                "status": "Error",
                "message": "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_dataclientdata_3
        get_dataclientdata_3 >> invoke_custom_ruby_code_4 >> invoke_custom_ruby_code_5 >> declare_variable_6 >> if_status_downcase_equals_to_canceled_7
        if_status_downcase_equals_to_canceled_7 >> rail.Label(
            'Yes') >> update_variable_8 >> if_status_downcase_equals_to_closed_9
        if_status_downcase_equals_to_canceled_7 >> rail.Label(
            'No') >> if_status_downcase_equals_to_closed_9
        if_status_downcase_equals_to_closed_9 >> rail.Label(
            'Yes') >> update_variable_10 >> if_status_downcase_equals_to_completed_11
        if_status_downcase_equals_to_closed_9 >> rail.Label(
            'No') >> if_status_downcase_equals_to_completed_11
        if_status_downcase_equals_to_completed_11 >> rail.Label(
            'Yes') >> update_variable_12 >> declare_variable_13
        if_status_downcase_equals_to_completed_11 >> rail.Label(
            'No') >> declare_variable_13 >> if_request_startdateday_present_14
        if_request_startdateday_present_14 >> rail.Label(
            'Yes') >> update_variable_15 >> declare_variable_16
        if_request_startdateday_present_14 >> rail.Label(
            'No') >> declare_variable_16 >> if_request_enddateday_present_17
        if_request_enddateday_present_17 >> rail.Label(
            'Yes') >> update_variable_18 >> declare_list_19
        if_request_enddateday_present_17 >> rail.Label(
            'No') >> declare_list_19 >> if_request_requestiduri_present_20
        if_request_requestiduri_present_20 >> rail.Label(
            'Yes') >> insert_to_list_21 >> if_request_physicallocationuri_present_22
        if_request_requestiduri_present_20 >> rail.Label(
            'No') >> if_request_physicallocationuri_present_22
        if_request_physicallocationuri_present_22 >> rail.Label(
            'Yes') >> insert_to_list_23 >> if_request_equipmenturi_present_24
        if_request_physicallocationuri_present_22 >> rail.Label(
            'No') >> if_request_equipmenturi_present_24
        if_request_equipmenturi_present_24 >> rail.Label(
            'Yes') >> insert_to_list_25 >> log_customoeftoassign_26
        if_request_equipmenturi_present_24 >> rail.Label(
            'No') >> log_customoeftoassign_26 >> create_project_or_apply_modifications_28 >> update_allow_time_entry_against_tasks_only_29 >> foreach_request_33 >> if_foreach_request_33_resourceuri_present_34
        if_foreach_request_33_resourceuri_present_34 >> rail.Label(
            'Yes') >> assign_user_to_project_35 >> svc_workorder_logs_add_entry_36 >> foreach_request_33_end
        if_foreach_request_33_resourceuri_present_34 >> rail.Label(
            'No') >> svc_workorder_logs_add_entry_36 >> foreach_request_33_end
        foreach_request_33 >> foreach_request_33_end >> finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
