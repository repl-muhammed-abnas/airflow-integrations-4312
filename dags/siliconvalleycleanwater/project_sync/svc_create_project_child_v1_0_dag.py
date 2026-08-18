
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'siliconvalleycleanwater_project_sync_svc_create_project_child_v1_0_{config.instance}',
        description=f'SVC_create_project_Child - V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_data_clienturi_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_data_clienturi_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_data_clienturi_3 = rail.RepliconServiceOperator(
            task_id='get_data_clienturi_3',
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
                            "text": "{{ dag_run.conf.client }}",
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
                }
            }
        )

        invoke_custom_ruby_code_4 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_4',
            python_callable=lambda: {"clientlistoutput": list(map(lambda item: {
                "name": item['cells'][0].get('textValue'),
                "uri":  item['cells'][0].get('uri'),
                "code":  item['cells'][1].get('textValue'),
            }, rail.result('get_data_clienturi_3')['rows']))}
        )

        log_clienturi_5 = rail.PythonOperator(
            task_id='log_clienturi_5',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'invoke_custom_ruby_code_4')['clientlistoutput'], 'code', rail.get_dag_run_conf()['client'], ('uri'))
        )

        declare_variable_6 = rail.SetVariableOperator(
            task_id='declare_variable_6',
            append=False,
            name='status',
            value='In Progress'
        )

        if_status_downcase_equals_to_onhold_7 = rail.IfOperator(
            task_id='if_status_downcase_equals_to_onhold_7',
            test='''{{ dag_run.conf.Status.lower()=='on hold' }}''',
            yes_task="update_variable_8",
            no_task="if_status_downcase_equals_to_completed_9",
        )

        update_variable_8 = rail.SetVariableOperator(
            task_id='update_variable_8',
            append=False,
            name='status',
            value='Deferred'
        )

        if_status_downcase_equals_to_completed_9 = rail.IfOperator(
            task_id='if_status_downcase_equals_to_completed_9',
            test='''{{ dag_run.conf.Status.lower()=='completed' }}''',
            yes_task="update_variable_10",
            no_task="declare_variable_11",
        )

        update_variable_10 = rail.SetVariableOperator(
            task_id='update_variable_10',
            append=False,
            name='status',
            value='Completed'
        )

        declare_variable_11 = rail.SetVariableOperator(
            task_id='declare_variable_11',
            append=False,
            name='startdate',
            value=None
        )

        if_request_startdateday_present_12 = rail.IfOperator(
            task_id='if_request_startdateday_present_12',
            test='''{{ dag_run.conf.startdate | is_truthy }}''',
            yes_task="update_variable_13",
            no_task="declare_variable_14",
        )

        update_variable_13 = rail.SetVariableOperator(
            task_id='update_variable_13',
            append=False,
            name='startdate',
            value=lambda: {
                "date": rail.parse_date(rail.get_dag_run_conf()['startdate'], "%Y-%m-%d")
            }
        )

        declare_variable_14 = rail.SetVariableOperator(
            task_id='declare_variable_14',
            append=False,
            name='enddate',
            value=None
        )

        if_request_enddateday_present_15 = rail.IfOperator(
            task_id='if_request_enddateday_present_15',
            test='''{{ dag_run.conf.enddate | is_truthy }}''',
            yes_task="update_variable_16",
            no_task="declare_variable_17",
        )

        update_variable_16 = rail.SetVariableOperator(
            task_id='update_variable_16',
            append=False,
            name='enddate',
            value=lambda: {
                 "date": rail.parse_date(rail.get_dag_run_conf()['enddate'], "%Y-%m-%d")
            }
        )

        declare_variable_17 = rail.SetVariableOperator(
            task_id='declare_variable_17',
            append=False,
            name='client',
            value=None
        )

        if_log_clienturi_5_present_18 = rail.IfOperator(
            task_id='if_log_clienturi_5_present_18',
            test='''{{ result('log_clienturi_5') | is_truthy }}''',
            yes_task="update_variable_19",
            no_task="create_project_or_apply_modifications_21",
        )

        update_variable_19 = rail.SetVariableOperator(
            task_id='update_variable_19',
            append=False,
            name='client',
            value={
                "clients": [
                    {
                        "client": {
                            "uri": "{{ result('log_clienturi_5') }}",
                            "name": null,
                            "code": null,
                            "parameterCorrelationId": null
                        },
                        "costAllocationPercentage": 100
                    }
                ],
                "effectiveDate": null
            }
        )

        create_project_or_apply_modifications_21 = rail.RepliconServiceOperator(
            task_id='create_project_or_apply_modifications_21',
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
                    "descriptionToApply": null,
                    "percentCompletedToApply": null,
                    "startDateToApply": rail.get_dag_run_var('startdate'),
                    "endDateToApply": rail.get_dag_run_var('enddate'),
                    "clientBillingAllocationMethodToApply": "urn:replicon:client-billing-allocation-method:split",
                    "clientAssignmentsSchedulesToApply": rail.get_dag_run_var('client'),
                    "statusToApply": {
                        "uri": null,
                        "name": rail.get_dag_run_var('status'),
                    } if rail.get_dag_run_var('status') else null,
                    "programToApply": {
                        "program": {
                            "uri": null,
                            "name": "Projects"
                        }
                    }
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": rail.render_template("{{ current_time() }}")
            }
        )

        update_allow_time_entry_against_tasks_only_22 = rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only_22',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data={
                "projectUri": "{{ result('create_project_or_apply_modifications_21').uri }}",
                "allowTimeEntryAgainstTasksOnly": "false"
            }
        )

        foreach_request_26 = rail.ForEachOperator(
            task_id='foreach_request_26',
            items="{{ dag_run.conf.projectdata | to_json }}",
            start_task='if_foreach_request_26_resourceuri_present_27',
            end_task='foreach_request_26_end'
        )

        if_foreach_request_26_resourceuri_present_27 = rail.IfOperator(
            task_id='if_foreach_request_26_resourceuri_present_27',
            test='''{{ result('foreach_request_26').resourceuri | is_truthy }}''',
            yes_task="assign_user_to_project_28",
            no_task="svc_project_sync_logs_add_entry_31",
        )

        assign_user_to_project_28 = rail.RepliconServiceOperator(
            task_id="assign_user_to_project_28",
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment2",
            data={
                "projectUri": "{{ result('create_project_or_apply_modifications_21').uri }}",
                "userUris":  ["{{ result('foreach_request_26').resourceuri }}"],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        svc_project_sync_logs_add_entry_29 = rail.WriteLogOperator(
            task_id='svc_project_sync_logs_add_entry_29',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Success",
            properties={
                "project_number": "{{ dag_run.conf.projectnumber| sn }}",
                "project_name": "{{ dag_run.conf.projectname| sn }}",
                "project_status": "{{ dag_run.conf.Status | sn}}",
                "project_manager_id": "{{ result('foreach_request_26').resourcename  | sn }}",
                "actual_begin_date": "{{ dag_run.conf.startdate| sn }}",
                "actual_end_date": "{{ dag_run.conf.enddate| sn }}",
                "status": "Success",
                "details": "Project created and Resource is assigned to Project successfully",
            }
        )

        svc_project_sync_logs_add_entry_31 = rail.WriteLogOperator(
            task_id='svc_project_sync_logs_add_entry_31',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Exception",
            properties={
                "project_number": "{{ dag_run.conf.projectnumber| sn }}",
                "project_name": "{{ dag_run.conf.projectname| sn }}",
                "project_status": "{{ dag_run.conf.Status| sn }}",
                "project_manager_id": "{{ result('foreach_request_26').resourcename | sn }}",
                "actual_begin_date": "{{ dag_run.conf.startdate | sn}}",
                "actual_end_date": "{{ dag_run.conf.enddate | sn}}",
                "status": "Exception",
                "details": "Project created successfully. However resource did not get assigned to Project since the user is not available in Replicon/not provided",
            }
        )

        foreach_request_26_end = rail.EmptyOperator(
            task_id='foreach_request_26_end',
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
                "project_number": "{{ dag_run.conf.projectnumber| sn }}",
                "project_name": "{{ dag_run.conf.projectname| sn }}",
                "project_status": "{{ dag_run.conf.Status| sn }}",
                "project_manager_id": "",
                "actual_begin_date": "{{ dag_run.conf.startdate | sn}}",
                "actual_end_date": "{{ dag_run.conf.enddate | sn}}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_data_clienturi_3
        get_data_clienturi_3 >> invoke_custom_ruby_code_4 >> log_clienturi_5 >> declare_variable_6 >> if_status_downcase_equals_to_onhold_7
        if_status_downcase_equals_to_onhold_7 >> rail.Label(
            'Yes') >> update_variable_8 >> if_status_downcase_equals_to_completed_9
        if_status_downcase_equals_to_onhold_7 >> rail.Label(
            'No') >> if_status_downcase_equals_to_completed_9
        if_status_downcase_equals_to_completed_9 >> rail.Label(
            'Yes') >> update_variable_10 >> declare_variable_11
        if_status_downcase_equals_to_completed_9 >> rail.Label(
            'No') >> declare_variable_11 >> if_request_startdateday_present_12
        if_request_startdateday_present_12 >> rail.Label(
            'Yes') >> update_variable_13 >> declare_variable_14
        if_request_startdateday_present_12 >> rail.Label(
            'No') >> declare_variable_14 >> if_request_enddateday_present_15
        if_request_enddateday_present_15 >> rail.Label(
            'Yes') >> update_variable_16 >> declare_variable_17
        if_request_enddateday_present_15 >> rail.Label(
            'No') >> declare_variable_17 >> if_log_clienturi_5_present_18
        if_log_clienturi_5_present_18 >> rail.Label(
            'Yes') >> update_variable_19 >> create_project_or_apply_modifications_21
        if_log_clienturi_5_present_18 >> rail.Label(
            'No') >> create_project_or_apply_modifications_21 >> update_allow_time_entry_against_tasks_only_22 >> foreach_request_26 >> if_foreach_request_26_resourceuri_present_27
        if_foreach_request_26_resourceuri_present_27 >> rail.Label(
            'Yes') >> assign_user_to_project_28 >> svc_project_sync_logs_add_entry_29 >> foreach_request_26_end
        if_foreach_request_26_resourceuri_present_27 >> rail.Label(
            'No') >> svc_project_sync_logs_add_entry_31 >> foreach_request_26_end
        foreach_request_26 >> foreach_request_26_end >> finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
