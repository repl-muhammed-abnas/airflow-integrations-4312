
from datetime import timedelta, datetime
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'broadridge_new_project_child_{config.instance}',
        description=f'Broadridge_new_project_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='load_input'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='load_input',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        load_input = rail.LoadCSVFileOperator(
            task_id="load_input",
            document="{{dag_run.conf.inputfile}}"
        )

        create_collection_from_list = rail.CreateCollectionOperator(
            task_id='create_collection_from_list',
            source="{{ result('load_input') }}",
            name="inputdata",
            columns={
                'Project Name': 'projectname',
                'Project Code': 'projectcode',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Project Manager': 'projectmanager',
                'Client Code': 'clientcode',
                'Task Name': 'taskname',
                'Task Team Assignment': 'taskteam',
                'Task Start Date': 'taskstartdate',
                'Task End Date': 'taskenddate',
                'TaskOutlinelevel': 'taskoutlinelevel',
                'TaskOutlineNumber': 'taskoutlinenumber',
                'Metis_ProjectUID': 'metisprojectuid',
                'Metis_TaskUID': 'metistaskuid'
            }
        )

        query_list_new_project = rail.QueryCollectionOperator(
            task_id='query_list_new_project',
            query="""SELECT  inputdata.projectname, inputdata.projectcode, inputdata.startdate, inputdata.enddate, inputdata.projectmanager, inputdata.clientcode, inputdata.taskname, inputdata.taskteam, inputdata.taskstartdate, inputdata.taskenddate, inputdata.taskoutlinelevel, inputdata.taskoutlinenumber, inputdata.metisprojectuid, inputdata.metistaskuid FROM  inputdata WHERE projectname = '{{ dag_run.conf.projectname}}'""",
        )

        load_query_list = rail.PythonOperator(
            task_id='load_query_list',
            python_callable=lambda: rail.load_all_records(
                rail.result('query_list_new_project'))
        )

        if_first_clientcode_present = rail.IfOperator(
            task_id='if_first_clientcode_present',
            test="{{result('query_list_new_project','length') > 0}}",
            yes_task="getclientbasedoncode",
            no_task="if_request_projectname_ends_with_fb",
        )

        getclientbasedoncode = rail.RepliconServiceOperator(
            task_id='getclientbasedoncode',
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda: {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:client-list-column:code",
                    "urn:replicon:client-list-column:client"
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
                            "text": rail.result('load_query_list')[0]['clientcode'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
        )

        log_clientnameexactmatch = rail.PythonOperator(
            task_id='log_clientnameexactmatch',
            python_callable=lambda: rail.result(
                'getclientbasedoncode')['rows'][0]['cells'][1]['uri'] if rail.result(
                'getclientbasedoncode')['rows'] and rail.result(
                'getclientbasedoncode')['rows'][0] and rail.result(
                'getclientbasedoncode')['rows'][0]['cells'][0] and rail.result(
                'getclientbasedoncode')['rows'][0]['cells'][0]['dataType'] else null
        )

        if_request_projectname_ends_with_fb = rail.IfOperator(
            task_id='if_request_projectname_ends_with_fb',
            test='''{{ dag_run.conf.projectname | ends_with('FB') }}''',
            yes_task="create_project",
            no_task="if_request_projectname_not_ends_with_fb",
        )

        create_project = rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": rail.result('load_query_list')[0]['projectname'],
                    },
                    "codeToApply": {
                        "value": rail.result('load_query_list')[0]['metisprojectuid'],
                    },
                    "descriptionToApply": null,
                    "percentCompletedToApply": null,
                    "startDateToApply": {
                        "date": {
                            "year": datetime.strptime(rail.result('load_query_list')[0]['startdate'], "%m/%d/%Y").year,
                            "month": datetime.strptime(rail.result('load_query_list')[0]['startdate'], "%m/%d/%Y").month,
                            "day": datetime.strptime(rail.result('load_query_list')[0]['startdate'], "%m/%d/%Y").day
                        }
                    },
                    "endDateToApply": {
                        "date": {
                            "year": datetime.strptime(rail.result('load_query_list')[0]['enddate'], "%m/%d/%Y").year,
                            "month": datetime.strptime(rail.result('load_query_list')[0]['enddate'], "%m/%d/%Y").month,
                            "day": datetime.strptime(rail.result('load_query_list')[0]['enddate'], "%m/%d/%Y").day
                        }
                    },
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:fixed-bid"
                    },
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "uri": rail.result('log_clientnameexactmatch') if rail.result('log_clientnameexactmatch') else null,
                                    "name":  null,
                                    "code": null,
                                    "parameterCorrelationId": null
                                },
                                "costAllocationPercentage": "100"
                            }
                        ],
                        "effectiveDate": null
                    },
                    "statusToApply": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":project-status-label:bb226be5-7478-45e7-b4bc-4fe2a3b18d1e",
                        "name": null
                    },
                    "projectWorkflowStateToApply": null,
                    "clientRepresentativeToApply": null,
                    "programToApply": null,
                    "projectLeaderToApply": null,
                    "isProjectLeaderApprovalRequired": null,
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": null,
                    "estimatedHoursToApply": null,
                    "budgetedHoursToApply": null,
                    "estimatedCostToApply": {
                        "value": {
                            "amount": "0",
                            "currency": {
                                "uri": null,
                                "name": null,
                                "symbol": "USD$"
                            }
                        }
                    },
                    "budgetedCostToApply": null,
                    "expenseBudgetedCostToApply": null,
                    "totalEstimatedContractValueToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": null,
                    "billingContractToApply": null,
                    "fixedBid": null,
                    "customFieldsToApply": [],
                    "resourceAssignmentModifications": null,
                    "resourceProjectAssignmentModifications": null,
                    "billingContractModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": [],
                    "portfolioToApply": null,
                    "locationToApply": null,
                    "divisionToApply": null,
                    "serviceCenterToApply": null,
                    "costCenterToApply": null,
                    "departmentGroupToApply": null,
                    "employeeTypeGroupToApply": null
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }


        )

        update_project_fixed_bid_rate_12 = rail.RepliconServiceOperator(
            task_id='update_project_fixed_bid_rate_12',
            endpoint="/services/FixedBidProjectService1.svc/UpdateProjectFixedBidRate",
            data=lambda: {
                "projectUri": rail.result('create_project')['uri'],
                "rate": {
                    "amount": "0",
                    "currencyUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":currency:1"
                },
                "projectFixedBidBillingFrequencyUri": "urn:replicon:fixed-bid-frequency:monthly"
            }
        )

        update_project_metis_u_i_d_13 = rail.RepliconServiceOperator(
            task_id='update_project_metis_u_i_d_13',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_project')['uri'],
                "customFieldUri": dag_run.conf['metis_projectuid_custom_field_uri'],
                "value": rail.result('load_query_list')[0]['metisprojectuid']
            }
        )

        update_project_codecustomfield_14 = rail.RepliconServiceOperator(
            task_id='update_project_codecustomfield_14',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_project')['uri'],
                "customFieldUri": dag_run.conf['project_code_custom_field_uri'],
                "value": rail.result('load_query_list')[0]['projectcode']
            }
        )

        if_request_projectname_not_ends_with_fb = rail.IfOperator(
            task_id='if_request_projectname_not_ends_with_fb',
            test='''{{ dag_run.conf.projectname | ends_with('FB') | is_falsy }}''',
            yes_task="create_project_if_data_not_matches",
            no_task="log_finalprojecturi",
        )

        create_project_if_data_not_matches = rail.RepliconServiceOperator(
            task_id='create_project_if_data_not_matches',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": rail.result('load_query_list')[0]['projectname'],
                    },
                    "codeToApply": {
                        "value": rail.result('load_query_list')[0]['metisprojectuid'],
                    },
                    "descriptionToApply": null,
                    "percentCompletedToApply": null,
                    "startDateToApply": {
                        "date": {
                            "year": datetime.strptime(rail.result('load_query_list')[0]['startdate'], "%m/%d/%Y").year,
                            "month": datetime.strptime(rail.result('load_query_list')[0]['startdate'], "%m/%d/%Y").month,
                            "day": datetime.strptime(rail.result('load_query_list')[0]['startdate'], "%m/%d/%Y").day
                        }
                    },
                    "endDateToApply": {
                        "date": {
                            "year": datetime.strptime(rail.result('load_query_list')[0]['enddate'], "%m/%d/%Y").year,
                            "month": datetime.strptime(rail.result('load_query_list')[0]['enddate'], "%m/%d/%Y").month,
                            "day": datetime.strptime(rail.result('load_query_list')[0]['enddate'], "%m/%d/%Y").day
                        }
                    },
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:time-and-material"
                    },
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "uri": rail.result('log_clientnameexactmatch') if rail.result('log_clientnameexactmatch') else null,
                                    "name":  null,
                                    "code": null,
                                    "parameterCorrelationId": null
                                },
                                "costAllocationPercentage": "100"
                            }
                        ],
                        "effectiveDate": null
                    },
                    "statusToApply": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":project-status-label:bb226be5-7478-45e7-b4bc-4fe2a3b18d1e",
                        "name": null
                    },
                    "projectWorkflowStateToApply": null,
                    "clientRepresentativeToApply": null,
                    "programToApply": null,
                    "projectLeaderToApply": null,
                    "isProjectLeaderApprovalRequired": null,
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": null,
                    "estimatedHoursToApply": null,
                    "budgetedHoursToApply": null,
                    "estimatedCostToApply": {
                        "value": {
                            "amount": "0",
                            "currency": {
                                "uri": null,
                                "name": null,
                                "symbol": "USD$"
                            }
                        }
                    },
                    "budgetedCostToApply": null,
                    "expenseBudgetedCostToApply": null,
                    "totalEstimatedContractValueToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                        "billingRateFrequency": null,
                        "billingRateFrequencyDuration": null,
                        "billingRates": []
                    },
                    "billingContractToApply": null,
                    "fixedBid": null,
                    "customFieldsToApply": [],
                    "resourceAssignmentModifications": null,
                    "resourceProjectAssignmentModifications": null,
                    "billingContractModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": [],
                    "portfolioToApply": null,
                    "locationToApply": null,
                    "divisionToApply": null,
                    "serviceCenterToApply": null,
                    "costCenterToApply": null,
                    "departmentGroupToApply": null,
                    "employeeTypeGroupToApply": null
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }


        )

        update_project_metis_u_i_d = rail.RepliconServiceOperator(
            task_id='update_project_metis_u_i_d',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_project_if_data_not_matches')['uri'],
                "customFieldUri": dag_run.conf['metis_projectuid_custom_field_uri'],
                "value":  rail.result('load_query_list')[0]['metisprojectuid']
            }
        )

        update_project_code_custom_field = rail.RepliconServiceOperator(
            task_id='update_project_code_custom_field',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_project_if_data_not_matches')['uri'],
                "customFieldUri": dag_run.conf['project_code_custom_field_uri'],
                "value": rail.result('load_query_list')[0]['projectcode']
            }
        )

        log_finalprojecturi = rail.PythonOperator(
            task_id='log_finalprojecturi',
            python_callable=lambda:  rail.result('create_project')['uri'] if rail.result(
                'create_project') and rail.result(
                'create_project')['uri'] else rail.result('create_project_if_data_not_matches')['uri']
        )

        if_first_projectmanager_present = rail.IfOperator(
            task_id='if_first_projectmanager_present',
            test='''{{ result('load_query_list')[0].projectmanager| is_truthy }}''',
            yes_task="log_projectmanagernamemodified",
            no_task="process_task_child",
        )

        log_projectmanagernamemodified = rail.PythonOperator(
            task_id='log_projectmanagernamemodified',
            python_callable=lambda:  rail.result('load_query_list')[
                0]['projectmanager'].replace(";", ",")
        )

        list_project_leaders = rail.RepliconServiceOperator(
            task_id='list_project_leaders',
            endpoint="/services/ProjectService1.svc/GetEligibleProjectLeaders",
        )

        log_projectmanageruri = rail.PythonOperator(
            task_id='log_projectmanageruri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('list_project_leaders'), 'displayText', rail.result('log_projectmanagernamemodified'), 'uri', null) if rail.result('list_project_leaders') else null
        )

        if_log_projectmanageruri_present = rail.IfOperator(
            task_id='if_log_projectmanageruri_present',
            test='''{{ result('log_projectmanageruri') | is_truthy }}''',
            yes_task="update_project_leader",
            no_task="process_task_child",
        )

        update_project_leader = rail.RepliconServiceOperator(
            task_id='update_project_leader',
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data={
                "projectUri": "{{ result('log_finalprojecturi') }}",
                "userUri": "{{ result('log_projectmanageruri') }}"
            }
        )

        process_task_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_task_child',
            retries=0,
            items="{{result('query_list_new_project')}}",
            trigger_dag_id=f'broadridge_project_import_task_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda dag_run, item: {
                "task_items": item,
                "action": "add",
                "projecturi": rail.result('log_finalprojecturi'),
                "projectid": rail.result('log_finalprojecturi').split(":")[-1] if rail.result('log_finalprojecturi') else null,
                "jobid": dag_run.conf['jobid'],
                "lookup_table": dag_run.conf['lookup_table']
            }
        )

        wait_for_process_task_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_task_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_task_child") }}'
        )

        add_success_entries = rail.WriteLogOperator(
            task_id='add_success_entries',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['projectname'],
                "status": "Success",
                "failure/reason": "Added",
                "taskname": rail.result('load_query_list')[-1]['taskname'],
                "jobid": dag_run.conf['jobid']
            }
        )

        catch = rail.EmptyOperator(
            task_id='catch',
            trigger_rule='one_failed',
        )

        log_failure_entries = rail.WriteLogOperator(
            task_id='log_failure_entries',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['projectname'],
                "status": "Failed",
                "failure/reason": rail.render_template("{{ get_error_message() }}"),
                "taskname": rail.result('load_query_list')[0]['taskname'],
                "jobid": dag_run.conf['jobid']
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> load_input
        load_input >> create_collection_from_list >> query_list_new_project
        query_list_new_project >> load_query_list >> if_first_clientcode_present
        if_first_clientcode_present >> rail.Label(
            'Yes') >> getclientbasedoncode
        getclientbasedoncode >> log_clientnameexactmatch >> if_request_projectname_ends_with_fb
        if_first_clientcode_present >> rail.Label(
            'No') >> if_request_projectname_ends_with_fb
        if_request_projectname_ends_with_fb >> rail.Label(
            'Yes') >> create_project >> update_project_fixed_bid_rate_12 >> update_project_metis_u_i_d_13
        update_project_metis_u_i_d_13 >> update_project_codecustomfield_14 >> if_request_projectname_not_ends_with_fb

        if_request_projectname_ends_with_fb >> rail.Label(
            'No') >> if_request_projectname_not_ends_with_fb
        if_request_projectname_not_ends_with_fb >> rail.Label(
            'Yes') >> create_project_if_data_not_matches >> update_project_metis_u_i_d >> update_project_code_custom_field
        update_project_code_custom_field >> log_finalprojecturi
        if_request_projectname_not_ends_with_fb >> rail.Label(
            'No') >> log_finalprojecturi >> if_first_projectmanager_present
        if_first_projectmanager_present >> rail.Label(
            'Yes') >> log_projectmanagernamemodified
        log_projectmanagernamemodified >> list_project_leaders >> log_projectmanageruri >> if_log_projectmanageruri_present
        if_log_projectmanageruri_present >> rail.Label(
            'Yes') >> update_project_leader >> process_task_child
        if_log_projectmanageruri_present >> rail.Label(
            'No') >> process_task_child >> wait_for_process_task_child >> add_success_entries >> catch >> log_failure_entries
        log_failure_entries >> log_to_sumo
        if_first_projectmanager_present >> rail.Label(
            'No') >> process_task_child

    return dag


rail.for_each_instance(create_dag)
