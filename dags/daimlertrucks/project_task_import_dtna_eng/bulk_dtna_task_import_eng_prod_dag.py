
from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
null = None
null_urn = "urn:replicon:list-type:null"


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'bulk_dtna_taskimport_eng_prod_{config.instance}',
        description=f'Bulk_DTNA_Task Import_ENG_Prod {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=10,
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
            no_task='search_projects_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_projects_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_filtered_data(response, dag_run):
            data = response.json()['d']['rows']
            projectinfo = list(filter(lambda x: x['projectname'] == dag_run.conf['projectname'], map(lambda item: {
                "projecturi": item['cells'][0]['uri'],
                "projectname": item['cells'][0].get('textValue'),
            }, data)))
            return projectinfo[0] if projectinfo else {}

        search_projects_3 = rail.RepliconServiceOperator(
            task_id='search_projects_3',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text"
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
                            "text": dag_run.conf['projectname'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=get_filtered_data
        )

        log_required_project_uri_4 = rail.PythonOperator(
            task_id='log_required_project_uri_4',
            python_callable=lambda:  rail.result(
                'search_projects_3')['projecturi']
        )

        log_required_task_name_5 = rail.PythonOperator(
            task_id='log_required_task_name_5',
            python_callable=lambda dag_run:  dag_run.conf['taskcode'] +
            "-" + dag_run.conf['taskdescription']
        )

        get_all_project_task_7 = rail.RepliconServiceOperator(
            task_id='get_all_project_task_7',
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data={
                    "parentUri": '{{ result("log_required_project_uri_4") }}'
            },
        )

        if_request_status_equals_to_1_8 = rail.IfOperator(
            task_id='if_request_status_equals_to_1_8',
            test="{{ dag_run.conf.status == 1 }}",
            yes_task="log_task_status_9",
            no_task="if_request_status_equals_to_1_10",
        )

        log_task_status_9 = rail.PythonOperator(
            task_id='log_task_status_9',
            python_callable=lambda:  'no'
        )

        if_request_status_equals_to_1_10 = rail.IfOperator(
            task_id='if_request_status_equals_to_1_10',
            test="{{ dag_run.conf.status == -1 }}",
            yes_task="log_task_status_11",
            no_task="log_task_status_12",
        )

        log_task_status_11 = rail.PythonOperator(
            task_id='log_task_status_11',
            python_callable=lambda:  'yes'
        )

        log_task_status_12 = rail.PythonOperator(
            task_id='log_task_status_12',
            python_callable=lambda: rail.result(
                'log_task_status_9') or rail.result('log_task_status_11')
        )

        if_exist_task_present_13 = rail.IfOperator(
            task_id='if_exist_task_present_13',
            test='''{{ result('get_all_project_task_7') | is_truthy }}''',
            yes_task="foreach_get_all_project_task_7_13",
            no_task="if_log_20_blank_dataforeachforeachcolumn_1_19",
        )

        foreach_get_all_project_task_7_13 = rail.ForEachOperator(
            task_id='foreach_get_all_project_task_7_13',
            items="{{ result('get_all_project_task_7') | to_json }}",
            start_task='if_code_downcase_equals_to_dataworkato_servicereceive_requestrequesttaskcodedowncase_14',
            end_task='foreach_get_all_project_task_7_7_13_end'
        )

        if_code_downcase_equals_to_dataworkato_servicereceive_requestrequesttaskcodedowncase_14 = rail.IfOperator(
            task_id='if_code_downcase_equals_to_dataworkato_servicereceive_requestrequesttaskcodedowncase_14',
            test='''{{ result('foreach_get_all_project_task_7_13').task.code | lower == dag_run.conf.taskcode | lower }}''',
            yes_task="log_task_name_15",
            no_task="foreach_get_all_project_task_7_7_13_end",
        )

        log_task_name_15 = rail.PythonOperator(
            task_id='log_task_name_15',
            python_callable=lambda: rail.result(
                'foreach_get_all_project_task_7_13')['task']['name']
        )

        log_task_uri_16 = rail.PythonOperator(
            task_id='log_task_uri_16',
            python_callable=lambda:  rail.result(
                'foreach_get_all_project_task_7_13')['task']['uri']
        )

        if_description_downcase_not_equals_to_dataworkato_servicereceive_requestrequesttaskdescriptiondowncase_17 = rail.IfOperator(
            task_id='if_description_downcase_not_equals_to_dataworkato_servicereceive_requestrequesttaskdescriptiondowncase_17',
            test='''{{ result('foreach_get_all_project_task_7_13').task.description | lower != dag_run.conf.taskdescription | lower }}''',
            yes_task="log_task_description_18",
            no_task="foreach_get_all_project_task_7_7_13_end",
        )

        log_task_description_18 = rail.PythonOperator(
            task_id='log_task_description_18',
            python_callable=lambda dag_run:  dag_run.conf['taskdescription']
        )

        foreach_get_all_project_task_7_7_13_end = rail.EmptyOperator(
            task_id='foreach_get_all_project_task_7_7_13_end',
        )

        if_log_20_blank_dataforeachforeachcolumn_1_19 = rail.IfOperator(
            task_id='if_log_20_blank_dataforeachforeachcolumn_1_19',
            test='''{{ result('log_task_uri_16') | is_falsy }}''',
            yes_task="create_project_task_21",
            no_task="log_task_uri_25",
        )

        create_project_task_21 = rail.RepliconServiceOperator(
            task_id='create_project_task_21',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=lambda dag_run: {
                "project": {
                    "uri": rail.result('log_required_project_uri_4')
                },
                "task": {
                    "target": {
                        "name": rail.result('log_required_task_name_5')
                    },
                    "name": rail.result('log_required_task_name_5'),
                    "code": dag_run.conf['taskcode'],
                    "description": dag_run.conf['taskdescription'],
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "true",
                    "isClosed": 0 if rail.result('log_task_status_12') and rail.result('log_task_status_12') == 'no' else 1,
                    "customFieldValues": [
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:474cdfb3-4e1d-4e87-b0c5-453559721e76",
                                "name": null,
                                "groupUri": "urn:replicon:object-type:task"
                            },
                            "text": dag_run.conf['deptcntlcdudf']
                        },
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:851755a0-e004-485b-93a9-5c1997381852",
                                "name": null,
                                "groupUri": "urn:replicon:object-type:task"
                            },
                            "text": dag_run.conf['ewrconditionudf']
                        },
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:52eb35da-d6d2-4e21-bd7e-d469cd5f5cb5",
                                "name": null,
                                "groupUri": "urn:replicon:object-type:task"
                            },
                            "text": dag_run.conf['jobworktypeudf']
                        },
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:cfac323e-088b-4124-abbf-43a4b56bfe72",
                                "name": null,
                                "groupUri": "urn:replicon:object-type:task"
                            },
                            "text": dag_run.conf['projectengineerudf']
                        }
                    ],
                    "estimatedCost": {
                        "amount": "0",
                        "currency": {
                            "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":currency:1",
                            "name": null,
                            "symbol": null
                        }
                    },
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                    "assignedResources": [
                        {
                            "uri": null,
                            "resourcePlaceholderParameterCorrelationId": null,
                            "user": null,
                            "department": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:11",
                                "name": null,
                                "parent": null,
                                "parameterCorrelationId": null
                            },
                            "placeholder": null,
                            "location": null,
                            "division": null,
                            "costCenter": null,
                            "serviceCenter": null,
                            "departmentGroup": null,
                            "employeeTypeGroup": null
                        }
                    ]
                }
            },
        )

        dtna_task_import_add_entry_22 = rail.WriteLogOperator(
            task_id='dtna_task_import_add_entry_22',
            message="Created Successfully",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['projectname'],
                "taskname": rail.result('log_required_task_name_5'),
                "status": "Created Successfully",
                "reason": "",
                "child_job_id": get_dagrun_ecid(dag_run)
            }
        )

        def get_task_uri(task1, task2):
            return rail.result(task1) if rail.result(task1) else rail.result(
                task2)['uri'] if rail.result(task2) else None

        log_task_uri_25 = rail.PythonOperator(
            task_id='log_task_uri_25',
            python_callable=lambda:  get_task_uri(
                'log_task_uri_16', 'create_project_task_21')
        )

        if_log_20_present_dataforeachforeachcolumn_1_26 = rail.IfOperator(
            task_id='if_log_20_present_dataforeachforeachcolumn_1_26',
            test='''{{ result('log_task_uri_16') | is_truthy }}''',
            yes_task="trigger_dag_run_live_dtna_child_update_task_eng_prod28",
            no_task="finish",
        )

        trigger_dag_run_live_dtna_child_update_task_eng_prod28 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_dtna_child_update_task_eng_prod28',
            retries=0,
            items=[1],
            trigger_dag_id=f'dtna_child_update_task_eng_prod_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "projecturi": rail.result('log_required_project_uri_4'),
                "taskuri": rail.result('log_task_uri_25'),
                "taskcode": dag_run.conf['taskcode'],
                "taskdescription": rail.result('log_task_description_18') if rail.result('log_task_description_18') else None,
                "isclosed": dag_run.conf['status'],
                "customfield_dept_cntl_cd": dag_run.conf['deptcntlcdudf'],
                "customfield_job_work_type": dag_run.conf['jobworktypeudf'],
                "customfield_projectengineer": dag_run.conf['projectengineerudf'],
                "customfield_ewrcondition": dag_run.conf['ewrconditionudf'],
                "company_key": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_dtna_child_update_task_eng_prod28 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_dtna_child_update_task_eng_prod28',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_dtna_child_update_task_eng_prod28") }}'
        )

        dtna_task_import_add_entry_29 = rail.WriteLogOperator(
            task_id='dtna_task_import_add_entry_29',
            message="Updated Successfully",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['projectname'],
                "taskname": rail.result('log_required_task_name_5'),
                "status": "Updated Successfully",
                "reason": "",
                "child_job_id": get_dagrun_ecid(dag_run)
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> search_projects_3
        search_projects_3 >> log_required_project_uri_4 >> \
            log_required_task_name_5 >> get_all_project_task_7 >> if_request_status_equals_to_1_8
        if_request_status_equals_to_1_8 >> rail.Label(
            'Yes') >> log_task_status_9 >> if_request_status_equals_to_1_10
        if_request_status_equals_to_1_8 >> rail.Label(
            'No') >> if_request_status_equals_to_1_10
        if_request_status_equals_to_1_10 >> rail.Label(
            'Yes') >> log_task_status_11 >> log_task_status_12
        if_request_status_equals_to_1_10 >> rail.Label(
            'No') >> log_task_status_12 >> if_exist_task_present_13
        if_exist_task_present_13 >> rail.Label('Yes') >> foreach_get_all_project_task_7_13 >> \
            if_code_downcase_equals_to_dataworkato_servicereceive_requestrequesttaskcodedowncase_14
        if_code_downcase_equals_to_dataworkato_servicereceive_requestrequesttaskcodedowncase_14 >> rail.Label(
            'Yes') >> log_task_name_15 >> log_task_uri_16 >> \
            if_description_downcase_not_equals_to_dataworkato_servicereceive_requestrequesttaskdescriptiondowncase_17
        if_description_downcase_not_equals_to_dataworkato_servicereceive_requestrequesttaskdescriptiondowncase_17 >> rail.Label(
            'Yes') >> log_task_description_18 >> foreach_get_all_project_task_7_7_13_end
        if_description_downcase_not_equals_to_dataworkato_servicereceive_requestrequesttaskdescriptiondowncase_17 >> rail.Label(
            'No') >> foreach_get_all_project_task_7_7_13_end
        if_code_downcase_equals_to_dataworkato_servicereceive_requestrequesttaskcodedowncase_14 >> rail.Label(
            'No') >> foreach_get_all_project_task_7_7_13_end
        foreach_get_all_project_task_7_13 >> foreach_get_all_project_task_7_7_13_end >> \
            if_log_20_blank_dataforeachforeachcolumn_1_19
        if_log_20_blank_dataforeachforeachcolumn_1_19 >> rail.Label(
            'Yes') >> create_project_task_21 >> dtna_task_import_add_entry_22 >> log_task_uri_25
        if_log_20_blank_dataforeachforeachcolumn_1_19 >> rail.Label(
            'No') >> log_task_uri_25 >> if_log_20_present_dataforeachforeachcolumn_1_26
        if_log_20_present_dataforeachforeachcolumn_1_26 >> rail.Label(
            'Yes') >> trigger_dag_run_live_dtna_child_update_task_eng_prod28 >> \
            wait_for_completion_trigger_dag_run_live_dtna_child_update_task_eng_prod28 >> \
            dtna_task_import_add_entry_29 >> finish
        if_log_20_present_dataforeachforeachcolumn_1_26 >> rail.Label(
            'No') >> finish >> log_to_sumo
        if_exist_task_present_13 >> rail.Label(
            'No') >> if_log_20_blank_dataforeachforeachcolumn_1_19

    return dag


rail.for_each_instance(create_dag)
