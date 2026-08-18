
from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_project_sync_child_{config.instance}',
        description=f'deltek_costpoint_project_sync_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        },
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
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log",
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ImportService1.svc/BulkGetProjects2",
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": "{{ dag_run.conf.item.root_project_id }}",
                        "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda data: null if data['errors'] else data['results'][0]
        )

        get_costpoint_projects = rail.DeltekCostPointServiceOperator(
            task_id='get_costpoint_projects',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company="{{ dag_run.conf.item.data[0] | attr_or_default('_company') | sn }}",
            data={
                "filter": {
                    "id": "polaris_exp_project",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJMBASIC_PROJ",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "like%",
                                                "value": "{{ dag_run.conf.item.root_project_id }}"
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
        )

        get_project_leader_info_from_replicon = rail.RepliconServiceOperator(
            task_id='get_project_leader_info_from_replicon',
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=lambda: {
                "users": [{"employeeId": get_project_data()[2].get('EMPL_ID')}]
            },
            data_handler=lambda data: data[0]
        )

        def get_tasks_param(data, parent_id, root_dept_uri, level_no):
            return list(map(lambda x: {
                            "task": {
                                "target": {
                                    'name':  x['PROJ_NAME'] if len(list(filter(lambda p: p['PROJ_NAME'] == x['PROJ_NAME'], data))) == 1 else f"{x['PROJ_NAME']}_{x['PROJ_ID']}"
                                },
                                "name": x['PROJ_NAME'] if len(list(filter(lambda p: p['PROJ_NAME'] == x['PROJ_NAME'], data))) == 1 else f"{x['PROJ_NAME']}_{x['PROJ_ID']}",
                                "code":  x['PROJ_ID'],
                                "description": x['PROJ_LONG_NAME'],
                                "timeEntryDateRange": {
                                    "startDate": rail.parse_date(x.get('PROJ_START_DT'), config.date_time_format),
                                    "endDate": rail.parse_date(x.get('PROJ_END_DT'), config.date_time_format),
                                },
                                "percentCompleted": "0",
                                "isTimeEntryAllowed": "true" if x['ALLOW_CHARGES_FL'] == 'Y' and x['ACTIVE_FL'] == 'Y' else "false",
                                "estimatedHours": null,
                                "isClosed": "false",
                                "customFieldValues": [],
                                "extensionFieldValues": [],
                                "estimatedCost": null,
                                "costTypeUri": null,
                                "assignedResources": [],
                                "timeAndMaterials": null,
                                "keyValues": [],
                                "historicalKeyValues": []
                            },
                            "childTasks": get_tasks_param(data, x['PROJ_ID'], root_dept_uri, x['LVL_NO'])
                            },
                        filter(
                            lambda x: x['LVL_NO'] == level_no+1 and x['PROJ_ID'].startswith(parent_id), data)))

        def get_task_params():
            task_hierarchy = []
            root_project_id = rail.get_dag_run_conf()[
                'item']['root_project_id']
            data = list(
                map(lambda x: x['row']['data'], rail.result('get_costpoint_projects')))
            get_apply_tasks_param(
                task_hierarchy, root_project_id, 1, None, data)
            return {
                "project": {
                    "code": root_project_id,
                },
                "taskHierarchy": task_hierarchy,
                "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

        def get_apply_tasks_param(task_hierarchy, root_project_id, level_no, parent_req, data):
            prime_level = filter(lambda x: x['LVL_NO'] == level_no + 1
                                 and x['PROJ_ID'].startswith(root_project_id), data)
            for prime_level_info in prime_level:
                target_task_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_task_info_from_replicon'), 'code', prime_level_info['PROJ_ID'], 'uri', None)

                parent_request = parent_req if prime_level_info[
                    'LVL_NO'] != 2 and target_task_uri is None else None
                task_hierarchy.append({
                    "target": {
                        "uri": target_task_uri,
                        "parent": parent_request,
                        "project": {
                            "code": root_project_id,
                        } if target_task_uri is None else None,
                    },
                    "taskModificationToApply": {
                        "name": prime_level_info['PROJ_NAME'] if len(list(filter(lambda p: p['PROJ_NAME'] == prime_level_info['PROJ_NAME'], data))) == 1 else f"{prime_level_info['PROJ_NAME']}_{prime_level_info['PROJ_ID']}",
                        "codeToApply": {
                            "value": prime_level_info['PROJ_ID']
                        },
                        "descriptionToApply": {
                            "value": prime_level_info['PROJ_LONG_NAME']
                        },
                        "isClosed": "false",
                        "timeEntryStartDateToApply": {
                            "date": rail.parse_date(prime_level_info.get('PROJ_START_DT'), config.date_time_format)
                        },
                        "timeEntryEndDateToApply": {
                            "date": rail.parse_date(prime_level_info.get('PROJ_END_DT'), config.date_time_format)
                        },
                        "timeAndExpenseEntryTypeToApply": {
                            "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                        },
                        "isTimeEntryAllowed": "true" if prime_level_info['ALLOW_CHARGES_FL'] == 'Y' and prime_level_info['ACTIVE_FL'] == 'Y' else "false",
                    }
                })

                parent_req = {
                    "uri": target_task_uri,
                    "name": None if target_task_uri else prime_level_info['PROJ_NAME'],
                    "parent": null,
                    "project": {
                        "code": root_project_id,
                    } if target_task_uri is None else None,
                    "parameterCorrelationId": null
                }

                get_apply_tasks_param(
                    task_hierarchy, prime_level_info['PROJ_ID'], prime_level_info['LVL_NO'], parent_req, data)

        def get_add_project_and_task_param():
            root_dept_uri = f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1"
            root_project_id, data, root_project_info = get_project_data()
            return {
                "project": {
                    "target": {
                        "uri": null,
                        "name": null,
                        "code": root_project_info['PROJ_ID'],
                        "parameterCorrelationId": null
                    },
                    "projectInfo": {
                        "name": root_project_info['PROJ_NAME'],
                        "code":  root_project_info['PROJ_ID'],
                        "description":  root_project_info['PROJ_LONG_NAME'],
                        "timeEntryDateRange": {
                            "startDate": rail.parse_date(root_project_info.get('PROJ_START_DT'), config.date_time_format),
                            "endDate": rail.parse_date(root_project_info.get('PROJ_END_DT'), config.date_time_format),
                        },
                        # "projectStatusLabel": {
                        #     "uri": null,
                        #     "name": 'In-Progress' if root_project_info['ACTIVE_FL'] == 'Y' else 'Cancelled'
                        # },
                        "percentCompleted": "0",
                        "clients": [
                            {
                                "client": {
                                    "uri": null,
                                    "name": root_project_info['CUST_NAME'],
                                    "code": null,
                                    "parameterCorrelationId": null
                                },
                                "costAllocationPercentage": 100
                            }
                        ] if root_project_info.get('CUST_NAME') else [],
                        "program": null,
                        "projectLeader": {
                            "uri": null,
                            "loginName": null,
                            "employeeId": root_project_info.get('EMPL_ID'),
                            "parameterCorrelationId": null
                        } if rail.result('get_project_leader_info_from_replicon') else null,
                        "customFieldValues": [
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Purchase Order No', 'uri'),
                                },
                                "text": root_project_info.get('CUST_PO_ID')
                            },
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Project Classification', 'uri'),
                                },
                                "text": root_project_info.get('S_PROJ_RPT_DC')
                            },
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Company', 'uri'),
                                },
                                "text": rail.get_dag_run_conf()['item']['data'][0].get('_company')
                            },
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Opportunity ID', 'uri'),
                                },
                                "text": root_project_info.get('OPP_ID')
                            }
                        ],
                        "isTimeEntryAllowed": "true" if root_project_info['ALLOW_CHARGES_FL'] == 'Y' and root_project_info['ACTIVE_FL'] == 'Y' else "false",
                        "costTypeUri": null,
                        "estimatedHours": null,
                        "estimatedCost": null,
                        "estimatedExpenses": null,
                        "budget": null,
                        "isProjectLeaderApprovalRequired": "true",
                        "estimationModeUri": "urn:replicon:project-estimation-mode:task-based",
                        "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                        "timeAndMaterials": {
                            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                            "billingRateFrequency": null,
                            "billingRateFrequencyDuration": null,
                            "billingRates": []
                        },
                        "defaultBillingCurrency": null
                    },
                    "tasks": get_tasks_param(data, root_project_id, root_dept_uri, 1),
                    "team": null,
                    "expenses": null,
                    "timeAndMaterials": null,
                    "fixedBid": null
                }
            }

        def get_project_data():
            root_project_id = rail.get_dag_run_conf()[
                'item']['root_project_id']
            data = list(
                map(lambda x: x['row']['data'], rail.result('get_costpoint_projects')))
            root_project_info = next(filter(
                lambda x: x['PROJ_ID'] == root_project_id, data), None)
            return root_project_id, data, root_project_info

        assign_project_leader_permission = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_project_leader_permission',
            items=lambda: [1] if rail.result(
                'get_project_leader_info_from_replicon') else [],
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: {
                "userUri": rail.result('get_project_leader_info_from_replicon')['uri'],
                "permissionSetUri": rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['permission_sets'], 'name', config.project_manager_permission_name, 'uri')
            }
        )

        def do_get_task_info_from_replicon():
            tasks = []
            if rail.result('get_project_details'):
                cp_data = get_project_data()[1]
                get_task_data_replicon(rail.result('get_project_details')[
                    'tasks'], tasks, cp_data)
            return tasks

        def get_new_task_name(data, task_code):
            task_info = list(
                filter(lambda x: x['PROJ_ID'] == task_code, data))[0]
            tasks_by_name = list(
                filter(lambda x: x['PROJ_NAME'] == task_info['PROJ_NAME'], data))
            return task_info['PROJ_NAME'] if len(tasks_by_name) == 1 else f"{task_info['PROJ_NAME']}_{task_info['PROJ_ID']}"

        def get_task_data_replicon(tasks, result, cp_data):
            for task in tasks:
                if task['task']['code']:
                    result.append(
                        {'code': task['task']['code'], 'name': task['task']['name'], 'uri': task['task']['uri'], 'new_name': get_new_task_name(cp_data, task['task']['code'])})
                get_task_data_replicon(task['childTasks'], result, cp_data)

        get_task_info_from_replicon = rail.PythonOperator(
            task_id='get_task_info_from_replicon',
            python_callable=do_get_task_info_from_replicon
        )

        rename_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='rename_tasks',
            items=lambda: list(filter(lambda x: x['name'] != x['new_name'], rail.result(
                'get_task_info_from_replicon'))),
            endpoint="/services/TaskService1.svc/UpdateName",
            data=lambda item: {
                "taskUri": item['uri'],
                "name": item['new_name']
            }
        )

        if_project_present = rail.IfOperator(
            task_id='if_project_present',
            test='''{{ result('get_project_details') | is_truthy }}''',
            yes_task="update_project",
            no_task="add_project_and_task",
        )

        add_project_and_task = rail.RepliconServiceOperator(
            task_id='add_project_and_task',
            endpoint="/services/ImportService1.svc/PutProject4",
            data=get_add_project_and_task_param
        )

        def update_project_create_or_modifiy():
            root_project_id = rail.get_dag_run_conf()[
                'item']['root_project_id']
            data = list(
                map(lambda x: x['row']['data'], rail.result('get_costpoint_projects')))
            root_project_info = next(filter(
                lambda x: x['PROJ_ID'] == root_project_id, data), None)

            division_name = rail.find_first_by_attr_and_get_attr(
                rail.get_dag_run_conf()['divisions'], 'code', get_project_data()[2]['ORG_ID'], 'name')

            return {
                "target": {
                    "code": root_project_info['PROJ_ID'],
                },
                "modifications": {
                    "nameToApply": {
                        "value": root_project_info['PROJ_NAME']
                    },
                    "codeToApply": {
                        "value": root_project_info['PROJ_ID']
                    },
                    "descriptionToApply": {
                        "value": root_project_info['PROJ_LONG_NAME']
                    },
                    "percentCompletedToApply": "0",
                    "startDateToApply": {
                        "date": rail.parse_date(root_project_info.get('PROJ_START_DT'), config.date_time_format)
                    },
                    "endDateToApply": {
                        "date": rail.parse_date(root_project_info.get('PROJ_END_DT'), config.date_time_format)
                    },
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:time-and-material"
                    },
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "name": root_project_info['CUST_NAME'],
                                },
                                "costAllocationPercentage": 100
                            }
                        ],
                        "effectiveDate": null
                    } if root_project_info.get('CUST_NAME') else null,
                    "projectLeaderToApply": {
                        "user": {
                            "employeeId": root_project_info.get('EMPL_ID'),
                        }
                    } if rail.result('get_project_leader_info_from_replicon') else null,
                    "isProjectLeaderApprovalRequired": config.project_leader_approval,
                    "isTimeEntryAllowed": "true" if root_project_info['ALLOW_CHARGES_FL'] == 'Y' and root_project_info['ACTIVE_FL'] == 'Y' else "false",
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                    },
                    "fixedBid": null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Purchase Order No', 'uri'),
                            },
                            "text": root_project_info.get('CUST_PO_ID')
                        },
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Project Classification', 'uri'),
                            },
                            "text": root_project_info.get('S_PROJ_RPT_DC')
                        },
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Company', 'uri'),
                            },
                            "text": rail.get_dag_run_conf()['item']['data'][0].get('_company')
                        },
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Opportunity ID', 'uri'),
                            },
                            "text": root_project_info.get('OPP_ID')
                        }
                    ],
                    "divisionToApply": {
                        "division": {"name": division_name}
                    } if division_name else None,
                    "keyValuesToApply": get_polaris_key_values(),
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

        def get_polaris_key_values():
            keyValuesToApply = [
                {
                    "keyUri": 'polaris-psa:slack-channel',
                    "value": {}
                },
                {
                    "keyUri": 'urn:replicon:project-key-value-key:external-dependency',
                    "value": {
                        "collection": [
                            {
                                "text": 'Project has resource requests',
                                "uri": 'urn:replicon:external-dependency:psa'
                            }
                        ]
                    }
                },
                {
                    "keyUri": 'urn:replicon:project-key-value-key:project-management-type',
                    "value": {
                        "uri": 'urn:replicon:project-management-type:managed'
                    }
                }
            ]

            return keyValuesToApply

        update_project = rail.RepliconServiceOperator(
            task_id='update_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=update_project_create_or_modifiy
        )

        update_task = rail.RepliconServiceOperator(
            task_id='update_task',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            data=get_task_params
        )

        def update_managed_project():
            return {
                "target": {
                    "uri": rail.result('add_project_and_task')['uri']
                },
                "modifications": {
                    "keyValuesToApply": get_polaris_key_values(),
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

        update_manage_project = rail.RepliconServiceOperator(
            task_id='update_manage_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=update_managed_project
        )

        update_division = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_division',
            items=lambda: [1] if get_project_data()[2] and rail.find_first_by_attr_and_get_attr(
                rail.get_dag_run_conf()['divisions'], 'code', get_project_data()[2]['ORG_ID']) else [],
            endpoint="/services/ProjectService1.svc/UpdateDivision2",
            data=lambda: {"projectUri": (rail.result('add_project_and_task') or {}).get('uri') or (rail.result('get_project_details')['project'] or {}).get('uri'),
                          "division": {"name": rail.find_first_by_attr_and_get_attr(
                              rail.get_dag_run_conf()['divisions'], 'code', get_project_data()[2]['ORG_ID'], 'name')}}
        )

        add_log_entry = rail.WriteLogOperator(
            task_id='add_log_entry',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            items=lambda: rail.get_dag_run_conf()['item']['data'],
            properties={
                "proj_id": "{{ item.row.data.PROJ_ID }}",
                "proj_name":  "{{ item.row.data.PROJ_NAME }}",
                "action": "{{ 'Add'  if result('get_project_details') | is_falsy else 'Update' }}",
                "status": "Success",
                "details": "",
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            message="na",
            severity="Error",
            items=lambda: rail.get_dag_run_conf()['item']['data'],
            properties={
                "proj_id": "{{ item.row.data.PROJ_ID }}",
                "proj_name":  "{{ item.row.data.get('PROJ_NAME', '') }}",
                "action": "{{ 'Add'  if result('get_project_details') | is_falsy else 'Update' }}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_log
        create_log >> get_project_details >> get_costpoint_projects >> \
            get_project_leader_info_from_replicon >> assign_project_leader_permission >> \
            get_task_info_from_replicon >> rename_tasks >> if_project_present
        if_project_present >> rail.Label(
            'Yes') >> update_project >> update_task >> add_log_entry
        if_project_present >> rail.Label('No') >> add_project_and_task >> \
            update_manage_project >> update_division >> add_log_entry >> finish
        finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
