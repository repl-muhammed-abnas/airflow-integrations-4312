
from datetime import timedelta
import itertools
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
                    "id": "replicon_exp_project",
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

        get_workforce_user_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_workforce_user_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company="{{ dag_run.conf.item.data[0] | attr_or_default('_company') | sn }}",
            data=lambda: {
                "filter": {
                    "id": "replicon_exp_project_workforce",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJM_PROJEMPL_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "like%",
                                                "value": get_project_data()[0]
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

        def map_workforce_billingrates():
            data = list(itertools.chain(
                *list(map(lambda x: x['row']['children'], rail.result('get_workforce_user_costpoint')))))
            billing_rates = []
            for item in data:
                for child in item['row'].get('children', []):
                    if (child['row']['data'].get('BILL_LAB_CAT_CD') or child['row']['data'].get('PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD')) and \
                            child['row']['data'].get('PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID'):
                        billing_rates.append({'employeeId': child['row']['data'].get(
                            'PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID'), 'billing_ratecode': child['row']['data'].get('BILL_LAB_CAT_CD') or child['row']['data'].get('PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD')})

            return billing_rates

        get_billing_rates_costpoint = rail.PythonOperator(
            task_id='get_billing_rates_costpoint',
            python_callable=map_workforce_billingrates
        )

        def do_user_data_handler(data):
            emp_ids = map_workforce_empid()
            return list(map(lambda x: {"employeeId": x, 'userDetails': data[emp_ids.index(x)]}, emp_ids))

        def map_workforce_empid():
            data = list(set(map(lambda x: x['row']['data'].get('EMPL_ID'), filter(lambda x: x['row']['data'].get('EMPL_ID'),
                                                                                  list(itertools.chain(
                                                                                       *list(map(lambda x: x['row']['children'], rail.result('get_workforce_user_costpoint')))))))))
            return data

        get_users_from_replicon = rail.RepliconServiceOperator(
            task_id='get_users_from_replicon',
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=lambda: {
                "users": list(map(lambda x: {"employeeId": x}, map_workforce_empid()))

            },
            data_handler=do_user_data_handler
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
                                "assignedResources": get_assigned_resource_param_task(x, root_dept_uri),
                                "timeAndMaterials": null,
                                "keyValues": [],
                                "historicalKeyValues": []
                            },
                            "childTasks": get_tasks_param(data, x['PROJ_ID'], root_dept_uri, x['LVL_NO'])
                            },
                        filter(
                            lambda x: x['LVL_NO'] == level_no+1 and x['PROJ_ID'].startswith(parent_id), data)))

        def get_assigned_resource_param_task(item, root_dept_uri):
            if item['PROJ_WORK_FRC_FL'] != 'Y':
                return [
                    {
                        "department": {'uri': root_dept_uri},
                    }
                ]
            return list(map(lambda x: {'user': {"uri": rail.find_first_by_attr_and_get_attr(rail.result(
                                       'get_users_from_replicon'), 'employeeId', x['row']['data'].get('EMPL_ID'), 'userDetails')['uri']}},
                            filter(lambda x: x['row']['data'].get('EMPL_ID') and
                                   rail.find_first_by_attr_and_get_attr(rail.result(
                                       'get_users_from_replicon'), 'employeeId', x['row']['data'].get('EMPL_ID'), 'userDetails'),
                                   next(map(lambda x: x['row']['children'],
                                            filter(lambda x: x['row']['data']['PROJ_ID']
                                                   == item['PROJ_ID'],
                                                   rail.result('get_workforce_user_costpoint'))), []))))

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
                        "client": {
                            "uri": null,
                            "name": root_project_info['CUST_NAME'],
                            "code": null,
                            "parameterCorrelationId": null
                        } if root_project_info.get('CUST_NAME') else null,
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
                    "team": {
                        "teamMembers": get_project_resource_param_project(root_dept_uri, root_project_info)
                    },
                    "expenses": null,
                    "timeAndMaterials": {
                        "billingRates": get_billing_rates_param()
                    },
                    "fixedBid": null
                }
            }

        def get_billing_rates_param():
            rates = [
                {
                    "billingRate": {
                        "uri": "urn:replicon:project-specific-billing-rate",
                        "name": null
                    },
                    "rateSchedule": null
                },
                {
                    "billingRate": {
                        "uri": "urn:replicon:user-specific-billing-rate",
                        "name": null
                    },
                    "rateSchedule": null
                }
            ]

            for rate in rail.result('get_billing_rates_costpoint'):
                rates.append({
                    "billingRate": {
                        "uri": rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['billing_rates'], 'code', rate['billing_ratecode'], 'uri'),
                        "name": null
                    },
                    "rateSchedule": null
                })
            return rates

        def get_project_resource_param_project(root_dept_uri, root_project_info):
            if root_project_info['PROJ_WORK_FRC_FL'] != 'Y':
                return [
                    {
                        "resource": {
                            "uri": root_dept_uri,
                            "resourcePlaceholderParameterCorrelationId": null,
                            "user": null,
                            "department": null,
                            "placeholder": null,
                            "location": null,
                            "division": null,
                            "costCenter": null,
                            "serviceCenter": null,
                            "departmentGroup": null,
                            "employeeTypeGroup": null
                        },
                        "resourcePlaceholder": null,
                        "timeAndMaterials": {
                            "billingRatesAllowedForBillingTimeUris": [
                                "urn:replicon:project-specific-billing-rate",
                            ]
                        }
                    }
                ]
            return list(map(lambda x: {
                        "resource": {
                            "uri": null,
                            "resourcePlaceholderParameterCorrelationId": null,
                            "user":
                                {"uri": rail.find_first_by_attr_and_get_attr(rail.result(
                                    'get_users_from_replicon'), 'employeeId', x, 'userDetails')['uri']},
                            "department": null,
                            "placeholder": null,
                            "location": null,
                            "division": null,
                            "costCenter": null,
                            "serviceCenter": null,
                            "departmentGroup": null,
                            "employeeTypeGroup": null
                        },
                        "resourcePlaceholder": null,
                        "timeAndMaterials": {
                            "billingRatesAllowedForBillingTimeUris": get_project_resource_billingrates(x)
                        }
                        }, filter(lambda x: x and rail.find_first_by_attr_and_get_attr(rail.result('get_users_from_replicon'), 'employeeId', x, 'userDetails'),
                                  map_workforce_empid())))

        def get_project_resource_billingrates(empid):
            data = [
                # "urn:replicon:project-specific-billing-rate",
                # "urn:replicon:user-specific-billing-rate",
            ]
            for rate in filter(lambda x: x['employeeId'] == empid, rail.result('get_billing_rates_costpoint')):
                data.append(rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()[
                            'billing_rates'], 'code', rate['billing_ratecode'], 'uri'))
            return data

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

        add_project_and_task = rail.RepliconServiceOperator(
            task_id='add_project_and_task',
            endpoint="/services/ImportService1.svc/PutProject3",
            data=get_add_project_and_task_param
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
                "proj_name":  "{{ item.row.data.PROJ_NAME }}",
                "action": "{{ 'Add'  if result('get_project_details') | is_falsy else 'Update' }}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_log
        create_log >> get_project_details >> get_costpoint_projects >> get_workforce_user_costpoint >> get_billing_rates_costpoint >> get_users_from_replicon >> \
            get_project_leader_info_from_replicon >> assign_project_leader_permission >> get_task_info_from_replicon >> rename_tasks >> add_project_and_task >> update_division >> add_log_entry >> finish
        finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
