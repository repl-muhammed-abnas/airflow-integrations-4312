
from datetime import timedelta
import uuid
import itertools
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_mo_project_sync_child_{config.instance}',
        description=f'deltek_mo_costpoint_project_sync_child_{config.instance}',
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

        group_data_by_root_task = rail.PythonOperator(
            task_id='group_data_by_root_task',
            python_callable=lambda dag_run: [{'root_task_id': k, 'data': list(g)} for k, g in itertools.groupby(
                (dag_run.conf['item']['data']), lambda x: x['MO_OPER_SEQ_NO'])]
        )

        def get_project_req(dag_run):
            project = []
            project.append({
                "uri": null,
                "name": null,
                "code": dag_run.conf['item']['root_project_id'],
                "parameterCorrelationId": null
            })
            project.append({
                "uri": null,
                "name": null,
                "code": dag_run.conf['item']['data'][0]['BLD_PROJ_ID'].split('.')[0],
                "parameterCorrelationId": null
            })

            return {
                "projects": project
            }

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ImportService1.svc/BulkGetProjects2",
            data=get_project_req,
            data_handler=lambda data: data['results'] if data['results'] else []
        )

        get_project_division_details = rail.RepliconServiceOperator(
            task_id='get_project_division_details',
            endpoint='/services/projectservice1.svc/BulkGetProjectDetails3',
            data=lambda dag_run: {
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": dag_run.conf['item']['data'][0]['BLD_PROJ_ID'].split('.')[0],
                        "parameterCorrelationId": null
                    }
                ]
            }
        )

        def get_task_request(task_details, root_dept_uri):
            task_request = []
            for task in task_details:
                task_request.append({
                    "task": {
                        "target": {
                                    "name": task['root_task_id']
                                    },
                        "name": task['root_task_id'],
                        "code": task['root_task_id'],
                        "description": task['root_task_id'],
                        "timeEntryDateRange": {
                            "startDate": null,
                            "endDate": null
                        },
                        "percentCompleted": "0",
                        "isTimeEntryAllowed": "false" if task['data'] and len(task['data']) > 0 else "true",
                        "estimatedHours": null,
                        "isClosed": "false",
                        "customFieldValues": [],
                        "extensionFieldValues": [],
                        "estimatedCost": null,
                        "costTypeUri": null,
                        "assignedResources": [
                            {
                                "department": {'uri': root_dept_uri},
                            }
                        ],
                        "timeAndMaterials": {
                            "timeAndExpenseEntryTypeUri": config.billable_uri
                        },
                        "keyValues": [],
                        "historicalKeyValues": []
                    },
                    "childTasks": get_child_task_request(task['data'], root_dept_uri)
                })
            return task_request

        def get_child_task_request(child_task_array, root_dept_uri):
            childTasks = []
            for child_task in child_task_array:
                childTasks.append({
                    "task": {
                        "target": {
                            "name": child_task['MO_OPER_STEP_NO']
                        },
                        "name": child_task['MO_OPER_STEP_NO'],
                        "code": child_task['MO_OPER_STEP_NO'],
                        "description": child_task['MO_OPER_STEP_NO'],
                        "timeEntryDateRange": {
                            "startDate": null,
                            "endDate": null
                        },
                        "percentCompleted": "0",
                        "isTimeEntryAllowed": "true",
                        "estimatedHours": null,
                        "isClosed": "false",
                        "customFieldValues": [],
                        "extensionFieldValues": [],
                        "estimatedCost": null,
                        "costTypeUri": null,
                        "assignedResources": [
                            {
                                "department": {'uri': root_dept_uri},
                            }
                        ],
                        "timeAndMaterials": null,
                        "keyValues": [],
                        "historicalKeyValues": []
                    },
                    "childTasks": []
                })
            return childTasks

        def get_task_modifications(task_info, parent_request, target_task_uri, is_parent_task=True, allow_timeentry=True):
            return {
                "target": {
                    "uri": target_task_uri,
                    "parent": null if target_task_uri else parent_request,
                    "project": None,
                },
                "taskModificationToApply": {
                    "name": task_info['root_task_id'] if is_parent_task else task_info['MO_OPER_STEP_NO'],
                    "codeToApply": {
                        "value": task_info['root_task_id'] if is_parent_task else task_info['MO_OPER_STEP_NO']
                    },
                    "descriptionToApply": {
                        "value": task_info['root_task_id'] if is_parent_task else task_info['MO_OPER_STEP_NO']
                    },
                    "isClosed": "false",
                    "timeAndExpenseEntryTypeToApply": {
                        "value": config.billable_uri
                    },
                    "isTimeEntryAllowed": allow_timeentry,
                }
            }

        def get_update_task_params():
            root_project_id = rail.get_dag_run_conf()[
                'item']['root_project_id']
            return {
                "project": {
                    "code": root_project_id,
                },
                "taskHierarchy": get_task_hierarchy(root_project_id),
                "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

        def get_task_hierarchy(root_project_id):
            task_hierarchy = []
            task_details = rail.result('group_data_by_root_task')

            for task in task_details:
                parent_task_code = task['root_task_id']
                target_parent_task_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_task_info_from_replicon'), 'code', str(parent_task_code), 'uri', None)
                parent_allow_timeentry = False if task['data'] and len(
                    task['data']) > 0 else True
                parent_task_req = get_task_modifications(
                    task, None, target_parent_task_uri, True, parent_allow_timeentry)
                task_hierarchy.append(parent_task_req)
                for child_task in task['data']:
                    child_task_code = child_task['MO_OPER_STEP_NO']
                    target_path_code = f"{child_task_code} | {parent_task_code}"
                    target_child_task_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                        'get_task_info_from_replicon'), 'full_path_code', target_path_code, 'uri', None)
                    parent_req = {
                        "uri": target_child_task_uri,
                        "name": null if target_child_task_uri else parent_task_code,
                        "project": {
                            "uri": null,
                            "name": root_project_id,
                            "code": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    }

                    child_task_req = get_task_modifications(
                        child_task, parent_req, target_child_task_uri, False, True)
                    task_hierarchy.append(child_task_req)
            return task_hierarchy

        def get_date_range():
            order_date = rail.get_dag_run_conf()['item']['data'][0]['ORD_DT']
            if order_date:
                return {
                    "startDate": rail.parse_date(rail.get_dag_run_conf()['item']['data'][0]['ORD_DT'], config.costpoint_date_format),
                    "endDate": null,
                }
            return None

        def get_add_project_and_task_param(dag_run):
            root_dept_uri = f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1"
            root_project_id = get_project_data()
            is_time_entry_allowed = True if rail.get_dag_run_conf()['item']['data'] and len(
                rail.get_dag_run_conf()['item']['data']) > 0 else False
            return {
                "project": {
                    "target": {
                        "uri": null,
                        "name": null,
                        "code": root_project_id,
                        "parameterCorrelationId": null
                    },
                    "projectInfo":
                    {
                        "name": "MO_" + root_project_id,
                        "code":  root_project_id,
                        "description":  root_project_id,
                        "timeEntryDateRange": get_date_range(),
                        "projectStatusLabel": {
                            "uri": null,
                            "name": 'In Progress'
                        },
                        "percentCompleted": "0",
                        "clients": [],
                        "program": null,
                        "projectLeader": null,
                        "customFieldValues": [
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', config.proj_referance_project_id, 'uri'),
                                },
                                "text": rail.get_dag_run_conf()['item']['data'][0]['BLD_PROJ_ID']
                            },
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', config.proj_mo_project_flag, 'uri'),
                                },
                                "text": 'yes'
                            }],
                        "isTimeEntryAllowed": is_time_entry_allowed,
                        "costTypeUri": null,
                        "estimatedHours": null,
                        "estimatedCost": null,
                        "estimatedExpenses": null,
                        "budget": null,
                        "isProjectLeaderApprovalRequired": "true",
                        "estimationModeUri": "urn:replicon:project-estimation-mode:task-based",
                        "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                        "timeAndMaterials": {
                            "timeAndExpenseEntryTypeUri": config.billable_uri,
                            "billingRateFrequency": null,
                            "billingRateFrequencyDuration": null,
                            "billingRates": []
                        },
                        "defaultBillingCurrency": null
                    },
                    "tasks": get_task_request(rail.result('group_data_by_root_task'), root_dept_uri),
                    "team": get_team_members(root_dept_uri, dag_run),
                    "expenses": null,
                    "timeAndMaterials": {
                        "billingRates": get_billing_rates_param(dag_run)
                    },
                    "fixedBid": null
                }
            }

        def get_team_members(root_dept_uri, dag_run):
            return {
                "teamMembers": [
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
                            "billingRatesAllowedForBillingTimeUris": get_project_resource_billingrates(dag_run)
                        }
                    }
                ]
            }

        def get_project_resource_billingrates(dag_run):
            mo_project = list(filter(
                lambda x: x['project']['code'] == get_build_project_code(dag_run), rail.result('get_project_details')))
            existing_billing_rates = mo_project[
                0]['timeAndMaterials']['projectBillingRates'] if mo_project else []
            billing_rates_allowed_billingtime_uris = []
            for rate in existing_billing_rates:
                billing_rates_allowed_billingtime_uris.append(
                    rate['billingRate']['uri'])
            return billing_rates_allowed_billingtime_uris

        def get_build_project_code(dag_run):
            if dag_run.conf['item']['data'][0]['BLD_PROJ_ID']:
                return dag_run.conf['item']['data'][0]['BLD_PROJ_ID'].split('.')[0]
            return ""

        def get_billing_rates_param(dag_run):
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

            mo_project = list(filter(
                lambda x: x['project']['code'] == get_build_project_code(dag_run), rail.result('get_project_details')))
            existing_billing_rates = mo_project[
                0]['timeAndMaterials']['projectBillingRates'] if mo_project else []
            for rate in existing_billing_rates:
                rates.append({
                    "billingRate": {
                        "uri": rate['billingRate']['uri'],
                        "name": null
                    },
                    "rateSchedule": null
                })
            return rates

        def get_project_data():
            root_project_id = rail.get_dag_run_conf()[
                'item']['root_project_id']
            return root_project_id

        def do_get_task_info_from_replicon(dag_run):
            tasks = []
            mo_project = list(filter(
                lambda x: x['project']['code'] == dag_run.conf['item']['root_project_id'], rail.result('get_project_details')))
            if mo_project:
                get_task_data_replicon(None, mo_project[0][
                    'tasks'], tasks)
            return tasks

        def get_task_data_replicon(paranet_task, tasks, result):
            for task in tasks:
                p_task = " | ".join([task['task']['code'], paranet_task]
                                    ) if paranet_task else "|".join([task['task']['code']])
                if task['task']['code']:
                    result.append(
                        {'code': task['task']['code'], 'name': task['task']['name'], 'uri': task['task']['uri'], 'full_path_code': p_task})
                get_task_data_replicon(
                    task['task']['code'], task['childTasks'], result)

        get_task_info_from_replicon = rail.PythonOperator(
            task_id='get_task_info_from_replicon',
            python_callable=do_get_task_info_from_replicon
        )

        def is_project_present(dag_run):
            if rail.result('get_project_details'):
                mo_project = list(filter(
                    lambda x: x['project']['code'] == dag_run.conf['item']['root_project_id'], rail.result('get_project_details')))
                return True if mo_project else False
            return False

        if_project_present = rail.IfOperator(
            task_id='if_project_present',
            test=is_project_present,
            yes_task="get_task_info_from_replicon",
            no_task="add_project_and_task",
        )

        add_project_and_task = rail.RepliconServiceOperator(
            task_id='add_project_and_task',
            endpoint="/services/ImportService1.svc/PutProject4",
            data=get_add_project_and_task_param
        )

        def update_project_create_or_modifiy(dag_run):
            root_project_id = get_project_data()
            proj_division_uri = get_project_division()
            is_time_entry_allowed = True if rail.get_dag_run_conf()['item']['data'] and len(
                rail.get_dag_run_conf()['item']['data']) > 0 else False
            return {
                "target": {
                    "code": root_project_id,
                },
                "modifications": {
                    "nameToApply": {
                        "value": "MO_" + root_project_id
                    },
                    "codeToApply": {
                        "value": root_project_id
                    },
                    "descriptionToApply": {
                        "value": root_project_id
                    },
                    "percentCompletedToApply": "0",
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:time-and-material"
                    },
                    "startDateToApply": {
                        "date": rail.parse_date(rail.get_dag_run_conf()['item']['data'][0]['ORD_DT'], config.costpoint_date_format) if rail.get_dag_run_conf()['item']['data'][0]['ORD_DT'] else None
                    },
                    "isProjectLeaderApprovalRequired": config.project_leader_approval,
                    "isTimeEntryAllowed": is_time_entry_allowed,
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": config.billable_uri,
                        "billingRates": get_billing_rates_param(dag_run)
                    },
                    "resourceProjectAssignmentModifications": get_update_billing_rate(dag_run),
                    "fixedBid": null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', config.proj_referance_project_id, 'uri'),
                            },
                            "text": rail.get_dag_run_conf()['item']['data'][0]['BLD_PROJ_ID']
                        },
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', config.proj_mo_project_flag, 'uri'),
                            },
                            "text": 'yes'
                        }],
                    "divisionToApply": {
                        "division": {
                            "uri": proj_division_uri,
                            "parentUri": null,
                            "name": null
                        }
                    } if proj_division_uri else None,
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

        def get_update_billing_rate(dag_run):
            return {
                "resourcesToAdd": [
                    {
                        "resource": {
                            "user": null,
                            "department": {
                                "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1"
                            }
                        },
                        "billingRates": get_update_billingrates(dag_run)
                    }
                ],
                "resourcesToRemove": []
            }

        def get_update_billingrates(dag_run):
            mo_project = list(filter(
                lambda x: x['project']['code'] == get_build_project_code(dag_run), rail.result('get_project_details')))
            existing_billing_rates = mo_project[
                0]['timeAndMaterials']['projectBillingRates'] if mo_project else []
            billing_rates_allowed_billingtime_uris = []
            for rate in existing_billing_rates:
                billing_rates_allowed_billingtime_uris.append(
                    {
                        "uri": rate['billingRate']['uri']
                    })
            return billing_rates_allowed_billingtime_uris

        update_project = rail.RepliconServiceOperator(
            task_id='update_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=update_project_create_or_modifiy
        )

        update_task = rail.RepliconServiceOperator(
            task_id='update_task',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            data=get_update_task_params
        )

        def get_project_division():
            project_details = rail.result('get_project_division_details')
            if project_details and project_details[0]['projectDetails'] and project_details[0]['projectDetails']['division']:
                return project_details[0]['projectDetails']['division']['uri']
            return None

        def get_prof_division_req():
            prof_division_uri = get_project_division()
            return {
                "projectUri": rail.result('add_project_and_task').get('uri'),
                "division": {
                    "uri": get_project_division(),
                    "parentUri": null,
                    "name": null
                } if prof_division_uri else None
            }

        update_division = rail.RepliconServiceOperator(
            task_id='update_division',
            endpoint='/services/ProjectService1.svc/UpdateDivision2',
            data=get_prof_division_req
        )

        add_log_entry = rail.WriteLogOperator(
            task_id='add_log_entry',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            items=lambda: rail.get_dag_run_conf()['item']['data'],
            properties={
                "proj_id": "{{ item.MO_ID }}",
                "proj_name":  "{{ item.MO_ID }}",
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
                "proj_id": "{{ item.MO_ID }}",
                "proj_name":  "{{ item.MO_ID }}",
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
        create_log >> group_data_by_root_task >> get_project_details >> get_project_division_details >> \
            if_project_present
        if_project_present >> rail.Label(
            'Yes') >> get_task_info_from_replicon >> update_project >> update_task >> add_log_entry
        if_project_present >> rail.Label('No') >> add_project_and_task >> \
            update_division >> add_log_entry >> finish
        finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
