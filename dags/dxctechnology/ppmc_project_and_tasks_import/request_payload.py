from datetime import datetime
import json

from airflow.operators.python import get_current_context
import rail

null = None


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_dag_run_conf():
    return get_current_context()['dag_run'].conf


def get_task_user_report_generation_batch_param():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_task_user_report_details')['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_task_base_report_generation_batch_param():
    project_details = rail.result('get_project_details')
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_task_base_report_details')['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                            rail.result('get_task_base_report_details')[
                                'filterConfiguration']['enabledFilters'],
                            'displayText',
                            'ProjectFilter',
                            'uri'),
                        "value": project_details[0]['projectDetails']['uri'].split(':')[-1] if project_details[0]['projectDetails'] else None
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_distinct_project_for_wbs(wbs):
    with rail.lib.readers.get_data_reader(rail.result('query_valid_data')) as reader:
        data = filter(lambda x: x['wbs'] == wbs, reader)
        return list(
            map(json.loads,
                set(
                    map(lambda x:
                        json.dumps({
                            "wbs": x['wbs'],
                            "taskname": x['task1name'],
                            "taskcode": x['task1code'],
                            "startdate": x['task1startdate'],
                            "enddate": x['task1enddate']
                        }, ensure_ascii=False),
                        data
                        )
                )
                )
        )


def get_distinct_task_for_wbs(wbs):
    with rail.lib.readers.get_data_reader(rail.result('query_valid_data')) as wbs_reader:
        data = filter(lambda x: x['wbs'] == wbs, wbs_reader)
        with rail.lib.readers.get_data_reader(rail.result('query_user_with_empid')) as emp_reader:
            emp_data = list(emp_reader)
            return list(
                map(json.loads,
                    set(
                        map(lambda x:
                            json.dumps({
                                "wbs": x['wbs'],
                                "task1code": x['task1code'],
                                "task2name": x['task2name'],
                                "task2code": x['task2code'],
                                "task2startdate": x['task2startdate'],
                                "task2enddate": x['task2enddate'],
                                "task2estimatedhours": x['task2estimatedhours'],
                                "aid": x['aid'],
                                "eidresource": x['eidresource'],
                                "aid_udfuri": rail.find_first_by_attr_and_get_attr(rail.result('get_task_custom_fields'), 'displayText', "AID", 'uri'),
                                "resourceuri": rail.find_first_by_attr_and_get_attr(emp_data, 'Employee_ID', x['eidresource'], 'UserUri'),
                                "systemid": x['systemid']
                            }, ensure_ascii=False),
                            data
                            )
                    )
                    )
            )


def get_resources(task):
    task = filter(lambda x: bool(x['resourceuri']), task)
    return list(set(map(lambda x: x['resourceuri'], task)))

# pylint: disable=line-too-long
def get_project_task_child_dag_confg(item):
    wbs = item['wbs']
    task = get_distinct_task_for_wbs(wbs)
    return {
        "wbsname": wbs,
        "task": task,
        "ppmcprojects": get_distinct_project_for_wbs(wbs),
        "resources": get_resources(task),
        "systemid": task[0]['systemid'],
        "companycodeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_department_groups'), 'displayText', "DXC", 'uri'),
        "orgunituri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divsions'), 'displayText', "PPMC", 'uri'),
        "tasktypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_task_custom_fields'), 'displayText', "Task Type", 'uri'),
        "tasktypeoption_ppmcproject": rail.find_first_by_attr_and_get_attr(rail.result('get_task_type_udf_dropdown_options'), 'displayText', "PPMC Project & Task", 'uri'),
        "tasktypeoption_ppmctask": rail.find_first_by_attr_and_get_attr(rail.result('get_task_type_udf_dropdown_options'), 'displayText', "PPMC Task", 'uri'),
        "ppmctaskrequiredoef": rail.find_first_by_attr_and_get_attr(rail.result('get_project_oefs'), 'name', "PPMC Task Required", 'uri'),
        "ppmctaskrequiredtaguri": rail.find_first_by_attr_and_get_attr(rail.result('get_task_required_oef_details')['tags'], 'name', "Y", 'uri'),
    }


def get_task(item):
    task_list = get_data_from_document(
        rail.result('create_tasklist_collection'))
    return list(filter(lambda x: x['task1code']
                == item['taskcode'], task_list))


def get_call_task_import_child_dag_confg(item):
    conf = get_dag_run_conf()
    return {
        "wbsname": conf['wbsname'],
        "systemid": conf['systemid'],
        "task": get_task(item),
        "tasktypeuri": conf['tasktypeuri'],
        "tasktypeoption_ppmcproject": conf['tasktypeoption_ppmcproject'],
        "tasktypeoption_ppmctask": conf['tasktypeoption_ppmctask'],
        "alltasksfromproject": get_data_from_document(rail.result('query_all_task')),
        "attrlist": get_data_from_document(rail.result('query_merge_task')),
        "projecturi": rail.result('get_project_details')[0]['projectDetails']['uri']
    }


def get_put_eligible_teammember_access_param():
    return {
        "projectUri": "{{ result('get_project_details')[0].projectDetails.uri }}",
        "teamMemberDataAccessScopes": [
            {
                "locations": [],
                "divisions": [
                    {
                        "uri": "{{ dag_run.conf.orgunituri }}",
                        "parentUri": null,
                        "name": null
                    }
                ],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [
                    {
                        "uri": "{{ dag_run.conf.companycodeuri }}",
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    }
                ],
                "employeeTypeGroups": []
            }
        ]
    }


def get_update_ppmc_required_oef_param():
    return {
        "objectUri": "{{ result('get_project_details')[0].projectDetails.uri }}",
        "value": {
            "definition": {
                "uri": "{{ dag_run.conf.ppmctaskrequiredoef }}",
                "name": null
            },
            "tag": {
                "uri": "{{ dag_run.conf.ppmctaskrequiredtaguri }}",
                "slug": null,
                "tagName": null
            },
            "numericValue": null,
            "textValue": null,
            "fileValue": null,
            "jsonValue": null
        }
    }


def get_put_key_value_for_project_param():
    return {
        "projectUri": "{{ result('get_project_details')[0].projectDetails.uri }}",
        "keyValue": {
            "keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
            "value": {
                "uri": "urn:replicon:project-team-member-assignment-type:manually-assign-task",
                "slug": null,
                "bool": null,
                "date": null,
                "number": null,
                "text": null,
                "time": null,
                "calendarDayDurationValue": null,
                "workdayDurationValue": null,
                "dateRange": null,
                "collection": []
            }
        }
    }


def get_create_parenttask_collection_source():
    project_list = get_data_from_document(
        rail.result('create_draft_parenttask_collection'))
    return list(
        map(lambda x: {
            'Task_Name': x['Task_Name'],
            'Task_Type': x['Task_Type'],
            'Attribute2': x['Attribute2'],
            'Attribute1': x['Attribute1'],
            'TaskUri': x['TaskUri'],
            'Attribute1_Name': get_attr1_name(x['Task_Name__Full_Path_']),
            'Attribute2_Name': get_attr2_name(x['Task_Name__Full_Path_']),
        }, project_list)
    )


def get_attr1_name(task_full_path):
    if not task_full_path:
        return None
    return task_full_path.split(" / ")[0]


def get_attr2_name(task_full_path):
    if not task_full_path:
        return None
    return ''.join(task_full_path.split(" / ")[0:2])


def can_update_task(item):
    conf = get_dag_run_conf()
    tasks = conf['alltasksfromproject']
    return bool(rail.find_first_by_attr_and_get_attr(
        tasks, 'Task_Name', f'{conf["systemid"]}-{item["task1code"]}-{item["task2code"]}', 'TaskUri'))


def can_update_task_for_attlist(item):
    conf = get_dag_run_conf()
    child_tasks = rail.result('get_children_tasks')
    for tasks in child_tasks:
        if rail.find_first_by_attr_and_get_attr(
                tasks, 'name', f'{conf["systemid"]}-{item["task1code"]}-{item["task2code"]}', 'uri'):
            return True

    return False


def get_process_task_dag_id(postfix, item):
    return f'dxctechnology_ppmc_project_task_import_child_task_update{postfix}' if can_update_task(item) \
        else f'dxctechnology_ppmc_project_task_import_child_task_create{postfix}'


def get_resource_uris(item):
    return list(set(map(lambda x: x['resourceuri'],
                        filter(lambda x: x['resourceuri'] and
                        x['task2code'] == item['task2code'], get_dag_run_conf()['task']))))

def get_process_task_conf(item):
    conf = get_dag_run_conf()
    task_uri = rail.find_first_by_attr_and_get_attr(
        conf['alltasksfromproject'], 'Task_Name', f'{conf["systemid"]}-{item["task1code"]}-{item["task2code"]}', 'TaskUri')

    if task_uri:  # update
        return {
            'name': f'{conf["systemid"]}-{item["task1code"]}-{item["task2code"]}',
            'description': item['task2name'],
            'startdate': item['task2startdate'],
            'enddate': item['task2enddate'],
            'projecturi': conf['projecturi'],
            'taskuri': task_uri,
            'parenttask': None,
            'tasktypeuri': conf['tasktypeuri'],
            'tasktypeoption_ppmcproject': conf['tasktypeoption_ppmcproject'],
            'tasktypeoption_ppmctask': conf['tasktypeoption_ppmctask'],
            'wbsname': conf['wbsname'],
            'taskdata': None,
            'aid': item['aid'],
            'aid_udfuri': item['aid_udfuri'],
            'task2estimatedhours': item['task2estimatedhours'],
            'resourceuris': get_resource_uris(item),
            'attrlist': conf['attrlist'],
        }

    return {  # add
        'wbsname': conf['wbsname'],
        'name': f'{conf["systemid"]}-{item["task1code"]}-{item["task2code"]}',
        'description': item['task2name'],
        'startdate': item['task2startdate'],
        'enddate': item['task2enddate'],
        'projecturi': conf['projecturi'],
        'attributesparenttaskuri': conf['attrlist'],
        'tasktypeuri': conf['tasktypeuri'],
        'tasktypeoption_ppmcproject': conf['tasktypeoption_ppmcproject'],
        'taskdata': None,
        'tasktypeoption_ppmctask': conf['tasktypeoption_ppmctask'],
        'wbsparenttaskuri': rail.find_first_by_attr_and_get_attr(conf['alltasksfromproject'], 'Task_Name', conf['wbsname'], 'TaskUri'),
        'resourceuris': get_resource_uris(item),
        'aid': item['aid'],
        'aid_udfuri': item['aid_udfuri'],
        'task2estimatedhours': item['task2estimatedhours'],
        'attrlist': conf['attrlist'],
    }


def get_process_att_task_dag_id(postfix, item):
    return f'dxctechnology_ppmc_project_task_import_child_task_update{postfix}' if can_update_task_for_attlist(item) \
        else f'dxctechnology_ppmc_project_task_import_child_task_create{postfix}'


def map_attr_task():
    conf = get_dag_run_conf()
    tasks = rail.load_all_records(rail.result('query_distinct_task'))
    attrlist = conf['attrlist']
    data = []
    for task in tasks:
        for attr_task in attrlist:
            child_tasks = rail.result('get_children_tasks')[attrlist.index(attr_task)]
            child_task_uri = rail.find_first_by_attr_and_get_attr(
                child_tasks, 'name', f'{conf["systemid"]}-{task["task1code"]}-{task["task2code"]}', 'uri')
            if child_task_uri:  # update
                for child_task in child_tasks:
                    if child_task['name'] == f'{conf["systemid"]}-{task["task1code"]}-{task["task2code"]}':
                        data.append(
                            {**task, 'taskuri': child_task_uri, 'wbsparenttaskuri': None})

            else:  # add
                data.append(
                    {**task, 'taskuri': None, 'wbsparenttaskuri': attr_task['TaskUri']})
    return data


def get_replicon_date(date_str):
    if not date_str:
        return None
    # date format in 20060401
    date = datetime.strptime(date_str, '%Y%m%d')
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }


def get_replicon_hours(str_decimal_hours):
    if not str_decimal_hours:
        return null

    decimal_hours = float(str_decimal_hours)
    hours = int(decimal_hours)
    minutes = int((decimal_hours * 60) % 60)
    seconds = int((decimal_hours * 3600) % 60)
    return {
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds
    }


def get_put_parent_task_param():
    conf = get_dag_run_conf()
    return {
        "project": {
            "uri": conf['projecturi'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": conf['name'],
                "parent": {
                    "uri": conf['wbsparenttaskuri'],
                    "name": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "parameterCorrelationId": null
            },
            "name": conf['name'],
            "code": conf['description'],
            "description": conf['description'],
            "timeEntryDateRange": {
                "startDate": get_replicon_date(conf['startdate']),
                "endDate": get_replicon_date(conf['enddate']),
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": get_replicon_hours(conf['task2estimatedhours']),
            "isClosed": "false",
            "customFieldValues": [
                {
                    "customField": {
                        "uri": conf['tasktypeuri'],
                        "name": null,
                        "groupUri": null
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {
                        "uri": conf['tasktypeoption_ppmcproject'],
                        "name": null
                    },
                    "number": null
                }
            ],
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "assignedResources":  list(map(lambda x: {'uri': x}, conf['resourceuris'])),
            "keyValues": [],
            "historicalKeyValues": [],
            "extensionFieldValues": []
        }
    }


def get_put_task_with_parent_param():
    conf = get_dag_run_conf()
    return list(
        map(lambda x:
            json.dumps({
                "project": {
                    "uri": conf['projecturi'],
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": conf['name'],
                        "parent": {
                            "uri": x['TaskUri'],
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": conf['name'],
                    "code": conf['description'],
                    "description": conf['description'],
                    "timeEntryDateRange": {
                        "startDate": get_replicon_date(conf['startdate']),
                        "endDate": get_replicon_date(conf['enddate']),
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "true",
                    "estimatedHours": get_replicon_hours(conf['task2estimatedhours']),
                    "isClosed": "false",
                    "customFieldValues": [
                        {
                            "customField": {
                                "uri": conf['tasktypeuri'],
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": conf['tasktypeoption_ppmcproject'],
                                "name": null
                            },
                            "number": null
                        }
                    ],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "assignedResources":  list(map(lambda x: {'uri': x}, conf['resourceuris'])),
                    "keyValues": [],
                    "historicalKeyValues": [],
                    "extensionFieldValues": []
                }
            }, ensure_ascii=False),
            conf['attrlist'])
    )


def get_put_task_without_parent_param():
    conf = get_dag_run_conf()
    return {
        "project": {
            "uri": conf['projecturi'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": conf['name'],
                "parent": null,
                "parameterCorrelationId": null
            },
            "name": conf['name'],
            "code": conf['description'],
            "description": conf['description'],
            "timeEntryDateRange": {
                "startDate": get_replicon_date(conf['startdate']),
                "endDate": get_replicon_date(conf['enddate']),
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": get_replicon_hours(conf['task2estimatedhours']),
            "isClosed": "false",
            "customFieldValues": [
                {
                    "customField": {
                        "uri": conf['tasktypeuri'],
                        "name": null,
                        "groupUri": null
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {
                        "uri": conf['tasktypeoption_ppmcproject'],
                        "name": null
                    },
                    "number": null
                }
            ],
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "assignedResources":  list(map(lambda x: {'uri': x}, conf['resourceuris'])),
            "keyValues": [],
            "historicalKeyValues": [],
            "extensionFieldValues": []
        }
    }

def is_ppmc_task_required():
    ppmc_project_extension_field_values = rail.find_first_by_attr_and_get_attr(
        rail.result('get_project_details')[
            0]['projectDetails']['extensionFieldValues'],
        'definition.displayText', "PPMC Task Required")
    return ppmc_project_extension_field_values['tag']['displayText'].upper() == 'Y' if ppmc_project_extension_field_values else False
