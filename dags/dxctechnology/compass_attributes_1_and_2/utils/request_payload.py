import os
import hashlib
import uuid
import rail
from dxctechnology.compass_attributes_1_and_2.utils import custom_methods

null = None


def get_process_each_wbs(item, attribute_number):
    def get_attributes(attributes):
        attributes_list = []
        for attribute in attributes:
            name = attribute['attribute']
            description = attribute['description'] if attribute['description'] else ''
            enddate = attribute['enddate']
            md5 = hashlib.md5((name + ',' + description +
                              ',' + str(enddate)).encode('utf-8')).hexdigest()
            attributes_list.append({
                'AttributeNumber': attribute['attributenumber'],
                'Attribute': name,
                'Description': description,
                'EndDate': enddate,
                'tasktypeoptionuri':
                    rail.find_first_by_attr_and_get_attr(rail.result(
                        'get_all_customfield_drop_down_options'), 'displayText', attribute_number, 'uri'),
                'md5': md5
            })
        return attributes_list
    return {
        'wbs': item['wbs'],
        'attributes': get_attributes(item['attributes']),
        'tasktypeuri': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_customfields'), 'displayText', 'Task Type', 'uri'),
        'filename': os.path.split(rail.result("new_file_sensor"))[1]
    }


def get_create_task_conf(dag_run, item, project_details, team_member_details):
    return {
        'level': item['number'],
        'name': item['name'],
        'description': item['code'] if item['code'] else null,
        'enddate': custom_methods.get_end_date(rail.result('get_project_date_range')['enddate'], item['enddate'], '%d/%m/%Y'),
        'startdate': custom_methods.get_start_date(rail.result('get_project_date_range')['startdate'], '%d/%m/%Y'),
        'projecturi': rail.result(project_details)['uri'],
        'parenttaskuri': null,
        'tasktypeuri': dag_run.conf['tasktypeuri'],
        'tasktypeoptionuri': item['tasktype'],
        'iwowbsprojecturi': null,
        'iwostartdate': null,
        'iwoenddate': null,
        'iwoparenttaskuri': null,
        'userlist': custom_methods.get_userlist(team_member_details),
        'iwouserlist': [
            {
                'uri': null
            }
        ],
        'parenttaskstartdate': null,
        'parenttaskendate': null,
        'isiwoproject': null,
        'projectname': rail.result(project_details)['name'],
        'level0taskuri': null,
        'attribute2list': [],
        'filename': dag_run.conf['filename']

    }


def get_copy_attribute_1_data(create_collection):
    rows = custom_methods.get_data_from_document(
        rail.result(create_collection))
    return list(
        map(lambda x: {
            'attributenumber': x['number'],
            'name': x['name'],
            'description': x['code'] if x['code'] else null,
            'enddate': x['enddate'],
            'tasktypeoptionuri': x['tasktype'],
            'md5': x['md5'],
            'uri': x['uri']
        }, rows)
    )


def get_copy_data_conf(dag_run, project_details, task_query_name):
    project_end_date = custom_methods.get_replicon_date(
        rail.result('get_project_date_range')['enddate'], '%d/%m/%Y')
    return {
        'data': get_copy_attribute_1_data(task_query_name['task_collection']),
        'sourcetask': task_query_name['sourcetask'],
        'wbs': rail.result(project_details)['name'],
        'projectenddate': rail.result('get_project_date_range')['enddate'],
        'projectendateday': project_end_date['day'] if project_end_date else null,
        'projectendatemonth': project_end_date['month'] if project_end_date else null,
        'projectendateyear': project_end_date['year'] if project_end_date else null,
        'projecturi': rail.result(project_details)['uri'],
        'filename': dag_run.conf['filename']
    }


def get_add_attribute_1_data():
    rows = custom_methods.get_data_from_document(
        rail.result('created_tasks_collection'))
    return list(
        map(lambda x: {
            'name': x['name'],
            'code': x['code'],
            'uri': x['uri'],
            'enddateday': x['enddateday'],
            'enddatemonth': x['endatemonth'],
            'enddateyear': x['enddateyear']
        }, rows)
    )


def get_add_data_conf(dag_run):
    return {
        'data': get_add_attribute_1_data(),
        'wbs': dag_run.conf['wbs'],
        'projectenddate': dag_run.conf['projectenddate'],
        'projectendateday': dag_run.conf['projectendateday'],
        'projectendatemonth': dag_run.conf['projectendatemonth'],
        'projectendateyear': dag_run.conf['projectendateyear'],
        'projecturi': dag_run.conf['projecturi'],
        'filename': dag_run.conf['filename']
    }


def build_copy_parameters(dag_run):
    return list(
        map(lambda x: {
            "sourceTask": {
                "uri": dag_run.conf["sourcetask"]
            },
            "taskName": x["name"],
            "hierarchyCopyOptionUri": "urn:replicon:task-copy-hierarchy-copy-option:copy-all-descendants"
        }, dag_run.conf["data"])
    )


def get_create_task_copy_batch(dag_run):
    return {
        "copyParameters": build_copy_parameters(dag_run)
    }


def build_task_hierarchy(dag_run):
    return list(
        map(lambda x: {
            "target": {
                "uri": x["uri"]
            },
            "taskModificationToApply": {
                "codeToApply": {
                    "value": x["code"]
                }
            },
            "timeEntryEndDateToApply": {
                "date": {
                    "year": x["enddateyear"],
                    "month": x["enddatemonth"],
                    "day": x["enddateday"],
                }
            }
        }, dag_run.conf["data"])
    )


def get_create_task_hierarchy(dag_run):
    return {
        "project": {
            "uri":  dag_run.conf['projecturi'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "taskHierarchy": build_task_hierarchy(dag_run),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": "Job ID PropertiesUser ID Properties"
    }


def get_update_task_conf(dag_run, item, project_details):
    return {
        'data': [
            {
                'name': item['name'],
                'code': item['code'],
                'uri': item['uri'],
                'enddateday': custom_methods.get_lower_date(
                    rail.result('get_project_date_range')['enddate'], item['enddate'], '%d/%m/%Y', '%Y%m%d')['day'],
                'enddatemonth': custom_methods.get_lower_date
                (rail.result('get_project_date_range')[
                 'enddate'], item['enddate'], '%d/%m/%Y', '%Y%m%d')['month'],
                'enddateyear': custom_methods.get_lower_date(
                    rail.result('get_project_date_range')['enddate'], item['enddate'], '%d/%m/%Y', '%Y%m%d')['year']
            }
        ],
        'wbs': rail.result(project_details)['name'],
        'projectenddate': rail.result('get_project_date_range')['enddate'],
        'projectendateday': rail.result(project_details)['timeEntryDateRange']['endDate']['day']
        if rail.result(project_details)['timeEntryDateRange']['endDate'] else null,
        'projectendatemonth': rail.result(project_details)['timeEntryDateRange']['endDate']['month']
        if rail.result(project_details)['timeEntryDateRange']['endDate'] else null,
        'projectendateyear': rail.result(project_details)['timeEntryDateRange']['endDate']['year']
        if rail.result(project_details)['timeEntryDateRange']['endDate'] else null,
        'projecturi': rail.result(project_details)['uri'],
        'filename': dag_run.conf['filename']
    }


def get_update_task_hierarchy(dag_run):
    return list(
        map(lambda x: {
            "target": {
                "uri": x['uri']
            },
            "taskModificationToApply": {
                "codeToApply": {
                    "value": x['code']
                },
                "timeEntryEndDateToApply": {
                    "date": {
                        "year": x['enddateyear'],
                        "month": x['enddatemonth'],
                        "day": x['enddateday']
                    }
                },
                "isClosed": "false"
            }
        }, dag_run.conf['data'])
    )


def get_create_modify_task_hierarchy(dag_run):
    return {
        "project": {
            "uri": dag_run.conf['projecturi'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "taskHierarchy": get_update_task_hierarchy(dag_run),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_put_task_data(dag_run, parent, datefmt='%Y-%m-%d'):
    return {
        "project": {"uri": dag_run.conf['projecturi']},
        "task": {
            "target": {
                "name": dag_run.conf['name'],
                "parent": {"uri": dag_run.conf['parenttaskuri']} if parent else null
            },
            "name": dag_run.conf['name'],
            "code": dag_run.conf['description'],
            "timeEntryDateRange": {
                "startDate": custom_methods.get_replicon_date(dag_run.conf['startdate'], datefmt),
                "endDate": custom_methods.get_replicon_date(dag_run.conf['enddate'], '%Y-%m-%d')
            },
            "customFieldValues": [
                {
                    "customField": {"uri": dag_run.conf['tasktypeuri']},
                    "dropDownOption": {"uri": dag_run.conf['tasktypeoptionuri']},
                }
            ],
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "percentCompleted": 0,
            "isTimeEntryAllowed": True,
            "isClosed": False,
            "assignedResources": [{'uri': user['uri']} for user in dag_run.conf['userlist']],
        }
    }


def get_process_each_wbs_attribute_1(dag_run, item, project_details):
    return {
        "records": {
            "wbs": rail.result(project_details)['name'],
            "attribute1name": item['name'],
            "attributes": dag_run.conf['attributes']
        },
        "tasktypeuri": dag_run.conf['tasktypeuri'],
        "attribute2optionuri": null,
        "attribute1uri": item['uri'],
        "wbsuri": rail.result(project_details)['uri'],
        "wbsstartdate": rail.result('get_project_date_range')['startdate'],
        "wbsenddate": rail.result('get_project_date_range')['enddate'],
        "attribute1startdate": item['startdate'],
        "attribute1enddate": item['enddate'],
        "filename": dag_run.conf['filename']
    }


def get_attribute2_start_date(dag_run):
    start_date = custom_methods.get_lower_date(
        dag_run.conf['parenttaskstartdate'], dag_run.conf['startdate'], '%d/%m/%Y', '%d/%m/%Y')
    return {
        "year": str(start_date['year']),
        "month": str(start_date['month']),
        "day": str(start_date['day'])
    } if start_date else null


def get_attribute2_end_date(dag_run):
    end_date = custom_methods.get_lower_date(
        dag_run.conf['parenttaskendate'], dag_run.conf['enddate'], '%d/%m/%Y', '%Y-%m-%d')
    return {
        "year": str(end_date['year']),
        "month": str(end_date['month']),
        "day": str(end_date['day'])
    } if end_date else null


def get_create_modify_task2(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['taskuri']
        },
        "project": {
            "uri": dag_run.conf['projecturi'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "codeToApply": {
                "value": dag_run.conf['description']
            },
            "timeEntryStartDateToApply": {
                "date": get_attribute2_start_date(dag_run) if get_attribute2_start_date(dag_run) else null
            },
            "timeEntryEndDateToApply": {
                "date": get_attribute2_end_date(dag_run)
            }
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_create_update_timeentry_date_range_batch(dag_run):
    return {
        "taskUri": dag_run.conf['taskuri'],
        "dateRange": {
            "startDate": get_attribute2_start_date(dag_run) if get_attribute2_start_date(dag_run) else null,
            "endDate": get_attribute2_end_date(dag_run)
        }
    }


def get_update_task_data(item, dag_run):
    return {
        "target": {"uri": item['uri']},
        "project": {"uri": dag_run.conf['projecturi']},
        "modifications": {
            "timeEntryEndDateToApply": {"date": item['enddate']}
        },
        "unitOfWorkId": str(uuid.uuid4()),
    }
