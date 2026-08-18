from datetime import datetime
import os
import rail
null = None
DATE_FORMAT = "%d.%m.%Y"

def get_task_name(dag_run):
    return dag_run.conf['task_name'] + (' - ' + dag_run.conf['task_code'] if dag_run.conf['task_code'] else '')

def get_date_from_str_date(date_str):
    return datetime.strptime(date_str, DATE_FORMAT)

def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        date = get_date_from_str_date(date_str)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def is_end_date_before_start_date(dag_run):
    end_date_rep = get_replicon_date(dag_run.conf['task_end_date'])
    end_date = datetime(int(end_date_rep['year']), int(
        end_date_rep['month']), int(end_date_rep['day']))

    task_start_date = get_replicon_date(dag_run.conf['task_start_date'])
    start_date = datetime(int(task_start_date['year']), int(
        task_start_date['month']), int(task_start_date['day']))

    return end_date < start_date


def attribute_payload(item):
    return {
        'file_name': os.path.split(rail.result('new_file_sensor'))[1],
        "wbs": item['wbs'],
        'gsap_task_uri': rail.result('get_gsap_task_uri')[0]['uri'],
        "get_parent_wbs_column_uri": rail.result('get_parent_wbs_column_uri'),
        "filter_definition": rail.result('get_all_filter_definition'),
        "get_all_gsap_tasks_from_replicon" : rail.result("get_all_gsap_tasks_from_replicon")
    }


def get_specific_attribute_system_level_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:object-extension-tag-list-column:name",
            "urn:replicon:object-extension-tag-list-column:code",
            "urn:replicon:object-extension-tag-list-column:description",
            "urn:replicon:object-extension-tag-list-column:object-extension-tag",
            "urn:replicon:object-extension-tag-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uri":dag_run.conf['gsap_task_uri']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:code"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "rightExpression": null,
                    "value": {
                        "text": dag_run.conf['task_name']
                    }
                }
            }
        }
    }


def get_project_details(dag_run):
    return {
        "projects": [
            {
                "uri": null,
                "name": dag_run.conf['wbs'],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def get_child_project_details(dag_run):
    return {
        "projects": [
            {
                "uri": null,
                "name": dag_run.conf['childWbs'],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def get_specific_attribute_project_level(dag_run):
    return {
        "page": "1",
        "pageSize": "10000",
        "textSearch": {
            "queryText": get_task_name(dag_run),
            "searchInDisplayText": "false",
            "searchInName": "true",
            "searchInCode": "false"
        },
        "project": {
            "uri": null,
            "name": dag_run.conf['WBS'],
            "code": null,
            "parameterCorrelationId": null
        },
        "objectExtensionFieldDefinition": {
            "uri": null,
            "name": "GSAP Task"
        }
    }

def get_start_end_date(dag_run, date_to_apply):
    if dag_run.conf[f'task_{date_to_apply}']:
        return get_replicon_date(dag_run.conf[f'task_{date_to_apply}'])

    return null

def update_attribute_dates_project(dag_run):

    end_date_update = get_replicon_date(dag_run.conf['task_end_date']) if dag_run.conf['task_end_date'] else null
    start_date_update = get_replicon_date(dag_run.conf['task_start_date']) if dag_run.conf['task_start_date'] else null
    attribute_uri = rail.result('get_specific_attribute_uri_system_level')[
        0]['cells'][3]['uri']
    return {
        "project": {
            "uri": null,
            "name": dag_run.conf['WBS'],
            "code": null,
            "parameterCorrelationId": null
        },
        "objectExtensionFieldTags": {
            "tagsToAdd": [
                {
                    "target": {
                        "uri": attribute_uri,
                        "slug": null,
                        "tagName": {
                            "name": null,
                            "tagDefinitionUri": dag_run.conf['gsap_task_uri']
                        }
                    },
                    "isEnabled": "true",
                    "dateRange": {
                        "startDate": start_date_update,
                        "endDate": end_date_update,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    }
                }
            ],
            "tagsToRemove": []
        }
    }




def get_child_wbs_payload(dag_run):
    return {
        "page": 1,
        "pagesize": 100000,
        "columnUris": [
            "urn:replicon:project-list-column:project",
            dag_run.conf['get_parent_wbs_column_uri'][0]['uri']
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": dag_run.conf['filter_definition'][0]['uri']
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
                    "text": dag_run.conf['wbs'],
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
    }

def get_gsap_task_from_replicon_payload():
    return {
        "page": 1,
        "pagesize": 100000,
        "columnUris": [
            "urn:replicon:object-extension-tag-list-column:name",
            "urn:replicon:object-extension-tag-list-column:code",
            "urn:replicon:object-extension-tag-list-column:description",
            "urn:replicon:object-extension-tag-list-column:object-extension-tag",
            "urn:replicon:object-extension-tag-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uri": rail.result('get_gsap_task_uri')[0]['uri'],
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
}

def get_all_gsap_task_payload():
    return {
        "page": "1",
        "pageSize": "1000000",
        "textSearch": null,
        "project": {
            "uri": rail.result('get_project_details')[0]['uri'],
        },
        "objectExtensionFieldDefinition": {
            "name": "GSAP Task"
        }
    }


def batch_update_gsap_task_payload(items, dag_run):
    return {
        "project": {
            "uri": rail.result('get_project_details')[0]['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "objectExtensionFieldTags": {
            "tagsToAdd": list(map(lambda task :{
                    "target": {
                        "uri": task['replicon_oef_task_details'].get('uri', null),
                        "tagName": null
                    },
                    "isEnabled": "true",
                    "dateRange": {
                        "startDate": get_replicon_date(task['task_start_date']),
                        "endDate":  get_replicon_date(task['task_end_date']),
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    }
                }, items)),
            "tagsToRemove": []
        }
    }

def batch_disable_gsap_task_payload(items, dag_run):
    return {
        "project": {
            "uri": rail.result('get_project_details')[0]['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "objectExtensionFieldTags": {
            "tagsToAdd": list(map(lambda task :{
                    "target": {
                        "uri": task.get('uri', null),
                        "tagName": null
                    },
                    "isEnabled": "false",
                    "dateRange": {
                        "startDate": task['start_date'],
                        "endDate":  task['end_date'],
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    }
                }, items)),
            "tagsToRemove": []
        }
    }
