from datetime import datetime
import os
import rail
null = None

def get_task_name(dag_run):
    return dag_run.conf['task_name'] + (' - ' + dag_run.conf['task_code'] if dag_run.conf['task_code'] else '')

def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        DATE_FORMAT = "%d.%m.%Y"
        date = datetime.strptime(date_str, DATE_FORMAT)
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

    if end_date <= datetime(year=datetime.now().year, month=1, day=1):
        return True

    task_start_date = get_replicon_date(dag_run.conf['task_start_date'])
    start_date = datetime(int(task_start_date['year']), int(
        task_start_date['month']), int(task_start_date['day']))

    return end_date < start_date


def attribute_payload(item):
    return {
        'file_name': os.path.split(rail.result('new_file_sensor'))[1],
        "wbs": item['wbs'],
        'gsap_task_uri': rail.result('get_gsap_task_uri')[0]['uri'],
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
                        "tagName": null
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
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            rail.result('get_all_columns')[0]['uri']
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": rail.result('get_all_filter_defination')[0]['uri']
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
