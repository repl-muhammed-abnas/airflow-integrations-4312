from datetime import datetime
import rail
null = None


def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%Y%m%d')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def is_end_date_before_start_date():
    end_date_rep = get_replicon_date(get_conf()['EndDate'])
    end_date = datetime(int(end_date_rep['year']), int(
        end_date_rep['month']), int(end_date_rep['day']))
    start_date = datetime(int(get_conf()['start_date_year']), int(
        get_conf()['start_date_month']), int(get_conf()['start_date_day']))
    return end_date < start_date


def attribute_payload(item):
    return {
        "wbs": item['WBS'],
        'attribute_1_2_uri': rail.result('get_attribute_1_uri')[0]['uri'] if 'Attributes_1' in rail.result('new_file_sensor')
        else rail.result('get_attribute_2_uri')[0]['uri'],
        'attribute_number': "1" if 'Attributes_1' in rail.result('new_file_sensor') else "2",
    }


def get_specific_attribute_system_level_payload():
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
                        "uri": get_conf()['attribute_1_2_uri']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "rightExpression": null,
                    "value": {
                        "text": get_conf()['attribute_value']
                    }
                }
            }
        }
    }


def add_attribute_system_level():
    return {
        "modifications": {
            "target": {
                "uri": get_conf()['attribute_1_2_uri'],
                "name": null
            },
            "tagsToUpdate": [
                {
                    "target": {
                        "uri": null,
                        "slug": null,
                        "tagName": {
                            "name": get_conf()['attribute_name'],
                            "tagDefinitionUri": null
                        }
                    },
                    "name": get_conf()['attribute_name'],
                    "code": get_conf()['attribute_value'],
                    "description": null,
                    "isEnabled": "true"
                }
            ]
        }
    }


def get_project_details():
    return {
        "projects": [
            {
                "uri": null,
                "name": get_conf()['wbs'],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def get_child_project_details():
    return {
        "projects": [
            {
                "uri": null,
                "name": get_conf()['childWbs'],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def get_specific_attribute_project_level():
    return {
        "page": "1",
        "pageSize": "10000",
        "textSearch": {
            "queryText": get_conf()['attribute_value'] + " - " + get_conf()['Description'],
            "searchInDisplayText": "false",
            "searchInName": "true",
            "searchInCode": "false"
        },
        "project": {
            "uri": null,
            "name": get_conf()['WBS'],
            "code": null,
            "parameterCorrelationId": null
        },
        "objectExtensionFieldDefinition": {
            "uri": null,
            "name": f"Attribute {get_conf()['AttributeNumber']}"
        }
    }


def get_update_attribute_end_date_project():
    if not get_conf()['end_date_year']:
        end_date_update = get_replicon_date(get_conf()['EndDate'])
    else:
        end_date_rep = get_replicon_date(get_conf()['EndDate'])
        end_date_feed = datetime(int(end_date_rep['year']), int(
            end_date_rep['month']), int(end_date_rep['day']))
        end_date_project = datetime(int(get_conf()['end_date_year']), int(
            get_conf()['end_date_month']), int(get_conf()['end_date_day']))
        if end_date_feed > end_date_project:
            end_date_update = {
                'year': get_conf()['end_date_year'],
                'month': get_conf()['end_date_month'],
                'day': get_conf()['end_date_day']
            }
        else:
            end_date_update = get_replicon_date(get_conf()['EndDate'])
    attribute_uri = rail.result('get_specific_attribute_uri_system_level')[
        0]['cells'][3]['uri']
    return {
        "project": {
            "uri": null,
            "name": get_conf()['WBS'],
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
                        "startDate": {
                            "year": get_conf()['start_date_year'],
                            "month": get_conf()['start_date_month'],
                            "day": get_conf()['start_date_day']
                        },
                        "endDate": end_date_update,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    }
                }
            ],
            "tagsToRemove": []
        }
    }


def get_add_attribute_code_system():
    return {
        "modifications": {
            "target": {
                "uri": get_conf()['attribute_1_2_uri'],
                "name": null
            },
            "tagsToUpdate": [
                {
                    "target": {
                        "uri": null,
                        "slug": null,
                        "tagName": {
                            "name": get_conf()['attribute_name'],
                            "tagDefinitionUri": rail.result('get_specific_attribute_uri_system_level')[0]['cells'][3]['uri']
                        }
                    },
                    "name": get_conf()['attribute_name'],
                    "code": get_conf()['attribute_value'],
                    "description": null,
                    "isEnabled": "true"
                }
            ]
        }
    }
