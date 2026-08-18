import rail
from uuid import uuid4
from datetime import datetime


def get_field_value(payload, *field_names):
    for field_name in field_names:
        value = payload.get(field_name)
        if value not in [None, '', ' ']:
            return value
    return None


def parse_time_to_dict(time_str):
    if not time_str:
        return None

    parts = time_str.split(':')
    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    second = int(parts[2]) if len(parts) > 2 else 0

    return {
        "hour": hour,
        "minute": minute,
        "second": second
    }


def build_custom_metadata_billable(project_uri, task_uri, client_uri, comments=None):
    null = None
    meta_data_key_uri = "urn:replicon:time-entry-metadata-key:"
    meta_data_list = []

    meta_data_list.append({
        "keyUri": f"{meta_data_key_uri}is-billable",
        "value": {
            "bool": True,
            "uri": null,
            "text": null
        }
    })

    meta_data_list.append({
        "keyUri": f"{meta_data_key_uri}client",
        "value": {
            "bool": null,
            "uri": client_uri,
            "text": null
        }
    })

    if task_uri:
        meta_data_list.append({
            "keyUri": f"{meta_data_key_uri}task",
            "value": {
                "bool": null,
                "uri": task_uri,
                "text": null
            }
        })
    elif project_uri:
        meta_data_list.append({
            "keyUri": f"{meta_data_key_uri}project",
            "value": {
                "bool": null,
                "uri": project_uri,
                "text": null
            }
        })

    if comments:
        meta_data_list.append({
            "keyUri": f"{meta_data_key_uri}comments",
            "value": {
                "bool": null,
                "uri": null,
                "text": comments
            }
        })

    from time import time
    meta_data_list.append({
        "keyUri": "urn:replicon:widget-ui-metadata-key:initial-row-number",
        "value": {
            "number": int(time()),
        }
    })

    return meta_data_list


def get_seconds_from_hours(hours):
    return int(float(hours) * 3600) if hours != '' else 0


def build_put_time_entry_payload_tsd(user_uri, project_uri, task_uri, client_uri, entry_date, hours, comments=None, oef_values=None):
    return {
        "timeEntryRevisionGroup": {
            "target": None,
            "user": {"uri": user_uri},
            "entryDate": rail.parse_date(entry_date, "%Y-%m-%d"),
            "timeAllocationTypeUris": ["urn:replicon:time-allocation-type:project", "urn:replicon:time-allocation-type:attendance"],
            "interval": {
                "hours": {
                    "hours": 0,
                    "minutes": 0,
                    "seconds": get_seconds_from_hours(hours),
                    "milliseconds": 0,
                    "microseconds": 0
                },
                "timePair": None
            },
            "customMetadata": build_custom_metadata_billable(project_uri, task_uri, client_uri, comments),
            "extensionFieldValues": oef_values or []
        },
        "unitOfWorkId": str(uuid4())
    }


def build_put_time_entry_payload_inout(user_uri, project_uri, task_uri, client_uri, entry_date, in_time, out_time, comments=None, oef_values=None):
    return {
        "timeEntryRevisionGroup": {
            "target": None,
            "user": {"uri": user_uri},
            "entryDate": rail.parse_date(entry_date, "%Y-%m-%d"),
            "timeAllocationTypeUris": ["urn:replicon:time-allocation-type:project", "urn:replicon:time-allocation-type:attendance"],
            "interval": {
                "timePair": {
                    "startTime": parse_time_to_dict(in_time),
                    "endTime": parse_time_to_dict(out_time)
                }
            },
            "customMetadata": build_custom_metadata_billable(project_uri, task_uri, client_uri, comments),
            "extensionFieldValues": oef_values or []
        },
        "unitOfWorkId": str(uuid4())
    }


def build_bulk_put_time_punch_payload(user_uri, entry_date, punch_in, punch_out, project_uri=None, client_uri=None, task_uri=None, oef_values=None):
    null = None

    entry_date_dict = rail.parse_date(entry_date, "%Y-%m-%d")

    punch_in_time_dict = parse_time_to_dict(punch_in)
    punch_in_datetime = {
        "year": str(entry_date_dict["year"]),
        "month": str(entry_date_dict["month"]),
        "day": str(entry_date_dict["day"]),
        "hour": str(punch_in_time_dict["hour"]),
        "minute": str(punch_in_time_dict["minute"]),
        "second": str(punch_in_time_dict["second"]),
        "timeZoneUri": null
    }

    punch_out_time_dict = parse_time_to_dict(punch_out)
    punch_out_datetime = {
        "year": str(entry_date_dict["year"]),
        "month": str(entry_date_dict["month"]),
        "day": str(entry_date_dict["day"]),
        "hour": str(punch_out_time_dict["hour"]),
        "minute": str(punch_out_time_dict["minute"]),
        "second": str(punch_out_time_dict["second"]),
        "timeZoneUri": null
    }

    return {
        "timePunches": [
            {
                "timePunch": {
                    "target": {"parameterCorrelationId": null, "uri": null, "slug": null},
                    "user": {"uri": user_uri, "loginName": null, "employeeId": null, "parameterCorrelationId": null},
                    "punchTime": punch_in_datetime,
                    "actionUri": "urn:replicon:time-punch-action:in",
                    "punchInAttributes": {
                        "activity": null,
                        "project": {"uri": project_uri, "name": null, "code": null, "parameterCorrelationId": null},
                        "client": {"uri": client_uri, "name": null, "code": null, "parameterCorrelationId": null},
                        "task": {"uri": task_uri, "name": null, "parent": null, "parameterCorrelationId": null},
                        "billingRate": null,
                        "isBillable": null
                    },
                    "punchStartBreakAttributes": null,
                    "extensionFieldValues": oef_values or [],
                    "rawTimePunchUri": null
                }
            },
            {
                "timePunch": {
                    "target": {"parameterCorrelationId": null, "uri": null, "slug": null},
                    "user": {"uri": user_uri, "loginName": null, "employeeId": null, "parameterCorrelationId": null},
                    "punchTime": punch_out_datetime,
                    "actionUri": "urn:replicon:time-punch-action:out",
                    "punchStartBreakAttributes": null,
                    "extensionFieldValues": oef_values or [],
                    "rawTimePunchUri": null
                }
            }
        ],
        "bulkPutTimePunchBehaviour": {
            "bulkPutTimePunchBehaviourErrorHandlingOptionUri": "urn:replicon:bulk-put-time-punch-behaviour-error-handling-option:keep-partial-modifications"
        },
        "unitOfWorkId": str(uuid4())
    }


def get_user_by_employee_id_payload(dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    user_id = get_field_value(payload, 'userid')

    return {
        "users": [
            {
                "employeeId": user_id,
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def get_project_details_payload(dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    project_code = get_field_value(payload, 'projectcode', 'project_code')

    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:code",
            "urn:replicon:project-list-column:status",
            "urn:replicon:project-list-column:client"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:project-list-filter:code"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {"text": project_code}
            }
        }
    }


def get_task_list_payload(dag_run):
    project_uri = rail.result("get_project_details")["project_uri"]

    return {
        "page": 1,
        "pagesize": 10000000,
        "columnUris": [
            "urn:replicon:task-list-column:task",
            "urn:replicon:task-list-column:code",
            "urn:replicon:task-list-column:parent",
            "urn:replicon:task-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:task-list-filter:project"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {"uri": project_uri}
            }
        }
    }


def page_handler(request, response):
    if len((response or {}).get('rows') or []) >= request['pagesize']:
        request['page'] += 1
        return request
    return None


def get_task_team_payload(dag_run):
    task_uri = rail.result("get_task_list")["task_uri"]

    return {
        "taskUris": [task_uri],
        "asOfDate": None
    }


def get_client_details_payload(dag_run):
    client_uri = rail.result("get_project_details")["client_uri"]

    return {
        "clientUri": client_uri
    }


def get_existing_time_entries_payload(dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    user_uri = rail.result("get_user_report")["user_uri"]
    entry_date_str = get_field_value(payload, 'entrydate', 'entry_date')
    entry_date = datetime.strptime(entry_date_str, '%Y-%m-%d')

    return {
        "page": "1",
        "pagesize": "111111",
        "columnUris": [
            "urn:replicon:time-entry-revision-group-list-column:entry-date",
            "urn:replicon:time-entry-revision-group-list-column:time-entry-revision-group",
            "urn:replicon:time-entry-revision-group-list-column:hours",
            "urn:replicon:time-entry-revision-group-list-column:in-time",
            "urn:replicon:time-entry-revision-group-list-column:out-time"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-entry-revision-group-list-filter:user"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {"uri": user_uri}
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-entry-revision-group-list-filter:date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": {"year": entry_date.year, "month": entry_date.month, "day": entry_date.day},
                            "endDate": {"year": entry_date.year, "month": entry_date.month, "day": entry_date.day}
                        }
                    }
                }
            }
        }
    }


def _resolve_assignee_tag_uri():
    try:
        uri = rail.result("get_project_level_assignee_tags", "assignee_tag_uri")
        if uri:
            return uri
    except Exception:
        pass
    try:
        uri = rail.result("check_assignee_in_global", "assignee_tag_uri")
        if uri:
            return uri
    except Exception:
        pass
    return None


def build_extension_field_values():
    extension_field_values = []

    try:
        assignee_name_oef_uri = rail.result("get_oef_definition_types", "assignee_name_oef_uri")
        assignee_tag_uri = _resolve_assignee_tag_uri()
        if assignee_name_oef_uri and assignee_tag_uri:
            extension_field_values.append({
                "definition": {"uri": assignee_name_oef_uri, "name": "Assignee Name"},
                "tag": {"uri": assignee_tag_uri}
            })
    except Exception:
        pass

    try:
        dropdown_results = rail.result("get_dropdown_oef_values")
        if dropdown_results:
            for item in dropdown_results:
                if item.get('dropdown_found') and item.get('dropdown_uri'):
                    extension_field_values.append({
                        "definition": {"uri": item['oef_uri'], "name": item['oef_name']},
                        "tag": {"uri": item['dropdown_uri']}
                    })
    except Exception:
        pass

    try:
        text_oefs = rail.result("get_oef_definition_types", "text_oefs")
        if text_oefs:
            for oef in text_oefs:
                if oef.get('oef_type') == 'number':
                    extension_field_values.append({
                        "definition": {"uri": oef['oef_uri'], "name": oef['oef_name']},
                        "numericValue": float(oef['oef_value'])
                    })
                else:
                    extension_field_values.append({
                        "definition": {"uri": oef['oef_uri'], "name": oef['oef_name']},
                        "textValue": oef['oef_value']
                    })
    except Exception:
        pass

    return extension_field_values


def build_time_entry_or_punch_payload(dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    template_type = rail.result("get_or_create_timesheet")["template_type"]
    user_uri = rail.result("get_user_report")["user_uri"]
    entry_date = get_field_value(payload, 'entrydate')

    project_uri = rail.result("get_project_details")["project_uri"]
    client_uri = rail.result("get_project_details")["client_uri"]
    task_uri = rail.result("get_task_list")["task_uri"]

    oef_values = build_extension_field_values()
    comments = get_field_value(payload, 'comments')

    if template_type == 'Punch':
        punch_in = get_field_value(payload, 'punchin')
        punch_out = get_field_value(payload, 'punchout')
        return build_bulk_put_time_punch_payload(
            user_uri, entry_date, punch_in, punch_out, project_uri, client_uri, task_uri, oef_values
        )

    if template_type == 'TSD':
        hours = get_field_value(payload, 'hours')
        return build_put_time_entry_payload_tsd(
            user_uri, project_uri, task_uri, client_uri, entry_date, hours, comments, oef_values
        )
    elif template_type == 'In/Out':
        in_time = get_field_value(payload, 'intime')
        out_time = get_field_value(payload, 'outtime')
        return build_put_time_entry_payload_inout(
            user_uri, project_uri, task_uri, client_uri, entry_date, in_time, out_time, comments, oef_values
        )
    else:
        raise ValueError(f"Unknown template type: {template_type}")


def get_project_level_assignee_tags_payload(dag_run):
    project_uri = rail.result("get_project_details")["project_uri"]
    assignee_name_oef_uri = rail.result("get_oef_definition_types", "assignee_name_oef_uri")

    return {
        "page": 1,
        "pageSize": 100,
        "textSearch": {
            "queryText": "",
            "searchInDisplayText": True,
            "searchInName": False,
            "searchInCode": False,
        },
        "project": {
            "uri": project_uri
        },
        "objectExtensionFieldDefinition": {
            "uri": assignee_name_oef_uri
        }
    }


def get_global_assignee_tag_payload(dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    assignee_id = get_field_value(payload, 'assigneeid')
    assignee_name_oef_uri = rail.result("get_oef_definition_types", "assignee_name_oef_uri")

    return {
        "page": "1",
        "pagesize": "100000",
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
                        "uri": assignee_name_oef_uri,
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
                    "value": {
                        "text": assignee_id,
                    }
                }
            }
        }
    }


def apply_assignee_to_project_payload():
    return {
        "project": {
            "uri": rail.result("get_project_details")["project_uri"],
            "name": None,
            "code": None,
            "parameterCorrelationId": None
        },
        "objectExtensionFieldTags": {
            "tagsToAdd": [
                {
                    "target": {
                        "uri": rail.result("check_assignee_in_global", "assignee_tag_uri"),
                        "slug": None,
                        "tagName": None
                    },
                    "isEnabled": "1",
                    "dateRange": {
                        "startDate": None,
                        "endDate": None,
                        "relativeDateRangeUri": None,
                        "relativeDateRangeAsOfDate": None
                    }
                }
            ],
            "tagsToRemove": []
        }
    }


def get_oef_definition_types_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition",
            "urn:replicon:object-extension-tag-definition-list-column:object-extension-definition-type",
            "urn:replicon:object-extension-tag-definition-list-column:has-bindings",
            "urn:replicon:object-extension-tag-definition-list-column:name"
        ],
        "sort": [],
        "filterExpression": None
    }
