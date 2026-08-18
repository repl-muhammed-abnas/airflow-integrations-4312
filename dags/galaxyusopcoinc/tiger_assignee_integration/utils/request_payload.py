import os
import rail

null = None


def do_has_file_content():
    with rail.existing_artifact(rail.result('decrypt_file')) as artifact:
        return os.path.getsize(artifact.local_filename) > 0

def get_process_assignee_ids_add_conf(item):
    return{
        **dict(item.items()),
        **{'assigneenameuri': rail.result('get_timeentry_oefs')['assigneenameuri']}
    }


def get_process_assignee_ids_update_conf(item):
    return{
        **dict(item.items()),
        **{'assigneenameuri': rail.result('get_timeentry_oefs')['assigneenameuri']}
    }


def update_name(dag_run):
    return {
        "objectExtensionTagUri": rail.result('create_new_draft'),
        "name": dag_run.conf['assigneeid']
    }


def update_code(status, dag_run):
    return {
        "objectExtensionTagUri": rail.result('create_new_draft') if status == 'add' else dag_run.conf['assigneeuri'],
        "code": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
    }


def get_process_each_replicon_client(item):
    return {
        **dict(item.items()),
        **{'assigneenameuri': rail.result('get_timeentry_oefs')['assigneenameuri'],
           'create_exception_log' : rail.result('create_exception_log'),
           'create_error_log' : rail.result('create_error_log')
           }
    }


def get_all_projects(dag_run):
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:client"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:project-list-filter:client"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": dag_run.conf['clienturi'],
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
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

def apply_assigneeids_modification2(dag_run, item):
    return {
        "project": {
            "uri": dag_run.conf['projecturi'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "objectExtensionFieldTags": {
            "tagsToAdd": item['add'],
            "tagsToRemove": item['remove']
        }
    }

# def apply_assigneeids_modification(dag_run):
#     return {
#         "project": {
#             "uri": dag_run.conf['projecturi'],
#             "name": null,
#             "code": null,
#             "parameterCorrelationId": null
#         },
#         "objectExtensionFieldTags": {
#             "tagsToAdd": rail.load_all_records(dag_run.conf['tagstoadd']),
#             "tagsToRemove": rail.load_all_records(dag_run.conf['tagstoremove'])
#         }
#     }


def get_process_each_project(item, dag_run):
    return{
        **dict(item.items()),
        **{'tagstoadd': rail.result('tags_to_add_payload'),
        'tagstoremove': rail.result('tags_to_remove_payload'),
        'assigneedetails': rail.result('query_assignee_details_for_client')},
        #'successlogs': rail.result("create_success_log"),
        'errorlogs': dag_run.conf["create_error_log"]
    }

def get_assignee_details_data(dag_run):
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
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                "uri": dag_run.conf['assigneenameuri'],
                "uris": [],
                "bool": null,
                "date": null,
                "money": null,
                "number": null,
                "text": null,
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:text"
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
                "text": dag_run.conf['assignee_id'],
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
            },
            "value": null,
            "filterDefinitionUri": null
        }
        }

def process_get_assignee_details_conf(item):
    return{
        "assignee_id": item['assigneeid'],
        "assigneenameuri": rail.result('get_timeentry_oefs')['assigneenameuri']
    }
