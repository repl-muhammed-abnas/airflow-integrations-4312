# pylint: disable=too-many-statements
from uuid import uuid4
from rail import find_first_by_attr_and_get_attr, result

def get_conf_payload(item):
    users_report_data = result("get_all_task_custom_fields")
    return {
        "campaign_key": item["campaign_key"],
        "agency_name": item["agency_name"],
        "advertiser": item["advertiser"],
        "brand": item["brand"],
        "product": item["product"],
        "campaign": item["campaign"],
        "offer_key": item["offer_key"],
        "offer_description": item["offer_description"],
        "campaign_status": item["campaign_status"],
        "campaign_lastupdated": item["campaign_lastupdated"],
        "project_name": item["project_name"],
        "client_group": item["client_group"],
        "mergedprojectname": f'{item["advertiser"]} - {item["offer_description"]}',
        "customfield_agengynameuri": find_first_by_attr_and_get_attr(users_report_data, "displayText", "Agency Name", "uri", ""),
        "customfield_branduri": find_first_by_attr_and_get_attr(users_report_data, "displayText", "Brand Name", "uri", ""),
        "customfield_campaign_lastupdated_uri": find_first_by_attr_and_get_attr(users_report_data, "displayText", "campaign_lastupdated", "uri", ""),
        "customfield_projectname_uri": find_first_by_attr_and_get_attr(users_report_data, "displayText", "project_name", "uri", ""),
        "customfield_product_uri": find_first_by_attr_and_get_attr(users_report_data, "displayText", "product", "uri", "")
    }

def get_service_center(client_group):
    return list(map(lambda x: {"serviceCenter": {"name": x['value']}}, client_group)) if client_group else []


def create_project_data_payload(dag_run):
    return {
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf['mergedprojectname']
            },
            "codeToApply": {
                "value": dag_run.conf['offer_key']
            },
            "descriptionToApply": {
                "value": dag_run.conf['offer_description']
            },
            "isTimeEntryAllowed": "false",
            "timeAndMaterials": {
                "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable"
            },
            "resourceAssignmentModifications": {
                "resourcesToAdd": get_service_center(dag_run.conf['client_group'])
            }
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def update_project_data_payload(dag_run):
    return {
        "target": {
            "uri": result('get_project_details')['uri']
        },
        "modifications": {
            "codeToApply": {
                "value": dag_run.conf['offer_key']
            },
            "descriptionToApply": {
                "value": dag_run.conf['offer_description']
            },
            "isTimeEntryAllowed": "false",
            "resourceAssignmentModifications": {
                "resourcesToAdd": get_service_center(dag_run.conf['client_group'])
            }
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_update_billing_rate_resource_uri(item):
    enabled_service_center = result('get_enabled_service_centers')
    return find_first_by_attr_and_get_attr(enabled_service_center, "displayText", item['value'], 'uri')

def get_resource_allocations(client_group):
    return list(map(lambda x: {"resource": {"serviceCenter": {"name": x['value']}}}, client_group)) if client_group else []

def create_update_task(dag_run):
    return {
        "target": None,
        "project": {
            "uri": result('create_project_data')['uri'],
            "name": None,
            "code": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "name": dag_run.conf['campaign'],
            "codeToApply": {
                "value": dag_run.conf['campaign_key']
            },
            "descriptionToApply": None,
            "isClosed": "false",
            "timeEntryStartDateToApply": None,
            "timeEntryEndDateToApply": None,
            "timeAndExpenseEntryTypeToApply": {
                "value": "urn:replicon:time-and-expense-entry-type:billable"
            },
            "isTimeEntryAllowed": "true",
            "costTypeToApply": None,
            "estimatedHoursToApply": None,
            "estimatedCostToApply": None,
            "resourceAssignmentModifications": None,
            "resourceTaskAssignmentModifications": {
                "resourceAllocationsToAdd": get_resource_allocations(dag_run.conf['client_group'])
            },
            "customFieldsToApply": [
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_projectname_uri']
                    },
                    "text": dag_run.conf['project_name']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_product_uri']
                    },
                    "text": dag_run.conf['product']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_campaign_lastupdated_uri']
                    },
                    "text": dag_run.conf['campaign_lastupdated']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_agengynameuri']
                    },
                    "text": dag_run.conf['agency_name']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_branduri']
                    },
                    "text": dag_run.conf['brand']
                }
            ],
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": []
        },
        "unitOfWorkId": str(uuid4())
    }

def get_create_task_payload(dag_run):
    return {
        "target": None,
        "project": {
            "uri": result('get_project_details')['uri'],
            "name": None,
            "code": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "name": dag_run.conf['campaign'],
            "codeToApply": {
                "value": dag_run.conf['campaign_key']
            },
            "descriptionToApply": None,
            "isClosed": "false",
            "timeAndExpenseEntryTypeToApply": {
                "value": "urn:replicon:time-and-expense-entry-type:billable"
            },
            "isTimeEntryAllowed": "true",
            "costTypeToApply": None,
            "estimatedHoursToApply": None,
            "estimatedCostToApply": None,
            "resourceTaskAssignmentModifications": {
                "resourceAllocationsToAdd": get_resource_allocations(dag_run.conf['client_group'])
            },
            "customFieldsToApply": [
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_projectname_uri']
                    },
                    "text": dag_run.conf['project_name']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_product_uri']
                    },
                    "text": dag_run.conf['product']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_campaign_lastupdated_uri']
                    },
                    "text": dag_run.conf['campaign_lastupdated']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_agengynameuri']
                    },
                    "text": dag_run.conf['agency_name']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_branduri']
                    },
                    "text": dag_run.conf['brand']
                }
            ],
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": []
        },
        "unitOfWorkId": str(uuid4())
    }

def get_update_task_payload(dag_run):
    return {
        "target": {
            "uri": find_first_by_attr_and_get_attr(result('get_all_project_task'), "task.code", dag_run.conf['campaign_key'], "task.uri"),
            "name": None,
            "parent": None,
            "parameterCorrelationId": None
        },
        "project": {
            "uri": result('get_project_details')['uri'],
            "name": None,
            "code": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "name": dag_run.conf['campaign'],
            "codeToApply": {
                "value": dag_run.conf['campaign_key']
            },
            "descriptionToApply": None,
            "isClosed": "true" if dag_run.conf['campaign_status'] and dag_run.conf['campaign_status'].lower() == "closed" else "false",
            "timeAndExpenseEntryTypeToApply": {
                "value": "urn:replicon:time-and-expense-entry-type:billable"
            },
            "isTimeEntryAllowed": "true",
            "costTypeToApply": None,
            "estimatedHoursToApply": None,
            "estimatedCostToApply": None,
            "resourceTaskAssignmentModifications": {
                "resourceAllocationsToAdd": get_resource_allocations(dag_run.conf['client_group'])
            },
            "customFieldsToApply": [
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_projectname_uri']
                    },
                    "text": dag_run.conf['project_name']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_product_uri']
                    },
                    "text": dag_run.conf['product']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_campaign_lastupdated_uri']
                    },
                    "text": dag_run.conf['campaign_lastupdated']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_agengynameuri']
                    },
                    "text": dag_run.conf['agency_name']
                },
                {
                    "customField": {
                    "uri": dag_run.conf['customfield_branduri']
                    },
                    "text": dag_run.conf['brand']
                }
            ],
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": []
        },
        "unitOfWorkId": str(uuid4())
    }

def create_client_request(dag_run):
    return {
        "target": None,
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf["clientname"]
            }
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }
