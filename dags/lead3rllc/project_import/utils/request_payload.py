import rail
import uuid
from functools import lru_cache

null = None


def add_missing_values_in_replicon_payload(dag_run_ecid):
    return {
        'parentjobid': dag_run_ecid,
        'netsuite_project_type_oef_uri': rail.result('get_required_oef_uri_for_projects')['netsuite_project_type_uri'],
        'missing_field_value_import_logs': rail.result('create_add_client_and_missing_field_values_log')
    }


def payload_to_get_all_replicon_clients():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:active"
        ],
        "sort": [],
        "filterExpression": null
    }


def trigger_add_client_payload(item, dag_run):
    return {
        "parentjobid": dag_run.conf["parentjobid"],
        'company_name': item['company_name'],
        'missing_field_value_import_logs': dag_run.conf['missing_field_value_import_logs']
    }


def get_create_client_payload(dag_run):
    return {
        "target": null,
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf['company_name']
            },
            "statusToApply": True
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_activate_client_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['client_uri']
        },
        "modifications": {
            "statusToApply": True
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def trigger_add_or_enable_customfield_dropdown_options_payload(dag_run):
    return {
        "parentjobid": dag_run.conf["parentjobid"],
        'oef_uri': dag_run.conf["netsuite_project_type_oef_uri"],
        'missing_field_value_import_logs': dag_run.conf['missing_field_value_import_logs']
    }


@lru_cache(maxsize=8)
def get_dropdown_options_to_enable():
    return rail.load_all_records(rail.result('query_get_netsuiteprojecttype_dropdown_values_present_and_disabled_in_replicon'))


@lru_cache(maxsize=8)
def get_new_dropdown_options_to_add():
    return rail.load_all_records(rail.result('query_get_netsuiteprojecttype_dropdown_values_not_in_replicon'))


def get_oef_dropdown_option_uris():
    existing_dropdowns_list = rail.result(
        'get_all_dropdown_values_for_oef')
    dropdown_options_to_enable = get_dropdown_options_to_enable()
    list_dropdown_options_to_enable = [
        item['tag_name'] for item in dropdown_options_to_enable] if dropdown_options_to_enable else []
    final_dropdown_list = list(map(lambda x: {
        "target": {
            "uri": x['tag_uri'],
            "slug": null,
            "tagName": null
        },
        "name": x['tag_name'],
        "code": null,
        "description": null,
        "isEnabled": True if x['tag_name'] in list_dropdown_options_to_enable else x['enabled']
    }, existing_dropdowns_list)) if existing_dropdowns_list else []

    new_dropdown_options_to_add = get_new_dropdown_options_to_add()

    final_dropdown_list.extend(map(lambda x: {
        "target": {
            "uri": null,
            "slug": x['netsuite_project_type'],
            "tagName": null
        },
        "name": x['netsuite_project_type'],
        "code": null,
        "description": null,
        "isEnabled": True
    }, new_dropdown_options_to_add)) if new_dropdown_options_to_add else final_dropdown_list

    return final_dropdown_list


def get_dropdown_success_details():
    dropdown_options_to_enable = get_dropdown_options_to_enable()
    new_dropdown_options_to_add = get_new_dropdown_options_to_add()

    options_enabled = [item['tag_name']
                       for item in dropdown_options_to_enable] if dropdown_options_to_enable else []
    options_added = [item['netsuite_project_type']
                     for item in new_dropdown_options_to_add] if new_dropdown_options_to_add else []

    return "Dropdown options added : " + rail.smartjoin_by_delim(
        options_added, ',') + " ; Dropdown options enabled : " + rail.smartjoin_by_delim(options_enabled, ',')


def get_all_department_groups_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:effectively-enabled"
        ],
        "sort": [],
        "filterExpression": null
    }


def add_department_group_payload(item, dag_run):
    return {
        "parentjobid": dag_run.conf["parentjobid"],
        'parent_department_group_uri': rail.result('get_parent_department_group_uri'),
        'department_group_name': item['deal_type'],
        'missing_field_value_import_logs': dag_run.conf['missing_field_value_import_logs']
    }


def create_department_group_payload(dag_run):
    return {
        "departmentGroup": {
            "uri": null,
            "parent": {
                "uri": dag_run.conf['parent_department_group_uri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "name": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": dag_run.conf['department_group_name'],
            "codeToApply": null,
            "descriptionToApply": null,
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def is_projectmanager_permission(response):
    if response:
        if rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:project-management', 'permissionSet'):
            return True
    return False


def add_project_payload(project_uri, dag_run):
    modifications_to_apply = {
        "statusToApply": {
            "uri": null,
            "name": "Tentative"
        },
        "keyValuesToApply": [
            {
                "keyUri": "urn:replicon:project-key-value-key:project-management-type",
                "value": {
                    "uri": "urn:replicon:project-management-type:managed"
                }
            }
        ],
    }

    if dag_run.conf['company_name']:
        modifications_to_apply.update({
            "clientAssignmentsSchedulesToApply": {
                "clients": [
                    {
                        "client": {
                            "name": dag_run.conf['company_name'],
                        },
                        "costAllocationPercentage": "100"
                    }
                ]
            },
        })

    modifications_to_apply.update({
        "startDateToApply":  {
            "date": rail.parse_date(dag_run.conf['contract_start_date'], "%m/%d/%Y") if dag_run.conf['contract_start_date'] else null
        }
    })

    modifications_to_apply.update({
        "endDateToApply":  {
            "date": rail.parse_date(dag_run.conf['contract_end_date'], "%m/%d/%Y") if dag_run.conf['contract_end_date'] else null
        }
    })

    modifications_to_apply.update({
        "projectLeaderToApply": {
            "user": {
                "uri": dag_run.conf['user_uri']
            } if dag_run.conf['engagement_lead'] else null
        }
    })

    if dag_run.conf['amount_in_company_currency']:
        modifications_to_apply.update({
            "totalEstimatedContractValueToApply": {
                "value": {
                    "amount": dag_run.conf['amount_in_company_currency'],
                    "currency": {
                        "uri": null,
                        "name": null,
                        "symbol": null
                    }
                }
            }
        })

    if dag_run.conf['netsuite_project_type']:
        modifications_to_apply.update({
            "objectExtensionFieldsToApply": [
                {
                    "definition": {
                        "uri": dag_run.conf['netsuite_project_type_oef_uri']
                    },
                    "tag": {
                        "tagName": {
                            "name": dag_run.conf['netsuite_project_type'],
                            "tagDefinitionUri": null
                        }
                    }
                }
            ]
        })

    if dag_run.conf['deal_type']:
        modifications_to_apply.update({
            "departmentGroupToApply": {
                "departmentGroup": {
                    "uri": dag_run.conf['department_group_uri']
                }
            },
        })

    return {
        "target": {
            "uri": project_uri
        },
        "modifications": modifications_to_apply,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
