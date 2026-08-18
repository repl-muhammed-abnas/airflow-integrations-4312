from datetime import datetime
import json
import uuid
import rail

null = None

def effective_dateformat_payload(effective_date):
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }

def get_custom_field_values(uri, value):
    return {
        "customField": {
            "uri": uri
        },
        "text": value
    }

def get_existing_client_data():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:client-list-column:code",
            "urn:replicon:client-list-column:client"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": json.loads(rail.result('get_details_of_company_from_hubspot'))['id']
                }
            }
        }
    }

def get_project_data_for_client_data():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:project-list-filter:client"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "uri": rail.result('get_existing_client_data')[0]['clienturi']
                }
            }
        }
    }

def get_existing_owner_data():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": json.loads(rail.result('get_company_owner_details_from_hubspot'))['email']
                }
            }
        }
    }

def get_solution_consultant_data():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": json.loads(rail.result('get_solutionconsultant_details_from_hubspot'))['email']
                }
            }
        }
    }

def get_client_custom_field_payload(client_data):
    custom_field_payload = []
    custom_field_dict = dict(map(dict.popitem, list(map(lambda x:{x['displayText']: x['uri'] }, rail.result('get_client_custom_fields')))))
    if client_data['properties']['industry']:
        custom_field_payload.append(get_custom_field_values(custom_field_dict['Industry'], client_data['properties']['industry']))
    if client_data['properties']['division']:
        custom_field_payload.append(get_custom_field_values(custom_field_dict['Division (AUS, US)'], client_data['properties']['division']))
    if client_data['properties']['division']:
        custom_field_payload.append(get_custom_field_values(custom_field_dict['Finance Division'], client_data['properties']['division']))
    return custom_field_payload

def get_client_address(client_data):
    value = ''
    if client_data['properties']['address']:
        value = value + client_data['properties']['address']
    if client_data['properties']['address2']:
        value = value + client_data['properties']['address2']

    if value:
        return {
            "value":value
        }
    return null

def get_client_contact_name():
    if ('associations' in json.loads(rail.result('get_details_of_company_from_hubspot'))) and ('contacts' in json.loads(
        rail.result('get_details_of_company_from_hubspot'))['associations']):
        return {
            "value":json.loads(rail.result('get_primary_contact_name_from_hubspot'))['properties']['firstname'] + ' ' + json.loads(
                rail.result('get_primary_contact_name_from_hubspot'))['properties']['lastname']
        } if json.loads(rail.result('get_primary_contact_name_from_hubspot'))['properties']['firstname'] and json.loads(
            rail.result('get_primary_contact_name_from_hubspot'))['properties']['lastname'] else null
    return null

def get_client_manager_to_apply():
    if json.loads(rail.result('get_details_of_company_from_hubspot'))['properties']['hubspot_owner_id'] and \
        rail.result('search_owner_present_in_replicon'):
        return {
            "user": {
                "uri": rail.result('search_owner_present_in_replicon')['uri']
            }
        }
    return null

def get_address_data(client_data):
    return {
        "address": get_client_address(client_data),
        "city": {
            "value": client_data['properties']['city']
        } if client_data['properties']['city'] else null,
        "stateProvince": {
            "value": client_data['properties']['state']
        } if client_data['properties']['state'] else null,
        "country": {
            "value": {
                "uri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_country_uri'), 'displayText', client_data['properties']['country'], 'uri')
            }
        } if client_data['properties']['country'] else null,
        "zipPostalCode": {
            "value": client_data['properties']['zip']
        } if client_data['properties']['zip'] else null,
        "phoneNumber": {
            "value": client_data['properties']['phone']
        } if client_data['properties']['phone'] else null,
        "website": {
            "value": client_data['properties']['website']
        } if client_data['properties']['website'] else null
    }

def add_client_payload():
    client_data = json.loads(rail.result('get_details_of_company_from_hubspot'))
    return {
        "target": {
            "code": client_data['id']
        } if rail.result('get_existing_client_data_based_on_code') else null,
        "modifications": {
            "nameToApply": {
                "value": client_data['properties']['name']
            },
            "codeToApply": {
                "value": client_data['id']
            },
            "descriptionToApply": {
                "value": client_data['properties']['description']
            } if client_data['properties']['description'] else null,
            "clientContactToApply": get_client_contact_name(),
            "clientAddressToApply": get_address_data(client_data),
            "billingAddressToApply": get_address_data(client_data),
            "customFieldsToApply": get_client_custom_field_payload(client_data)
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_custom_field_payload(project_data, instance):
    custom_field_payload = []
    custom_field_dict = dict(map(dict.popitem, list(map(lambda x:{x['displayText']: x['uri'] }, rail.result('get_project_custom_fields')))))
    if (('associations' in project_data) and ('companies' in project_data['associations']) and rail.result(
        'get_existing_client_data_based_on_code')):
        division = json.loads(rail.result('get_details_of_company_from_hubspot'))['properties']['division']
        if division:
            custom_field_payload.append(get_custom_field_values(custom_field_dict['Business Entity'], division))
    if project_data['properties']['arr']:
        custom_field_payload.append(get_custom_field_values(custom_field_dict['ARR (Local Currency)'], project_data['properties']['arr']))
    if (instance == 'trial' and project_data['properties']['non_arr__local_currency_']):
        custom_field_payload.append(get_custom_field_values(
            custom_field_dict['Non ARR (Local Currency)'], project_data['properties']['non_arr__local_currency_']))
    elif (instance == 'prod' and project_data['properties']['non_arr']):
        custom_field_payload.append(get_custom_field_values(
            custom_field_dict['Non ARR (Local Currency)'], project_data['properties']['non_arr']))
    if project_data['properties']['contract___rosterfy_entity']:
        custom_field_payload.append(get_custom_field_values(
            custom_field_dict['Contract - Rosterfy Entity'], project_data['properties']['contract___rosterfy_entity']))
    if project_data['properties']['industry']:
        custom_field_payload.append(get_custom_field_values(custom_field_dict['Industry'], project_data['properties']['industry']))
    if (instance == 'trial' and project_data['properties']['currency_']):
        custom_field_payload.append(get_custom_field_values(custom_field_dict['Currency'], project_data['properties']['currency_']))
    elif (instance == 'prod' and project_data['properties']['deal_currency_code']):
        custom_field_payload.append(get_custom_field_values(custom_field_dict['Currency'], project_data['properties']['deal_currency_code']))
    return custom_field_payload

def get_project_name(project_data):
    pipeline_data = rail.result('get_pipeline_and_dealstage_name')
    if pipeline_data['pipeline'] == 'Sales':
        if pipeline_data['dealstage'] == '3. Solution & Demo':
            value = "Pre-Sales - " + project_data['properties']['dealname']
        elif pipeline_data['dealstage'] == 'Closed won':
            value = project_data['properties']['dealname']
    elif pipeline_data['pipeline'] == 'Services':
        value = "Service - " + project_data['properties']['dealname']
    elif pipeline_data['pipeline'] == 'Customer Success':
        value = "Customer Success - " + project_data['properties']['dealname']
    return {
        "value":value
    }

def get_project_code(project_data):
    pipeline_data = rail.result('get_pipeline_and_dealstage_name')
    if (pipeline_data['pipeline'] == 'Sales') and (pipeline_data['dealstage'] == '3. Solution & Demo'):
        return project_data['id'] + "-sales"
    return project_data['id']

def get_client_assignment_payload(project_data, caller):
     
    if ((project_data.get('associations')) and (project_data['associations'].get('companies'))):
        if rail.result('get_existing_client_data_based_on_code') or ((rail.result(
            'get_pipeline_and_dealstage_name')['pipeline'] == 'Sales') and (rail.result(
                'get_pipeline_and_dealstage_name')['dealstage'] == '3. Solution & Demo')):
            return {
                "clients": [
                    {
                        "client": {
                            "code": project_data['associations']['companies']['results'][0]['id']
                        },
                        "costAllocationPercentage": "100"
                    }
                ]
            }
    return null

def add_project_payload(caller, instance):
    project_data = json.loads(rail.result('get_details_of_deal'))
    startdate = datetime.strptime(project_data['properties']['closedate'].split('T')[0], '%Y-%m-%d')

    return {
        "target": {
            "code": get_project_code(project_data)
        } if (caller == 'update') and rail.result('search_project_with_code')[0]['projectDetails'] and rail.result(
            'search_project_with_code')[0]['projectDetails']['uri'] else null,
        "modifications": {
            "nameToApply": get_project_name(project_data) if project_data['properties']['dealname'] else null,
            "codeToApply": {
                "value": get_project_code(project_data)
            },
            "startDateToApply": {
                "date": effective_dateformat_payload(startdate)
            },
            "billingTypeToApply": {
                "value": "urn:replicon:billing-type:billing-contract"
            },
            "clientAssignmentsSchedulesToApply": get_client_assignment_payload(project_data, caller),
            "statusToApply": {
                "uri": rail.result('get_initiate_project_status_level')
            } if (caller == 'add') else null,
            "totalEstimatedContractValueToApply": {
                "value": {
                    "amount": project_data['properties']['non_arr__local_currency_'],
                    "currency": {
                        "uri": null,
                        "name": null,
                        "symbol": project_data['properties']['currency_']
                    }
                }
            } if (instance == 'trial'and project_data['properties']['non_arr__local_currency_']) else
            {
                "value": {
                    "amount": project_data['properties']['non_arr'],
                    "currency": {
                        "uri": null,
                        "name": null,
                        "symbol": project_data['properties']['deal_currency_code']
                    }
                }
            } if (instance == 'prod' and project_data['properties']['non_arr']) else null,
            "defaultBillingCurrencyToApply": {
                "currency": {
                    "uri": null,
                    "name": null,
                    "symbol": project_data['properties']['currency_']
                }
            } if (instance == 'trial'and project_data['properties']['currency_']) else 
            {
                "currency": {
                    "uri": null,
                    "name": null,
                    "symbol": project_data['properties']['deal_currency_code']
                }
            } if (instance == 'prod' and project_data['properties']['deal_currency_code']) else null,
            "customFieldsToApply": get_custom_field_payload(project_data, instance)
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def create_task(dag_run):
    return {
        "target": None,
        "project": {
            "code": dag_run.conf['projectcode']
        },
        "modifications": {
            "name": dag_run.conf['task_name'],
            "timeAndExpenseEntryTypeToApply": {
                "value": "urn:replicon:time-and-expense-entry-type:billable" if (
                    dag_run.conf['billtype'] == 'billable') else "urn:replicon:time-and-expense-entry-type:non-billable"
            }
        },
        "unitOfWorkId": str(uuid.uuid4())
    }

def search_project_with_code():
    project_data = json.loads(rail.result('get_details_of_deal'))
    return {
        "projects": [
            {
                "code": get_project_code(project_data)
            }
        ]
    }
