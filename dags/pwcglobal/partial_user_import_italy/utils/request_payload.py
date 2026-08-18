import rail
null = None
# pylint: disable=line-too-long


def get_user_by_partyid_and_legal_entity_uri_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:division"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['user_party_id']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:division"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": dag_run.conf['legal_entity_uri']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "bool": "true"
                    }
                }
            }
            }
        }
    }


def applyusermodification3_payload(dag_run, user_uri):
    custom_fields_to_apply = []
    if dag_run.conf['remote_work_contract_status']:
        custom_fields_to_apply.append({
            "customField": {
                "uri": dag_run.conf['remote_work_contract_status_field_uri']
            },
            "text": null,
            "date": null,
            "dropDownOption": {
                "uri": dag_run.conf['remote_work_contract_status_field_dropdown_uris']['Y'] if str(dag_run.conf['remote_work_contract_status']).upper() == 'Y' else (dag_run.conf['remote_work_contract_status_field_dropdown_uris']['N'] if str(dag_run.conf['remote_work_contract_status']).upper() == 'N' else '')
            }
        })
    else:
        custom_fields_to_apply.append({
            "customField": {
                "uri": dag_run.conf['remote_work_contract_status_field_uri']
            }
        })

    if dag_run.conf['remote_work_contract_effective_date']:
        custom_fields_to_apply.append({
            "customField": {
                "uri": dag_run.conf['remote_work_contract_effective_date_field_uri']
            },
            "text": null,
            "date": {
                'year': int(dag_run.conf['remote_work_contract_effective_date'].split('/')[2]),
                'month': int(dag_run.conf['remote_work_contract_effective_date'].split('/')[1]),
                'day': int(dag_run.conf['remote_work_contract_effective_date'].split('/')[0]),
            }
        })
    else:
        custom_fields_to_apply.append({
            "customField": {
                "uri": dag_run.conf['remote_work_contract_effective_date_field_uri']
            }
        })

    return {
        "user": {
            "uri": user_uri
        },
        "modifications": {
            "customFieldValuesToApply": custom_fields_to_apply
        },
        "userModificationOptionUri": null
    }
