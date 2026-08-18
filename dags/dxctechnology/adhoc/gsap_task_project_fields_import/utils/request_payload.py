
null = None

def get_task_name(dag_run):
    return dag_run.conf['task_name'] + (' - ' + dag_run.conf['task_code'] if dag_run.conf['task_code'] else '')


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
