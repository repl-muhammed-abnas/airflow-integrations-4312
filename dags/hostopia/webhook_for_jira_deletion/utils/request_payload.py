import rail



def get_project_in_replicon_data():
    project_name = rail.result("get_triggered_data")['projectname']
    return {
        "page": "1",
        "pagesize": "1111",
        "columnUris": [
                "urn:replicon:project-list-column:project"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:project-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": f'{project_name}'
                }
            }
        }
    }
