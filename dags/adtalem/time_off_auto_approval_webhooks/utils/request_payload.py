import rail

def get_user_payload(uri):
    return {
        "page": "1",
        "pagesize": "1111",
        "columnUris": [
                "urn:replicon:user-list-column:user"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": uri
                }
            }
        }
    }

def check_uri():
    get_user_identity= rail.result("get_actual_user_identity")['uri']
    currently_waiting_on_approver= rail.result("get_currently_waiting_on_approvers")[0]['uri'] if rail.result(
        "get_currently_waiting_on_approvers") else None
    if currently_waiting_on_approver:
        return bool(get_user_identity == currently_waiting_on_approver)
    return False
