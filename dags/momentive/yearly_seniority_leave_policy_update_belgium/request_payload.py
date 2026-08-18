import rail

def request_payload_user():
    null=None
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:start-date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:division"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                "uri": rail.result("get_user_division_uri"),
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
                "filterDefinitionUri": "urn:replicon:user-list-filter:time-off-type"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                "uri": rail.result("get_user_timeoff_type_uri"),
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
            "value": null,
            "filterDefinitionUri": null
        }
    }

def request_payload_put_timeoff_policy(dag_run):
    res = rail.result("get_user_timeoff_policy_set")
    return {
            "timeOffAccount": {
                "userUri": dag_run.conf["useruri"],
                "timeOffTypeUri": dag_run.conf["timeoffuri"]
            },
            "policySetScheduleEntries": res
        }
