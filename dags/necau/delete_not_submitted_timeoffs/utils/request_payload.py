def get_timeoff_payload():
    return {
            "page": "1",
            "pagesize": "100000",
            "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-status",
                "urn:replicon:time-off-list-column:time-off-owner",
                "urn:replicon:time-off-list-column:start-date",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon:time-off-list-column:total-duration",
                "urn:replicon:time-off-list-column:end-date"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-off-list-filter:approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "value": {
                    "uri": "urn:replicon:approval-status:open"
                }
            }
        }
    }
