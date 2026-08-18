null = None

def get_payload_oef_uri():
    return {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition",
                    "urn:replicon:object-extension-tag-definition-list-column:name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:object-extension-tag-definition-list-filter:text"
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
                        "text": "Authorizer",
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null
                    },
                    "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
