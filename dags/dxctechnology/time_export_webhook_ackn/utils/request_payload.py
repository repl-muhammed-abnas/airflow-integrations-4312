import rail
from dxctechnology.time_export_webhook_ackn.utils import custom_method

null = None


def get_all_past_time_export_payload():
    time_export_id = custom_method.get_dag_run_conf(
    )['webhook']['data']['timeExportID'].split("|")[0]
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:time-data-export-list-column:time-data-export",
                "urn:replicon:time-data-export-list-column:status"
        ],
        "sort": [
            {
                "columnUri": "urn:replicon:time-data-export-list-column:creation-date",
                "isAscending": "false"
            }
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:time-data-export-list-filter:text"
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
                    "text": time_export_id,
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
        }
    }


def get_update_oef_value_payload(oef_value):
    oef_value_uri = list(filter(lambda x: x['displayText'] == oef_value, rail.result(
        "get_all_time_export_oef_bindings")))
    return {
        "objectUri": rail.result("get_all_past_time_export")[0]['cells'][0]['uri'],
        "value": {
            "definition": {
                "uri": oef_value_uri[0]['uri'],
                "name": null
            },
            "tag": null,
            "numericValue": null,
            "textValue": "Yes",
            "fileValue": null,
            "jsonValue": null
        }
    }
