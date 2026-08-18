import math
import rail

null = None


def get_country_hierarchy_payload():
    return {
        "page": "1",
        "pagesize": "20000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:name",
            "urn:replicon:location-list-column:code",
            "urn:replicon:location-list-column:description",
            "urn:replicon:location-list-column:effectively-enabled",
            "urn:replicon:location-list-column:full-path"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": "true",
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:location-list-filter:text"
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
                        "text": "PwC Network",
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
            },
            "value": null,
            "filterDefinitionUri": null
        },
        "hierarchyListDataOptionUris": [
            "urn:replicon:hierarchy-list-data-option:include-ancestor-rows",
            "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
        ]
    }


def get_report_generate_batch_payload():
    return {
        "reportParameters": [
            {
                "filterValues": rail.result('create_report_list'),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                "reportUri": rail.result('get_report_details')['uri']
            }
        ]
    }

def round_hours(hours):
    last_digit = hours[-1]
    if last_digit == '5':
        return round(math.ceil(float(hours) * 10.0) / 10.0, 1)
    return round(float(hours), 1)

def get_csv_rows(item):
    activity_list = item['Activity_Code'].split('/')
    [activity_list.append(None) for i in range(
        0, 4-len(activity_list)) if len(activity_list) < 4]
    row_data = [
        item['Local_Staff_ID'],
        activity_list[0],
        activity_list[1],
        activity_list[2],
        activity_list[3],
        null,
        item['Entry_Date'],
        round_hours(item['Hours']),
        null
    ]
    return row_data
