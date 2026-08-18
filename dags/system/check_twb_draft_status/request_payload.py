from datetime import datetime, timedelta
import rail
null = None


def output_payload():
    data = rail.result('get_timeexport_details')
    return list(map(lambda x: {
        "time_export_name": x['time_export_name'],
        "creation_time": x['creation_time'],
        "creator": x['creator'],
        "twb_uri": x['twb_uri']
    }, data))


def get_timeexportdata_payload():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:time-data-export-list-column:time-data-export",
            "urn:replicon:time-data-export-list-column:status",
            "urn:replicon:time-data-export-list-column:creation-date",
            "urn:replicon:time-data-export-list-column:creator"
        ],
        "sort": [
            {
                "columnUri": "urn:replicon:time-data-export-list-column:status",
                "isAscending": "true"
            }
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:time-data-export-list-filter:creation-date-range"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
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
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": {
                        "startDate": {
                            "year": (datetime.now()-timedelta(days=30)).strftime("%Y"),
                            "month": (datetime.now()-timedelta(days=30)).strftime("%m"),
                            "day": (datetime.now()-timedelta(days=30)).strftime("%d")
                        },
                        "endDate": {
                            "year": (datetime.now()-timedelta(days=1)).strftime("%Y"),
                            "month": (datetime.now()-timedelta(days=1)).strftime("%m"),
                            "day": (datetime.now()-timedelta(days=1)).strftime("%d")
                        },
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
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
