import rail
import pendulum
from dxctechnology.ftp_time_export_v6.utils import custom_method

null = None


def get_all_past_time_export_payload():
    current_time_in_utc = pendulum.now(tz='utc')
    two_days_lookup_period = current_time_in_utc.subtract(days=7)
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:time-data-export-list-column:time-data-export",
                "urn:replicon:time-data-export-list-column:status",
                "urn:replicon:time-data-export-list-column:creation-date"
        ],
        "sort": [
            {
                "columnUri": "urn:replicon:time-data-export-list-column:creation-date",
                "isAscending": "false"
            }
        ],
        "filterExpression": {
            "leftExpression": {
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
                        "text": "REG-FTP",
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
                                "year": two_days_lookup_period.strftime("%Y"),
                                "month": two_days_lookup_period.strftime("%m"),
                                "day": two_days_lookup_period.strftime("%d")
                            },
                            "endDate": null,
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
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_last_time_export_details_payload():
    return {
        "target": {
            "uri": rail.result("get_all_past_time_export")[0]["cells"][0]["uri"],
            "name": null
        }
    }


def get_specific_time_export_details_payload():
    return {
        "target": {
            "uri": custom_method.get_dag_run_conf()['uri'],
            "name": null
        }
    }


def output_payload():
    data = rail.result('gather_all_unckn_export_details')
    return list(map(lambda x: {
        "identifier": x['Identifier'],
        "creation_time": x['createdatetime']
    }, data))
