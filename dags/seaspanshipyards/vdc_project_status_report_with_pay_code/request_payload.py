import rail
null = None


def get_user_data_request(dag_run):
    return {
        "userUri": "urn:replicon-tenant:" + rail.get_tenant_slug() +
        ':user:' + dag_run.conf['webhook']['data']["requestorid"]
    }


def payroll_batch_request(dag_run):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
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
                            "year": str(dag_run.conf['webhook']['data']["daterange"].split("-")[0][4:]),
                            "month": str(dag_run.conf['webhook']['data']["daterange"].split("-")[0][:2]),
                            "day": str(dag_run.conf['webhook']['data']["daterange"].split("-")[0][2:4])
                        },
                        "endDate": {
                            "year": str(dag_run.conf['webhook']['data']["daterange"].split("-")[1][4:]),
                            "month": str(dag_run.conf['webhook']['data']["daterange"].split("-")[1][:2]),
                            "day": str(dag_run.conf['webhook']['data']["daterange"].split("-")[1][2:4])
                        },
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        },
        "fileFormatScriptUri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_paycode_scripts"),
            "displayText",
            "VDC Project Status Report with Pay Code",
            "uri")
    }
