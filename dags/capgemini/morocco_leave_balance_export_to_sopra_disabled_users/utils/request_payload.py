from datetime import datetime, timedelta
import pendulum
import rail

null = None

def get_leave_request_report_filters():
    datefilter = rail.find_first_by_attr_and_get_attr(rail.result('get_leave_balance_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "AsOfDateFilter", 'uri')
    user_filter = rail.find_first_by_attr_and_get_attr(rail.result('get_leave_balance_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "UserFilter", 'uri')
    return [
    	{
    		"reportFilterUri": datefilter,
    		"value": "DateRange"
    	},
    	{
    		"reportFilterUri": datefilter,
    		"value": rail.result("logging_details")["export_end_date"]
    	},
    	{
    		"reportFilterUri": datefilter,
    		"value": rail.result("logging_details")["export_start_date"]
    	}
    ] + [{
    		"reportFilterUri": user_filter,
    		"value": user_data["uri"].split(':')[-1]
    	} for user_data in rail.result("get_users_disabled_and_enddate_in_daterange")]

def get_report_parameters():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_leave_balance_report_details')["uri"],
                "filterValues": get_leave_request_report_filters(),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_location_uri_payload(location):
    return {
        "page": "1",
        "pageSize": "100",
        "textSearch": {
                "queryText": location,
                "searchInDisplayText": "false",
                "searchInName": "true",
                "searchInDescription": "false",
                "searchInCode": "false"
        }
    }

def get_users_disabled_and_enddate_in_daterange_payload(dag_run, schedules, time_zone):
    current_date = pendulum.now(time_zone)
    start_date = (datetime.strptime(dag_run.conf["user_end_date_range_start"], "%m/%d/%Y") if dag_run.conf and dag_run.conf["user_end_date_range_start"] else
    	datetime.strptime(schedules[schedules.index(current_date.strftime("%d/%m/%Y")) - 1], "%d/%m/%Y"))
    end_date = (datetime.strptime(dag_run.conf["user_end_date_range_end"], "%m/%d/%Y") if dag_run.conf and dag_run.conf["user_end_date_range_end"] else
    	current_date - timedelta(days=1))
    return {
		"page": "1",
		"pagesize": "10000",
		"columnUris": [
			"urn:replicon:user-list-column:user"
		],
		"sort": [],
		"filterExpression": {
			"leftExpression": {
				"leftExpression": {
					"leftExpression": null,
					"operatorUri": null,
					"rightExpression": null,
					"value": null,
					"filterDefinitionUri": "urn:replicon:user-list-filter:end-date-range"
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
								"year": start_date.year,
								"month": start_date.month,
								"day": start_date.day
							},
							"endDate": {
								"year": end_date.year,
								"month": end_date.month,
								"day": end_date.day
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
			},
			"operatorUri": "urn:replicon:filter-operator:and",
			"rightExpression": {
				"leftExpression": {
					"leftExpression": {
						"leftExpression": null,
						"operatorUri": null,
						"rightExpression": null,
						"value": null,
						"filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
					},
					"operatorUri": "urn:replicon:filter-operator:equal",
					"rightExpression": {
						"leftExpression": null,
						"operatorUri": null,
						"rightExpression": null,
						"value": {
							"uri": null,
							"uris": [],
							"bool": "false",
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
						"filterDefinitionUri": "urn:replicon:user-list-filter:location"
					},
					"operatorUri": "urn:replicon:filter-operator:in-hierarchy",
					"rightExpression": {
						"leftExpression": null,
						"operatorUri": null,
						"rightExpression": null,
						"value": {
							"uri": rail.result("get_allowed_location_uris"),
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
			},
			"value": null,
			"filterDefinitionUri": null
		}
	}
