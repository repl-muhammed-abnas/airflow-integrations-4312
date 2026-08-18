import pendulum
import rail

null = None

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

def get_cost_center_payload(item):
    return {
    	"page": "1",
    	"pageSize": "100",
    	"textSearch": {
    		"queryText": item,
    		"searchInDisplayText": "false",
    		"searchInName": "true",
    		"searchInDescription": "false",
    		"searchInCode": "false"
    	}
    }

def get_costcenter_hierarchy():
    return {
        "page": "1",
        "pagesize": "10000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:full-path",
            "urn:replicon:cost-center-list-column:cost-center"
        ],
        "sort": [],
        "filterExpression": null
    }

def get_allowed_location_uris_payload(export_locations):
    return {
        "page": "1",
        "pagesize": "10000000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
        ],
        "filterExpression": {
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
                    "text": export_locations,
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
        "hierarchyListDataOptionUris": [
            "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
        ]
    }

def get_create_payrun_download_batch_payload():
    payrunuri = rail.result('get_payrun_batch_result')['payRunUri']
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [payrunuri],
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
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        },
        "fileFormatScriptUri": rail.result("get_morocco_overtime_payroll_script")
    }

def get_create_payroll_batch_payload(time_zone):
    current_time = pendulum.now(time_zone)
    return {
	    "columnUris": [],
	    "filterExpression": {
	    	"leftExpression": {
	    		"leftExpression": {
	    			"leftExpression": {
	    				"leftExpression": {
	    					"filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
	    				},
	    				"operatorUri": "urn:replicon:filter-operator:in",
	    				"rightExpression": {
	    					"value": {
	    						"dateRange": {
	    							"startDate": rail.result("logging_details")["export_start_date_json"],
	    							"endDate": rail.result("logging_details")["export_end_date_json"],
	    							"relativeDateRangeUri": null,
	    							"relativeDateRangeAsOfDate": null
	    						}
	    					}
	    				}
	    			},
	    			"operatorUri": "urn:replicon:filter-operator:and",
	    			"rightExpression": {
	    				"leftExpression": {
	    					"filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run-status"
	    				},
	    				"operatorUri": "urn:replicon:filter-operator:in",
	    				"rightExpression": {
	    					"value": {
	    						"uris": [
	    							"urn:replicon:payable-time-pay-run-status:none"
	    						]
	    					}
	    				}
	    			}
	    		},
	    		"operatorUri": "urn:replicon:filter-operator:and",
	    		"rightExpression": {
	    			"leftExpression": {
	    				"filterDefinitionUri": "urn:replicon:pay-run-filter:payable-time-approval-status"
	    			},
	    			"operatorUri": "urn:replicon:filter-operator:in",
	    			"rightExpression": {
	    				"value": {
	    					"uris": [
	    						"urn:replicon:payable-time-approval-status:approved"
	    					]
	    				}
	    			}
	    		}
	    	},
	    	"operatorUri": "urn:replicon:filter-operator:and",
	    	"rightExpression": {
	    		"leftExpression": {
	    			"leftExpression": {
	    				"leftExpression": {
	    					"filterDefinitionUri": "urn:replicon:pay-run-filter:location"
	    				},
	    				"operatorUri": "urn:replicon:filter-operator:in-hierarchy",
	    				"rightExpression": {
	    					"value": {
	    						"uris": [rail.result("get_allowed_location_uris")]
	    					}
	    				}
	    			},
	    			"operatorUri": "urn:replicon:filter-operator:and",
	    			"rightExpression": {
	    				"leftExpression": {
	    					"filterDefinitionUri": "urn:replicon:pay-run-filter:cost-center"
	    				},
	    				"operatorUri": "urn:replicon:filter-operator:in-hierarchy",
	    				"rightExpression": {
	    					"value": {
	    						"uris": rail.result("get_allowed_costcenter_uris")
	    					}
	    				}
	    			}
	    		},
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:as-of-date-time-utc"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "dateTimeUtc": {
                                "year": current_time.year,
                                "month": current_time.month,
                                "day": current_time.day,
                                "hour": current_time.hour,
                                "minute": current_time.minute,
                                "second": current_time.second,
                                "millisecond": 0
                            }
                        }
                    }
                }
	    	}
	    }
    }

def get_payload():
    return {
        "target": {
            "uri": rail.result('get_payrun_batch_result')['payRunUri']
        }
    }
