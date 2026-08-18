import pendulum
from dateutil.relativedelta import relativedelta
from airflow.exceptions import AirflowFailException
import rail
null = None


def retrieve_export_uri(response):
    if response['error']:
        raise AirflowFailException(response)
    return response['timeDataExportUri']


def create_export_status_batch_payload(status):
    return {
        "target": {
            "uri": rail.result("get_export_uri_failed"),
            "name": null
        },
        "statusUri": f"urn:replicon:time-data-export-status:{status}"
    }

def get_c1_divisions_payload():
    return {
	    "page": 1,
	    "pagesize": 1000,
	    "columnUris": [
	    	"urn:replicon:division-list-column:division",
	    	"urn:replicon:division-list-column:code"
	    ],
	    "sort": [],
	    "filterExpression": {
	    	"leftExpression": {
	    		"leftExpression": null,
	    		"operatorUri": null,
	    		"rightExpression": null,
	    		"value": null,
	    		"filterDefinitionUri": "urn:replicon:division-list-filter:text"
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
	    			"text": "C1",
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
	    }
    }

def get_gsap_compass_divisions_payload():
    return {
		"page": 1,
		"pagesize": 1000,
		"columnUris": [
			"urn:replicon:division-list-column:division",
			"urn:replicon:division-list-column:code"
		],
		"sort": [],
		"filterExpression": {
			"leftExpression": {
				"leftExpression": {
					"leftExpression": null,
					"operatorUri": null,
					"rightExpression": null,
					"value": null,
					"filterDefinitionUri": "urn:replicon:division-list-filter:text"
				},
				"operatorUri": "urn:replicon:filter-operator:text-search",
				"rightExpression": {
					"leftExpression": null,
					"operatorUri": null,
					"rightExpression": null,
					"value": {
						"text": "GSAP",
						"numberRange": null
					},
					"filterDefinitionUri": null
				},
				"value": null,
				"filterDefinitionUri": null
			},
			"operatorUri": "urn:replicon:filter-operator:or",
			"rightExpression": {
				"leftExpression": {
					"leftExpression": null,
					"operatorUri": null,
					"rightExpression": null,
					"value": null,
					"filterDefinitionUri": "urn:replicon:division-list-filter:text"
				},
				"operatorUri": "urn:replicon:filter-operator:text-search",
				"rightExpression": {
					"leftExpression": null,
					"operatorUri": null,
					"rightExpression": null,
					"value": {
						"text": "COMPASS",
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

def get_all_past_time_export_data_payload(time_export_prefix, c1_time_export_filter_data):
    return {
		"page": 1,
		"pagesize": 100000,
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
					"filterDefinitionUri": "urn:replicon:time-data-export-list-filter:cancelled"
				},
				"operatorUri": "urn:replicon:filter-operator:equal",
				"rightExpression": {
					"value": {
						"bool": "false"
					}
				}
			},
			"operatorUri": "urn:replicon:filter-operator:and",
			"rightExpression": {
				"leftExpression": {
					"leftExpression": {
						"filterDefinitionUri": "urn:replicon:time-data-export-list-filter:text"
					},
					"operatorUri": "urn:replicon:filter-operator:text-search",
					"rightExpression": {
						"value": {
							"text": time_export_prefix
						}
					}
				},
				"operatorUri": "urn:replicon:filter-operator:and",
				"rightExpression": {
					"leftExpression": {
						"filterDefinitionUri": "urn:replicon:time-data-export-list-filter:creation-date-range"
					},
					"operatorUri": "urn:replicon:filter-operator:in",
					"rightExpression": {
						"value": {
							"dateRange": {
								"startDate": {
                                    "year": c1_time_export_filter_data['ackdateyear'],
                                    "month": c1_time_export_filter_data['ackdatemonth'],
                                    "day": c1_time_export_filter_data['ackdateday']
                                },
								"endDate": null
							}
						}
					}
				}
			}
		}
	}

def create_c1_reg_time_export_payload():
    c1_time_export_filter_data = rail.result("get_filter_data_for_c1_reg_time_export")
    return {
		"columnUris": [],
		"sort": [],
		"filterExpression": {
			"leftExpression": {
				"leftExpression": {
					"leftExpression": {
						"leftExpression": {
							"filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
						},
						"operatorUri": "urn:replicon:filter-operator:in",
						"rightExpression": {
							"value": {
								"dateRange": {
									"startDate": {
										"year": c1_time_export_filter_data["processingstartdateyear"],
										"month": c1_time_export_filter_data["processingstartdatemonth"],
										"day": c1_time_export_filter_data["processingstartdateday"]
									},
									"endDate": {
										"year": c1_time_export_filter_data["processingenddateyear"],
										"month": c1_time_export_filter_data["processingenddatemonth"],
										"day": c1_time_export_filter_data["processingenddateday"]
									},
									"relativeDateRangeUri": null,
									"relativeDateRangeAsOfDate": null
								}
							}
						}
					},
					"operatorUri": "urn:replicon:filter-operator:and",
					"rightExpression": {
						"leftExpression": {
							"filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
						},
						"operatorUri": "urn:replicon:filter-operator:in",
						"rightExpression": {
							"value": {
								"uris": [
									"urn:replicon:time-data-item-time-data-export-status:none"
								]
							}
						}
					}
				},
				"operatorUri": "urn:replicon:filter-operator:and",
				"rightExpression": {
					"leftExpression": {
						"filterDefinitionUri": "urn:replicon:time-data-export-filter:employee-type-group"
					},
					"operatorUri": "urn:replicon:filter-operator:not-in",
					"rightExpression": {
						"value": {
							"uris": [
                                c1_time_export_filter_data["contractoruri"],
                                c1_time_export_filter_data["agencycontractoruri"]
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
							"filterDefinitionUri": "urn:replicon:time-data-export-filter:division"
						},
						"operatorUri": "urn:replicon:filter-operator:in",
						"rightExpression": {
							"value": {
								"uris": c1_time_export_filter_data["companycodelist"]
							}
						}
					},
					"operatorUri": "urn:replicon:filter-operator:and",
					"rightExpression": {
						"leftExpression": {
							"filterDefinitionUri": c1_time_export_filter_data["oeffilter"]
						},
						"operatorUri": "urn:replicon:filter-operator:not-in",
						"rightExpression": {
							"value": {
								"uris": [
                                    c1_time_export_filter_data["oeffilteroption"],
                                    c1_time_export_filter_data["oeffilteroption1"]
								]
							}
						}
					}
				},
				"operatorUri": "urn:replicon:filter-operator:and",
				"rightExpression": {
					"leftExpression": {
						"filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-approval-status"
					},
					"operatorUri": "urn:replicon:filter-operator:in",
					"rightExpression": {
						"value": {
							"uris": [
								"urn:replicon:approval-status:approved"
							]
						}
					}
				}
			}
		},
		"fileFormatScriptUri": c1_time_export_filter_data["fileformaturi"]
	}

def create_c1_iwo_time_export_payload():
    c1_time_export_filter_data = rail.result("get_filter_data_for_c1_iwo_time_export")
    return {
		"columnUris": [],
		"sort": [],
		"filterExpression": {
			"leftExpression": {
				"leftExpression": {
					"leftExpression": {
						"leftExpression": {
							"filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
						},
						"operatorUri": "urn:replicon:filter-operator:in",
						"rightExpression": {
							"value": {
								"dateRange": {
									"startDate": {
										"year": c1_time_export_filter_data["processingstartdateyear"],
										"month": c1_time_export_filter_data["processingstartdatemonth"],
										"day": c1_time_export_filter_data["processingstartdateday"]
									},
									"endDate": {
										"year": c1_time_export_filter_data["processingenddateyear"],
										"month": c1_time_export_filter_data["processingenddatemonth"],
										"day": c1_time_export_filter_data["processingenddateday"]
									},
									"relativeDateRangeUri": null,
									"relativeDateRangeAsOfDate": null
								}
							}
						}
					},
					"operatorUri": "urn:replicon:filter-operator:and",
					"rightExpression": {
						"leftExpression": {
							"filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
						},
						"operatorUri": "urn:replicon:filter-operator:in",
						"rightExpression": {
							"value": {
								"uris": [
									"urn:replicon:time-data-item-time-data-export-status:none"
								]
							}
						}
					}
				},
				"operatorUri": "urn:replicon:filter-operator:and",
				"rightExpression": {
					"leftExpression": {
						"filterDefinitionUri": "urn:replicon:time-data-export-filter:employee-type-group"
					},
					"operatorUri": "urn:replicon:filter-operator:not-in",
					"rightExpression": {
						"value": {
							"uris": [
                                c1_time_export_filter_data["contractoruri"],
                                c1_time_export_filter_data["agencycontractoruri"]
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
							"filterDefinitionUri": "urn:replicon:time-data-export-filter:division"
						},
						"operatorUri": "urn:replicon:filter-operator:in",
						"rightExpression": {
							"value": {
								"uris": c1_time_export_filter_data["companycodelist"]
							}
						}
					},
					"operatorUri": "urn:replicon:filter-operator:and",
					"rightExpression": {
						"leftExpression": {
							"filterDefinitionUri": c1_time_export_filter_data["oeffilter"]
						},
						"operatorUri": "urn:replicon:filter-operator:in",
						"rightExpression": {
							"value": {
								"uris": [
                                    c1_time_export_filter_data["oeffilteroptioncp"],
                                    c1_time_export_filter_data["oeffilteroptionc1"]
								]
							}
						}
					}
				},
				"operatorUri": "urn:replicon:filter-operator:and",
				"rightExpression": {
					"leftExpression": {
						"filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-approval-status"
					},
					"operatorUri": "urn:replicon:filter-operator:in",
					"rightExpression": {
						"value": {
							"uris": [
								"urn:replicon:approval-status:approved"
							]
						}
					}
				}
			}
		},
		"fileFormatScriptUri": c1_time_export_filter_data["fileformaturi"]
	}

def get_past_14days_time_exports_for_C1_payload(dag_run):
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
					"filterDefinitionUri": "urn:replicon:time-data-export-list-filter:cancelled"
				},
				"operatorUri": "urn:replicon:filter-operator:equal",
				"rightExpression": {
					"value": {
						"bool": "false"
					}
				}
			},
			"operatorUri": "urn:replicon:filter-operator:and",
			"rightExpression": {
				"leftExpression": {
					"filterDefinitionUri": "urn:replicon:time-data-export-list-filter:creation-date-range"
				},
				"operatorUri": "urn:replicon:filter-operator:in",
				"rightExpression": {
					"value": {
						"dateRange": {
							"startDate": {
								"year": dag_run.conf["startdateyear"],
								"month": dag_run.conf["startdatemonth"],
								"day": dag_run.conf["startdateday"]
							},
							"endDate": {
								"year": dag_run.conf["todayplus1dateyear"],
								"month": dag_run.conf["todayplus1datemonth"],
								"day": dag_run.conf["todayplus1dateday"]
							}
						}
					}
				}
			}
		}
	}

def get_create_download_batch(export_uri, fileformat_uri):
    return {
		"columnUris": [],
		"sort": [],
		"filterExpression": {
			"leftExpression": {
				"leftExpression": null,
				"operatorUri": null,
				"rightExpression": null,
				"value": null,
				"filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
			},
			"operatorUri": "urn:replicon:filter-operator:in",
			"rightExpression": {
				"leftExpression": null,
				"operatorUri": null,
				"rightExpression": null,
				"value": {
					"uri": null,
					"uris": [export_uri],
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
		"fileFormatScriptUri": fileformat_uri
	}

def get_conf_for_process_ack_payload(dag_run, config):
    today = pendulum.now(config.utc_timezone)
    today_plus_1days = today + relativedelta(days=1)
    today_plus_14days = today - relativedelta(days=14)
    return {
        'todayplus1date': str(today_plus_1days),
        'todayplus1dateday': today_plus_1days.strftime("%d"),
        'todayplus1datemonth': today_plus_1days.strftime("%m"),
        'todayplus1dateyear': today_plus_1days.strftime("%Y"),
        'startdate': str(today_plus_14days),
        'startdateday': today_plus_14days.strftime("%d"),
        'startdatemonth': today_plus_14days.strftime("%m"),
        'startdateyear': today_plus_14days.strftime("%Y"),
        "oef_name": dag_run.conf["oefname"],
        "twbname": dag_run.conf["twbname"],
    }
