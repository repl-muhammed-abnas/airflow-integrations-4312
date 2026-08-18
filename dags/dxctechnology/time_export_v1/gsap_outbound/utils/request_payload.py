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

def get_gsap_divisions_payload():
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
	    			"text": "GSAP",
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

def get_c1_compass_divisions_payload():
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
						"text": "C1",
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

def get_past_time_export_data_payload(time_export_prefix, gsap_time_export_filter_data):
    return {
		"page": "1",
		"pagesize": "100000",
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
                                    "year": gsap_time_export_filter_data['ackdateyear'],
                                    "month": gsap_time_export_filter_data['ackdatemonth'],
                                    "day": gsap_time_export_filter_data['ackdateday']
                                },
								"endDate": null
							}
						}
					}
				}
			}
		}
	}

def create_gsap_reg_time_export_payload():
    gsap_time_export_filter_data = rail.result("get_filter_data_for_gsap_reg_time_export")
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
										"year": gsap_time_export_filter_data["processingstartdateyear"],
										"month": gsap_time_export_filter_data["processingstartdatemonth"],
										"day": gsap_time_export_filter_data["processingstartdateday"]
									},
									"endDate": {
										"year": gsap_time_export_filter_data["processingenddateyear"],
										"month": gsap_time_export_filter_data["processingenddatemonth"],
										"day": gsap_time_export_filter_data["processingenddateday"]
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
						"leftExpression": {
							"filterDefinitionUri": "urn:replicon:time-data-export-filter:employee-type-group"
						},
						"operatorUri": "urn:replicon:filter-operator:not-in",
						"rightExpression": {
							"value": {
								"uris": [
                                	gsap_time_export_filter_data["contractoruri"],
                                	gsap_time_export_filter_data["agencycontractoruri"]
                				]
							}
						}
					},
					"operatorUri": "urn:replicon:filter-operator:and",
					"rightExpression": {
						"leftExpression": {
							"filterDefinitionUri": "urn:replicon:time-data-export-filter:division"
						},
						"operatorUri": "urn:replicon:filter-operator:in",
						"rightExpression": {
							"value": {
								"uris": gsap_time_export_filter_data["companycodelist"]
							}
						}
					}
				}
			},
			"operatorUri": "urn:replicon:filter-operator:and",
			"rightExpression": {
				"leftExpression": {
					"leftExpression": {
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
					},
					"operatorUri": "urn:replicon:filter-operator:and",
					"rightExpression": {
						"leftExpression": {
							"filterDefinitionUri": gsap_time_export_filter_data["oeffilter"]
						},
						"operatorUri": "urn:replicon:filter-operator:not-in",
						"rightExpression": {
							"value": {
								"uris": [gsap_time_export_filter_data["oeffilteroption"]]
							}
						}
					}
				},
				"operatorUri": "urn:replicon:filter-operator:and",
				"rightExpression": {
					"leftExpression": {
						"filterDefinitionUri": gsap_time_export_filter_data["oeffilter1"]
					},
					"operatorUri": "urn:replicon:filter-operator:not-in",
					"rightExpression": {
						"value": {
							"uris": [gsap_time_export_filter_data["oeffilteroption1"]]
						}
					}
				}
			}
		},
		"fileFormatScriptUri": gsap_time_export_filter_data["fileformaturi"]
	}

def create_gsap_iwo_time_export_payload():
    gsap_time_export_filter_data = rail.result("get_filter_data_for_gsap_iwo_time_export")
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
										"year": gsap_time_export_filter_data["processingstartdateyear"],
										"month": gsap_time_export_filter_data["processingstartdatemonth"],
										"day": gsap_time_export_filter_data["processingstartdateday"]
									},
									"endDate": {
										"year": gsap_time_export_filter_data["processingenddateyear"],
										"month": gsap_time_export_filter_data["processingenddatemonth"],
										"day": gsap_time_export_filter_data["processingenddateday"]
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
								gsap_time_export_filter_data["contractoruri"],
                                gsap_time_export_filter_data["agencycontractoruri"]
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
								"uris": gsap_time_export_filter_data["companycodelist"]
							}
						}
					},
					"operatorUri": "urn:replicon:filter-operator:and",
					"rightExpression": {
						"leftExpression": {
							"filterDefinitionUri": gsap_time_export_filter_data["oeffilter"]
						},
						"operatorUri": "urn:replicon:filter-operator:in",
						"rightExpression": {
							"value": {
								"uris": [
									gsap_time_export_filter_data["oeffilteroptionc1"],
                                    gsap_time_export_filter_data["oeffilteroptiongs"]
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
		"fileFormatScriptUri": gsap_time_export_filter_data["fileformaturi"]
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

def get_conf_for_process_ack_payload(dag_run):
    return {
        **dict(dag_run.conf),
        **{
            "sender": "gsap",
            "erp": "gsap"
		}
    }

def get_update_oef_acknowlegement_payload(dag_run):
    return {
        "objectUri": dag_run.conf['timeexporturi'],
        "value": {
            "definition": {
            	"uri": dag_run.conf["oefuri"]
            },
            "textValue": "Yes"
        }
    }
