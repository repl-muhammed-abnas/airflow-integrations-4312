import uuid
import pendulum
import rail

null = None

def get_uuid():
    return uuid.uuid4()

def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def smart_join_by_attr(collection, path_or_tuple, attr, path, delimiter=""):
    mapped_list = [x[path] for x in collection if x[path_or_tuple] == attr]
    return delimiter.join(mapped_list) if mapped_list else None


def get_process_timeexport_location(item):
    ignored_keys = ('export_needed', 'UAT')
    return {
        **{k.lower(): v for k, v in item.items() if k not in ignored_keys},
        **{
            'location_uri': smart_join_by_attr(rail.result('get_enabled_locations'), 'displayText', item['location'], 'uri'),
            'file_format_uri': smart_join_by_attr(rail.result('get_all_scripts'), 'displayText', item['file_format_name'], 'uri'),
            'employee_type_uri': smart_join_by_attr(rail.result('get_all_employee_type_groups'), 'displayText', 'Dummy User', 'uri'),
            'export_period': rail.result('get_export_period')
        }
    }


def get_paris_timenow_in_fmt(fmt='%Y-%m-%dT%H:%M:%S'):
    return pendulum.now('Europe/Paris').strftime(fmt)


def get_today_date_in_paris_timezone():
    now = pendulum.now('Europe/Paris')
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_current_timesheet_period_payload(dag_run):
    current_date_time_in_paris_tz = get_today_date_in_paris_timezone()
    return {
        "page": 1,
        "pagesize": 2,
        "columnUris": [
            "urn:replicon:timesheet-list-column:timesheet",
            "urn:replicon:timesheet-list-column:timesheet-owner",
            "urn:replicon:timesheet-list-column:timesheet-period"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-period-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": current_date_time_in_paris_tz,
                            "endDate": current_date_time_in_paris_tz
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:location-of-timesheet-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:in-hierarchy",
                "rightExpression": {
                    "value": {
                        "uri": dag_run.conf['location_uri']
                    }
                }
            }
        }
    }


def get_create_timedata_item_batch(dag_run):
    return {
        "columnUris": [
            "urn:replicon:time-data-export-column:user"
        ],
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
                                "dateRange": rail.result('map_twb_enddate_startdate')
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
                                dag_run.conf['employee_type_uri']
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
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:location"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [
                                    dag_run.conf['location_uri']
                                ]
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [
                                    "urn:replicon:time-entry-type:worked-time",
                                    "urn:replicon:time-entry-type:time-off"
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
                                "urn:replicon:approval-status:approved",
                                "urn:replicon:approval-status:waiting"
                            ]
                        }
                    }
                }
            }
        }
    }


def get_search_user_by_location_payload(dag_run):
    return {
        "page": 1,
        "pagesize": 10,
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:location"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:location"
                },
                "operatorUri": "urn:replicon:filter-operator:in-hierarchy",
                "rightExpression": {
                    "value": {
                        "uri": dag_run.conf['location_uri']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "bool": "true",
                    }
                }
            }
        }
    }


def get_process_user_batch_conf(item, index, dag_run):
    paris_time_now = pendulum.now('Europe/Paris')
    offset_time = paris_time_now.add(seconds=index*5).strftime('%Y%m%d%H%M%S')
    ignored_keys = ('_ancestry', '_ecid', '_replication_position')
    return {
        **{k: v for k, v in dag_run.conf.items() if k not in ignored_keys},
        **{
            'user_uri_batch': item,
            'twb_start_end_date': rail.result('map_twb_enddate_startdate'),
            'total_user_count': rail.result('get_user_list_collection', 'length'),
            'process_start_time': rail.result('process_start_time'),
            'start_time': offset_time,
            'process_start': f"{offset_time} - Process started : {dag_run.conf['location']}",
            'export_file_name': f"Time Extract_{offset_time}_{dag_run.conf['code']}"
        }
    }


def get_current_past_period_conf(dag_run,config):
    ignored_keys = ('_ancestry', '_ecid', '_replication_position')
    return {
        **{k: v for k, v in dag_run.conf.items() if k not in ignored_keys},
        **{
            'user_object_set_uri': rail.result('create_user_object_set'),
            'timeofftype_chargecode_mapper': config.timeofftype_chargecode_mapper
        }
    }


def get_export_request(dag_run):
    return {
        "columnUris": [],
        "filterExpression":  {
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
                                    "startDate": dag_run.conf['twb_start_end_date']['startDate'],
                                    "endDate": dag_run.conf['twb_start_end_date']['endDate']
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
                                "uris": [dag_run.conf['employee_type_uri']]
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:location"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [dag_run.conf['location_uri']]
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
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:user"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [dag_run.conf['user_object_set_uri']]
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [
                                    "urn:replicon:time-entry-type:worked-time",
                                    "urn:replicon:time-entry-type:time-off"
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
                                "urn:replicon:approval-status:approved",
                                "urn:replicon:approval-status:waiting"
                            ]
                        }
                    }
                }
            }
        }
    }


def get_transaction_date_list():
    list_document = get_data_from_document(rail.result('valid_extracted_data'))
    return [x['TransactionDate'] for x in list_document if x['TransactionDate']]


def get_post_to_endpoints_conf(item, dag_run, group_id):
    return {
        ** item,
        ** {
            "process_start": dag_run.conf['process_start'],
            "process_start_time": dag_run.conf['process_start_time'],
            "export_file_name": dag_run.conf['export_file_name'],
            "worklocation": dag_run.conf["worklocation"],
            "location": dag_run.conf["location"],
            "code": dag_run.conf["code"],
            "file_format_name": dag_run.conf["file_format_name"],
            "payload_artifact": rail.result("prepare_api_payload"),
            'usercount': dag_run.conf['total_user_count'],
            'daterange': dag_run.conf['twb_start_end_date'],
            'twbrowcount': rail.result(f'{group_id}.create_timeexport_collection', 'length') or
            rail.result('valid_extracted_data', 'length') or 0,
            'filename': dag_run.conf['export_file_name']+".csv"
        }
    }

def get_final_extract_data_row(item):
    return [
        item['TransactionDate'] if item['TransactionDate'] else "",
        item['TimeEntryId'] if item['TimeEntryId'] else "",
        item['iwfr_InternalPerson_PartyId'] if item['iwfr_InternalPerson_PartyId'] else "",
        item['iwfr_PwCLegalEntity_Partyid'] if item['iwfr_PwCLegalEntity_Partyid'] else "",
        item['Timesheet_Start_Date'] if item['Timesheet_Start_Date'] else "",
        item['Timesheet_End_Date'] if item['Timesheet_End_Date'] else "",
        item['HoursQuantity'] if item['HoursQuantity'] else "",
        item['Comments'] if item['Comments'] else "",
        item['WorkLocation'] if item['WorkLocation'] else "",
        item['WorkCategory'] if item['WorkCategory'] else "",
        item['ResourceRole'] if item['ResourceRole'] else "",
        item['ChargeCode'] if item['ChargeCode'] else "",
        item['WorkItemType'] if item['WorkItemType'] else "",
        get_uuid()
    ]
