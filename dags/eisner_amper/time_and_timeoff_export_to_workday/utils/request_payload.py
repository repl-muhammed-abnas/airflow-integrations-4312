from datetime import datetime as dt, timedelta
import hashlib
import numpy as np
import rail
from rail.lib.artifact import existing_artifact
import pendulum

def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "dag_start_time": current_time.strftime("%Y%m%d%H%M%S")
    }


def getallfilterdefinitions():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:division-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": "C1"
                }
            }
        }
    }


def get_all_us_company_codes():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": "US"
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "bool": "true"
                    }
                }
            }
        }
    }


def get_all_us_cost_codes():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:cost-center-list-filter:effectively-enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "bool": "true"
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:cost-center-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": "US01"
                    }
                }
            }
        }
    }


def getallobjectExtensionfielddetails():
    return {
        "bindingContextUri": "urn:replicon:object-type:project"
    }


def getobjectextensiontagdefinitiondetails():
    return {
        "objectExtensionTagDefinitionUri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_object_Extensionfield_details'), 'name', 'Project Profile', 'uri')
    }


def getobjectextensiontagdefinitiondetail_project_type():
    return {
        "objectExtensionTagDefinitionUri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_object_Extensionfield_details'), 'name', 'Project Type', 'uri')
    }


def get_timedata_batch_data():
    return {
        "columnUris": [],
        "filterExpression": {
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
                                    "year": (dt.utcnow() - timedelta(days=90)).strftime("%Y"),
                                    "month": (dt.utcnow() - timedelta(days=90)).strftime("%m"),
                                    "day": (dt.utcnow() - timedelta(days=90)).strftime("%d")
                                },
                                "endDate": {
                                    "year": (dt.utcnow()).strftime("%Y"),
                                    "month": (dt.utcnow()).strftime("%m"),
                                    "day": (dt.utcnow()).strftime("%d")
                                }
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
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": [
                                rail.find_first_by_attr_and_get_attr(rail.result(
                                    'get_enabled_employeetype_groups'), 'displayText', 'Hourly – Exempt', 'uri'),
                                rail.find_first_by_attr_and_get_attr(rail.result(
                                    'get_enabled_employeetype_groups'), 'displayText', 'Hourly – Non-Exempt', 'uri'),
                                rail.find_first_by_attr_and_get_attr(rail.result(
                                    'get_enabled_employeetype_groups'), 'displayText', 'Standard – Non-Exempt', 'uri'),
                                rail.find_first_by_attr_and_get_attr(rail.result(
                                    'get_enabled_employeetype_groups'), 'displayText', 'Standard – Exempt', 'uri')
                            ]
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
        }
    }


def get_update_timedata_name():
    return {"target": {"uri": rail.result('get_timedata_batch_result')['timeDataExportUri']},
            "name": "Replicon__Time_off_and_Time_"+(dt.utcnow()).strftime("%Y%m%d%H%M%S")}


def get_update_timedata_name_cancelled():
    return {
        "target":
        {
            "uri": rail.result('get_timedata_batch_result')['timeDataExportUri']
        },
            "name": "Canncelled_Replicon_Time_"+(dt.utcnow()).strftime("%Y%m%d%H%M%S")
    }


def process_timeoff_export_conf():
    return {
        'Downloadurl': rail.result('get_timedata_batch_result')['timeDataExportUri'],
        'Fileformaturi': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_scripts'), 'displayText', 'WD Timeoff Export', 'uri'),
        'Timeexporturi': rail.result('get_timedata_batch_result')['timeDataExportUri'],
        'Twbname': "Replicon_Time_Off_"+rail.result('get_logging_details')['dag_start_time'],
        'processingstartdateday': int((dt.utcnow() - timedelta(days=90)).strftime("%d")),
        'processingstartdatemonth': int((dt.utcnow() - timedelta(days=90)).strftime("%m")),
        'processingstartdateyear': int((dt.utcnow() - timedelta(days=90)).strftime("%Y")),
        'processingenddateday': int((dt.utcnow()).strftime("%d")),
        'processingenddatemonth': int((dt.utcnow()).strftime("%m")),
        'processingenddateyear': int((dt.utcnow()).strftime("%Y")),
        'oeffilterforprojecttype': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_filter_definitions'), 'name', 'Project Type', 'uri'),
        'oeffilteroptionfor01': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_object_extension_tag_definition_detail_project_type'), 'name', '01', 'uri'),
        'oeffilterforprojectprofile': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_filter_definitions'), 'name', 'Project Profile', 'uri'),
        'oeffilteroptionforYP04': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_object_extension_tag_definition_details'), 'name', 'YP04', 'uri'),
        'employeetypegroup1': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_enabled_employeetype_groups'), 'displayText', 'Hourly – Exempt', 'uri'),
        'employeetypegroup2': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_enabled_employeetype_groups'), 'displayText', 'Hourly – Non-Exempt', 'uri'),
        'employeetypegroup3': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_enabled_employeetype_groups'), 'displayText', 'Standard – Non-Exempt', 'uri'),
        'employeetypegroup4': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_enabled_employeetype_groups'), 'displayText', 'Standard – Exempt', 'uri'),
        'Projectprofileoefname': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_object_extension_tag_definition_detail_project_type'), 'name', '01', 'uri')
    }


def process_timedata_export_conf():
    return {
        'Downloadurl': rail.result('get_timedata_batch_result')['timeDataExportUri'],
        'Fileformaturi': rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_scripts'), 'displayText', 'WD Time Data format', 'uri'),
        'Timeexporturi': rail.result('get_timedata_batch_result')['timeDataExportUri'],
        'Twbname': "Replicon_Time_Block_"+rail.result('get_logging_details')['dag_start_time']
    }


def get_timedata_download_batch_data(dag_run):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [dag_run.conf['Timeexporturi']]
                }
            }
        },
        "fileFormatScriptUri": dag_run.conf['Fileformaturi']
    }


def get_time_download_batch_data(dag_run):
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
                                        "year": dag_run.conf['processingstartdateyear'],
                                        "month": dag_run.conf['processingstartdatemonth'],
                                        "day": dag_run.conf['processingstartdateday']
                                    },
                                    "endDate": {
                                        "year": dag_run.conf['processingenddateyear'],
                                        "month": dag_run.conf['processingenddatemonth'],
                                        "day": dag_run.conf['processingenddateday']
                                    }
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
                                    "urn:replicon:time-data-item-time-data-export-status:complete"
                                ]
                            }
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "oeffilterforprojectprofile Step 1"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [
                                    dag_run.conf['oeffilteroptionforYP04']
                                ]
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": dag_run.conf['oeffilterforprojecttype']
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [
                                    dag_run.conf['oeffilteroptionfor01']
                                ]
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
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:cost-center"
                        },
                        "operatorUri": "urn:replicon:filter-operator:not-in",
                        "rightExpression": {
                            "value": {
                                "uris": rail.result('get_all_us_cost_codes')
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:department-group"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": rail.result('get_all_us_company_codes')
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
        "fileFormatScriptUri": dag_run.conf['Fileformaturi']
    }


def get_time_data_csv_rows(item, index):
    if not item:
        return []
    Date_time = item['entrydate']

    if item["projecttype"] == "02":
        timeentrycode = "Holiday"
    else:
        timeentrycode = "Standard Hours"

    return {
        'Header_Line': (np.arange(rail.result('query_finaltimedata_records', 'length')) // 1000 + 1)[index],
        'Line_Key': int(index)+1,
        'Employee_ID': item["employeeid"],
        'Date': (dt.strptime(Date_time, '%d/%m/%Y').date()).strftime("%Y-%m-%d"),
        'Time_Entry_Code': timeentrycode,
        'Hours': item['hours']

    }.values()


def get_timeoff_data_csv_rows(item):
    if not item:
        return []

    return {
        'employeeid': item["employeeid"],
        'entrydate': item["entrydate"],
        'projectcode': item["projectcode"],
        'taskcode': item["taskcode"],
        'hours': item["hours"],
        'timeentrycode': item['timeentrycode'],
        'md5': hashlib.md5((str(item["employeeid"]) + ',' + str(item["entrydate"]) + ',' +
                            str(item["projectcode"]) + ',' + str(item["taskcode"]) +
                            ',' + str(item["hours"]) + ',' + str(item["timeentrycode"])).encode()).hexdigest()

    }.values()


def get_timedata_data_csv_rows(item):
    if not item:
        return []

    return {
        'employeeid': item["employeeid"],
        'entrydate': item["entrydate"],
        'projectcode': item["projectcode"],
        'taskcode': item["taskcode"],
        'hours': item["hours"],
        'timeentrycode': item['timeentrycode'],
        'md5': hashlib.md5((str(item["employeeid"]) + ',' + str(item["entrydate"]) + ',' +
                            str(item["projectcode"]) + ',' + str(item["taskcode"]) +
                            ',' + str(item["hours"]) + ',' + str(item["timeentrycode"])).encode()).hexdigest()

    }.values()


def get_final_data_csv_rows(item):
    if not item:
        return []

    return {
        'Uniqueid': item["Employee_ID"] + "_" + (dt.strptime(item["Entry_Date"], '%d/%m/%Y').date()).strftime("%Y%m%d") + "_" + item['Time_Entry_Code'],
        'employeeid': item["Employee_ID"],
        'entrydate': (dt.strptime(item["Entry_Date"], '%d/%m/%Y').date()).strftime("%Y-%m-%d"),
        'timeentrycode': item['Time_Entry_Code'],
        'hours': str(float(item['Hours'])),
        'Additonal': None

    }.values()


def get_email_file_data():
    with existing_artifact(rail.result('write_header_xml_file'), mode='r') as artifact:
        email_file_data = artifact.file.read()
        return email_file_data
