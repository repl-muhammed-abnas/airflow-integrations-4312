import pendulum
import rail
from datetime import datetime
from tsystems.time_export_to_sap import config
import uuid
import time

null = None


def get_uuid():
    return uuid.uuid4()


def get_berlin_timenow_in_fmt(fmt='%Y-%m-%dT%H:%M:%S'):
    return pendulum.now(config.pacific_timezone).strftime(fmt)


def parse_date(date_str):
    dt = datetime.strptime(date_str, '%d.%m.%Y')
    return {
        "year": dt.year,
        "month": dt.month,
        "day": dt.day
    }


def get_export_data_from_mapper(dag_run):
    time.sleep(1)
    timestamp = get_berlin_timenow_in_fmt('%Y%m%d_%H%M%S')

    start_date_str = dag_run.conf.get('time_entry_start_date', '')
    end_date_str = dag_run.conf.get('time_entry_end_date', '')
    start_date = parse_date(start_date_str)
    end_date = parse_date(end_date_str)

    legal_unit = dag_run.conf.get('legal_unit', '')
    company_code = dag_run.conf.get('company_code', '')
    if legal_unit == ["All"]:
        identifier = "_".join(company_code)
    else:
        identifier = "_".join(legal_unit)

    export_file_name = f"REPLICON_ICM_TSI_PROJTIME_{identifier}_" + timestamp
    log_filename = f"Time_extract_log_" + timestamp + ".txt"
    twb_file_name = f"REPLICON_ICM_Export_" + timestamp

    return {
        'start_date': start_date,
        'end_date': end_date,
        'export_file_name': export_file_name,
        'log_filename': log_filename,
        'twb_file_name': twb_file_name,
    }


def get_export_request(dag_run):
    if len(rail.result('get_oef_field_values')) == 0 and len(rail.result('get_required_companycode_uris')) == 0:
        return {
            "columnUris": [],
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
                                        "startDate": rail.result('get_export_data')['start_date'],
                                        "endDate": rail.result('get_export_data')['end_date']
                                    }
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
                                        "urn:replicon:time-entry-type:worked-time"
                                    ]
                                }
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:timesheet-only-approval-status"
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
            }
        }
    elif len(rail.result('get_oef_field_values')) == 0 or len(rail.result('get_required_companycode_uris')) == 0:
        if len(rail.result('get_oef_field_values')) == 0:
            filter_def_uri = "urn:replicon:time-data-export-filter:location"
            uris = rail.result('get_required_companycode_uris')
        else:
            filter_def_uri = dag_run.conf['legal_unit_oef_uri']
            uris = rail.result('get_oef_field_values')
        return {
            "columnUris": [],
            "filterExpression": {
                "leftExpression": {
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
                                            "startDate": rail.result('get_export_data')['start_date'],
                                            "endDate": rail.result('get_export_data')['end_date']
                                        }
                                    }
                                }
                            },
                            "operatorUri": "urn:replicon:filter-operator:and",
                            "rightExpression": {
                                "leftExpression": {
                                    "filterDefinitionUri": filter_def_uri
                                },
                                "operatorUri": "urn:replicon:filter-operator:in",
                                "rightExpression": {
                                    "value": {
                                        "uris": uris
                                    }
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
                                        "urn:replicon:time-entry-type:worked-time"
                                    ]
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
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:timesheet-only-approval-status"
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
    else:
        return {
            "columnUris": [],
            "filterExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": {
                                "leftExpression": {
                                    "leftExpression": {
                                        "filterDefinitionUri": dag_run.conf['legal_unit_oef_uri']
                                    },
                                    "operatorUri": "urn:replicon:filter-operator:in",
                                    "rightExpression": {
                                        "value": {
                                            "uris": rail.result('get_oef_field_values')
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
                                                "urn:replicon:time-entry-type:worked-time"
                                            ]
                                        }
                                    }
                                }
                            },
                            "operatorUri": "urn:replicon:filter-operator:and",
                            "rightExpression": {
                                "leftExpression": {
                                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
                                },
                                "operatorUri": "urn:replicon:filter-operator:in",
                                "rightExpression": {
                                    "value": {
                                        "uris": [],
                                        "dateRange": {
                                            "startDate": rail.result('get_export_data')['start_date'],
                                            "endDate": rail.result('get_export_data')['end_date']
                                        }
                                    }
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
                                    "uris": rail.result('get_required_companycode_uris')
                                }
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:timesheet-only-approval-status"
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
            }
        }


import unicodedata

# Unicode to ASCII translation table
UNICODE_REPLACEMENTS = str.maketrans({
    '\u201c': '"', '\u201d': '"',  # curly quotes
    '\u2018': "'", '\u2019': "'",  # curly apostrophes
    '\u2013': '-', '\u2014': '-',  # en/em dashes
})

def sanitize_for_ascii(value):
    if not value:
        return ""
    return str(value).translate(UNICODE_REPLACEMENTS).encode('ascii', errors='ignore').decode('ascii')

def get_final_extract_data_row(item):
    return [sanitize_for_ascii(item.get(key, "")) for key in [
        'time_entry_id', 'employee_ID', 'project_ID', 'entry_date',
        'billing_entry', 'billing_rate_name', 'hours', 'task_name',
        'task_code', 'task_activity_name', 'task_description',
        'sap_activity_type', 'transaction_id'
    ]]


def create_export_status_complete_batch_payload(export_uri):
    return {
        "target": {
            "uri": rail.result(export_uri)
        },
        "statusUri": "urn:replicon:time-data-export-status:complete"
    }
