from datetime import datetime, timedelta
import uuid
from uuid import uuid4
import rail
from bearingpoint.sap_h4s4_timeoff_booking_import_v1.utils import custom_methods

null = None

DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = "%H:%M:%S"

def get_child_conf(item, dag_run):
    return {
        **item,
        'timeoff_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoff_description_details"), 'description', item['timeofftype'], 'uri'),
        'user_uri': rail.result('get_user_details')['uri'],
        'booking_id_oef_uri': dag_run.conf['booking_id_oef_uri'],
        'user_start_date': rail.result('get_user_details')['start_date'],
        'user_end_date': rail.result('get_user_details')['end_date'],
        'window_start': dag_run.conf['window_start'],
        'window_end': dag_run.conf['window_end'],
        'log': dag_run.conf['log']
    }


def get_approve_holiday_booking_payload():
    return {
        "timeOffUri": rail.result("put_and_submit_timeoff_booking_for_user")["uri"],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Replicon Admin"
    }


def get_booking_id_oef_value_payload():
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:object-extension-tag-definition-list-column:name",
            "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition"
        ],
        "sort": [],
        "filterExpression": null
    }


def get_time_off_booking_details(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" +
                    dag_run.conf['booking_id_oef_uri'].split(':')[-1],
                "urn:replicon:time-off-list-column:total-effective-hours"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-filter:" +
                    dag_run.conf['booking_id_oef_uri'].split(':')[-1]
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['booking_id'],
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": rail.parse_date(dag_run.conf['startdate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                            "endDate": rail.parse_date(dag_run.conf['enddate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                        }
                    }
                }
            }
        }
    }

def get_user_time_off_booking_details(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off",
            "urn:replicon:time-off-list-column:start-date",
            "urn:replicon:time-off-list-column:end-date",
            "urn:replicon:time-off-list-column:time-off-type",
            "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" +
                    dag_run.conf['booking_id_oef_uri'].split(':')[-1],
            "urn:replicon:time-off-list-column:time-off-owner"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "uri": dag_run.conf['user_uri'],
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "dateRange": {
                        "startDate": rail.parse_date(dag_run.conf['startdate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                        "endDate": rail.parse_date(dag_run.conf['enddate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                        }
                    }
                }
            }
        }
    }

def get_all_timeoffs_within_3month_window(dag_run):
    return {
        "page": "1",
        "pagesize": "10000000",
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off",
            "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" +
                    dag_run.conf['booking_id_oef_uri'].split(':')[-1]
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "dateRange": {
                        "startDate": rail.parse_date(dag_run.conf['window_start'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                        "endDate": rail.parse_date(dag_run.conf['window_end'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                    }
                }
            }
        }
    }


def get_all_timesheet_for_user(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:timesheet-list-column:timesheet-status",
            "urn:replicon:timesheet-list-column:timesheet",
            "urn:replicon:timesheet-list-column:timesheet-period",
            "urn:replicon:timesheet-list-column:timesheet-owner"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-period-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": rail.parse_date(dag_run.conf['window_start'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                            "endDate": rail.parse_date(dag_run.conf['window_end'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT)
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "uri": rail.result("get_user_details")['uri'],
                    }
                }
            }
        }
    }


def get_delete_time_entries_payload(dag_run):
    startdate = datetime.strptime(dag_run.conf['startdate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT)
    window_start = datetime.strptime(dag_run.conf['window_start'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT)
    effective_start = max(startdate, window_start).strftime(custom_methods.FEED_ENTRYDATE_DATE_FORMAT)
    return {
        "user": {
            "uri": dag_run.conf['user_uri']
        },
        "dateRange": {
            "startDate": rail.parse_date(effective_start, custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
            "endDate": rail.parse_date(dag_run.conf['enddate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT)
        },
        "timeEntryDeleteFilter": {
            "timeEntryDeleteFilterAccessOptionUri": "urn:replicon:time-entry-delete-filter-access-option:delete-all-time-entries"
        }
    }


def get_timeoff_dates_payload(dag_run):
    return {
        "timeOffStart": {
            "date": rail.parse_date(dag_run.conf['startdate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
            "relativeDuration": None,
        },
        "timeOffEnd": {
            "date": rail.parse_date(dag_run.conf['enddate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
            "relativeDuration": None,
        }
    } if dag_run.conf['startdate'] != dag_run.conf['enddate'] else {
        "timeOffStart": {
            "date": rail.parse_date(dag_run.conf['startdate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
            "specificDuration": {
                "seconds": int(float(dag_run.conf['hours']) * 3600)
            }
        }
    }


def get_create_and_publish_timeoff_payload(dag_run):
    return {
        "timeOff": {
            "target": null,
            "owner": {
                "uri": dag_run.conf['user_uri'],
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf['timeoff_uri'],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": get_timeoff_dates_payload(dag_run),
            "objectExtensionFieldValues": [
                {
                    "definition": {
                        "uri":  dag_run.conf['booking_id_oef_uri'],
                    },
                    "textValue": dag_run.conf['booking_id']
                }
            ]
        },
        "comments": 'Time Off Booking Submitted by Integration',
        "unitOfWorkId": str(uuid4())
    }


def get_submit_time_off_entry_payload():
    return {
        "timeOffUri": rail.result('put_and_submit_timeoff_booking_for_user_update')['timeoff_uri'],
        "unitOfWorkId": str(uuid4()),
        "comments": "Approved by Integration"
    }

def get_timeoff_booking_for_user_payload(item):
    return {
        "target": {
            "uri": item['timeoff_uri']
        } if item['timeoff_uri'] else None,
        "modifications": {
            "owner": {
                "value": {
                    "uri": item['user_uri'],
                }
            },
            "timeOffType": {
                "value": {
                    "uri": item['timeoff_type'],
                }
            },
            "entryConfigurationMethodUri": {
                "value": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule"
            },
            "multiDayUsingStartEndDate": {
                "value": {
                    "timeOffStart": {
                        "date": rail.parse_date(item['start_date'], "%m/%d/%Y"),
                        "relativeDuration": "urn:replicon:time-off-relative-duration:full-day"
                    },
                    "timeOffEnd": {
                        "date": rail.parse_date(item['end_date'], "%m/%d/%Y"),
                        "relativeDuration": "urn:replicon:time-off-relative-duration:full-day"
                    }
                }
            },
            "userExplicitEntries": [],
            "extensionFields": [],
            "customFields": []
        },
        "comments": 'Time Off Booking Submitted by Integration',
        "unitOfWorkId": str(uuid4())
    }


def get_user_timeoffs_within_window(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off",
            "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" +
                    dag_run.conf['booking_id_oef_uri'].split(':')[-1],
            "urn:replicon:time-off-list-column:time-off-owner"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "uri": rail.result('get_user_details')['uri'],
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": rail.parse_date(dag_run.conf['window_start'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                            "endDate": rail.parse_date(dag_run.conf['window_end'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                        }
                    }
                }
            }
        }
    }


def get_timesheets_before_window(dag_run):
    window_start = datetime.strptime(dag_run.conf['window_start'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT).date()
    end_date = window_start - timedelta(days=1)

    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:timesheet-list-column:timesheet-status",
            "urn:replicon:timesheet-list-column:timesheet",
            "urn:replicon:timesheet-list-column:timesheet-period",
            "urn:replicon:timesheet-list-column:timesheet-owner"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-period-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "endDate": rail.parse_date(end_date.strftime(custom_methods.FEED_ENTRYDATE_DATE_FORMAT), custom_methods.FEED_ENTRYDATE_DATE_FORMAT)
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "uri": rail.result("get_user_details")['uri'],
                    }
                }
            }
        }
    }
