# pylint: disable=no-else-return, inconsistent-return-statements useless-else-on-loop
from datetime import datetime
from io import StringIO
import rail
import pandas as pd
import numpy as np
import uuid

DATE_FORMAT = '%m/%d/%Y'
null = None

def process_timeoff_user_conf(item):
    # dag_run_conf = get_dag_run_conf()
    output = {}
    user_report_data = rail.result(
        "report_data_to_dict").get(item.get("EmployeeID"), {})

    output['Employeeid'] = item.get("EmployeeID")
    output['Timeofftype'] = item.get("Timeofftype")
    output['Startdate'] = item.get("StartDate")
    output['Uniqueid'] = item.get("UniqueID")
    output['Action'] = item.get("Action")
    output['Useruri'] = user_report_data.get("useruri")
    output['Timeoffuri'] = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_all_timeofftypes_list"), "timeoffname", item.get("Timeofftype"), "timeoffuri")
    output['Timeoffname'] = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_all_timeofftypes_list"), "timeoffname", item.get("Timeofftype"), "timeoffname")
    output['Timeoffstatus'] = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_all_timeofftypes_list"), "timeoffname", item.get("Timeofftype"), "status")
    output['Application root URL'] = rail.get_tenant_slug()
    output['Company key'] = rail.get_company_key()
    output['Timeoffhrs'] = item.get("Hrs")
    output['Enddate'] = item.get("EndDate")
    output['Bookingdate'] = get_replicon_date(item.get("StartDate"))
    output['Unique ID OEF URI'] = rail.find_first_by_attr_and_get_attr(
        rail.result("get_all_extension_fields"), "name", "Unique ID", "uri")
    output['UniqueID_oef_value'] = rail.find_first_by_attr_and_get_attr(
        rail.result("get_all_extension_fields"), "name", "Unique ID", "uri").split(':')[-1]
    output['User start date'] = user_report_data.get("User Start Date")
    output['User end date'] = user_report_data.get("User End Date")
    output['User email ID'] = user_report_data.get("User Email")
    output['User name'] = user_report_data.get("User First Name")
    output['md5'] = item.get("md5")
    return output


def get_user_detail_dict():
    report_data = rail.result(
        'run_report_group.get_report_result').get('reportGenerationResults')[0].get('payload')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")
    df1 = df.replace(np.nan, "")
    return df1.set_index('employeeid').T.to_dict()


def get_replicon_date(date_str, date_format=DATE_FORMAT):
    if not date_str:
        return None
    # date format in 20060401
    try:
        _date = datetime.strptime(date_str, date_format)
        return {
            "day": _date.day,
            "month": _date.month,
            "year": _date.year
        }
    except:  # pylint: disable=bare-except
        return None


def get_timesheet_details_by_date_payload(dag_run, date_format=DATE_FORMAT):

    _date = datetime.strptime(dag_run.conf["Startdate"], date_format)
    return {
        "userUri": dag_run.conf["Useruri"],
        "date": {
            "year": _date.year,
            "month": _date.month,
            "day": _date.day
        },
        "timesheetGetOptionUri": None
    }


def get_errror_logs():
    errored_logs = []
    all_logs = []
    error_logs_info = rail.result('gather_child_data')
    for record in error_logs_info:
        errored_logs_from_child = get_data_from_document(record)
        errored_logs += errored_logs_from_child
    for reocrd in errored_logs:
        if reocrd.get('properties'):
            all_logs.append(reocrd.get('properties'))
    return all_logs


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_merged_log_entries():
    return rail.result('get_merged_logs')

def put_timeoff_for_user_payload(dag_run):
    return {
        "timeOff": {
            "target": {
                "uri": rail.result('CreateDraft_timeoffbooking_for_user'),
            },
            "owner": {
                "uri": dag_run.conf['Useruri']
            },
            "timeOffType": {
                "uri": dag_run.conf['Timeoffuri']
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": {
                        "year": dag_run.conf['Bookingdate']['year'],
                        "month": dag_run.conf['Bookingdate']['month'],
                        "day": dag_run.conf['Bookingdate']['day']
                    },
                    "timeOfDay": None,
                    "relativeDuration": None,
                    "specificDuration": {
                        "hours": int(rail.result('calculate_delta_hours_for_update').get('new_hours', dag_run.conf['Timeoffhrs'])) if dag_run.conf['Action'] == 'UPDATE' else dag_run.conf['Timeoffhrs'],
                        "minutes": "0",
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    }}
            },
            "userExplicitEntries": [],
            "comments": "Added by Replicon Integration",
            "customFieldValues": []
        }
    }

def get_time_off_details_on_booking_id(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" + dag_run.conf['UniqueID_oef_value'],
                "urn:replicon:time-off-list-column:start-date",
                "urn:replicon:time-off-list-column:end-date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-filter:"+dag_run.conf['UniqueID_oef_value']
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
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
                    "text": dag_run.conf["Uniqueid"],
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
        }
    }

def put_timeoff_for_update_payload(dag_run):
    return {
        "timeOff": {
            "target": {
                "uri": rail.result('get_time_off_details_on_unique_id')[0]['timeoff_uri']
            },
            "owner": {
                "uri": dag_run.conf['Useruri']
            },
            "timeOffType": {
                "uri": dag_run.conf['Timeoffuri']
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": {
                        "year": dag_run.conf['Bookingdate']['year'],
                        "month": dag_run.conf['Bookingdate']['month'],
                        "day": dag_run.conf['Bookingdate']['day']
                    },
                    "specificDuration": {
                        "hours": rail.result('calculate_delta_hours_for_update').get('new_hours', dag_run.conf['Timeoffhrs']),
                        "minutes": "0",
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    }
                }
            },
            "userExplicitEntries": [],
            "comments": "Added by Replicon Integration",
            "customFieldValues": [],
            "objectExtensionFieldValues": [
                {
                    "definition": {
                        "uri": dag_run.conf["Unique ID OEF URI"]
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": dag_run.conf["Uniqueid"],
                    "fileValue": null,
                    "jsonValue": null
                }
            ]
        },
        "unitOfWorkId": str(uuid.uuid4())
    }

def timeoff_approval_payload(dag_run):
    return {
        "timeOffUri": rail.result('get_time_off_details_on_unique_id')[0]['timeoff_uri'] if dag_run.conf['Action'] == 'UPDATE' else  rail.result('Publish_TimeOffDraft_for_user')['uri'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Replicon Integration"
    }
