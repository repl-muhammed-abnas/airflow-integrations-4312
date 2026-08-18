from datetime import datetime
import json
import uuid
import rail
from airflow.models import Variable
from victoriashipyards.timesheet_auto_submission.utils import custom_methods


def get_report_generate_batch_payload():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_timesheet_submission_mapper(validationmessages, config):
    mapper_list = json.loads(
        Variable.get(config.timesheet_submission_mapper_var, default_var=[]))
    if validationmessages != "Null":
        validation_messages_list = validationmessages.lower().split("; ")
        mapper_list = list(map(lambda data: data["message"].lower(), filter(lambda data: data["type"].lower() == "validation", mapper_list)))
        if not list(filter(lambda item: item not in mapper_list, validation_messages_list)):
            return "Null"
    return validationmessages


def get_timesheet_audit_reopen_timesheets_payload():
    not_sumbitted_no_error_timesheets = custom_methods.read_collection(
        rail.result('not_sumbitted_no_error_timesheets'))
    timesheet_report_data = rail.result('get_reopen_timesheet_details')[
        'filterConfiguration']['enabledFilters']
    action_filter_uri = list(map(lambda enabled_filters: enabled_filters['uri'], filter(
        lambda enabled_filters: enabled_filters['displayText'] == "ActionFilter", timesheet_report_data)))
    timesheet_period_filter = list(map(lambda enabled_filters: enabled_filters['uri'], filter(
        lambda enabled_filters: enabled_filters['displayText'] == "TimesheetPeriodFilter", timesheet_report_data)))
    timesheet_start_date = list((datetime.strptime(timesheet['timesheetstartdate'], "%b %d, %Y")).strftime(
        "%m/%d/%Y") for timesheet in not_sumbitted_no_error_timesheets)
    timesheet_end_date = list((datetime.strptime(timesheet['timesheetenddate'], "%b %d, %Y")).strftime(
        "%m/%d/%Y") for timesheet in not_sumbitted_no_error_timesheets)
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_reopen_timesheet_details')['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": action_filter_uri[0],
                        "value": "4"
                    },
                    {
                        "reportFilterUri": timesheet_period_filter[0],
                        "value": None
                    },
                    {
                        "reportFilterUri": timesheet_period_filter[0],
                        "value": timesheet_start_date[0]
                    },
                    {
                        "reportFilterUri": timesheet_period_filter[0],
                        "value": timesheet_end_date[-1]
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_row_data(item, instance):
    return [
        item['timesheetperiod'],
        item['username'],
        get_timesheet_submission_mapper(item['validationmessages'], instance),
        item['approvalstatus'],
        item['timesheeturi'],
        item['timesheetstartdate'],
        item['timesheetenddate']
    ]


def get_force_approve_timesheet_payload():
    return {
        "timesheetUri": rail.get_current_context()['dag_run'].conf['timesheeturi'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Integration"
    }


def get_submit_timesheet_payload():
    return {
        "timesheetUri": rail.get_current_context()['dag_run'].conf['timesheeturi'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Submitted by Integration",
        "changeReason": None
    }
