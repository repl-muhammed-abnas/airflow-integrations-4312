from datetime import datetime as dt
import json
import os
import rail
from macquariegroup.recovery_reconciliation.mapper.recovery_field_mapper import recovery_field_mapper
from macquariegroup.recovery_reconciliation.utils.custom_methods import get_replicon_date_from_report_date

null = None


def get_report_parameters_for_timesheet_period_report():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_report_parameters():
    if not rail.result('get_report_details')['filterConfiguration']['enabledFilters']:
        raise Exception("No filters found for the base report")

    current_division_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')[
                                                                       'filterConfiguration']['enabledFilters'], 'displayText', "CurrentDivisionFilter", 'uri')
    filterValues = []
    for group in rail.result('get_required_division'):
        filterValues.append({
            "reportFilterUri": current_division_filter_uri,
            "value": group['uri'].split(':')[-1]
        })
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")['uri'],
                "filterValues": filterValues,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_all_available_timesheet_periods_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:timesheet-period-list-column:name",
            "urn:replicon:timesheet-period-list-column:enabled",
            "urn:replicon:timesheet-period-list-column:timesheet-period",
            "urn:replicon:timesheet-period-list-column:timesheet-period-type"
        ],
        "sort": [],
        "filterExpression": null
    }


def get_timesheet_period_to_apply(effective_date_to_apply, timesheet_period="Dummy Timesheet Period"):
    return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "name": timesheet_period
                    },
                    "effectiveDate": effective_date_to_apply
                }
            ],
            "endDate": null
        }
    }

def get_timesheet_period_payload_to_apply(dag_run, timesheet_period="Dummy Timesheet Period"):
    effective_date = list(filter(lambda item: item['employee_type'] == dag_run.conf['employee_type'], recovery_field_mapper))
    return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "name": timesheet_period
                    },
                    "effectiveDate": effective_date[0]['timesheet_period_assignment']
                }
            ],
            "endDate": null
        }
    }


def get_update_user_payload(dag_run, recovery_enable_status):
    return {
        "user": {
            "uri": dag_run.conf['user_uri']
        },
        "modifications": {
            "timesheetPeriodScheduleToApply": null if recovery_enable_status == "Yes" else get_timesheet_period_to_apply(
                rail.result('get_effective_dates')['timesheet_period']),
            "serviceCenterScheduleToApply": {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                        {
                            "serviceCenter": {
                                "name": recovery_enable_status
                            },
                            "effectiveDate": {
                                "year": dag_run.conf['running_date']['year'],
                                "month": dag_run.conf['running_date']['month'],
                                "day": dag_run.conf['running_date']['day']
                            } if recovery_enable_status == "Yes" else rail.result('get_effective_dates')['recovery_enable']
                        }
                    ]
                }
            },
            "userDetailsToApply": {
                "employmentEndDate": {"date" : rail.result('get_effective_dates')['recovery_enable']}
            } if recovery_enable_status != "Yes" else null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_common_trigger_config(item, set_recovery_enable_to):
    return {
        "file_name": os.path.split(rail.result('new_file_sensor'))[1],
        "user_uri": item['user_uri'],
        "login_name": item['login_name'],
        "set_recovery_enable_to": set_recovery_enable_to,
        "employee_recovery_enable_status": item['recovery_enabled'],
        "user_start_date": item['user_start_date'],
        "employee_type": item['employee_type'],
        "department": item['department'],
        "cost_center": item['cost_center'],
        "replicon_timesheet_period_data": rail.result("get_all_timesheet_periods_from_replicon"),
        "running_date": {
            "year": dt.now().year,
            "month": dt.now().month,
            "day": dt.now().day
        }
    }


def get_final_payload_sendemail(useruri, html_body, subject_line):
    final_payload = {"email": {
        "to": [
            {
                "user": {
                    "uri": useruri,
                    "loginName": null
                },
                "email": null
            }
        ],
        "cc": [],
        "bcc": [],
        "replyTo": null,
        "fromDisplayName": "Do-Not-Reply@deltek.com",
        "subject": subject_line,
        "htmlBody": html_body,
        "textBody": null,
        "attachments": []
    }}
    return json.dumps(final_payload)

def generate_timesheet_payload(dag_run):
    effective_date = list(filter(lambda item: item['employee_type'] == dag_run.conf['employee_type'], recovery_field_mapper))
    return {
            "userUri": dag_run.conf['user_uri'],
            "date": effective_date,
            "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
        }

def get_remove_user_end_date_payload(dag_run):
    return {
        "userUri": dag_run.conf['user_uri'],
        "dateRange": {
            "startDate": get_replicon_date_from_report_date(dag_run),
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }
