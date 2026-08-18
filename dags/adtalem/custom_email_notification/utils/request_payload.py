# pylint: disable=line-too-long
from datetime import datetime, timedelta
import json
import rail
null = None


def get_report1_params_for_reminder(dag_run):
    entrydatefilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_timesheet_period_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri')
    date = datetime.strftime(datetime.strptime(
        dag_run.conf['today'], "%d/%m/%Y"), "%m/%d/%Y")
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_all_report').get('timesheet_period_template'),
                "filterValues": [

                    {
                        "reportFilterUri": entrydatefilter,
                        "value": None,
                    },
                    {
                        "reportFilterUri": entrydatefilter,
                        "value": date,
                    },
                    {
                        "reportFilterUri": entrydatefilter,
                        "value": date,
                    },
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_report2_params_for_reminder():
    timesheetperiodFilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_not_submitted_timesheet_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "TimesheetPeriodFilter", 'uri')
    approvalstatusfilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_not_submitted_timesheet_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalStatusFilter", 'uri')
    start_date = rail.result('csv_data').get('Timesheet Start Date')
    end_date = rail.result('csv_data').get('Timesheet End Date')
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_all_report').get('report_notsubmitted_timesheets_uri'),
                "filterValues": [

                    {
                        "reportFilterUri": timesheetperiodFilter,
                        "value": None,
                    },
                    {
                        "reportFilterUri": timesheetperiodFilter,
                        "value": start_date,
                    },
                    {
                        "reportFilterUri": timesheetperiodFilter,
                        "value": end_date,
                    },
                    {
                        "reportFilterUri": approvalstatusfilter,
                        "value": "0"
                    },
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_report1_params1_for_pay(dag_run):
    entrydatefilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_timesheet_period_report1_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri')
    Current_Date = datetime.strptime(dag_run.conf['today'], "%d/%m/%Y")
    previous_date = Current_Date - timedelta(days=2)
    date = datetime.strftime(previous_date, "%m/%d/%Y")
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_all_report').get('repor1_uri'),
                "filterValues": [

                    {
                        "reportFilterUri": entrydatefilter,
                        "value": None,
                    },
                    {
                        "reportFilterUri": entrydatefilter,
                        "value": date,
                    },
                    {
                        "reportFilterUri": entrydatefilter,
                        "value": date,
                    },
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_report1_params2_for_pay(dag_run):
    entrydatefilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_timesheet_period_report2_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri')
    date = datetime.strftime(datetime.strptime(
        dag_run.conf['today'], "%d/%m/%Y"), "%m/%d/%Y")
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_all_report').get('repor1_uri'),
                "filterValues": [

                    {
                        "reportFilterUri": entrydatefilter,
                        "value": None,
                    },
                    {
                        "reportFilterUri": entrydatefilter,
                        "value": date,
                    },
                    {
                        "reportFilterUri": entrydatefilter,
                        "value": date,
                    },
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_report2_params_for_pay(dag_run):
    timesheetperiodFilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_not_submitted_timesheet_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "TimesheetPeriodFilter", 'uri')
    approvalstatusfilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_not_submitted_timesheet_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalStatusFilter", 'uri')
    start_date = rail.result('timesheet_period_report1_data').get('Timesheet Start Date') if dag_run.conf['type'].lower(
    ) == "regular" else rail.result('timesheet_period_report2_data').get('Timesheet Start Date')
    end_date = rail.result('timesheet_period_report1_data').get('Timesheet End Date')if dag_run.conf['type'].lower(
    ) == "regular" else rail.result('timesheet_period_report2_data').get('Timesheet End Date')
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_all_report').get('repor2_uri'),
                "filterValues": [

                    {
                        "reportFilterUri": timesheetperiodFilter,
                        "value": None,
                    },
                    {
                        "reportFilterUri": timesheetperiodFilter,
                        "value": start_date,
                    },
                    {
                        "reportFilterUri": timesheetperiodFilter,
                        "value": end_date,
                    },
                    {
                        "reportFilterUri": approvalstatusfilter,
                        "value": "0"
                    },
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_report2_params_for_paycheck(dag_run):
    timesheetperiodFilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_not_submitted_timesheet_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "TimesheetPeriodFilter", 'uri')
    approvalstatusfilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_not_submitted_timesheet_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalStatusFilter", 'uri')
    start_date = rail.result('timesheet_period_report1_data').get('Timesheet Start Date') if dag_run.conf['type'].lower(
    ) == "regular" else rail.result('timesheet_period_report2_data').get('Timesheet Start Date')
    end_date = rail.result('timesheet_period_report1_data').get('Timesheet End Date')if dag_run.conf['type'].lower(
    ) == "regular" else rail.result('timesheet_period_report2_data').get('Timesheet End Date')
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_all_report').get('repor2_uri'),
                "filterValues": [

                    {
                        "reportFilterUri": timesheetperiodFilter,
                        "value": None,
                    },
                    {
                        "reportFilterUri": timesheetperiodFilter,
                        "value": start_date,
                    },
                    {
                        "reportFilterUri": timesheetperiodFilter,
                        "value": end_date,
                    },
                    {
                        "reportFilterUri": approvalstatusfilter,
                        "value": "1"
                    },
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_regular_payload_sendemail(dag_run):
    subject = "Timesheet Reminder- Due Date is Monday, 12:00 p.m. CST"
    final_payload = {
        "email": {
            "to": [
                {
                    "user": {
                        "uri": dag_run.conf["useruri"],
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "cc": [
                {
                    "user": {
                        "uri": dag_run.conf["supervisoruri"],
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "bcc": [],
            "replyTo": null,
            "fromDisplayName": "Do-Not-Reply@deltek.com",
            "subject": subject,
            "htmlBody": rail.result('get_email_body'),
            "textBody": null,
            "attachments": []
        }
    }
    return json.dumps(final_payload)


def get_accelerated_payload_sendemail(dag_run):
    subject = f"ACCELERATED: Timesheet Reminder- Due Date is { rail.result('get_email_details').get('payrolldate') }, { rail.result('get_email_details').get('payrolldate_weekday') }, 12:00 p.m. CST"
    final_payload = {
        "email": {
            "to": [
                {
                    "user": {
                        "uri": dag_run.conf["useruri"],
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "cc": [
                {
                    "user": {
                        "uri": dag_run.conf["supervisoruri"],
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "bcc": [],
            "replyTo": null,
            "fromDisplayName": "Do-Not-Reply@deltek.com",
            "subject": subject,
            "htmlBody": rail.result('get_email_body'),
            "textBody": null,
            "attachments": []
        }
    }
    return json.dumps(final_payload)


def get_pay_payload_sendemail(dag_run):
    subject = "URGENT: Pay at RISK"
    final_payload = {
        "email": {
            "to": [
                {
                    "user": {
                        "uri": dag_run.conf["useruri"],
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "cc": [],
            "bcc": [],
            "replyTo": null,
            "fromDisplayName": "Do-Not-Reply@deltek.com",
            "subject": subject,
            "htmlBody": rail.result('get_email_body'),
            "textBody": null,
            "attachments": []
        }
    }
    return json.dumps(final_payload)


def get_paycheck_payload_sendemail(dag_run):
    subject = "URGENT: Pay Check at RISK"
    final_payload = {
        "email": {
            "to": [
                {
                    "user": {
                        "uri": dag_run.conf["supervisoruri"],
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "cc": [],
            "bcc": [],
            "replyTo": null,
            "fromDisplayName": "Do-Not-Reply@deltek.com",
            "subject": subject,
            "htmlBody": rail.result('get_email_body'),
            "textBody": null,
            "attachments": []
        }
    }
    return json.dumps(final_payload)
