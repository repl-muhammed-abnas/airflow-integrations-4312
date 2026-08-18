from pendulum import instance
from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid
import rail
null = None


'''
This list is for the position of the value in the approval status drop down list in report filter
    0 = Not Submitted
    1 = Waiting for Approval
    2 = Approved
    3 = Rejected
    4 = Submitting
As per requirement, whichever type of approval status timesheets are required in report result, these have to be added as values
for the ApprovalStatusFilter in the report filter configuration
'''
approval_status_list_for_timesheets_to_filter = [0]


def get_report_filters(report_filter_uris, integration_run_date_details, config):
    filter_values = []

    for status in approval_status_list_for_timesheets_to_filter:
        filter_values.append(
            {
                "reportFilterUri": report_filter_uris['approval_status_filter'],
                "value": status
            }
        )
    if integration_run_date_details['day'] < 16:
        timesheet_period_datetime = (datetime.strptime(
            integration_run_date_details['date'], config.DATE_FORMAT) - relativedelta(months=1))
        filter_values.extend([
            {
                "reportFilterUri": report_filter_uris['timesheet_priod_filter_uri'],
                "value": null
            },
            {
                "reportFilterUri": report_filter_uris['timesheet_priod_filter_uri'],
                "value": timesheet_period_datetime.replace(
                    day=16).strftime(config.DATE_FORMAT)
            },
            {
                "reportFilterUri": report_filter_uris['timesheet_priod_filter_uri'],
                "value": (instance(timesheet_period_datetime).end_of(
                    "month")).strftime(config.DATE_FORMAT)
            }
        ])
    if integration_run_date_details['day'] >= 16:
        timesheet_period_datetime = datetime.strptime(
            integration_run_date_details['date'], config.DATE_FORMAT)
        filter_values.extend([
            {
                "reportFilterUri": report_filter_uris['timesheet_priod_filter_uri'],
                "value": null
            },
            {
                "reportFilterUri": report_filter_uris['timesheet_priod_filter_uri'],
                "value": timesheet_period_datetime.replace(
                    day=1).strftime(config.DATE_FORMAT)
            },
            {
                "reportFilterUri": report_filter_uris['timesheet_priod_filter_uri'],
                "value": timesheet_period_datetime.replace(
                    day=15).strftime(config.DATE_FORMAT)
            }
        ])
    return filter_values


def get_report_generate_batch_payload(report_filter_uris, integration_run_date_details, config):
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")["uri"],
                "filterValues": get_report_filters(report_filter_uris, integration_run_date_details, config),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_repopulate_timesheet_payload(dag_run):
    return {
        "timesheet": {
            "uri": dag_run.conf["timesheet_uri"],
            "user": null,
            "date": null
        },
        "script": {
            "uri": dag_run.conf["script_uri"],
            "slug": null,
            "name": null
        },
        "overwrite": "true",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_enqueue_recalculate_script_payload(dag_run):
    return {
        "timesheet": {
            "uri": dag_run.conf["timesheet_uri"],
            "user": null,
            "date": null
        }
    }


def get_log_properties(item, dag_run, severity):
    matching_user_details_from_main_log = rail.find_first_by_attr_and_get_attr(
        dag_run.conf['items'], 'properties.timesheet_uri', (item if severity == 'success' else item['timesheetError']['timesheet']['uri']))
    return {
        'username': matching_user_details_from_main_log['properties']['username'] if bool(matching_user_details_from_main_log) else '',
        'employee_id': matching_user_details_from_main_log['properties']['employee_id'] if bool(matching_user_details_from_main_log) else '',
        'timesheeturi': item if severity == 'success' else item['timesheetError']['timesheet']['uri'],
        'status': ("Success" if item['timesheetError'] and item['timesheetError']['notifications'] and
                   item['timesheetError']['notifications'][0] and
                   item['timesheetError']['notifications'][0]['displayText'] == "Timesheet already submitted." else "Error") if (
            severity == 'error') else 'Success',
        'details': ("Timesheet already submitted" if item['timesheetError'] and item['timesheetError']['notifications'] and
                    item['timesheetError']['notifications'][0] and
                    item['timesheetError']['notifications'][0]['displayText'] == "Timesheet already submitted." else rail.render_template(
            "{{get_error_message()}}")) if severity == 'error' else 'Success',
    }
