# pylint: disable=broad-exception-raised line-too-long singleton-comparison no-else-return redefined-outer-name unused-variable
from datetime import timedelta, datetime
import json
from io import StringIO
from dateutil.relativedelta import relativedelta
import pendulum
import rail
import pandas as pd
import numpy as np


# def findItemByDisplayTextFromPayLoad(response, name):
#     for item in response.json()['d']:
#         if item['displayText'] == name:
#             return item['uri']
#     raise Exception('Unable to locate item {name}')


# def findItemByDisplayText(response, name):
#     for item in response:
#         if item['displayText'] == name:
#             return item['uri']
#     raise Exception('Unable to locate item {name}')


# def getReportUri(response, name):
#     responseString = response.replace("'", "\"")
#     responseJsonList = json.loads(responseString)
#     for item in responseJsonList:
#         if item['displayText'] == name:
#             return item['uri']
#     return None


def generate_csv_data_with_mapped_timeoff_uri():
    new_dict = {}
    all_timeoff_data = rail.result('get_all_time_off_types')
    for i in all_timeoff_data:
        new_dict[i['name']] = i['uri']
    # all_timeoff_data_df = pd.DataFrame.from_dict(all_timeoff_data)
    csv_str = rail.result('generate_base_report_in_batch.get_report_result').get(
        'reportGenerationResults')[0].get('payload')
    report_data_df = pd.read_csv(StringIO(csv_str), sep=",")
    report_data_df = report_data_df.replace(np.nan, "")
    report_data_df.columns = ["user_name", "user_uri",
                              "accrual_date", "hours_to_accrue", "time_off_type"]
    report_data_df['timeoff_uri'] = [new_dict.get(
        i) for i in report_data_df['time_off_type']]
    report_final_output = report_data_df[report_data_df['hours_to_accrue'] != ""]
    output = report_final_output.to_dict('records')
    return output


def group_data():
    final_list = []
    report_data = pd.DataFrame.from_dict(rail.result('get_report_data'))
    grouped_report_data = report_data.groupby(
        ['user_uri', 'time_off_type'])
    for group_name, df_group in grouped_report_data:
        final_list.append(df_group.to_dict('records'))
    return final_list


def getDataToBeProcessed(data):
    dataList = json.loads(data.replace("'", "\""))
    userUrisList = []

    for item in dataList:
        dateString = item["TimesheetEndDate"]
        datetime_object = datetime.strptime(dateString, '%b %d, %Y')

        hoursString = item["hours_to_accrue"]
        hoursDecimal = float(hoursString)
        hours = int(hoursDecimal)
        minutes = int((hoursDecimal*60) % 60)
        seconds = int((hoursDecimal*3600) % 60)
        milliseconds = 0
        microseconds = 0

        userUrisList.append({"UserUri": item["UserUri"], "Date": {
            "year": datetime_object.year,
            "month": datetime_object.month,
            "day": datetime_object.day
        }, "TimeToAccrue": {
            "hours": hours,
            "minutes": minutes,
            "seconds": seconds,
            "milliseconds": milliseconds,
            "microseconds": microseconds
        }})

    return userUrisList


def get_accrual_history_payload(dag_run):
    data = dag_run.conf['item']
    dates = rail.result('get_start_end_date')

    if dates and data:
        param = {
            "userUri": data[0].get('user_uri'),
            "timeOffTypeUri": data[0].get('timeoff_uri'),
            "dateRange": {
                "startDate": get_datetime_obj(dates.get('min_date')),
                "endDate": get_datetime_obj(dates.get('max_date')),
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
            }
        }

        return param
    return None


def get_min_max_date(dag_run):
    unique_dates = dag_run.conf['item']
    min_date = min(unique_dates, key=lambda d: datetime.strptime(
        d["accrual_date"], '%b %d, %Y'))
    max_date = max(unique_dates, key=lambda d: datetime.strptime(
        d["accrual_date"], '%b %d, %Y'))

    min_dt = datetime.strptime(
        min_date["accrual_date"], '%b %d, %Y').strftime('%m/%d/%Y')
    max_dt = datetime.strptime(
        max_date["accrual_date"], '%b %d, %Y').strftime('%m/%d/%Y')

    return {"min_date": min_dt, "max_date": max_dt}


def get_datetime_obj(date_str, fmt='%m/%d/%Y'):
    datetime_obj = datetime.strptime(date_str, fmt)
    return {
        'year': datetime_obj.year,
        'month': datetime_obj.month,
        'day': datetime_obj.day
    }


def get_data_for_processing(dag_run):
    final_list = []
    accrual_hours_bydate = {}
    data = dag_run.conf['item']
    accrual_history = rail.result('get_accrual_history')
    for entry in accrual_history:
        timespan = timedelta(**(accrual_history[0]['manualAccrualAdjustment']))
        total_hours = timespan.total_seconds() / 3600
        accrual_date = datetime(entry.get('date').get('year'), entry.get(
            'date').get('month'), entry.get('date').get('day')).strftime('%m/%d/%Y')
        accrual_hours_bydate[accrual_date] = total_hours
    for entry in data:
        date = datetime.strptime(
            entry["accrual_date"], '%b %d, %Y').strftime('%m/%d/%Y')
        hours = accrual_hours_bydate.get(date)
        if hours and entry["hours_to_accrue"] == hours:
            continue
        final_list.append(entry)

    return final_list


def get_error_logs():
    all_logs = []
    errored_logs = get_data_from_document(rail.result('create_log'))
    for record in errored_logs:
        if record.get('properties'):
            all_logs.append(record.get('properties'))
    return all_logs


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def final_status(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() == "failed" and \
                task_instance.task_id != kwargs['task_instance'].task_id:
            raise Exception(
                f"Task {task_instance.task_id} failed. Failing this DAG run")


def task_state(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() == "failed" and task_instance.task_id == "perform_accrual":
            return False
    return True


def get_hours_in_format(item):
    total_seconds = timedelta(
        seconds=item["hours_to_accrue"]*60*60).total_seconds()
    return {
        "hours": 0,
        "minutes": 0,
        "seconds": int(total_seconds),
        "milliseconds": 0,
    }


def get_accrual_params():
    item = rail.result('foreach_file_entry')
    if item:
        param = {
            "userUri": item["user_uri"],
            "timeOffTypeUri": item["timeoff_uri"],
            "date": get_datetime_obj(item["accrual_date"], fmt='%b %d, %Y'),
            "timeToAccrue": get_hours_in_format(item)}
        return param
    return None


def get_filter_fields(config):

    if config.user_report_filter_settings:
        return []
    else:
        now = pendulum.now(config.time_zone)
        startDate = (now - relativedelta(days=config.days_to_read_data)
                     ).strftime('%b, %d, %Y')
        endDate = now.strftime('%b, %d, %Y')
        filter_value = [
            {
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_timedata_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                "value": None,  # "LastTimesheetPeriod",
            },
            {
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_timedata_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                "value": startDate,  # None,
            },
            {
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_timedata_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                "value": endDate,  # None,
            },
        ]
        approval_dict = {
            "not submitted": "0",
            "waiting for approval": "1",
            "approved": "2",
            "rejected": "3",
            "submitting": "4"
        }
        approval_filters = config.approval_status.split(',')
        approval_filters_list = [approval_dict.get(
            value.lower()) for value in approval_filters if approval_dict.get(value.lower())]
        approval_filter_uri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_not_submitted_timesheet_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalStatusFilter", 'uri')
        for value in approval_filters_list:
            filter_value.append({
                "reportFilterUri": approval_filter_uri,
                "value": value,  # None,
            })
        return filter_value


def get_subject_details():
    if not rail.result('get_logs'):
        return "Run Completed With No Data"
    else:
        failure = rail.find_first_by_attr_and_get_attr(
            rail.result('get_logs'), 'status', "failed")
        error = rail.find_first_by_attr_and_get_attr(
            rail.result('get_logs'), 'status', "error")
        if failure or error:
            return "Run Completed With Errors"
        else:
            return "Run Completed Successfully"
