# pylint: disable=broad-exception-raised line-too-long singleton-comparison no-else-return inconsistent-return-statements
from datetime import datetime, timedelta
from io import StringIO
import numpy as np
import pandas as pd
import rail
import pendulum

def findItemByDisplayText(response, report_name1, report_name2):
    report = {}
    report['timesheet_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name1, 'uri')
    report['timeoff_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name2, 'uri')
    if report.get('timesheet_report_uri') and report.get('timeoff_report_uri'):
        return report
    raise Exception('Unable to locate reports')


def get_filtered_timesheet_data():
    timesheet_report_artifact = rail.load_json_artifact(rail.result(
        'run_report_for_timsheet.get_report_result'))
    timeentry_csv_string = timesheet_report_artifact.get('reportGenerationResults')[
        0].get('payload')
    timeEntryData = pd.read_csv(StringIO(timeentry_csv_string), sep=",")
    df1 = timeEntryData.replace(np.nan, "")
    if not df1.empty:
        updated_dataframe = df1[df1['Match'] == True]
        return updated_dataframe.to_dict('records')
    else:
        return []


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def date_filter_uri(filter_name):
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_entry_report_details')['filterConfiguration']['enabledFilters'], 'displayText', filter_name, 'uri')


def getmin_max():

    unique_dates = rail.load_all_records(
        rail.result("query_unique_timesheet_period"))
    start_date = min(unique_dates, key=lambda d: datetime.strptime(
        d["Timesheet_Start_Date"], '%m/%d/%y'))
    end_date = max(unique_dates, key=lambda d: datetime.strptime(
        d["Timesheet_End_Date"], '%m/%d/%y'))
    min_dt = datetime.strptime(
        start_date["Timesheet_Start_Date"], '%m/%d/%y').strftime('%b %d, %Y')
    max_dt = datetime.strptime(
        end_date["Timesheet_End_Date"], '%m/%d/%y').strftime('%b %d, %Y')

    return {"start_date": min_dt, "end_date": max_dt}


def timeoff_date_filter_uri(filter_name, task_name):
    return rail.find_first_by_attr_and_get_attr(
        rail.result(task_name)['filterConfiguration']['enabledFilters'], 'displayText', filter_name, 'uri')


def get_timesheet_uri_data(config):
    timeoff_report_artifact = rail.load_json_artifact(rail.result(
        'run_report_for_timeoff.get_report_result'))
    timeoff_csv_string = timeoff_report_artifact.get(
        'reportGenerationResults')[0].get('payload')
    timeoffEntryDataReport = pd.read_csv(StringIO(timeoff_csv_string), sep=",")
    timeoffEntryDataReport = timeoffEntryDataReport.replace(np.nan, "")
    if not timeoffEntryDataReport.empty:
        timeoffEntryDataReport_groupby_user = timeoffEntryDataReport.groupby(
            ['UserUri'])
    timesheet_data = rail.result('get_eligible_timesheet_details')
    final_timesheet_list = []
    for timedata_chunk in batch(timesheet_data, config.chunk_size):
        time_export_data = []
        for timedata in timedata_chunk:
            if timedata.get('TimesheetURI') not in time_export_data:
                if not timeoffEntryDataReport.empty and timedata.get('UserUri') in timeoffEntryDataReport_groupby_user.groups.keys():
                    timeoff_entries = timeoffEntryDataReport_groupby_user.get_group(
                        (timedata.get('UserUri')))
                    any_timeoff = len([entry for entry in timeoff_entries.to_dict('records') if datetime.strptime(
                        timedata['Timesheet Start Date'], '%m/%d/%y') <= datetime.strptime(entry['Time Off Date'], '%m/%d/%y') and datetime.strptime(entry['Time Off Date'], '%m/%d/%y') <= datetime.strptime(timedata['Timesheet End Date'], '%m/%d/%y')])
                    if any_timeoff == 0:
                        time_export_data.append(timedata.get('TimesheetURI'))
                else:
                    time_export_data.append(timedata.get('TimesheetURI'))
        if time_export_data:
            final_timesheet_list.append(time_export_data)
    return final_timesheet_list


def task_state(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() == "failed" and task_instance.task_id == "approve_timesheet_retries":
            return False
    return True


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


def get_subject_line():
    logs = rail.result('get_logs')
    if logs:
        for record in logs:
            if record.get('status') == 'failed':
                return "completed with errors"
    return "completed successfully"

def check_reclac_status(config, item):

    ldm_utc = item["lastDataModification"]["valueInUtc"] if item["lastDataModification"] else None
    if ldm_utc:
        ldm = pendulum.datetime(int(ldm_utc["year"]), int(ldm_utc["month"]), int(ldm_utc["day"]),
                int(ldm_utc["hour"]), int(ldm_utc["minute"]), int(ldm_utc["second"])) + timedelta(milliseconds=int(ldm_utc["millisecond"]))

        lsa_utc = item["lastSuccessfulAttempt"]["valueInUtc"] if item["lastSuccessfulAttempt"] else None
        if lsa_utc:
            lsa = pendulum.datetime(int(lsa_utc["year"]), int(lsa_utc["month"]), int(lsa_utc["day"]),
                        int(lsa_utc["hour"]), int(lsa_utc["minute"]), int(lsa_utc["second"])) + timedelta(milliseconds=int(lsa_utc["millisecond"]))

            if (ldm < pendulum.now().subtract(hours=config.modification_window) and ldm < lsa):
                return item["timesheet"]["uri"]
    else:
        return item["timesheet"]["uri"]

