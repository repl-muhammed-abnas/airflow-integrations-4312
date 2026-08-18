from io import StringIO
from datetime import datetime
import pendulum
import rail
import pandas as pd
import numpy as np
from cie_crl.auto_approve_ts_and_to.utils import request_payload

error_uri = "urn:replicon:severity:error"
rejected_uri = "urn:replicon:approval-status:rejected"

def get_get_ts_to_be_approved():
    report_data = rail.result('run_report.get_report_result').get(
        'reportGenerationResults')[0].get('payload')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")
    timesheet_uris = df['Timesheet URI'].to_list()
    return timesheet_uris

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def get_submit_timesheet_payload(config):
    report_data = rail.result('run_report.get_report_result').get(
        'reportGenerationResults')[0].get('payload')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")

    if df.empty:
        return []

    df = df[df['Approval Status'] == 'Not Submitted']
    timesheetlist = list(set(df['Timesheet URI'].to_list()))
    timesheet_list = []
    for timedata_chunk in batch(timesheetlist, config.submit_chunk_size):
        timesheet_list.append({
        "timesheetUris": timedata_chunk,
        "comments": "Submitted by integration",
        "submitOptions": []
    })

    return timesheet_list


def get_validation_msg_timesheet_payload():
    report_data = rail.result('run_report.get_report_result').get(
        'reportGenerationResults')[0].get('payload')
    csvStringIO = StringIO(report_data)
    df = pd.read_csv(csvStringIO, sep=",")

    if df.empty:
        return {"timesheetUris": set()}

    ts_uris = set(df['Timesheet URI'].to_list())
    return {"timesheetUris": ts_uris}


def filter_rejected_timeoff_uris():
    timesheet_uris = []
    timesheet_to_status_data = rail.result('get_overlapping_timeoff_for_timesheet')

    for timesheet_details in timesheet_to_status_data:
        is_timeoff_rejected = False
        for timeoff_status in timesheet_details:
            if "approvalStatus" in timeoff_status.keys():
                if timeoff_status['approvalStatus']['uri'] == rejected_uri:
                    is_timeoff_rejected = True
                    break
        if not is_timeoff_rejected:
            timesheet_uris.append(timesheet_details['timesheetUri'])
    return {"timesheetUris": timesheet_uris}

def get_recalculate_timesheet_payload():
    return rail.result("filter_rejected_timeoff_uris")


def filter_uris_without_error():
    ts_uris_without_error, ts_uris_with_error = [], []

    ts_validation_data = rail.result('get_ts_validation_details')
    for ts_uri_detail in ts_validation_data:
        if ts_uri_detail['validationResult']['validationMessages']:
            is_error = False
            for item in ts_uri_detail['validationResult']['validationMessages']:
                if item['severity'] == error_uri:
                    is_error = True
                    break
            if is_error:
                ts_uris_with_error.append(ts_uri_detail['objectUri'])
            else:
                ts_uris_without_error.append(ts_uri_detail['objectUri'])
        else:
            ts_uris_without_error.append(ts_uri_detail['objectUri'])

    return ts_uris_without_error, ts_uris_with_error

def get_timesheet_uri_data(config):
    timeoff_report_artifact = rail.result('run_to_report.get_report_result')
    timeoff_csv_string = timeoff_report_artifact.get('reportGenerationResults')[0].get('payload')
    timeoffEntryDataReport = pd.read_csv(StringIO(timeoff_csv_string), sep=",")
    timeoffEntryDataReport = timeoffEntryDataReport.replace(np.nan, "")
    if not timeoffEntryDataReport.empty:
        timeoffEntryDataReport_groupby_user = timeoffEntryDataReport.groupby(
            ['User Uri'])
    timesheet_report_artifact = rail.result(
        'run_report_for_waiting_ts.get_report_result')
    timesheet_csv_string = timesheet_report_artifact.get(
        'reportGenerationResults')[0].get('payload')
    timesheetEntryDataReport = pd.read_csv(StringIO(timesheet_csv_string), sep=",")
    timesheetEntryDataReport = timesheetEntryDataReport.replace(np.nan, "")
    if not timesheetEntryDataReport.empty:
        timesheet_data = timesheetEntryDataReport.to_dict('records')

    final_timesheet_list = []
    for timedata_chunk in batch(timesheet_data, config.chunk_size):
        time_export_data = []
        for timedata in timedata_chunk:
            if timedata.get('Timesheet URI') not in time_export_data:
                if not timeoffEntryDataReport.empty and timedata.get('User Uri') in timeoffEntryDataReport_groupby_user.groups.keys():
                    timeoff_entries = timeoffEntryDataReport_groupby_user.get_group(
                        (timedata.get('User Uri')))
                    any_timeoff = len([entry for entry in timeoff_entries.to_dict('records') if datetime.strptime(
                        timedata['Timesheet Start Date'], config.report_date_format) <= datetime.strptime(entry['Booking Start Date'], config.report_date_format) and \
                            datetime.strptime(entry['Booking End Date'], config.report_date_format) <= datetime.strptime(timedata['Timesheet End Date'], config.report_date_format)])
                    if any_timeoff == 0:
                        time_export_data.append(timedata.get('Timesheet URI'))
                else:
                    time_export_data.append(timedata.get('Timesheet URI'))
        if time_export_data:
            final_timesheet_list.append(time_export_data)
    return final_timesheet_list

def filter_entries_log_properties():
    entry_details = rail.result('approve_timesheet')
    if entry_details:
        return {'entry_child_chunk_logs': entry_details}
    return {'entry_child_chunk_logs': {}}


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)

def get_error_logs():
    errored_logs = []
    all_logs = []
    error_logs_info = rail.result('gather_entry_child_data')
    for record in error_logs_info:
        errored_logs_from_child = get_data_from_document(record)
        errored_logs += errored_logs_from_child
    for reocrd in errored_logs:
        if reocrd.get('properties'):
            all_logs.append(reocrd.get('properties'))
    if len(all_logs) > 0:
        return all_logs[0]
    return all_logs

def get_entry_data_from_artifacts():
    artifacts_from_file = rail.result('get_merged_entries_logs')
    if artifacts_from_file:
        entry_data = rail.load_all_records(artifacts_from_file.get('log_artifact'))
    return entry_data


def get_user_time_data():
    approved_ts_list = []
    final_list_for_email = []
    entry_data_from_artifact = rail.result('get_merged_entries_logs')['entry_child_chunk_logs']

    if entry_data_from_artifact:
        approved_ts_list = entry_data_from_artifact.get('completedUris')
        if approved_ts_list:
            report_data = rail.result('run_report_for_waiting_ts.get_report_result').get('reportGenerationResults')[0].get('payload')
            csvStringIO = StringIO(report_data)
            df = pd.read_csv(csvStringIO, sep=",")
            mask = df['Timesheet URI'].isin(approved_ts_list)
            mask_df = df[mask]
            if mask_df.empty:
                return []

            grouped_data = mask_df.groupby(['User Supervisor Email address']).apply(
                lambda x: x.set_index('User Supervisor Email address').to_dict(orient='records')).to_dict()
            return [{"id":id, "data":data}  for id, data in grouped_data.items() if len(data) > 0]

    return final_list_for_email


def filter_error_uris(config, dag_run):
    batch_res = rail.result('get_bulk_validation')
    final_uris, error_uris = [], []
    payload_uris = dag_run.conf["item"]
    print("payload_uris", payload_uris)
    for res in batch_res:
        isErrorUriPresent = False
        if res.get('validationResult') and res['validationResult'].get('validationMessages'):
            for msg in res['validationResult']['validationMessages']:
                if msg.get('severity', '') == config.error_severity:
                    isErrorUriPresent = True
            if isErrorUriPresent:
                error_uris.append(res['objectUri'])

    print("error_uris", error_uris)
    print("payload_uris", payload_uris)
    final_uris = list(set(payload_uris) - set(error_uris))
    return final_uris