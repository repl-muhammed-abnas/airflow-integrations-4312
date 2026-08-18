# pylint: disable=broad-exception-raised line-too-long singleton-comparison
from datetime import datetime
from io import StringIO
import csv
import json
import pandas as pd
import numpy as np
import rail
import pendulum
from rail.lib.artifact import new_artifact


def findItemByDisplayText(response, report_name1, report_name2):
    report = {}
    report['entry_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name1, 'uri')
    report['timesheet_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name2, 'uri')
    if report.get('entry_report_uri') and report.get('timesheet_report_uri'):
        return report
    raise Exception('Unable to locate reports')


def filter_deleted_uris(response):
    included_uris = []
    excluded_uris = []
    data = response.json()['d']
    for entry in data:
        if entry.get('error') == None:
            included_uris.append(entry.get('revisionGroupUri'))
        else:
            excluded_uris.append(entry.get('revisionGroupUri'))
    return {'included_uris': included_uris, 'excluded_uris': excluded_uris}


def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def get_formated_timesheet_data(config):
    artifact1 = rail.result('run_report_for_updated_entry.get_report_result')
    artifact1 = rail.load_json_artifact(artifact1)
    reponse1 = artifact1.get('reportGenerationResults')[0].get('payload')
    artifact2 = rail.result('run_report_for_timesheet.get_report_result')
    artifact2 = rail.load_json_artifact(artifact2)
    reponse2 = artifact2.get('reportGenerationResults')[0].get('payload')
    curr_time = get_eastern_timenow(config)
    curr_datetime = str(datetime.strftime(curr_time, "%m/%d/%Y"))
    curr_datetime_obj = datetime.strptime(curr_datetime, "%m/%d/%Y")
    output = []
    splitted_rows1 = reponse1.split('\r\n')
    reader1 = csv.DictReader(splitted_rows1, delimiter=',')
    reader_list1 = list(reader1)

    splitted_rows2 = reponse2.split('\r\n')
    reader2 = csv.DictReader(splitted_rows2, delimiter=',')
    reader_list2 = list(reader2)
    not_eligible = set()
    eligible = set()
    df1 = pd.DataFrame.from_records(reader_list1)
    df2 = pd.DataFrame.from_records(reader_list2)
    merged_df = pd.merge(df2, df1, how ='left', on=['Timesheet Period', 'UserUri'])
    merged_df = merged_df.replace(np.nan, "")
    data = merged_df.to_dict('records')
    merged_report_data = dict(enumerate(data))
    for pos in merged_report_data:
        if datetime.strptime(merged_report_data.get(pos).get('Timesheet Period').split('-')[-1].strip(), "%b %d, %Y") < curr_datetime_obj:
            if len(merged_report_data.get(pos).get('User End Date')) == 0 or (len(merged_report_data.get(pos).get('User End Date')) > 0 and datetime.strptime(merged_report_data.get(pos).get('Entry Date'), "%b %d, %Y") <= datetime.strptime(merged_report_data.get(pos).get('User End Date'), "%b %d, %Y")):
                if ((merged_report_data.get(pos).get('Time Entry Approval Status') == 'Not Submitted' or merged_report_data.get(pos).get('Time Entry Approval Status') == 'Waiting For Approval')
                        and isfloat(merged_report_data.get(pos).get('Hours')) or merged_report_data.get(pos).get('Approval Status') == 'Approved'):
                    not_eligible.add(merged_report_data.get(
                        pos).get('TimesheetURI'))
                else:
                    eligible.add(merged_report_data.get(
                        pos).get('TimesheetURI'))
    final_list = [uri for uri in eligible if uri not in not_eligible]
    for uris in batch(final_list, 100):
        output.append(uris)
    return output


def get_formated_entry_data(reponse, config):
    country = config.location
    output = []
    date_format = "%b %d, %Y"
    report_output = rail.load_json_artifact(reponse)
    csv_string = report_output.get('reportGenerationResults')[0].get('payload')
    df = pd.read_csv(StringIO(csv_string), sep=",")
    uri_initial = 'urn:replicon-tenant:' + \
        rail.get_tenant_slug() + ':time-entry-revision-group:'
    df["time_entry_uri"] = uri_initial + df["Entry ID"]
    df = df.replace(np.nan, "")
    df['Entry Date'] = pd.to_datetime(df['Entry Date'], format=date_format)
    df['User End Date'] = pd.to_datetime(
        df['User End Date'], format=date_format, errors='coerce')  # Coerce invalid dates to NaT
    filtered_df = df[(df['User End Date'].isna()) | (
        df['Entry Date'] <= df['User End Date'])]
    filtered_df['Entry Work Location'] = filtered_df.apply(
        lambda row: row['Work Location'] if row['Entry Work Location'] == "" else row['Entry Work Location'], axis=1)
    pattern = '|'.join(config.india_sub_string)
    if not filtered_df.empty and "India".lower() == country.lower() and rail.result('entry_by_request_or_time').get('to_run') == True and len(rail.result('entry_by_request_or_time').get('position')) == 0:
        country_filtered_df = filtered_df[filtered_df['Entry Work Location'].str.contains(
            pattern)]
    elif not filtered_df.empty and rail.result('entry_by_request_or_time').get('to_run') == True and len(rail.result('entry_by_request_or_time').get('position')) == 0:
        country_filtered_df = filtered_df[~filtered_df['Entry Work Location'].str.contains(
            pattern)]
    else:
        country_filtered_df = filtered_df

    if not country_filtered_df.empty:
        for uris in batch(country_filtered_df["time_entry_uri"].tolist(), config.chunk_size):
            output.append(uris)
    return output


def check_for_request(reponse, time, config):
    json_object = json.loads(reponse)
    output = {'to_run': False, 'status': 'NotStarted', 'position': ""}
    hours, minutes = config.schedule_time.split(":")
    tz = pendulum.timezone(config.timezone)
    set_time = tz.datetime(time.year, time.month,
                           time.day, int(hours), int(minutes))
    timespan = time - set_time
    if 0 <= timespan.total_minutes() < config.master_dag_interval:
        output = {'to_run': True, 'status': 'NotStarted', 'position': ""}
    if json_object.get('status'):
        for key in range(len(json_object.get('status').items())):
            if json_object.get('status')[str(key)].lower() == "notstarted":
                output = {'to_run': True,
                          'status': 'Started', 'position': str(key)}
                break
    return output


def update_request_status(csv_data):
    status = rail.result('entry_by_request_or_time')
    if status.get('status').lower() == 'started':
        json_object = json.loads(csv_data)
        json_object.get('status').update({status.get('position'): "Completed"})
        return json.dumps(json_object, indent=4)
    return None


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def get_eastern_timenow(config):
    return pendulum.now(config.timezone)


def set_csv_attributes(artifact, headers=None, delimiter=',', encoding='utf-8'):
    artifact.set_attribute("type", "csv")
    artifact.set_attribute("csv_delimiter", delimiter)
    artifact.set_attribute("csv_encoding", encoding)
    artifact.set_attribute("csv_column", headers)


def validate_csv_data(reader):
    for _ in reader:
        # read the whole file, just to make sure it is valid
        pass


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def write_artifact(df_to_csv_str):
    reader = csv.DictReader(StringIO(df_to_csv_str),
                            delimiter=',', fieldnames=None)
    validate_csv_data(reader)
    with new_artifact() as new:
        new.file.write(bytes(df_to_csv_str, 'utf-8'))
        set_csv_attributes(new)
    return new.name

def get_entry_errror_logs():
    errored_logs = []
    all_logs = []
    error_logs_info = rail.result('gather_entry_child_data')
    for record in error_logs_info:
        errored_logs_from_child = get_data_from_document(record)
        errored_logs += errored_logs_from_child
    for reocrd in errored_logs:
        if reocrd.get('properties'):
            all_logs.append(reocrd.get('properties'))

    all_child_logs_data = []
    for child_logs in all_logs:
        for item in child_logs.get('entry_child_chunk_logs', []):
            all_child_logs_data.append(item)

    if not all_child_logs_data:
        return {'log_artifact':"", 'has_data': False}

    df = pd.DataFrame(all_child_logs_data)
    df_to_csv_str = df.to_csv(index=False)
    artifact = write_artifact(df_to_csv_str)

    return {'log_artifact':artifact, 'has_data': True}

def get_ts_errror_logs():
    errored_logs = []
    all_logs = []
    error_logs_info = rail.result('gather_ts_child_data')
    for record in error_logs_info:
        errored_logs_from_child = get_data_from_document(record)
        errored_logs += errored_logs_from_child
    for reocrd in errored_logs:
        if reocrd.get('properties'):
            all_logs.append(reocrd.get('properties'))

    all_child_logs_data = []
    for child_logs in all_logs:
        for item in child_logs.get('ts_child_chunk_logs', []):
            all_child_logs_data.append(item)

    if not all_child_logs_data:
        return {'log_artifact':"", 'has_data': False}

    df = pd.DataFrame(all_child_logs_data)
    df_to_csv_str = df.to_csv(index=False)
    artifact = write_artifact(df_to_csv_str)

    return {'log_artifact':artifact, 'has_data': True}


def filter_success_fail_entries_count(response):
    output = []
    data = response.json()['d']
    for entry in data:
        approval_status = entry.get('details') and entry["details"].get("approvalStatus") and entry["details"]["approvalStatus"].get("displayText")
        output.append({ 'entry_id': entry.get("revisionGroupUri"), 'status': approval_status})
    return output

def filter_success_fail_ts_count(response):
    output = []
    data = response.json()['d']
    for ts in data:
        startDate = ts.get("dateRange",{}).get("startDate")
        endDate = ts.get("dateRange",{}).get("endDate")
        output.append({ 'ts_id': ts.get("uri"), 'statusUri': ts.get("statusUri"), 'startDate': startDate, 'endDate': endDate})

    return output

def filter_etnries_log_properties():
    entry_details = rail.result('get_success_or_fail_count')
    if entry_details:
        return {'entry_child_chunk_logs': entry_details}
    return {'entry_child_chunk_logs': []}

def filter_ts_log_properties():
    ts_details = rail.result('get_ts_success_or_fail_count')
    if ts_details:
        return {'ts_child_chunk_logs': ts_details}
    return {'ts_child_chunk_logs': []}

def create_ts_logs_str():
    artifacts_from_file = rail.result('get_ts_status_file_from_s3')
    current_run_artifact_uri = rail.result('get_merged_ts_logs').get("log_artifact")
    current_run_df = pd.DataFrame.from_dict({'artifacts_uris': [current_run_artifact_uri]})

    if artifacts_from_file:
        json_data = json.loads(artifacts_from_file)
        previous_logs_df = pd.DataFrame.from_dict(json_data)
        frames = [previous_logs_df, current_run_df]
        current_run_df = pd.concat(frames)

    return current_run_df.to_json(orient="records")

def create_entry_logs_str():
    artifacts_from_file = rail.result('get_entry_status_file_from_s3')
    current_run_artifact_uri = rail.result('get_merged_entries_logs').get("log_artifact")
    current_run_df = pd.DataFrame.from_dict({'entry_artifacts_uris': [current_run_artifact_uri]})

    if artifacts_from_file:
        json_data = json.loads(artifacts_from_file)
        previous_logs_df = pd.DataFrame.from_dict(json_data)
        frames = [previous_logs_df, current_run_df]
        current_run_df = pd.concat(frames)

    return current_run_df.to_json(orient="records")

def get_ts_data_from_artifacts():
    artifacts_from_file = rail.result('download_ts_details_from_file')
    total_uniq_approved_ts_uris, total_uniq_failed_ts_uris = [], []
    ts_data_list = []
    if artifacts_from_file:
        artifact_list = json.loads(artifacts_from_file).get("artifacts_uris")

        for _, artifact in artifact_list.items():
            ts_data = rail.load_all_records(artifact)
            ts_data_list.extend(ts_data)

    if ts_data_list:
        ts_data_df = pd.DataFrame.from_dict(ts_data_list)

        total_approved_ts = ts_data_df[ts_data_df['statusUri'].str.contains('urn:replicon:timesheet-status:approved')]
        total_failed_ts = ts_data_df[~ts_data_df['statusUri'].str.contains('urn:replicon:timesheet-status:approved')]

        total_uniq_approved_ts_uris = total_approved_ts['ts_id'].unique()
        total_uniq_failed_ts_uris = total_failed_ts['ts_id'].unique()

    update_file_df = pd.DataFrame()
    update_file_df = update_file_df.to_json(orient="records")

    return {
        "weekly_approved_ts_count": len(total_uniq_approved_ts_uris), 
        "weekly_failed_ts_count": len(total_uniq_failed_ts_uris), 
        "remove_file_content": update_file_df
        }

def get_entry_data_from_artifacts():
    artifacts_from_file = rail.result('download_entry_details_from_file')
    total_uniq_approved_entry_uris, total_uniq_failed_entry_uris = [], []
    entry_data_list = []
    if artifacts_from_file:
        artifact_list = json.loads(artifacts_from_file).get("entry_artifacts_uris")

        for _, artifact in artifact_list.items():
            entry_data = rail.load_all_records(artifact)
            entry_data_list.extend(entry_data)

    if entry_data_list:
        entry_data_df = pd.DataFrame.from_dict(entry_data_list)

        total_approved_entries = entry_data_df[entry_data_df['status'].str.contains('Approved')]
        total_failed_entries = entry_data_df[~entry_data_df['status'].str.contains('Approved')]

        total_uniq_approved_entry_uris = total_approved_entries['entry_id'].unique()
        total_uniq_failed_entry_uris = total_failed_entries['entry_id'].unique()

    update_file_df = pd.DataFrame()
    update_file_df = update_file_df.to_json(orient="records")

    return {
        "daily_approved_entry_count": len(total_uniq_approved_entry_uris), 
        "daily_failed_entry_count": len(total_uniq_failed_entry_uris), 
        "remove_file_content": update_file_df
        }
