# pylint: disable=broad-exception-raised line-too-long singleton-comparison
from datetime import datetime
from io import StringIO
import csv
import pandas as pd
import numpy as np
import rail
import pendulum
from rail.lib.artifact import new_artifact


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def filter_ts_log_properties():
    ts_details = rail.result('get_ts_success_or_fail_count')
    if ts_details:
        return {'ts_child_chunk_logs': ts_details}
    return {'ts_child_chunk_logs': []}


def validate_csv_data(reader):
    for _ in reader:
        # read the whole file, just to make sure it is valid
        pass
    
    
def set_csv_attributes(artifact, headers=None, delimiter=',', encoding='utf-8'):
    artifact.set_attribute("type", "csv")
    artifact.set_attribute("csv_delimiter", delimiter)
    artifact.set_attribute("csv_encoding", encoding)
    artifact.set_attribute("csv_column", headers)


def write_artifact(df_to_csv_str):
    reader = csv.DictReader(StringIO(df_to_csv_str),
                            delimiter=',', fieldnames=None)
    validate_csv_data(reader)
    with new_artifact() as new:
        new.file.write(bytes(df_to_csv_str, 'utf-8'))
        set_csv_attributes(new)
    return new.name


def filter_success_fail_ts_count(response):
    output = []
    data = response.json()['d']
    for ts in data:
        startDate = ts.get("dateRange",{}).get("startDate")
        endDate = ts.get("dateRange",{}).get("endDate")
        output.append({ 'ts_id': ts.get("uri"), 'statusUri': ts.get("statusUri"), 'startDate': startDate, 'endDate': endDate})

    return output


def get_formated_timesheet_data(config):
    artifact1 = rail.result('run_report_for_entry.get_report_result')
    artifact1 = rail.load_json_artifact(artifact1)
    reponse1 = artifact1.get('reportGenerationResults')[0].get('payload')
    artifact2 = rail.result('run_report_for_timesheet.get_report_result')
    artifact2 = rail.load_json_artifact(artifact2)
    reponse2 = artifact2.get('reportGenerationResults')[0].get('payload')
    curr_time = get_eastern_timenow(config)
    
    config_params = rail.result("get_all_variables")
    report_date_format = config_params.get("report_date_format", "%m/%d/%Y")
    
    curr_datetime = str(datetime.strftime(curr_time, report_date_format))
    curr_datetime_obj = datetime.strptime(curr_datetime, report_date_format)
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
        if datetime.strptime(merged_report_data.get(pos).get('Timesheet Period').split('-')[-1].strip(), report_date_format) < curr_datetime_obj:
            if merged_report_data.get(pos).get('Entry Date'):
                if len(merged_report_data.get(pos).get('User End Date')) == 0 or (len(merged_report_data.get(pos).get('User End Date')) > 0 and datetime.strptime(merged_report_data.get(pos).get('Entry Date'), report_date_format) <= datetime.strptime(merged_report_data.get(pos).get('User End Date'), report_date_format)):
                    if not (merged_report_data.get(pos).get('Time Entry Approval Status') == 'Approved'):
                        not_eligible.add(merged_report_data.get(
                            pos).get('Timesheet URI'))
                    else:
                        eligible.add(merged_report_data.get(
                            pos).get('Timesheet URI'))
    final_list = [uri for uri in eligible if uri not in not_eligible]

    for uris in batch(final_list, config_params.get("chunk_size", 100)):
        output.append(uris)

    return output


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


def get_eastern_timenow(config):
    return pendulum.now(config.timezone)


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

        
def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False