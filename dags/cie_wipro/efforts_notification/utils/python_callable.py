# pylint: disable=too-many-statements, too-many-arguments, line-too-long, too-many-branches, chained-comparison, too-many-nested-blocks, unused-variable, unsupported-membership-test, singleton-comparison, no-else-return
from datetime import datetime
import json
from io import StringIO
import pandas as pd
import numpy as np
import rail
import pendulum
import httplib2


def get_eastern_timenow(config):
    return pendulum.now(config.instance_tz)


def findItemByDisplayText(response, report_name1):
    report = {}
    report['effort_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name1, 'uri')
    if report.get('effort_report_uri'):
        return report
    raise Exception('Unable to locate reports')


def get_timeo(response, report_name1):
    report = {}
    report['effort_report_uri'] = rail.find_first_by_attr_and_get_attr(
        response.json()['d'], 'displayText', report_name1, 'uri')
    if report.get('effort_report_uri'):
        return report
    raise Exception('Unable to locate reports')


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def get_report_data(config, days, start_datetime, current_datetime):
    schedule_report_output = rail.load_json_artifact(
        rail.result("get_schedule_report_in_batch.get_report_result"))
    schedule_report_output = schedule_report_output.get('reportGenerationResults')[
        0].get('payload')
    schedule_report_df = pd.read_csv(
        StringIO(schedule_report_output), sep=config.seperator)
    fill_values = {'Time Off Hrs': 0, 'Hours Worked': 0, 'Scheduled Hrs': 0}
    schedule_report_df.fillna(fill_values, inplace=True)
    if not schedule_report_df.empty:
        schedule_report_df.fillna(fill_values, inplace=True)
        schedule_report_df['Total Worked Hours'] = schedule_report_df['Hours Worked'] + \
            schedule_report_df['Time Off Hrs']
        schedule_report_df = schedule_report_df.replace(np.nan, "")

        schedule_report_df['Entry Date'] = pd.to_datetime(
            schedule_report_df['Entry Date'], format=config.date_format, errors='coerce')
        schedule_report_df['Entry Date'] = pd.to_datetime(
            schedule_report_df['Entry Date'])

        start_date = pd.to_datetime(start_datetime.strftime(
            config.date_format), format=config.date_format)
        end_date = pd.to_datetime(current_datetime.strftime(
            config.date_format), format=config.date_format)
        all_dates = pd.date_range(start=start_date, end=end_date)
        users = schedule_report_df['user_uri'].unique()
        all_combinations = pd.MultiIndex.from_product(
            [users, all_dates], names=['user_uri', 'Entry Date'])
        schedule_report_df.set_index(['user_uri', 'Entry Date'], inplace=True)
        schedule_report_df = schedule_report_df.reindex(
            all_combinations, fill_value=0).reset_index()
        sorted_data = schedule_report_df.sort_values(by='Entry Date', ascending=False).groupby(
            'user_uri').apply(lambda x: x.reset_index(drop=True)).reset_index(drop=True)
        sorted_data['user_index'] = sorted_data.groupby(
            'user_uri').cumcount() + 1
        final_df = find_no_efforts_entered(
            sorted_data, days, config.minimum_efforts)
        # final_df.drop(columns=['date'], inplace=True)
        final_df['Entry Date'] = final_df['Entry Date'].dt.strftime(
            config.date_format)
        return final_df.to_dict('records')
    else:
        return []


def find_no_efforts_entered(df, days, minimum_efforts):
    filtered_df = df[((df['Total Worked Hours'] == 0) | (df['Hours Worked'] < minimum_efforts)) & (df['Scheduled Hrs'] > 0) &
                     (df['user_index'].isin(days))]
    return filtered_df


def group_data_by_notification_step():
    notification_df = pd.DataFrame.from_dict(rail.result('filter_report_data'))
    if not notification_df.empty:
        grouped_data = notification_df.groupby('user_index').apply(
            lambda x: x.to_dict(orient='records')).to_dict()
        return grouped_data
    return []


def send_failure_alert(config, msg):
    url = config.chat_webhook_url
    app_message = {"text": msg}
    message_headers = {"Content-Type": "application/json; charset=UTF-8"}
    http_obj = httplib2.Http()
    response = http_obj.request(
        uri=url,
        method="POST",
        headers=message_headers,
        body=json.dumps(app_message),
    )


def get_replicon_date(dag_run, config):
    print(dag_run)
    if not dag_run['Entry Date']:
        return None
    # date format in 20060401
    try:
        _date = datetime.strptime(
            dag_run['Entry Date'], config.date_format)
        return {
            "day": _date.day,
            "month": _date.month,
            "year": _date.year
        }
    except:  # pylint: disable=bare-except
        return None


def get_merged_logs_data():
    errored_logs = []
    all_logs = []
    time_data_final_export = []
    first_artifact_list = rail.result('gather_first_child_data')
    third_artifact_list = rail.result('gather_final_child_data')
    artifact_list = first_artifact_list + third_artifact_list
    for record in artifact_list:
        errored_logs_from_child = get_data_from_document(record)
        errored_logs += errored_logs_from_child
    for reocrd in errored_logs:
        if reocrd.get('properties'):
            all_logs.append(reocrd.get('properties'))
    return all_logs


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)
