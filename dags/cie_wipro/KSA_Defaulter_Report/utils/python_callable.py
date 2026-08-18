# pylint: disable=too-many-statements, too-many-arguments, line-too-long, too-many-branches, chained-comparison, too-many-nested-blocks, unused-variable, unsupported-membership-test, singleton-comparison, no-else-return
from datetime import datetime, timedelta
import json
from io import StringIO
import pandas as pd
import numpy as np
import rail
import pendulum
import httplib2


def get_start_end_dateformat(config):
    # Get the current date
    current_date = get_eastern_timenow(config)

    # Calculate the first day of the current month
    first_day_of_current_month = current_date.replace(day=1)

    # Calculate the last day of the previous month
    end_date_previous_month = first_day_of_current_month - timedelta(days=1)

    # Calculate the first day of the previous month
    start_date_previous_month = end_date_previous_month.replace(
        day=1) - timedelta(days=config.booking_count - 1)
    return start_date_previous_month.strftime('%b %d, %Y'), end_date_previous_month.strftime('%b %d, %Y')


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


def get_defaulter_user_data(config):
    report_output = rail.load_json_artifact(
        rail.result("get_timeoff_booking_report_in_batch.get_report_result"))
    to_report_output = report_output.get('reportGenerationResults')[
        0].get('payload')
    to_report_df = pd.read_csv(
        StringIO(to_report_output), sep=config.seperator)
    to_report_df = to_report_df.replace(np.nan, "")
    final_data = []
    if not to_report_df.empty:
        filtered_report_df = to_report_df[to_report_df['Time Off Comments']
                                          == config.time_off_comments_value]
        filtered_report_df['entry_date'] = pd.to_datetime(filtered_report_df['Time Off Date'], format=config.date_format)
        to_groupby_user = filtered_report_df.groupby(
            'UserUri')
        defaulter_users = [uri for uri,
                           group in to_groupby_user if len(group) >= config.booking_count]
        if defaulter_users:
            booking_start_end_date = to_groupby_user.apply(lambda group: pd.Series({
                    'start_date': group.loc[group['entry_date'].idxmin(), 'Time Off Date'],
                    'end_date': group.loc[group['entry_date'].idxmax(), 'Time Off Date']
                })).reset_index()
        for uri in defaulter_users:
            data = to_groupby_user.get_group(
                uri).to_dict('records')[0] if to_groupby_user.get_group(uri).to_dict('records') else {}
            user_data = booking_start_end_date[booking_start_end_date['UserUri'] == uri]
            user_data = user_data.to_dict("records")[0] if not user_data.empty else {}
            if data:
                final_data.append({
                    "Employee ID": data.get("Employee ID"),
                    "Employee Name": data.get("User Name"),
                    "Company Code": data.get("Legal Entity (Current)"),
                    "Name of EE grp": data.get("Employee Type (Current)"),
                    "Name of EE subgroup": data.get("Employee Band"),
                    "Country": data.get("Country (Current)"),
                    "Start Date": user_data.get("start_date"),
                    "End date": user_data.get("end_date"),
                    "Reminder to Emp": "Yes",
                    "Reminder to Manager": "Yes",
                    "Reminder to HR": "Yes",
                    "Status": "Open"
                })

    return final_data


def find_no_efforts_entered(df, days):
    filtered_df = df[(df['Total Worked Hours'] == 0) &
                     (df['user_index'].isin(days))]
    return filtered_df


def group_data_by_notification_step():
    notification_df = pd.DataFrame.from_dict(rail.result('filter_report_data'))
    grouped_data = notification_df.groupby('user_index').apply(
        lambda x: x.to_dict(orient='records')).to_dict()

    return grouped_data


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
