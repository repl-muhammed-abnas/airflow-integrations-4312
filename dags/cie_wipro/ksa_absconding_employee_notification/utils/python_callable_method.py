from io import StringIO
from datetime import datetime, timedelta
import pendulum
import rail
import pandas as pd
import numpy as np


def get_timenow(config):
    return pendulum.now(config.time_zone)


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)

def get_merged_logs_data():
    errored_logs = []
    all_logs = []

    first_artifact_list = rail.result('gather_first_notification_data')
    second_artifact_list = rail.result('gather_second_notification_data')
    third_artifact_list = rail.result('gather_third_notification_data')
    forth_artifact_list = rail.result('gather_forth_notification_data')
    artifact_list = first_artifact_list + second_artifact_list + third_artifact_list + forth_artifact_list

    for record in artifact_list:
        errored_logs_from_child = get_data_from_document(record)
        errored_logs += errored_logs_from_child
    for reocrd in errored_logs:
        if reocrd.get('properties'):
            all_logs.append(reocrd.get('properties'))
    return all_logs

def _all_gap_days_non_working(older_dt, newer_dt, holiday_dates, weekend_days):
    """
    Walk every day strictly between older_dt (older) and newer_dt (more recent).
    Return True if every gap day is a weekend or a holiday.
    Returns True immediately when there are no gap days (day_diff == 1).
    """
    d = older_dt.date() + timedelta(days=1)
    end = newer_dt.date()
    while d < end:
        if d.strftime('%A').lower() not in weekend_days and d not in holiday_dates:
            return False
        d += timedelta(days=1)
    return True


def get_timeoff_report_data(config):
    final_list = []

    # Fetch and parse CSV data
    timeoff_report_artifact = rail.result('run_to_report.get_report_result')
    timeoff_csv_string = timeoff_report_artifact.get('reportGenerationResults')[0].get('payload')
    timeoff_df = pd.read_csv(StringIO(timeoff_csv_string), sep=";")

    holiday_report_artifact = rail.result('run_holiday_report.get_report_result')
    holiday_csv_string = holiday_report_artifact.get('reportGenerationResults')[0].get('payload')
    holiday_df = pd.read_csv(StringIO(holiday_csv_string), sep=";")

    # Derive weekend days from config work-week boundaries (once, before per-user loop)
    _all_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    _start = _all_days.index(config.weekStartDay)
    _end = _all_days.index(config.weekEndDay)
    _work_days, _i = set(), _start
    while True:
        _work_days.add(_all_days[_i % 7])
        if _i % 7 == _end:
            break
        _i += 1
    weekend_days = set(_all_days) - _work_days

    # Build holiday dates set (once, before per-user loop)
    holiday_dates = set()
    if not holiday_df.empty:
        holiday_df.replace(np.nan, '', inplace=True)
        holiday_df['Holiday_Date'] = pd.to_datetime(
            holiday_df['Date'], format=config.date_format, errors='coerce'
        )
        holiday_dates = set(
            holiday_df[holiday_df['Is Holiday'].str.strip().str.lower() == 'yes']['Holiday_Date'].dropna().dt.date
        )

    if timeoff_df.empty:
        return final_list

    # clean and filter data:
    timeoff_df.replace(np.nan, '', inplace=True)
    timeoff_df = timeoff_df[
        (timeoff_df['Time Off Comments'] == config.time_off_comments_value) & 
        (timeoff_df['Time Off Days'] == 1) & 
        (timeoff_df['Approval Status'] == 'Approved')
        ]

    # Resets the DataFrame index for cleaner iteration.
    timeoff_df = timeoff_df.reset_index(drop=True)
    timeoff_df['Booking_Datetime'] = pd.to_datetime(timeoff_df['Booked On'], format=config.date_format)
    timeoff_df['timeoff start date'] = pd.to_datetime(timeoff_df['Booking Start Date'], format=config.date_format)

    # Group by User
    df_group = timeoff_df.groupby('User Name')

    for user, value in df_group:
        sorted_df = value.sort_values(by=['Booking_Datetime'], ascending=[False])

        # Reset counter if today and yesterday both days doesn't have auto dec leave
        # for 1st reminder triggeres on 6th day after continous 5 day booking
        current_date = datetime.now().date()
        has_timeoff_today = any(sorted_df['Booking_Datetime'].dt.date == current_date)
        if not has_timeoff_today:
            first_booking_date = sorted_df.iloc[0]['Booking_Datetime'].date()
            dayDifference = abs((first_booking_date - current_date).days)
            if dayDifference > 1:
                continue

        counter, prevRow = 0, None
        prevTimeoffStartDate = None

        continousBookingStartDate, continousBookingEndDate = None, None
        for index, row in sorted_df.iterrows():
            timeoffStartDate = row['timeoff start date']

            if prevTimeoffStartDate is None:
                counter += 1
                continousBookingEndDate = timeoffStartDate
                prevTimeoffStartDate = timeoffStartDate
                continue

            if _all_gap_days_non_working(timeoffStartDate, prevTimeoffStartDate, holiday_dates, weekend_days):
                counter += 1
            else:
                # gap contains a working day — continuity broken
                break

            prevRow = row
            continousBookingStartDate = timeoffStartDate
            prevTimeoffStartDate = timeoffStartDate

        if prevRow is not None:
            prevRow['Booking_Datetime'] = ""
            prevRow['timeoff start date'] = ""
            prevRow['continousBookingStartDate'] = continousBookingStartDate.strftime('%d %b %Y')
            prevRow['continousBookingEndDate'] = continousBookingEndDate.strftime('%d %b %Y')
            prevRow['gpoUserUri'] = None
            prevRow['hrManagerUserUri'] = None

            # Append to final list based on consecutive absent days
            if has_timeoff_today:
                if counter in [6, 10, 13, 15]:
                    reminder_index = 0
                    if counter == 6:
                        reminder_index = config.firstReminder
                    elif counter == 10:
                        reminder_index = config.secondReminder
                    elif counter == 13:
                        reminder_index = config.thirdReminder
                    elif counter == 15:
                        reminder_index = config.forthReminder
                    
                    prevRow['reminder_index'] = reminder_index
                    final_list.append(dict(prevRow))
            else:
                if counter == 5:
                    prevRow['reminder_index'] = config.firstReminder
                    final_list.append(dict(prevRow))

    return final_list

def group_data_by_notification_step():
    notification_df = pd.DataFrame.from_dict(rail.result('update_timeoff_report_data'))
    grouped_data = notification_df.groupby('reminder_index').apply(
        lambda x: x.to_dict(orient='records')).to_dict()

    return grouped_data

def get_gpo_and_hrs_empid():
    notification_data = rail.result('get_timeoff_report_data')
    emp_ids_set = set()

    for entry in notification_data:
        if entry and entry.get("GPO ID") and str(entry.get("GPO ID")).strip():
            emp_ids_set.add(str(entry.get("GPO ID")))

        if entry and entry.get("HR Manager ID") and str(entry.get("HR Manager ID")).strip():
            emp_ids_set.add(str(entry["HR Manager ID"]))

    return list(emp_ids_set)

def get_gpo_and_hrs_uri(response):
    userUris = []
    data = response.json()['d']
    for entry in data:
        if entry and entry.get('uri'):
            userUris.append(entry['uri'])

    return userUris

def map_empid_to_uri(response):
    map_empId_to_uri = {}
    data = response.json()['d']
    for entry in data:
        if entry:
            empid = entry.get("employeeId")
            empuri = entry.get("uri")
            if empid and empuri:
                if empid not in map_empId_to_uri:
                    map_empId_to_uri[str(empid)] = empuri

    return map_empId_to_uri

def update_timeoff_report_data():
    notification_data = rail.result('get_timeoff_report_data')

    map_empid_and_uri = rail.result('get_gpo_and_manager_details')
    if not map_empid_and_uri:
        return notification_data
    
    for entry in notification_data:
        gpo_id = str(entry.get("GPO ID", "") or "").strip()
        if gpo_id and gpo_id in map_empid_and_uri:
            entry["gpoUserUri"] = map_empid_and_uri[gpo_id]

        hr_id = str(entry.get("HR Manager ID", "") or "").strip()
        if hr_id and hr_id in map_empid_and_uri:
            entry["hrManagerUserUri"] = map_empid_and_uri[hr_id]

    return notification_data
