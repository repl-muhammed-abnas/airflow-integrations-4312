from datetime import datetime
import rail


def get_formatted_date():
    us_records = rail.load_all_records(
        rail.result('query_list_for_us'))
    gb_records = rail.load_all_records(
        rail.result('query_list_for_gb'))

    us_timesheet_enddate = us_records[0][
        'timesheetenddate'] if us_records else None
    gb_timesheet_enddate = gb_records[0][
        'timesheetenddate'] if gb_records else None

    if us_timesheet_enddate:
        date_obj = datetime.strptime(us_timesheet_enddate, '%b %d, %Y').date()
        date_str = date_obj.strftime("%m%d")
    elif gb_timesheet_enddate:
        date_obj = datetime.strptime(gb_timesheet_enddate, '%b %d, %Y').date()
        date_str = date_obj.strftime("%m%d")
    else:
        date_str = None

    return date_str


def get_activity_code(activity):
    code = float(rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_locations')[
            'locationlistinput'], 'Location',
        rail.load_all_records(rail.result('query_list_for_us'))[0]['Location'], activity, ''))
    return code


def get_activity_code_data(data):
    value = float(rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_locations')[
            'locationlistinput'], 'Location',
        rail.load_all_records(rail.result('query_list_for_gb'))[0]['Location'], data, ''))
    return value


def get_modified_list_data(us_list):
    us_data = 'USD' + datetime.now().strftime("%y%m%d") + datetime.strptime(us_list,
                                                                            '%b %d, %Y').date().strftime('%y%m%d').ljust(12, ' ')
    return us_data


def get_modified_data(gb_list):
    gb_data = 'GBP' + datetime.now().strftime("%y%m%d") + datetime.strptime(gb_list,
                                                                            '%b %d, %Y').date().strftime('%y%m%d').ljust(12, ' ')
    return gb_data


def get_comments(comments, username):
    result_str = ((str(comments)[:126-len(username)] + "(" + str(username) + ")").ljust(128, " ")
                  if len(username) + len(comments) + 2 > 128
                  else (str(comments) + "(" + str(username) + ")").ljust(128, " "))if comments else ("(" + str(username) + ")").ljust(128, " ")

    return result_str


def get_comments_data(list_comments, list_username):
    result = ((str(list_comments)[:126-len(list_username)] +
               "(" + str(list_username) + ")").ljust(128, " ") if len(list_username) + len(list_comments) + 2 > 128
              else (str(list_comments) + "(" + str(list_username) + ")").ljust(128, " "))if list_comments else ("(" + str(list_username) + ")").ljust(128, " ")

    return result


def get_calculated_hours(workedhours, activitycodevalue, location_value):
    hours_worked = float(workedhours)
    activity_value = get_activity_code_data(
        'activity1value') if activitycodevalue == "OT-Client" else get_activity_code_data('activity2value')
    activity_multiplier = float(activity_value) if activitycodevalue else 1
    location_multiplier = activity_multiplier if location_value else 1
    total_hours = round(
        hours_worked * location_multiplier, 2)

    return str(total_hours).rjust(11, " ") + "N"


def get_calculated_working_hours(hoursworked, activitycode, location):
    worked_hours = float(hoursworked)
    activity_data = get_activity_code(
        'activity1value') if activitycode == "OT-Client" else get_activity_code('activity2value')
    activity_value_multiplier = float(activity_data) if activitycode else 1
    location_value_multiplier = activity_value_multiplier if location else 1
    total_worked_hours = round(
        worked_hours * location_value_multiplier, 2)

    return str(total_worked_hours).rjust(11, " ") + "N"


def remove_quotes():
    file_in_string = rail.read_artifact(
        rail.result('create_csv_for_us'))
    return file_in_string.replace('"', '')


def remove_quote():
    file = rail.read_artifact(
        rail.result('create_csv_for_gb'))
    return file.replace('"', '')


def do_merge_csv_data():
    data_us = rail.load_all_records(rail.result(
        'query_list_for_us'))
    data_gb = rail.load_all_records(rail.result('query_list_for_gb'))
    return data_us+data_gb
