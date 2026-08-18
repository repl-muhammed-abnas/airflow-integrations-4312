from datetime import date, datetime, timedelta
import pendulum
import rail
from sqlalchemy import null

input_date_format = "%d/%m/%Y"


def validate_current_import_files():
    return True


def convert_to_date(str_date, date_format="%b %d, %Y"):
    try:
        if not str_date:
            return None
        return datetime.strptime(str_date, date_format)
    except:  # pylint: disable=bare-except
        return null


def get_week_patterns(user_uri):
    wk1_pattern, wk2_pattern, combined_pattern = None, None, None
    user_wk1_pattern, user_wk2_pattern, user_shift_name, user_start_date = None, None, None, None
    user_data = rail.load_all_records(rail.result('query_userdata'))
    filtered_user_info = list(
        filter(lambda item: item['user_uri'] == user_uri, user_data))
    if len(filtered_user_info) > 0:
        user_wk1_pattern = filtered_user_info[0]['user_wk1_pattern']
        user_wk2_pattern = filtered_user_info[0]['user_wk2_pattern']
        user_shift_name = filtered_user_info[0]['user_shift_name']
        user_shift_name = user_shift_name.split('|')[0] if user_shift_name else None
        user_start_date = filtered_user_info[0]['user_start_date']

    if not user_wk1_pattern and user_wk2_pattern:
        wk1_pattern = "No Assignment"
    elif user_wk1_pattern and not user_wk2_pattern:
        wk2_pattern = user_wk1_pattern
    elif not user_wk1_pattern and not user_wk2_pattern:
        combined_pattern = "Mon/Tue/Wed/Thu/Fri"

    return {
        "week1pattern": wk1_pattern,
        "week2pattern": wk2_pattern,
        "combinedpattern": combined_pattern,
        "user_shift_name": user_shift_name,
        "user_wk1_pattern": user_wk1_pattern,
        "user_wk2_pattern": user_wk2_pattern,
        "user_start_date": user_start_date
    }


def get_derived_patterns(pattern_info):
    week1_pattern_derived, week2_pattern_derived, start_date_derived = None, None, None
    if pattern_info['week1pattern']:
        week1_pattern_derived = pattern_info['week1pattern']
    else:
        week1_pattern_derived = pattern_info['user_wk1_pattern'].split(
            ".")[-1] if pattern_info['user_wk1_pattern'] else pattern_info['combinedpattern']
    if pattern_info['week2pattern']:
        week2_pattern_derived = pattern_info['week2pattern']
    else:
        week2_pattern_derived = pattern_info['user_wk2_pattern'].split(
            ".")[-1] if pattern_info['user_wk2_pattern'] else pattern_info['combinedpattern']
    shift_name_derived = pattern_info['user_shift_name'].split(
        "|")[0] if pattern_info['user_shift_name'] else "Default Office Schedule - 7.6hrs"
    user_shift_start_date = convert_to_date(
        pattern_info['user_start_date']) if pattern_info['user_start_date'] else None
    day_of_week_start_date = user_shift_start_date.strftime("%A")

    if "Saturday" == day_of_week_start_date:
        start_date_derived = user_shift_start_date
    elif "Sunday" == day_of_week_start_date:
        start_date_derived = user_shift_start_date - timedelta(days=1)
    else:
        day_number = user_shift_start_date.weekday()
        begin_of_week = user_shift_start_date - timedelta(days=day_number)
        start_date_derived = begin_of_week - timedelta(days=2)
    return week1_pattern_derived, week2_pattern_derived, start_date_derived, shift_name_derived


def get_shift_effective_dates(to_info):
    pattern_info = get_week_patterns(to_info['user_uri'])
    booking_start_date = datetime.strptime(
        to_info['start_date'], '%Y%m%d')
    booking_end_date = datetime.strptime(
        to_info['end_date'], '%Y%m%d')
    week1_pattern_derived, week2_pattern_derived, start_date_derived, shift_name_derived = get_derived_patterns(
        pattern_info)

    booking_end_day = booking_end_date.strftime("%A")
    booking_start_day = booking_start_date.strftime("%A")
    start_week_for_booking_start_date = None
    if "Saturday" == booking_start_day:
        start_week_for_booking_start_date = booking_start_date - \
            timedelta(days=0)
    elif "Sunday" == booking_start_day:
        start_week_for_booking_start_date = booking_start_date - \
            timedelta(days=1)
    else:
        day_number = booking_start_date.weekday()
        begin_of_week = booking_start_date - timedelta(days=day_number)
        start_week_for_booking_start_date = begin_of_week - timedelta(days=2)
    start_week_for_booking_end_date = None
    if "Saturday" == booking_end_day:
        start_week_for_booking_end_date = booking_end_date - timedelta(days=0)
    elif "Sunday" == booking_end_day:
        start_week_for_booking_end_date = booking_end_date - timedelta(days=1)
    else:
        day_number = booking_end_date.weekday()
        begin_of_week = booking_end_date - timedelta(days=day_number)
        start_week_for_booking_end_date = begin_of_week - timedelta(days=2)
    end_week_for_booking_end_date = start_week_for_booking_end_date + \
        timedelta(days=6)
    differance_start_and_end_week = (end_week_for_booking_end_date -
                                     start_week_for_booking_start_date).days
    number_of_days = range(differance_start_and_end_week + 1)
    final_effective_date = []
    for day in number_of_days:
        effective_date_by_range = start_week_for_booking_start_date + \
            timedelta(days=day)
        effective_date_by_range_day = effective_date_by_range.strftime("%A")
        start_week_for_effective_date = None
        if "Saturday" == effective_date_by_range_day:
            start_week_for_effective_date = effective_date_by_range - \
                timedelta(days=0)
        elif "Sunday" == effective_date_by_range_day:
            start_week_for_effective_date = effective_date_by_range - \
                timedelta(days=1)
        else:
            day_number = effective_date_by_range.weekday()
            begin_of_week = effective_date_by_range - \
                timedelta(days=day_number)
            start_week_for_effective_date = begin_of_week - timedelta(days=2)
        differance_start_week_start_week_derived = (
            start_week_for_effective_date - start_date_derived).days / 86400 / 7
        week_number = "week2" if differance_start_week_start_week_derived % 2 else "week1"
        pattern_derived = week1_pattern_derived if week_number == "week1" else week2_pattern_derived
        final_effective_date.append({
            "effective_date": effective_date_by_range.strftime('%Y%m%d'),
            "pattern": pattern_derived,
            "shift_name": shift_name_derived,
            "start_date": to_info['start_date'],
            "end_date": to_info['end_date'],
            "user_uri": to_info['user_uri'],
            "timeoff_type_name": to_info['timeoff_type_name'],
            "user_name": to_info['user_name']
        })
    return {
        "effective_dates": final_effective_date,
        "week_start_date": start_week_for_booking_start_date.strftime('%Y%m%d'),
        "week_end_date": (start_week_for_booking_start_date + timedelta(days=differance_start_and_end_week + 1)).strftime('%Y%m%d'),
        "user_name": to_info['user_name'],
        "user_uri": to_info['user_uri'],
        "timeoff_type_name": to_info['timeoff_type_name'],
        "booking_start_date": to_info['start_date'],
        "booking_end_date": to_info['end_date']
    }


def get_final_effective_dates(task_name):
    final_effective_dates = []
    timeoff_infos = rail.result(task_name)
    for timeoff_info in timeoff_infos:
        shift_effective_dates = get_shift_effective_dates(timeoff_info)
        existing_shift_effective_dates = get_existing_user_info(
            shift_effective_dates, final_effective_dates)
        if len(existing_shift_effective_dates) == 0:
            final_effective_dates.append(shift_effective_dates)
    return final_effective_dates


def get_existing_user_info(timeoff_info, final_effective_dates):
    return list(filter(lambda x: x['week_start_date'] == timeoff_info['week_start_date'] \
        and x['week_end_date'] == timeoff_info['week_end_date'] \
            and x['user_name'] == timeoff_info['user_name'], final_effective_dates))


def get_assignment_by_date(effective_date, current_assignments):
    return list(filter(lambda x: x['effectivedate'] == effective_date and x['additionaloncallshift'] == 'No', current_assignments))

# pylint: disable=too-many-arguments


def get_assign_delete_shift_info(effective_date_info, all_shift_info, effective_date_day, shift_summary, shifts_to_delete, shifts_to_assign):
    shift_code = rail.find_first_by_attr_and_get_attr(
        all_shift_info, 'uri', shift_summary['shift']['uri'], "code")
    display_text = shift_summary['shift']['displayText']
    publish_state = shift_summary['publishState'].split(":")[-1]
    if not effective_date_day in effective_date_info['pattern']:
        if shift_code:
            if not shift_code.startswith("Additional Shift") and not shift_code.startswith("on-call"):
                shifts_to_delete.append(shift_summary['assignmentUri'])
        else:
            shifts_to_delete.append(shift_summary['assignmentUri'])
    else:
        if shift_code:
            if not shift_code.startswith("Additional Shift") and not shift_code.startswith("on-call") and display_text != effective_date_info['shift_name']:
                shifts_to_delete.append(shift_summary['assignmentUri'])
        if not shift_code and display_text != effective_date_info['shift_name']:
            shifts_to_delete.append(shift_summary['assignmentUri'])

        if display_text == effective_date_info['shift_name']:
            shifts_to_assign.append({
                "effective_date": effective_date_info['start_date'],
                "name": display_text,
                "state": publish_state,
                "assignment_uri": shift_summary['assignmentUri']
            })
    return shifts_to_delete, shifts_to_assign


def get_assignment_category(effective_date_info, shift_summary_info):
    need_assignment = False
    shifts_to_delete = []
    shifts_to_assign = []
    all_shift_info = rail.result('get_all_shift_details')
    effective_date = datetime.strptime(
        effective_date_info['effective_date'], '%Y%m%d')
    effective_date_day = effective_date.strftime("%a")
    for shift_summary in shift_summary_info:
        shifts_to_delete, shifts_to_assign = get_assign_delete_shift_info(
            effective_date_info, all_shift_info, effective_date_day, shift_summary, shifts_to_delete, shifts_to_assign)
    if len(shifts_to_assign) > 0:
        for index, assign_shift in enumerate(shifts_to_assign):
            if index != 0:
                shifts_to_delete.append(assign_shift['assignment_uri'])
            if index == 0 and assign_shift['state'] != 'published':
                shifts_to_delete.append(assign_shift['assignment_uri'])
                need_assignment = True
    elif effective_date_day in effective_date_info['pattern']:
        need_assignment = True
    return shifts_to_delete, need_assignment


def get_timeoff_date_range():
    start_date, end_date = None, None
    current_aus_time = pendulum.now('Australia/Melbourne')
    week_day_number = current_aus_time.weekday()
    week_day_number = 5 - week_day_number if week_day_number <= 5 else week_day_number
    start_date = current_aus_time + timedelta(days=week_day_number)
    end_date = current_aus_time + timedelta(days=week_day_number + 90)
    return {
        "start_date": start_date.strftime('%Y%m%d'),
        "end_date": end_date.strftime('%Y%m%d')
    }


def get_shift_actions(dag_run):
    shifts_to_delete = []
    shifts_to_assign = []
    record_status = []
    effective_dates = dag_run.conf['effective_dates']
    shift_summary_info = rail.result('get_shift_schedule_summary')
    if effective_dates:
        for effective_date_info in effective_dates:
            shift_summary_by_date = get_shift_summary_by_date(
                effective_date_info['effective_date'], shift_summary_info)
            shifts_delete, need_assignment = get_assignment_category(
                effective_date_info, shift_summary_by_date)
            shifts_to_delete += shifts_delete
            if need_assignment:
                shifts_to_assign.append(effective_date_info)
                record_status.append({
                    "booking_date": effective_date_info["effective_date"],
                    "user_name": effective_date_info["user_name"],
                    "pattern": effective_date_info["pattern"],
                    "status": "Success",
                    "reason": "Added"
                })
            else:
                record_status.append({
                    "booking_date": effective_date_info["effective_date"],
                    "user_name": effective_date_info["user_name"],
                    "pattern": effective_date_info["pattern"],
                    "status": "Skipped",
                    "reason": "Previously assigned"
                })
    return {
        "shifts_to_delete": shifts_to_delete,
        "shifts_to_assign": shifts_to_assign,
        "records_status": record_status
    }


def get_shift_summary_by_date(effective_date, shift_summary_info):
    return list(filter(
        lambda item: date(item["date"]["year"], item["date"]
                          ["month"], item["date"]["day"]).strftime('%Y%m%d')
        == effective_date, shift_summary_info))
