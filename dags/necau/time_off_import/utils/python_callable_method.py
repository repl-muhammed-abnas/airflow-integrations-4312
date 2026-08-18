from datetime import date, datetime, timedelta
import rail
from airflow.utils.state import TaskInstanceState
from sqlalchemy import null
from necau.time_off_import.utils import custom_method

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


def get_week_patterns():
    wk1_pattern, wk2_pattern, combined_pattern = None, None, None
    user_udf_collenctions = rail.result('get_user_info')[
        'customFieldValues']
    user_wk1_pattern = rail.find_first_by_attr_and_get_attr(
        user_udf_collenctions, 'customField.displayText', 'Auto schedule assignment - days Wk1', 'text')
    user_wk2_pattern = rail.find_first_by_attr_and_get_attr(
        user_udf_collenctions, 'customField.displayText', 'Auto schedule assignment - days Wk2', 'text')
    user_shift_name = rail.find_first_by_attr_and_get_attr(
        user_udf_collenctions, 'customField.displayText', 'Auto schedule assignment - shift', 'text')
    user_shift_name = user_shift_name.split('|')[0] if user_shift_name else None
    user_start_date = rail.find_first_by_attr_and_get_attr(
        user_udf_collenctions, 'customField.displayText', 'Auto schedule assignment - start date Wk1', 'text')

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


def get_shift_effective_dates(dag_run):
    pattern_info = get_week_patterns()
    booking_start_date = datetime.strptime(
        dag_run.conf['start_date'], input_date_format)
    booking_end_date = datetime.strptime(
        dag_run.conf['end_date'], input_date_format)
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
            "start_date": dag_run.conf['start_date'],
            "end_date": dag_run.conf['end_date'],
            "user_uri": dag_run.conf['user_uri'],
            "timeoff_type_name": dag_run.conf['leave_description'],
            "user_name": dag_run.conf['preferred_name'] + " "+dag_run.conf['surname']
        })
    return final_effective_date


def get_weekly_iterations():
    shift_information = rail.result('get_shift_week_info')
    start_date_derived = datetime.strptime(
        shift_information['startdatederived'], '%Y%m%d')
    todays_time_in_melbournetime = datetime.strptime(
        shift_information['todaystimeinmelbournetime'], '%Y%m%d')
    start_date = start_date_derived - \
        timedelta(
            shift_information['numberofdaystobesubstractedfrombeginningoftheweek'])
    end_Date = todays_time_in_melbournetime - \
        timedelta(
            shift_information['numberofdaystobesubstractedfrombeginningoftheweek2'])
    day_diff_or_iterations = (end_Date - start_date).days / 7
    resulting_value = "week2" if day_diff_or_iterations % 2 else "week1"

    return {
        "daydiffforiterations": day_diff_or_iterations,
        "resultingvalue": resulting_value
    }


def get_effective_dates_by_status(effective_dates_info):
    effective_dates_to_consider = list(filter(
        lambda item: item['Dayoftheweektobeused'] == 'Yes', effective_dates_info))
    effectivedates_to_ignore = list(
        filter(lambda item: item['Dayoftheweektobeused'] == 'No', effective_dates_info))
    dates_in_week = [x["Effectivedate"]
                     for x in effective_dates_to_consider]

    return {
        "effective_dates_to_consider": effective_dates_to_consider,
        "effective_dates_to_ignore": effectivedates_to_ignore,
        "dates_in_week": dates_in_week
    }


def get_week1_info():
    effective_dates_info = []
    iteration_info = rail.result('get_shift_day_diff')
    usr_shift_info = rail.result('get_shift_week_info')
    number_of_days = range(7)
    if iteration_info['resultingvalue'] and usr_shift_info['endofthecurrentweekfriday']:
        week_pattern = usr_shift_info['week1patternderived'] if "week1" in iteration_info[
            'resultingvalue'] else usr_shift_info['week2patternderived']
        start_date_of_currentweek = datetime.strptime(
            usr_shift_info['startdateofcurrentweek'], '%Y%m%d')
        end_of_current_week = datetime.strptime(
            usr_shift_info['endofcurrentweek'], '%Y%m%d')
        end_of_current_week_friday = datetime.strptime(
            usr_shift_info['endofthecurrentweekfriday'], '%Y%m%d')

        for day in number_of_days:
            day_of_week_used = "No"
            effective_date = start_date_of_currentweek + timedelta(days=day)
            if effective_date > start_date_of_currentweek and start_date_of_currentweek < end_of_current_week:
                if effective_date.strftime("%a") in week_pattern and effective_date <= end_of_current_week_friday:
                    day_of_week_used = "Yes"

            effective_date_info = {
                'Effectivedate': effective_date.strftime('%Y%m%d'),
                'Dayoftheweektobeused': day_of_week_used,
                'Datetobeused': effective_date.strftime("%a")
            }

            effective_dates_info.append(effective_date_info)

    return get_effective_dates_by_status(effective_dates_info)


def get_week2_info():
    number_of_days = range(7)
    effective_dates_to_consider_week2 = []
    user_shift_info = rail.result('get_shift_week_info')
    start_to_second_week_sat = datetime.strptime(
        user_shift_info['startodsecondweeksat'], '%Y%m%d')
    end_of_current_week = datetime.strptime(
        user_shift_info['endofcurrentweek'], '%Y%m%d')
    week2_pattern_derived = user_shift_info['week2patternderived']
    end_week2 = datetime.strptime(user_shift_info['endweek2'], '%Y%m%d')

    for day in number_of_days:
        day_of_week2_tob_eused = "No"
        effectivedate = start_to_second_week_sat + timedelta(days=day)
        if start_to_second_week_sat < effectivedate < end_of_current_week:
            if effectivedate.strftime("%a") in week2_pattern_derived and effectivedate <= end_week2:
                day_of_week2_tob_eused = "Yes"

        effectiveDateinfo = {
            'Effectivedate': effectivedate.strftime('%Y%m%d'),
            'Dayoftheweektobeused': day_of_week2_tob_eused,
            'Datetobeused': effectivedate.strftime("%a")
        }

        effective_dates_to_consider_week2.append(effectiveDateinfo)

    return get_effective_dates_by_status(effective_dates_to_consider_week2)


def get_effective_date_week1_info():
    effective_dates_to_consider_week1 = []
    usr_shift_info = rail.result('get_shift_week_info')
    number_of_days = range(7)

    start_date_derived = datetime.strptime(
        usr_shift_info['startdatederived'], '%Y%m%d')
    todays_time_in_melbourne = datetime.strptime(
        usr_shift_info['todaystimeinmelbournetime'], '%Y%m%d')
    end_of_week1 = datetime.strptime(usr_shift_info['endofweek1'], '%Y%m%d')
    start_date_of_current_week = datetime.strptime(
        usr_shift_info['startdateofcurrentweek'], '%Y%m%d')
    end_of_current_week = datetime.strptime(
        usr_shift_info['endofcurrentweek'], '%Y%m%d')

    for day in number_of_days:
        effectivedate = start_date_derived + timedelta(days=day)
        day_of_week_used = "No"
        if start_date_derived < todays_time_in_melbourne < end_of_week1 and start_date_of_current_week < effectivedate < end_of_current_week:
            if effectivedate.strftime("%a") in usr_shift_info['week1patternderived']:
                if effectivedate <= end_of_week1:
                    day_of_week_used = "Yes"

        effectiveDateinfo = {
            'Effectivedate': effectivedate.strftime('%Y%m%d'),
            'Dayoftheweektobeused': day_of_week_used,
            'Datetobeused': effectivedate.strftime("%a")
        }

        effective_dates_to_consider_week1.append(effectiveDateinfo)

    return get_effective_dates_by_status(effective_dates_to_consider_week1)


def get_shift_uri(shift_details, shift_name_derived):
    return rail.find_first_by_attr_and_get_attr(shift_details, 'name', shift_name_derived, "uri")


def get_shifts_to_delete():
    week1_delete_list = rail.result('create_assigned_shift_details_week1')['shifts_to_delete'] if rail.result(
        'create_assigned_shift_details_week1') and rail.result('create_assigned_shift_details_week1')['shifts_to_delete'] else []
    week2_delete_list = rail.result('create_assigned_shift_details_week2')['shifts_to_delete'] if rail.result(
        'create_assigned_shift_details_week2') and rail.result('create_assigned_shift_details_week2')['shifts_to_delete'] else []
    shifts_to_delete = get_combination_of_array(
        week1_delete_list, week2_delete_list)
    return shifts_to_delete


def get_assignment_list():
    week1_shiftlist = rail.result('create_assigned_shift_details_week1')['shiftlistoutput'] if rail.result(
        'create_assigned_shift_details_week1') and rail.result('create_assigned_shift_details_week1')['shiftlistoutput'] else []
    week2_shiftlist = rail.result('create_assigned_shift_details_week2')['shiftlistoutput'] if rail.result(
        'create_assigned_shift_details_week2') and rail.result('create_assigned_shift_details_week2')['shiftlistoutput'] else []
    current_assignment_list = get_combination_of_array(
        week1_shiftlist, week2_shiftlist)
    return current_assignment_list


def get_weekly_effective_dates(week_number):
    weekly_effective_dates = []
    if week_number == "week1":
        if rail.result('week1_informations'):
            weekly_effective_dates = rail.result('week1_informations')['dates_in_week'] if rail.result(
                'week1_informations') and rail.result('week1_informations')['dates_in_week'] else []
        else:
            weekly_effective_dates = rail.result('effective_date_week1_info')['dates_in_week'] if rail.result(
                'effective_date_week1_info') and rail.result('effective_date_week1_info')['dates_in_week'] else []
    else:
        weekly_effective_dates = rail.result('effective_date_toconsider_week2')['dates_in_week'] if rail.result(
            'effective_date_toconsider_week2') and rail.result('effective_date_toconsider_week2')['dates_in_week'] else []
    return weekly_effective_dates


def get_assignment_by_date(effective_date, current_assignments):
    return list(filter(lambda x: x['effectivedate'] == effective_date and x['additionaloncallshift'] == 'No', current_assignments))


def get_shift_to_assign_for_week(dag_run, week_number):
    shift_to_assign_details_week = []
    weekly_effective_dates = get_weekly_effective_dates(week_number)
    usr_shift_info = rail.result('get_shift_week_info')
    shifts_to_delete = get_shifts_to_delete()
    current_assignment_list = get_assignment_list()

    for weekly_effective_date in weekly_effective_dates:
        effective_date = datetime.strptime(weekly_effective_date, '%Y%m%d')
        effective_date_day = effective_date.day
        effective_date_month = effective_date.month
        effective_date_year = effective_date.year
        shif_tname = usr_shift_info['shiftnamederived']
        shift_uri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_shift_details'), 'name', shif_tname, "uri")
        user_uri = dag_run.conf['Useruri']
        to_assign = "No"
        if len(shifts_to_delete) > 0:
            effectivedate_deleteshift = rail.find_first_by_attr_and_get_attr(
                shifts_to_delete, 'effectivedate', weekly_effective_date, "effectivedate")
            if effectivedate_deleteshift:
                to_assign = "Yes"
            else:
                to_assign = "No"
        else:
            current_assignment_lst = get_assignment_by_date(
                weekly_effective_date, current_assignment_list)
            if len(current_assignment_lst) > 0:
                to_assign = "No"
            else:
                to_assign = "Yes"

        shift_to_assign_details_week.append({
            "effectivedate": weekly_effective_date,
            "effectivedateday": effective_date_day,
            "effectivedatemonth": effective_date_month,
            "effectivedateyear": effective_date_year,
            "shiftname": shif_tname,
            "shifturi": shift_uri,
            "useruri": user_uri,
            "toassign": to_assign
        })

    shift_to_assign_week = list(
        filter(lambda item: item['toassign'] == 'Yes', shift_to_assign_details_week))
    shift_already_assigned_week = list(
        filter(lambda item: item['toassign'] == 'No', shift_to_assign_details_week))

    return {
        "shift_to_assign_week": shift_to_assign_week,
        "shift_already_assigned_week": shift_already_assigned_week
    }


def get_assigned_shift_details(week_name):
    shift_list_output = []
    weekly_effective_dates = []
    shift_summary_info = []
    if week_name == 'week1':
        shift_summary_info = rail.result('get_shift_schedule_summary_week1')
        if rail.result('week1_informations') and rail.result('week1_informations')['dates_in_week']:
            weekly_effective_dates = rail.result(
                'week1_informations')['dates_in_week']
        else:
            weekly_effective_dates = rail.result(
                'effective_date_week1_info')['dates_in_week']
    else:
        weekly_effective_dates = rail.result(
            'effective_date_toconsider_week2')['dates_in_week']
        shift_summary_info = rail.result('get_shift_schedule_summary_week2')

    all_shift_info = rail.result('get_all_shift_details')
    usr_shift_info = rail.result('get_shift_week_info')

    for shift_summary in shift_summary_info:
        additional_oncall_shift = "No"
        to_delete = "No"
        shift_code = rail.find_first_by_attr_and_get_attr(
            all_shift_info, 'uri', shift_summary['shift']['uri'], "code")
        display_text = shift_summary['shift']['displayText']
        shift_summary_date = date(
            shift_summary['date']['year'], shift_summary['date']['month'], shift_summary['date']['day']).strftime('%Y%m%d')
        publish_state = shift_summary['publishState'].split(":").pop()

        if shift_code:
            if shift_code.startswith("Additional Shift") or shift_code.startswith("on-call"):
                additional_oncall_shift = "Yes"
            else:
                to_delete = can_delete_shift(
                    display_text, usr_shift_info['shiftnamederived'], shift_summary_date, weekly_effective_dates, publish_state)
        else:
            to_delete = can_delete_shift(
                display_text, usr_shift_info['shiftnamederived'], shift_summary_date, weekly_effective_dates, publish_state)

        response = {
            "effectivedate": shift_summary_date,
            "name": shift_summary['shift']['displayText'],
            "state": publish_state,
            "assignmenturi": shift_summary['assignmentUri'],
            "todelete": to_delete,
            "additionaloncallshift": additional_oncall_shift,
        }

        shift_list_output.append(response)

    shifts_to_delete = list(
        filter(lambda item: item['todelete'] == 'Yes', shift_list_output))
    return {
        "shiftlistoutput": shift_list_output,
        "shifts_to_delete": shifts_to_delete
    }


def can_delete_shift(display_text, shift_name_derived, shift_summary_date, weekly_effective_dates, publish_state):
    to_delete = "No"
    if display_text == shift_name_derived:
        if shift_summary_date in weekly_effective_dates:
            if publish_state == "open":
                to_delete = "Yes"
        else:
            to_delete = "Yes"
    else:
        to_delete = "Yes"
    return to_delete


def get_combination_of_array(collection1, collection2):
    combined_collections = []
    if collection1 and collection2:
        combined_collections = [*collection1, *collection2]
    elif collection1:
        combined_collections = collection1
    else:
        combined_collections = collection2
    return combined_collections


def get_timeoff_booking_info(dag_run):
    timeoff_booking_informations = []
    timesheet_uri = rail.result('get_timesheet_for_date')[
        'timesheet']['uri'] if rail.result('get_timesheet_for_date') else ""
    timesheet_details = rail.result('get_timesheet_details')
    timeoff_bookings = rail.result("get_timeoff_details")
    for timeoff_booking in timeoff_bookings:
        custom_field_info = timeoff_booking["customFields"]
        sequence_key_info = list(filter(
            lambda item: item['customField']['name'] == 'sequencekey', custom_field_info))
        request_key_info = list(filter(
            lambda item: item['customField']['name'] == 'Request Key', custom_field_info))
        timeoff_booking_info = {
            "staff_member": dag_run.conf["staff_member"],
            "surname": dag_run.conf["surname"],
            "preferred_name": dag_run.conf["preferred_name"],
            "form_code": dag_run.conf["form_code"],
            "form_description": dag_run.conf["form_description"] if dag_run.conf["form_description"] else "",
            "request_key": dag_run.conf["request_key"],
            "request_key_existing": request_key_info[0]["text"],
            "creation_date": dag_run.conf["creation_date"] if dag_run.conf["creation_date"] else "",
            "creation_time": dag_run.conf["creation_time"] if dag_run.conf["creation_time"] else "",
            "seq_no": dag_run.conf["seq_no"],
            "leave_type": dag_run.conf["leave_type"],
            "leave_description": dag_run.conf["leave_description"],
            "start_date": dag_run.conf["start_date"],
            "end_date": dag_run.conf["end_date"],
            "action_status": dag_run.conf["action_status"],
            "timesheet_approval_status": timesheet_details['statusUri'].split(':')[-1] if timesheet_details else "",
            "timeoff_approval_status": timeoff_booking["approvalStatus"]["displayText"],
            "booking_uri": timeoff_booking["uri"],
            "user_email": dag_run.conf["user_email"],
            "supervisor_email": dag_run.conf["supervisor_email"],
            "sequencenol": sequence_key_info[0]["text"],
            "time_sheet_uri": timesheet_uri
        }

        timeoff_booking_informations.append(timeoff_booking_info)
    return timeoff_booking_informations


def get_timesheet_status(resource_attrifact):
    timesheet_details = rail.result(resource_attrifact)
    if timesheet_details:
        timesheet_status = timesheet_details['statusUri'].split(':')[-1]
        return timesheet_status in ['approved', 'waiting']
    return False


def is_days_taken_and_lap(dag_run):
    return float(dag_run.conf['days_taken']) < 1 and dag_run.conf['form_code'].lower() == 'lap'


def is_booking_multiday_and_lap(dag_run):
    days_taken = dag_run.conf['days_taken']
    days_taken_decimals = str(dag_run.conf['days_taken']).split('.')
    day_taken_without_partial = bool(
        len(days_taken_decimals) == 1 or (len(days_taken_decimals) == 2 and float(days_taken_decimals[1]) == 0))
    return float(days_taken) >= 1 and day_taken_without_partial and dag_run.conf['form_code'].lower() == 'lap'


def get_email_status(dag_run):
    return bool(dag_run.conf['user_email'] or dag_run.conf['supervisor_email'])


def get_user_super_email_ids(dag_run):
    user_email = dag_run.conf['user_email']
    supervisor_email = dag_run.conf['supervisor_email']
    return user_email+','+supervisor_email if user_email and supervisor_email else user_email or supervisor_email


def get_timeoff_action_status(dag_run):
    return dag_run.conf['action_status'].lower() == 'approved'

# pylint: disable=too-many-arguments


def get_assign_delete_shift_info(dag_run, all_shift_info, effective_date_day, shift_summary, shifts_to_delete, shifts_to_assign):
    shift_code = rail.find_first_by_attr_and_get_attr(
        all_shift_info, 'uri', shift_summary['shift']['uri'], "code")
    display_text = shift_summary['shift']['displayText']
    publish_state = shift_summary['publishState'].split(":")[-1]
    if not effective_date_day in dag_run.conf['pattern']:
        if shift_code:
            if not shift_code.startswith("Additional Shift") and not shift_code.startswith("on-call"):
                shifts_to_delete.append(shift_summary['assignmentUri'])
        else:
            shifts_to_delete.append(shift_summary['assignmentUri'])
    else:
        if shift_code:
            if not shift_code.startswith("Additional Shift") and not shift_code.startswith("on-call") and display_text != dag_run.conf['shift_name']:
                shifts_to_delete.append(shift_summary['assignmentUri'])
        elif display_text != dag_run.conf['shift_name']:
            shifts_to_delete.append(shift_summary['assignmentUri'])

        if display_text == dag_run.conf['shift_name']:
            if len(shifts_to_assign) == 0:
                shifts_to_assign.append({
                    "effective_date": dag_run.conf['start_date'],
                    "name": display_text,
                    "state": publish_state,
                    "assignment_uri": shift_summary['assignmentUri']
                })
            else:
                shifts_to_delete.append(shift_summary['assignmentUri'])
    return shifts_to_delete, shifts_to_assign


def get_assignment_category(dag_run):
    need_assignment = False
    shifts_to_delete = []
    shifts_to_assign = []
    shift_summary_info = rail.result('get_shift_schedule_summary')
    all_shift_info = rail.result('get_all_shift_details')
    effective_date = datetime.strptime(
        dag_run.conf['effective_date'], '%Y%m%d')
    effective_date_day = effective_date.strftime("%a")
    for shift_summary in shift_summary_info:
        shifts_to_delete, shifts_to_assign = get_assign_delete_shift_info(
            dag_run, all_shift_info, effective_date_day, shift_summary, shifts_to_delete, shifts_to_assign)
    if len(shifts_to_assign) > 0:
        for index, assign_shift in enumerate(shifts_to_assign):
            if index != 0:
                shifts_to_delete.append(assign_shift['assignment_uri'])
            if index == 0 and assign_shift['state'] != 'published':
                shifts_to_delete.append(assign_shift['assignment_uri'])
                need_assignment = True
    elif effective_date_day in dag_run.conf['pattern']:
        need_assignment = True
    return shifts_to_delete, effective_date.strftime('%Y%m%d'), need_assignment


def get_filter_groups(file_name, files_in_input_dir):
    file_groups = list(
        filter(lambda item: ".csv" in item['name'].lower() and file_name in item['name'], files_in_input_dir))
    return file_groups


def get_input_group(config):
    valid_input_set, invalid_input_set = [], []
    file_names = []
    files_in_input_dir = rail.result("list_import_files").get(
        config.timeoff_import_file_directory)
    if files_in_input_dir:
        files_in_input_dir.sort(key=lambda s: datetime.strptime(
            s['modify'], '%Y%m%d%H%M%S'), reverse=False)
        for item in files_in_input_dir:
            name_of_file = item['name'].split(" ")[-1]
            if name_of_file not in file_names:
                file_names.append(name_of_file)
        for file_name in file_names:
            file_groups = get_filter_groups(file_name, files_in_input_dir)
            if len(file_groups) == 3:
                if len(valid_input_set) == 0:
                    valid_input_set.append(file_groups)
            else:
                for item in file_groups:
                    invalid_input_set.append({'file_name': item['name']})
        valid_input_set = valid_input_set[0] if len(
            valid_input_set) > 0 else []
        valid_input_set.sort(key=lambda s: s['name'], reverse=False)
        return {
            "valid": valid_input_set,
            "invalid": invalid_input_set
        }
    return None


def get_errror_logs():
    errored_logs = []
    error_logs_info = rail.result('gather_error_logs')
    for record in error_logs_info:
        errored_logs_from_child = custom_method.get_data_from_document(record)
        errored_logs += errored_logs_from_child
    return errored_logs


def is_timeoff_present():
    timeoff_draft_multi_day_with_partial1 = rail.result(
        'publish_timeoff_draft_multi_day_with_partial1')
    timeoff_draft_multi_day_with_partial2 = rail.result(
        'publish_timeoff_draft_multi_day_with_partial2')
    timeoff_draft_multi_day = rail.result('publish_timeoff_draft_multi_day')
    timeoff_draft_single_day = rail.result('publish_timeoff_draft_single_day')
    error_response = str(rail.result('batch_task', 'error'))
    is_forbiden_error = "403 Forbidden" in error_response
    return is_forbiden_error \
        and bool(timeoff_draft_multi_day_with_partial1 or timeoff_draft_multi_day_with_partial2 or timeoff_draft_multi_day or timeoff_draft_single_day)


def is_error_in_opening(task_name):
    return bool(list(filter(lambda x: x.task_id == task_name and x.state == TaskInstanceState.FAILED,
                            rail.get_current_context()['dag_run'].get_task_instances())))


def is_task_triggered(task_name):
    return bool(list(filter(lambda x: x.task_id == task_name and x.state == TaskInstanceState.SUCCESS,
                            rail.get_current_context()['dag_run'].get_task_instances())))


def get_file_info(dag_run):
    return {
        "download_link": rail.result("warning_file_download_link"),
        "file_name": dag_run.conf['file_name']
    }
