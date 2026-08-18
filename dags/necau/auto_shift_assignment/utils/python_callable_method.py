from datetime import date, datetime, timedelta
import pendulum
import rail


def convert_to_date(str_date, date_format="%b %d, %Y"):
    if not str_date:
        return None
    try:
        return datetime.strptime(str_date, date_format).date()
    except Exception as e:
        raise Exception(f"Invalid date received. Date:{str_date}") from e


def get_week_patterns(dag_run):
    wk1_pattern, wk2_pattern, combined_pattern = None, None, None
    if not dag_run.conf['Wk1pattern'] and dag_run.conf['Wk2pattern']:
        wk1_pattern = "No Assignment"
    elif dag_run.conf['Wk1pattern'] and not dag_run.conf['Wk2pattern']:
        wk2_pattern = dag_run.conf['Wk1pattern']
    elif not dag_run.conf['Wk1pattern'] and not dag_run.conf['Wk2pattern']:
        combined_pattern = "Mon/Tue/Wed/Thu/Fri"
    return {
        "week1pattern": wk1_pattern,
        "week2pattern": wk2_pattern,
        "combinedpattern": combined_pattern
    }


def get_shift_week_informations(dag_run):
    pattern_info = get_week_patterns(dag_run)
    current_aus_time = pendulum.now('Australia/Melbourne')
    week1_pattern_derived, week2_pattern_derived, start_date_derived, start_date_of_current_week = None, None, None, None
    number_of_days_substracted_from_beginning_week, days_substracted_from_beginning_week2 = 0, 0

    if pattern_info['week1pattern']:
        week1_pattern_derived = pattern_info['week1pattern']
    else:
        week1_pattern_derived = dag_run.conf['Wk1pattern'].split(
            ".").pop() if dag_run.conf['Wk1pattern'] else pattern_info['combinedpattern']

    if pattern_info['week2pattern']:
        week2_pattern_derived = pattern_info['week2pattern']
    else:
        week2_pattern_derived = dag_run.conf['Wk2pattern'].split(".").pop(
        ) if dag_run.conf['Wk2pattern'] else pattern_info['combinedpattern']

    shift_name_derived = dag_run.conf['Shiftname'].split(
        "|")[0] if dag_run.conf['Shiftname'] else "Default Office Schedule - 7.6hrs"

    user_shift_start_date = convert_to_date(
        dag_run.conf['Startdate']) if dag_run.conf['Startdate'] else current_aus_time

    shift_day_number = user_shift_start_date.weekday()
    aus_day_number = current_aus_time.weekday()

    day_of_week_start_date = user_shift_start_date.strftime("%A")

    if "Saturday" == day_of_week_start_date:
        number_of_days_substracted_from_beginning_week = 0
        start_date_derived = user_shift_start_date
    elif "Sunday" == day_of_week_start_date:
        number_of_days_substracted_from_beginning_week = 1
        start_date_derived = user_shift_start_date - timedelta(days=1)
    else:
        number_of_days_substracted_from_beginning_week = 2
        begin_of_week = user_shift_start_date - \
            timedelta(days=shift_day_number)
        start_date_derived = begin_of_week - timedelta(days=2)

    end_of_week1 = start_date_derived + timedelta(days=6)
    start_of_second_week_sat = end_of_week1 + timedelta(days=1)
    end_week2 = start_of_second_week_sat + timedelta(days=6)

    dayof_current_Date = current_aus_time.strftime("%A")
    if "Saturday" == dayof_current_Date:
        days_substracted_from_beginning_week2 = 0
    elif "Sunday" == dayof_current_Date:
        days_substracted_from_beginning_week2 = 1
    else:
        days_substracted_from_beginning_week2 = 2

    if dayof_current_Date in ("Saturday", "Sunday"):
        start_date_of_current_week = current_aus_time - \
            timedelta(days=days_substracted_from_beginning_week2)
    else:
        begin_of_week = current_aus_time - timedelta(days=aus_day_number)
        start_date_of_current_week = begin_of_week - \
            timedelta(days=days_substracted_from_beginning_week2)

    end_of_current_week = start_date_of_current_week + timedelta(days=7)
    endofthecurrentweekfriday = start_date_of_current_week + timedelta(days=6)

    return {
        "todaystimeinmelbournetime": current_aus_time.strftime('%Y%m%d'),
        "week1patternderived": week1_pattern_derived,
        "week2patternderived": week2_pattern_derived,
        "shiftnamederived": shift_name_derived,
        "dayoftheweekforstartdate": day_of_week_start_date,
        "numberofdaystobesubstractedfrombeginningoftheweek": number_of_days_substracted_from_beginning_week,
        "startdatederived": start_date_derived.strftime('%Y%m%d'),
        "endofweek1": end_of_week1.strftime('%Y%m%d'),
        "startodsecondweeksat": start_of_second_week_sat.strftime('%Y%m%d'),
        "endweek2": end_week2.strftime('%Y%m%d'),
        "todaydayoftheweek": current_aus_time.strftime("%A"),
        "numberofdaystobesubstractedfrombeginningoftheweek2": days_substracted_from_beginning_week2,
        "startdateofcurrentweek": start_date_of_current_week.strftime('%Y%m%d'),
        "endofcurrentweek": end_of_current_week.strftime('%Y%m%d'),
        "endofthecurrentweekfriday": endofthecurrentweekfriday.strftime('%Y%m%d')
    }


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

def do_format_logs():
    log_artifacts = []
    log_records = []

    child_logs = rail.result("gather_child_logs")

    if child_logs:
        if isinstance(child_logs, list):
            log_artifacts.extend(child_logs)
        else:
            log_artifacts.append(child_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))

    return final_log_records
