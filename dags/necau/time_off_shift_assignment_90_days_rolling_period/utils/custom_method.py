from datetime import date
import rail
from rail.lib.ecid import get_dagrun_ecid


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_processed_shift():
    shift_processed = get_data_from_document(
        rail.result('get_processed_shift_info'))
    if len(shift_processed) > 0:
        return True
    return False


def get_timeoff_summary_info(response):
    timeoff_summarys = []
    for user_summary in response:
        for timeoff_summary in user_summary['timeOffSummaries']:
            to_summary = get_timeoff_summary(timeoff_summary)
            if to_summary:
                timeoff_summarys.append(to_summary)
    return timeoff_summarys


def get_timeoff_summary(time_Off_summary):
    if time_Off_summary:
        return {
            'start_date': date(time_Off_summary["startDateDetails"]["date"]["year"],
                               time_Off_summary["startDateDetails"]["date"]["month"],
                               time_Off_summary["startDateDetails"]["date"]["day"]).strftime('%Y%m%d'),
            'end_date': date(time_Off_summary["endDateDetails"]["date"]["year"],
                             time_Off_summary["endDateDetails"]["date"]["month"],
                             time_Off_summary["endDateDetails"]["date"]["day"]).strftime('%Y%m%d'),
            'user_uri': time_Off_summary['owner']['uri'],
            'timeoff_type_name': time_Off_summary['timeOffType']['name'],
            'user_name': time_Off_summary['owner']['displayText']
        }
    return None


def get_holiday_series_info(response):
    user_holidays = []
    for user_holiday in response:
        if user_holiday['holidays']:
            for holiday_summary in user_holiday['holidays']:
                hol_summary = get_holiday_summary(
                    holiday_summary, user_holiday['user'])
                if hol_summary:
                    user_holidays.append(hol_summary)
    return user_holidays


def get_holiday_summary(holiday_summary, holiday_user):
    if holiday_summary:
        return {
            'start_date': date(holiday_summary["date"]["year"], holiday_summary["date"]
                               ["month"], holiday_summary["date"]["day"]).strftime('%Y%m%d'),
            'end_date': date(holiday_summary["date"]["year"], holiday_summary["date"]
                             ["month"], holiday_summary["date"]["day"]).strftime('%Y%m%d'),
            'user_uri': holiday_user['uri'],
            'timeoff_type_name': holiday_summary['name'],
            'user_name': holiday_user['displayText']
        }
    return None


def has_shift_to_assign():
    assignment_category = rail.result('get_shift_actions')
    return assignment_category and assignment_category['shifts_to_assign'] and len(assignment_category['shifts_to_assign']) > 0


def has_shift_to_delete():
    assignment_category = rail.result('get_shift_actions')
    return assignment_category and assignment_category['shifts_to_delete'] and len(assignment_category['shifts_to_delete']) > 0


def get_update_record_properties(dag_run, item):
    return {
        'booking_date': item['booking_date'],
        'user_name': item['user_name'],
        'pattern': item['pattern'],
        'status': item['status'],
        'reason': item['reason'],
        'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run)
    }
