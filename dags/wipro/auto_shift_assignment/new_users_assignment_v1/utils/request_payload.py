from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import pendulum
import rail
null = None
DATE_FORMAT = "%Y/%m/%d"
REPORT_DATE_FORMAT = "%m/%d/%Y"

def get_user_report_payload(config):
    current_date = pendulum.now(config.time_zone)
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_user_report_details")["uri"],
                "filterValues": [{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                        rail.result('get_user_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'UDFFilter_User8_DefaultStartDate', 'uri'),
                    "value": 'DateRange'
                },
                    {
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                        rail.result('get_user_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'UDFFilter_User8_DefaultStartDate', 'uri'),
                    "value": (current_date + relativedelta(days=-1)).strftime(REPORT_DATE_FORMAT)
                },
                    {
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                        rail.result('get_user_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'UDFFilter_User8_DefaultStartDate', 'uri'),
                    "value": (current_date + relativedelta(days=-1)).strftime(REPORT_DATE_FORMAT)
                }],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_formated_user_row(item):

    return {
        'username': item['User Name'],
        'employeeid': item['Employee ID'],
        'useruri': item['User Uri'],
        'userstatus':  item['User Status']
    }.values()


def get_details(dag_run):

    useruri_list = []

    for item in dag_run.conf['item']:
        useruri_list.append(item['user_uri'])

    return useruri_list


def get_week_start_end_date(config, dag_run):
    startdate = dt.strptime(
        dag_run.conf['item'][0]['user_start_date'], DATE_FORMAT)
    if dag_run.conf['item'][0].get("onsite_direct_recruit", "").lower() == "assignee" and \
        dag_run.conf['item'][0]["onsite_start_date"]:
        startdate = dt.strptime(
            dag_run.conf['item'][0]['onsite_start_date'], DATE_FORMAT)
    lastdayofmonth = monthrange(startdate.year, startdate.month)
    enddate = dt(startdate.year, startdate.month, lastdayofmonth[1]).date()
    enddate = (enddate+relativedelta(months=int((rail.find_first_by_attr_and_get_attr(
        config.COUNTRY_MONTH_SHIFT_ASSIGNMENT, "country", dag_run.conf['item'][0]['country'], "month")))))

    str_startdate = startdate.strftime(DATE_FORMAT)
    str_enddate = enddate.strftime(DATE_FORMAT)
    return {
        'string_startdate': str_startdate,
        'string_enddate': str_enddate
    }


def get_shift_details_spain_m_t(config, dag_run):

    start_date = dt.strptime(get_week_start_end_date(
        config, dag_run)['string_startdate'], DATE_FORMAT)
    end_date = dt.strptime(get_week_start_end_date(
        config, dag_run)['string_enddate'], DATE_FORMAT)
    assignment_days = [
        "urn:replicon:day-of-week:monday",
        "urn:replicon:day-of-week:tuesday",
        "urn:replicon:day-of-week:wednesday",
        "urn:replicon:day-of-week:thursday"
    ]
    return {
        "shiftAssignments": {
            "assignmentDateRange": {
                "startDate": {
                    "year": start_date.year,
                    "month": start_date.month,
                    "day": start_date.day
                },
                "endDate": {
                    "year": end_date.year,
                    "month": end_date.month,
                    "day": end_date.day
                }
            },
            "relativeDates": [],
            "assignmentDaysOfWeek": assignment_days,
            "userUris": [dag_run.conf['item'][0]["user_uri"]],
            "shift": {
                "name": (rail.find_first_by_attr_and_get_attr(config.COUNTRY_MONTH_SHIFT_ASSIGNMENT,
                                                              "country", dag_run.conf['item'][0]['country'], "Spain_INETUM").get("default_shift"))
            },
            "note": "Published by shift automation",
            "publishState": "urn:replicon:shift-assignment-publish-state:published",
            "assignmentOptionUri": "urn:replicon:shift-assignment-option:replace-assignments-on-overlapping-day"
        }
    }


def get_shift_details_spain_f(config, dag_run):

    start_date = dt.strptime(get_week_start_end_date(
        config, dag_run)['string_startdate'], DATE_FORMAT)
    end_date = dt.strptime(get_week_start_end_date(
        config, dag_run)['string_enddate'], DATE_FORMAT)
    assignment_days = [
        "urn:replicon:day-of-week:friday"
    ]
    return {
        "shiftAssignments": {
            "assignmentDateRange": {
                "startDate": {
                    "year": start_date.year,
                    "month": start_date.month,
                    "day": start_date.day
                },
                "endDate": {
                    "year": end_date.year,
                    "month": end_date.month,
                    "day": end_date.day
                }
            },
            "relativeDates": [],
            "assignmentDaysOfWeek": assignment_days,
            "userUris": [dag_run.conf['item'][0]["user_uri"]],
            "shift": {
                "name": (rail.find_first_by_attr_and_get_attr(config.COUNTRY_MONTH_SHIFT_ASSIGNMENT,
                                                              "country", dag_run.conf['item'][0]['country'], "Spain_INETUM_FRIDAY")).get("default_shift")
            },
            "note": "Published by shift automation",
            "publishState": "urn:replicon:shift-assignment-publish-state:published",
            "assignmentOptionUri": "urn:replicon:shift-assignment-option:replace-assignments-on-overlapping-day"
        }
    }


def get_shift_details(config, dag_run):

    start_date = dt.strptime(get_week_start_end_date(
        config, dag_run)['string_startdate'], DATE_FORMAT)
    end_date = dt.strptime(get_week_start_end_date(
        config, dag_run)['string_enddate'], DATE_FORMAT)
    country = dag_run.conf['item'][0]['country']
    assignment_days = [
        "urn:replicon:day-of-week:monday",
        "urn:replicon:day-of-week:tuesday",
        "urn:replicon:day-of-week:wednesday",
        "urn:replicon:day-of-week:thursday",
        "urn:replicon:day-of-week:friday"
    ]
    ksa_assignment_days = rail.find_first_by_attr_and_get_attr(
        config.COUNTRY_MONTH_SHIFT_ASSIGNMENT, "country", country, "shift_assignment_days")
    if ksa_assignment_days:
        assignment_days = ksa_assignment_days
    default_shift = rail.find_first_by_attr_and_get_attr(
        config.COUNTRY_MONTH_SHIFT_ASSIGNMENT, "country", country, "default_shift")
    if country.lower() == "romania":
        default_shift = default_shift.get(dag_run.conf['item'][0]["legal_entity_code"])
        if default_shift:
            default_shift = default_shift.get(dag_run.conf['item'][0]["fj_identifier"])
    return {
        "shiftAssignments": {
            "assignmentDateRange": {
                "startDate": {
                    "year": start_date.year,
                    "month": start_date.month,
                    "day": start_date.day
                },
                "endDate": {
                    "year": end_date.year,
                    "month": end_date.month,
                    "day": end_date.day
                }
            },
            "relativeDates": [],
            "assignmentDaysOfWeek": assignment_days,
            "userUris": rail.result('get_all_records'),
            "shift": {
                "name": default_shift
            },
            "note": "Published by shift automation",
            "publishState": "urn:replicon:shift-assignment-publish-state:published",
            "assignmentOptionUri": "urn:replicon:shift-assignment-option:replace-assignments-on-overlapping-day"
        }
    }


def check_and_get_items(dag_run):

    return dag_run.conf['item']


def do_format_logs():
    log_artifacts = []
    log_records = []

    user_shift_logs = rail.result('gather_shift_logs')

    if user_shift_logs:
        if isinstance(user_shift_logs, list):
            log_artifacts.extend(user_shift_logs)
        else:
            log_artifacts.append(user_shift_logs)

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

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))

    return final_log_records
