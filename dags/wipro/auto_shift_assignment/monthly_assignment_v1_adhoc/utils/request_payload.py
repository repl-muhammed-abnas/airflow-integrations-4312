from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import pendulum
import rail
null = None
DATE_FORMAT = "%Y-%m-%d"


def get_user_report_payload(dag_run):
    get_specific_report_details = rail.result('get_user_report_details')
    filter_values = []
    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_report_details'
                                                                            )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentServiceCenterFilter', 'uri'),
        "value": null,
    })
    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_report_details'
                                                                            )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentServiceCenterFilter', 'uri'),
        "value": (rail.find_first_by_attr_and_get_attr(rail.result("get_all_locations"), "displayText", dag_run.conf['country'], "uri")).split(':')[-1],
    })
    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def group_romania_records():
    romania_users = rail.load_all_records(rail.result("query_enabled_users_data"))
    groups = {}
    for user in romania_users:
        key = user["legal_entity_code"] + "_" + user["fj_identifier"]
        if key not in groups:
            groups[key] = []
        groups[key].append(user)

    batches = []
    batch_index = 0
    for key, users_in_group in groups.items():
        legal_entity_code, fj_identifier = key.split("_")
        for i in range(0, len(users_in_group), 50):
            batches.append({
                "legal_entity_code": legal_entity_code,
                "fj_identifier": fj_identifier,
                "users": users_in_group[i:i + 50],
                "batch_index": batch_index,
                "total_users": len(users_in_group[i:i + 50])
            })
            batch_index += 1
    return batches

def get_details(dag_run):

    useruri_list = []
    items = dag_run.conf['item']
    if dag_run.conf["country"].lower() == "romania":
        for group in items:
            for user in group.get("users", []):
                useruri_list.append(user['user_uri'])
        return useruri_list

    for item in items:
        useruri_list.append(item['user_uri'])

    return useruri_list


def get_week_start_end_date(config, dag_run):
    current_date = pendulum.now(config.time_zone)
    startdate = (
        current_date + relativedelta(months=int(dag_run.conf['month']))).replace(day=1).date()
    if dag_run.conf.get("start_date"):
        startdate = dt.strptime(dag_run.conf.get("start_date"), "%d/%m/%Y").replace(day=1).date()
    lastdayofmonth = monthrange(startdate.year, startdate.month)
    enddate = dt(startdate.year, startdate.month, lastdayofmonth[1]).date()

    str_startdate = startdate.strftime(DATE_FORMAT)
    str_enddate = enddate.strftime(DATE_FORMAT)
    return {
        'string_startdate': str_startdate,
        'string_enddate': str_enddate
    }


def get_shift_details(config, dag_run):

    start_date = dt.strptime(get_week_start_end_date(
        config, dag_run)['string_startdate'], DATE_FORMAT)
    end_date = dt.strptime(get_week_start_end_date(
        config, dag_run)['string_enddate'], DATE_FORMAT)
    assignment_days = [
        "urn:replicon:day-of-week:monday",
        "urn:replicon:day-of-week:tuesday",
        "urn:replicon:day-of-week:wednesday",
        "urn:replicon:day-of-week:thursday",
        "urn:replicon:day-of-week:friday"
    ]
    country = dag_run.conf['country']
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
    items = dag_run.conf['item']
    if dag_run.conf["country"].lower() == "romania":
        users = []
        for group in items:
            users.extend(group.get("users", []))
        return users
    return items


def do_format_logs():
    log_artifacts = []
    log_records = []

    user_shift_logs = rail.result(
        'gather_shift_logs')
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


def get_default_shift():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:shift-list-column:name",
            "urn:replicon:shift-list-column:is-enabled"
        ]
    }
