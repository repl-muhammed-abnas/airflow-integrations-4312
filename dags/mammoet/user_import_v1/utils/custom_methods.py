from datetime import datetime
from functools import lru_cache
import json
from dateutil.parser import parse as date_parser
import rail

LOCATION_DELIMITER = " / "
PARENT_LEGAL_ENTITY = "Mammoet"
ACTIVITIES_TO_ASSIGN = [
        "Engineer","Project Manager","Controller",
     "Administrator","Safety Officer","Operator","Supervisor",
     "Yard","Truck Driver","Rigger","Welder","Mechanic"
    ]

DATE_FORMAT = "%Y-%m-%d"

def get_compose_required_field(item, config):
    if not item:
        return []
    country_details = get_location_details_from_code(config.LOCATION_CODE_MAPPER_TO_USE, item['legal_entity_code'][:2])
    return {
        **item,
        **{
            'country' : country_details.get('Country'),
            'country_code' : country_details.get('Country_Code'),
            'country_iso_code' : country_details.get('ISO_Code'),
            "legal_entity_full_path": f"{PARENT_LEGAL_ENTITY}{LOCATION_DELIMITER}{item['legal_entity']}"
        }
    }


def is_both_date_are_same(date1:str, date2:str):
    return date_parser(date1) == date_parser(date2)

def get_today_date(return_as_date=None):
    now = datetime.utcnow()
    if return_as_date:
        return now.date()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def get_replicon_date_from_str(date_str, date_format=DATE_FORMAT, use_parser=False):
    _date = date_parser(date_str)
    if not use_parser:
        _date = datetime.strptime(date_str, date_format)
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def get_group_details(group, payload_value):
    # rail.result can be cached. Check performance with and without it
    return rail.find_first_by_attr_and_get_attr(rail.result(f"get_replicon_{group}_details"),
            "full_path", payload_value)

lru_cache(maxsize=32)
def get_replicon_timeoffs():
    return rail.result("get_all_timeoffs")

lru_cache(maxsize=32)
def get_replicon_policies():
    return rail.result('get_all_polices')

lru_cache(maxsize=16)
def get_replicon_activities():
    return rail.result("get_all_activities")

lru_cache(maxsize=16)
def get_replicon_holiday_calender(calender_name):
    return rail.result("get_all_holiday_calenders").get(calender_name, {})

def get_location_details_from_code(LOCATION_CODE_MAPPER_TO_USE, country_code):
    return rail.find_first_by_attr_and_get_attr(
        LOCATION_CODE_MAPPER_TO_USE, 'Country_Code', country_code, default={})

def process_each_user_conf(dag_run, item, config):
    get_all_polices = get_replicon_policies()

    replicon_activities = get_replicon_activities()
    activities_to_assign = config.ACTIVITY_MAPPER_TO_USE.get(item['employee_type_name'], [])
    activities_found_in_replicon = list(map(lambda _activity: _activity['name'],
                                            filter(lambda activity: activity['name'] in activities_to_assign, replicon_activities['activities'])))

    get_all_permission_sets = rail.result("get_all_permission_sets")

    mapper_timeoffs = list(filter(lambda timeoff: timeoff['Country'] == item['country']
                                    and timeoff['Time Off Profile'] == item['timeoff_profile_code'], config.TIMEOFF_MAPPER_TO_USE))
    replicon_timeoffs = get_replicon_timeoffs()

    timesheet_template = rail.find_first_by_attr_and_get_attr(list(filter(lambda ts:ts['Country'] == item['country']
                                                ,config.TIMESHEET_MAPPER_TO_USE)), "Pay Rule Name", item['payrule_name'])
    if timesheet_template:
        timesheet_template['uri'] = rail.find_first_by_attr_and_get_attr(get_all_polices, 'name', timesheet_template['Timesheet Template'], 'uri')

    timezone_from_mapper = rail.find_first_by_attr_and_get_attr(
        config.TIMEZONE_MAPPER_TO_USE, "TimezoneID", item['time_zone']) if item['time_zone'] else None

    return{
        **{
            "payload_id": dag_run.conf['payload_id'],
            "supervisor_log": rail.result("create_supervisor_log"),
        },
        **item,
        **{
            "groups": {
                "legal_entities": get_group_details('legal_entities', f"{PARENT_LEGAL_ENTITY}{LOCATION_DELIMITER}{item['legal_entity']}"),
                "employee_type": get_group_details('employee_type', item['employee_type_name']),
                "pay_grade": get_group_details('pay_grade', item['pay_grade_name']),
                "location": get_group_details('location', f"{item['country']}{LOCATION_DELIMITER}{item['location']}"),
                "cost_center": get_group_details('cost_center', item['cost_center'])
            },
            "user_templates": {
                "timeoff_template": rail.find_first_by_attr_and_get_attr(get_all_polices, 'name', 'Time Off'),
                "timesheet_template": timesheet_template
            },
            "user_permissions": {
                "supervisor": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Supervisor'),
                "basic": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Project Resource with Reports'),
            },
            "custom_fields": {
                "voluntary_ot": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_user_custom_fields'), 'displayText', 'Voluntary OT'),
                "voluntary_ot_effective_date": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_user_custom_fields'), 'displayText', 'Voluntary OT Effective Date'),
                "location": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_user_custom_fields'), 'displayText', 'User Country')
            },
            "mapper_derived":{
                "timeoff_to_assign": list(map(lambda to: {**to, **{"uri": rail.find_first_by_attr_and_get_attr(
                    replicon_timeoffs, 'name', to['Time Off Type Name'], 'uri')}} , mapper_timeoffs)),
                "timesheet_template": timesheet_template,
                "timezone": timezone_from_mapper,
                "activities": activities_to_assign,
                "work_week": config.WORK_WEEK_MAPPER_TO_USE.get(item['country'], {}),
                "timesheet_period": config.TIMESHEET_PERIOD_MAPPER_TO_USE.get(item['country'], {})

            },
            "activities": {
                "activities": list(filter(lambda activity: activity['name'] in activities_to_assign, replicon_activities['activities'])),
                "exception": list(filter(lambda activity: activity not in activities_found_in_replicon, activities_to_assign))
            },
            "replicon_payrule_scripts": rail.find_first_by_attr_and_get_attr(rail.result('get_all_payrule_scripts'), 'displayText' , item['payrule_name']),
            "replicon_office_schedule": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_office_schedule'), 'displayText' , item['office_schedule_name']),
            "holiday_calender": get_replicon_holiday_calender(item['holiday_calendar_external_code'])
        }
    }


def do_format_logs(dag_run):
    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        return "Success"

    master_log = rail.load_all_records(dag_run.conf['exception_log'])

    for log in dag_run.conf['logs']:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)
    users = list(
        set(map(lambda x: x['properties'].get('employee_id', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for employeeid in users:
        user_logs = list(
            filter(lambda x: x['properties'].get('employee_id', '') == employeeid and x['properties'].get('details', ''), master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                'payload_id': dag_run.conf['payload_id'],
                'employee_id': employeeid,
                'login_name': first['properties'].get('login_name'),
                'status': get_status(user_logs),
                'action': first['properties'].get('action'),
                'details': ";".join(list(set(map(lambda x: x['properties'].get('details'), user_logs)))),
                'jobid': first['ecid'],
            })
    rail.set_result(key="success", val=len(list(filter(lambda log: log['status'].lower() == 'success', logs))))
    rail.set_result(key="error", val=len(list(filter(lambda log: log['status'].lower() == 'error', logs))))
    rail.set_result(key="exception", val=len(list(filter(lambda log: log['status'].lower() == 'error', logs))))
    return json.dumps(logs, ensure_ascii=False)


def get_timeoff_assignment_log_message(dag_run):
    log = []
    timeoff_not_found = list(map(lambda to: to['Time Off Type Name'],
                            filter(lambda timeoff: timeoff['uri'] is None, dag_run.conf['mapper_derived']['timeoff_to_assign'])))
    if timeoff_not_found:
        log.append(f"{rail.smartjoin_by_delim(timeoff_not_found, ';')} timeoffs not assigned to the user")

    timeoff_found = list(map(lambda to: to['Time Off Type Name'],
                            filter(lambda timeoff: timeoff['uri'], dag_run.conf['mapper_derived']['timeoff_to_assign'])))
    if timeoff_found:
        log.append(f"{rail.smartjoin_by_delim(timeoff_found, ';')} timeoffs assigned to the user")

    return rail.smartjoin_by_delim(log, ';')
