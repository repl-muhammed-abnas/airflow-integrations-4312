from datetime import datetime, timedelta, timezone
import rail
from uuid import uuid4

from moodys.user_sync.germany.mapper.user_sync_mapper import user_sync_mapper

null = None

# R46 - Pay Rule names (Germany).
PAYRULE_ABOVE_FULL_TIME = 'Germany above Full time Social security Payrule'
PAYRULE_BELOW_FULL_TIME = 'Germany below Full time Social security Payrule'
PAYRULE_ABOVE_PART_TIME = 'Germany above Part time Social security Payrule'
PAYRULE_BELOW_PART_TIME = 'Germany below Part time Social security Payrule'
PAYRULE_OTHERS = 'Moodys France others Part time'  # For Update scenario only

# R19 - Statutory Limit dropdown allowed values (Germany only).
STATUTORY_LIMIT_VALUES = ('Above Limit', 'Below Limit')


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, '%d/%m/%Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except ValueError:
        return None


MANDATORY_FIELDS = {
    "countryid": "Country ID",
    "loginname": "Login Name",
    "employeeid": "Employee ID",
    "startdate": "Start Date",
    "lastname": "LastName",
    "firstname": "FirstName",
    "timezone": "Time Zone",
    "effectivedate": "Effective Date",
    "employeetypename": "Employee Type Name",
    "divisionname": "Division Name",
    "locationname": "Location Name",
    "locationcode": "Location Code",
    "companyname": "Company Name",
    "companycode": "Company Code",
    "statutorylimit": "Statutory Limit",
    "ftepercent": "FTE%",
    "jobtitle": "Job Title",
}


def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item.get(payload_key):
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_location_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_dept_group_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_all_employee_grp_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:employee-type-group-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_user_data_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": dag_run.conf['loginname'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def test_valid_fields(dag_run):
    if not get_replicon_date(dag_run.conf['startdate']):
        return False
    if dag_run.conf['enddate']:
        if not get_replicon_date(dag_run.conf['enddate']):
            return False
    if dag_run.conf['rehire']:
        if not get_replicon_date(dag_run.conf['rehire']):
            return False
    return True


def get_invalid_fields_message(dag_run):
    log = []
    if not get_replicon_date(dag_run.conf['startdate']):
        log.append('Invalid Date format for Start Date')
    if dag_run.conf['enddate']:
        if not get_replicon_date(dag_run.conf['enddate']):
            log.append('Invalid Date format for End Date')
    if dag_run.conf['rehire']:
        if not get_replicon_date(dag_run.conf['rehire']):
            log.append('Invalid Date format for Rehire Date')

    return rail.smartjoin_by_delim(log, ";")


def _expected_activity_names(statutorylimit):
    """R20: full list of activities a Germany user receives for a given Statutory Limit.
    Reads 'activity' entries from user_sync_mapper.py (matches the Canada/US pattern
    of looking up reference data via list-filter over the mapper)."""
    if not statutorylimit:
        return []
    activity_entries = list(filter(
        lambda x: x['type'] == 'activity' and x['statutorylimit'] == statutorylimit,
        user_sync_mapper))
    return [entry['activityname'] for entry in activity_entries]


def _resolve_activities(statutorylimit):
    """Master-DAG-only helper: walks the mapper's activity entries for a given
    Statutory Limit, joins them against rail.result('get_all_activities'),
    and returns (missing_names, assigned_uris).

    `rail.result('get_all_activities')` is only resolvable in the master DAG
    (where the task lives). This function is therefore intended to be called
    from get_process_users_conf so both results can be passed through
    dag_run.conf into the process_users / process_new_users / process_update_users
    grandchild DAGs.
    """
    expected = _expected_activity_names(statutorylimit)
    if not expected:
        return [], []
    all_activities = rail.result('get_all_activities') or []
    by_name = {a.get('name'): a.get('uri') for a in all_activities}
    missing = [name for name in expected if name not in by_name]
    if missing:
        return missing, []
    assigned = [{"uri": by_name[name], "name": name} for name in expected]
    assigned = [{"uri": by_name[name], "name": name} for name in expected]
    return [], assigned


def get_missing_activity_names(dag_run):
    """Reads the missing-activity list resolved in the master DAG and passed
    through dag_run.conf. The child DAGs (process_new_users / process_update_users)
    don't have access to rail.result('get_all_activities') so the join must
    happen upstream."""
    return list(dag_run.conf.get('missingactivitynames') or [])


def _build_activities_modification(dag_run):
    """
    R20 update logic: when statutory limit changes Above<->Below, replace the user's
    activity schedule effective the supplied effective date. If Statutory Limit is
    unchanged, return null (no-op).
    """
    current_statlim = rail.find_first_by_attr_and_get_attr(
        rail.result('get_current_udf_values'), 'customField.displayText', 'Statutory Limit', 'text')
    if current_statlim == dag_run.conf.get('statutorylimit'):
        return null
    new_activities = dag_run.conf.get('assignedactivities')
    new_activities = dag_run.conf.get('assignedactivities')
    if not new_activities:
        return null
    return [{"uri": activity['uri']} for activity in new_activities]
    return [{"uri": activity['uri']} for activity in new_activities]


def get_holiday_calendar_name(item):
    """R45: Germany holiday calendar derived from location code.
    Reads 'holidaycalendar' entries from user_sync_mapper.py (mirrors the
    Canada-style list-filter lookup)."""
    holidaycalendar_details = list(filter(
        lambda x: x['type'] == 'holidaycalendar'
        and x['locationname'] == item.get('locationname')
        and x['locationcode'] == item.get('locationcode'),
        user_sync_mapper))
    if holidaycalendar_details:
        return holidaycalendar_details[0]['holidaycalendarname']
    return null


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_payrule_name(item):

    statlim = item.get('statutorylimit')
    hrs = _to_float(item.get('actualworkinghrs'))
    fte = _to_float(item.get('ftepercent'))
    if statlim not in STATUTORY_LIMIT_VALUES or hrs is None or fte is None:
        return null

    if statlim == 'Above Limit' and hrs >= 40 and fte == 1:
        return PAYRULE_ABOVE_FULL_TIME
    if statlim == 'Below Limit' and hrs == 40 and fte == 1:
        return PAYRULE_BELOW_FULL_TIME
    if statlim == 'Above Limit' and hrs < 40 and fte < 1:
        return PAYRULE_ABOVE_PART_TIME
    if statlim == 'Below Limit' and hrs < 40 and fte < 1:
        return PAYRULE_BELOW_PART_TIME
    return null


def get_process_users_conf(item, config):
    def get_location_uri(item):
        location_details = list(filter(
            lambda x: x['name'] == item['locationname'] and x['code'] == item['locationcode'],
            rail.result('get_all_locations')))
        if location_details:
            return location_details[0]['uri']
        return null

    def get_department_uri(item):
        dept_details = list(filter(
            lambda x: x['name'] == item['companyname'] and x['code'] == item['companycode'],
            rail.result('get_all_departments')))
        if dept_details:
            return dept_details[0]['uri']
        return null

    def get_timezone_uri(item):
        timezone_details = list(filter(
            lambda x: x['type'] == 'timezone' and x['timezone_ww'] == item['timezone'], user_sync_mapper))
        if timezone_details:
            timezonename = timezone_details[0]['timezone_replicon']
            return rail.find_first_by_attr_and_get_attr(rail.result('get_all_timezones'), 'displayText', timezonename, 'uri')
        return null

    payrulename = get_payrule_name(item)
    missing_activity_names, assigned_activities = _resolve_activities(
        item.get('statutorylimit'))

    schedule_to_assign = "shift" if (item['actualworkinghrs'] and item['ftepercent'] and (float(
        item['actualworkinghrs']) < 40 or float(item['ftepercent']) < 1)) else config.DEFAULT_SCHEDULE

    return {
        **dict(item.items()),
        **{
            'schedule_to_assign': schedule_to_assign,
            'missingactivitynames': missing_activity_names,
            'assignedactivities': assigned_activities,
            'timezoneuri': get_timezone_uri(item),
            'rehiredefinitionuri': rail.result('get_user_udfs')['rehiredefinitionuri'],
            'actualworkinghrsdefinitionuri': rail.result('get_user_udfs')['actualworkinghrsdefinitionuri'],
            'regularshiftuserdefinitionuri':  rail.result('get_user_udfs')['regularshiftuserdefinitionuri'],
            'employeecategorydefinitionuri': rail.result('get_user_udfs')['employeecategorydefinitionuri'],
            'employeecategory2definitionuri':  rail.result('get_user_udfs')['employeecategory2definitionuri'],
            'ftepercentdefinitionuri': rail.result('get_user_udfs')['ftepercentdefinitionuri'],
            'adpfiledefinitionuri': rail.result('get_user_udfs')['adpfiledefinitionuri'],
            'statutorylimitdefinitionuri': rail.result('get_user_udfs')['statutorylimitdefinitionuri'],
            'statutorylimitdropdownuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_statutorylimit_udf_dropdown_values'),
                'name', item['statutorylimit'], 'uri') if item['statutorylimit'] else null,
            'employeecategorydropdownuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_employeecategory_udf_dropdown_values'), 'name', item['employeecategory'], 'uri') if item['employeecategory'] else null,
            'employeetypeuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_employeetypes'), 'name', item['employeetypename'], 'uri'),
            'divisionuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions'), 'name', item['divisionname'], 'uri'),
            'locationuri': get_location_uri(item),
            'departmenturi': get_department_uri(item),
            'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"), 'displayText', config.TIMESHEET_TEMPLATE, "uri"),
            'timesheetapprovalpathuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_timesheet_approval_paths'), 'displayText', config.TIMESHEET_APPROVAL_PATH, 'uri'),
            'timesheetperiod': config.TIMESHEET_PERIOD,
            'timeofftemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"), 'displayText', config.TIMEOFF_TEMPLATE, "uri"),
            'timeoffapprovalpathuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_timeoff_approval_paths'), 'displayText', config.TIMEOFF_APPROVAL_PATH, 'uri'),
            'payrulename': payrulename,
            'payrulescripturi': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_payrule_scripts"), 'displayText', payrulename, 'uri') if payrulename else null,
            'supervisorpermissionuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permission_set'), 'displayText', 'Supervisor', 'uri'),
            'enduserwithreportspermissionuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permission_set'), 'displayText', 'End user with reports', 'uri'),
            'supervisor_log': rail.result('create_supervisor_log'),
            'get_required_time_off_types': rail.result('get_required_time_off_types'),
            'locationudfdefinitionuri': rail.result('get_user_udfs')['locationudfdefinitionuri'],
            'jobtitledefinitionuri': rail.result('get_user_udfs')['jobtitledefinitionuri'],
        }
    }


def get_process_new_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log': rail.result('create_user_log')
        }
    }


def get_process_update_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log': rail.result('create_user_log'),
            'useruri': rail.result('get_user_data')[0]['uri'],
            'userstatus':  rail.result('get_user_data')[0]['status'],
            'todaysdate': datetime.now(timezone.utc).strftime("%d/%m/%Y")
        }
    }


def get_udfs(userstatus, dag_run):
    udfs = []

    def add_udf_field_values(definitionuri, dropdownuri=null, textvalue=null, number=null):
        udfs.append({
            "customField": {
                "uri": definitionuri,
                "name": null,
                "groupUri": null
            },
            "text": textvalue,
            "date": null,
            "dropDownOption": {
                "uri": dropdownuri,
                "name": null
            } if dropdownuri != null else null,
            "number": number
        })

    if userstatus == 'adduser':
        if dag_run.conf['employeecategory']:
            add_udf_field_values(
                definitionuri=dag_run.conf['employeecategorydefinitionuri'], dropdownuri=dag_run.conf['employeecategorydropdownuri'])
        if dag_run.conf['ftepercent'] and dag_run.conf['ftepercent'] != 'None':
            add_udf_field_values(
                definitionuri=dag_run.conf['ftepercentdefinitionuri'], number=dag_run.conf['ftepercent'])
        if dag_run.conf['actualworkinghrs'] and dag_run.conf['actualworkinghrs'] != 'None':
            add_udf_field_values(
                definitionuri=dag_run.conf['actualworkinghrsdefinitionuri'], textvalue=dag_run.conf['actualworkinghrs'])
        if dag_run.conf['adpfile']:
            add_udf_field_values(
                definitionuri=dag_run.conf['adpfiledefinitionuri'], textvalue=dag_run.conf['adpfile'])
        if dag_run.conf['rehire']:
            add_udf_field_values(
                definitionuri=dag_run.conf['rehiredefinitionuri'], textvalue=dag_run.conf['rehire'])
        if dag_run.conf['statutorylimit']:
            add_udf_field_values(
                definitionuri=dag_run.conf['statutorylimitdefinitionuri'],
                dropdownuri=dag_run.conf['statutorylimitdropdownuri'])
        # V2.2: Employee Category 2 mirrors Employee Type per mapper Row 33 (global yellow row).
        if dag_run.conf.get('employeetypename') and dag_run.conf.get('employeecategory2definitionuri'):
            add_udf_field_values(
                definitionuri=dag_run.conf['employeecategory2definitionuri'], textvalue=dag_run.conf['employeetypename'])
        if dag_run.conf.get('jobtitle') and dag_run.conf.get('jobtitledefinitionuri'):
            add_udf_field_values(definitionuri=dag_run.conf['jobtitledefinitionuri'],
                                 textvalue=dag_run.conf['jobtitle'])

    if userstatus == 'updateuser':
        current_rehire = rail.find_first_by_attr_and_get_attr(
            rail.result('get_current_udf_values'), 'customField.displayText', 'Rehire', 'text')

        current_adpfile = rail.find_first_by_attr_and_get_attr(
            rail.result('get_current_udf_values'), 'customField.displayText', 'ADP File#', 'text')

        current_ftppercent = rail.find_first_by_attr_and_get_attr(
            rail.result('get_current_udf_values'), 'customField.displayText', 'FTE%', 'text')

        current_actualworkinghrs = rail.find_first_by_attr_and_get_attr(
            rail.result('get_current_udf_values'), 'customField.displayText', 'Actual Working Hours', 'text')

        current_employeecategory = rail.find_first_by_attr_and_get_attr(
            rail.result('get_current_udf_values'), 'customField.displayText', 'Employee Category', 'text')

        current_locationudf = rail.find_first_by_attr_and_get_attr(
            rail.result('get_current_udf_values'), 'customField.displayText', 'Location', 'text')

        current_statutorylimit = rail.find_first_by_attr_and_get_attr(
            rail.result('get_current_udf_values'), 'customField.displayText', 'Statutory Limit', 'text')

        if current_rehire != dag_run.conf['rehire']:
            add_udf_field_values(
                definitionuri=dag_run.conf['rehiredefinitionuri'], textvalue=dag_run.conf['rehire'])

        if current_adpfile != dag_run.conf['adpfile']:
            add_udf_field_values(
                definitionuri=dag_run.conf['adpfiledefinitionuri'], textvalue=dag_run.conf['adpfile'])

        if (dag_run.conf['ftepercent'] and dag_run.conf['ftepercent'] != 'None') and current_ftppercent != dag_run.conf['ftepercent']:
            add_udf_field_values(
                definitionuri=dag_run.conf['ftepercentdefinitionuri'], number=dag_run.conf['ftepercent'])

        if (dag_run.conf['actualworkinghrs'] and dag_run.conf['actualworkinghrs'] != 'None') \
                and current_actualworkinghrs != dag_run.conf['actualworkinghrs']:
            add_udf_field_values(
                definitionuri=dag_run.conf['actualworkinghrsdefinitionuri'], textvalue=dag_run.conf['actualworkinghrs'])

        if current_employeecategory != dag_run.conf['employeecategory']:
            add_udf_field_values(
                definitionuri=dag_run.conf['employeecategorydefinitionuri'], dropdownuri=dag_run.conf['employeecategorydropdownuri'])

        if current_locationudf == 'USA':
            add_udf_field_values(
                definitionuri=dag_run.conf['locationudfdefinitionuri'])

        if dag_run.conf['statutorylimit'] and current_statutorylimit != dag_run.conf['statutorylimit']:
            add_udf_field_values(
                definitionuri=dag_run.conf['statutorylimitdefinitionuri'],
                dropdownuri=dag_run.conf['statutorylimitdropdownuri'])

        # V2.2: Employee Category 2 mirrors Employee Type per mapper Row 33 (global yellow row).
        current_employeecategory2 = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'Employee Category 2', 'text')

        if (current_employeecategory2 != dag_run.conf.get('employeetypename')
                and dag_run.conf.get('employeetypename')
                and dag_run.conf.get('employeecategory2definitionuri')):
            add_udf_field_values(
                definitionuri=dag_run.conf['employeecategory2definitionuri'], textvalue=dag_run.conf['employeetypename'])

        current_jobtitle = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'Job Title', 'text')

        if current_jobtitle != dag_run.conf.get('jobtitle') and dag_run.conf.get('jobtitledefinitionuri'):
            add_udf_field_values(definitionuri=dag_run.conf['jobtitledefinitionuri'],
                                 textvalue=dag_run.conf['jobtitle'])

    return udfs


def get_login_status(dag_run):
    if dag_run.conf['enddate']:
        return False
    return True


def validate_enddate(dag_run):
    return datetime.strptime(dag_run.conf['enddate'], '%d/%m/%Y') > datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y')


def get_put_user_payload(dag_run, config):
    # pylint: disable=too-many-branches
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['loginname'],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['emailid'] if dag_run.conf['emailid'] else null,
            "employeeId": dag_run.conf['employeeid'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": null,
                        "officeSchedule": null,
                        "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                    },
                    "effectiveDate": null
                }
            ] if "shift" in dag_run.conf['schedule_to_assign'] else [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['schedule_to_assign'],
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['schedule_to_assign']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ],
            "workWeekStartDayUri": config.DEFAULT_WORK_WEEK,
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['startdate']),
                "endDate": get_replicon_date(dag_run.conf['enddate']) if dag_run.conf['enddate'] else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": get_login_status(dag_run),
                "loginName": dag_run.conf['loginname'],
                "SSOName": dag_run.conf['loginname'],
            },
            "holidayCalendar": {
                "uri": null,
                "name": get_holiday_calendar_name(dag_run.conf)
            },
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": dag_run.conf['enduserwithreportspermissionuri'],
                    "name": null
                }
            ],
            "policySets":
            [
                {
                    "uri": dag_run.conf['timeofftemplateuri'],
                    "name": null
                }
            ] if dag_run.conf['employeetypename'] == 'NA' else [
                {
                    "uri": dag_run.conf['timesheettemplateuri'],
                    "name": null
                },
                {
                    "uri": dag_run.conf['timeofftemplateuri'],
                    "name": null
                }
            ],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {
                "uri": dag_run.conf['timesheetapprovalpathuri'],
                "name": null
            },
            "expenseApprovalPath": null,
            "timeOffApprovalPath":  {
                "uri": dag_run.conf['timeoffapprovalpathuri'],
                "name": null
            },
            "customFieldValues": get_udfs('adduser', dag_run),
            "assignedActivities": [{"uri": activity['uri']} for activity in dag_run.conf['assignedactivities']],
            "timeZone": {
                "uri": dag_run.conf['timezoneuri'],
                "IANAName": null
            },
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf['locationuri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "divisionSchedule":  [
                {
                    "division": {
                        "uri": dag_run.conf['divisionuri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['departmenturi'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf['employeetypeuri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": dag_run.conf['timesheetperiod']
                    },
                    "effectiveDate": null
                }
            ],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrulescripturi'],
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": []
        }
    }


def put_timeoff_assignment_for_user(dag_run):
    timeofftype_uris = list(map(lambda x: x['timeofftypeuri'], rail.load_all_records(
        dag_run.conf['get_required_time_off_types'])))
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "timeOffTypeUris": timeofftype_uris
    }


def put_timeoff_assignment_for_existing_user(dag_run):
    """Same payload as put_timeoff_assignment_for_user but uses the user URI from
    dag_run.conf (already resolved in process_users.get_user_data) instead of
    rail.result('add_new_user'). Used by process_update_users to (re)assign the
    Germany time-off types after ApplyUserModifications3, which is needed when
    a user transfers in from another country."""
    timeofftype_uris = list(map(lambda x: x['timeofftypeuri'], rail.load_all_records(
        dag_run.conf['get_required_time_off_types'])))
    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": timeofftype_uris
    }


def get_data_for_supervisor_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:end-date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": dag_run.conf['supervisorid'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_remove_timeoff_payload():
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "timeOffTypeUris": []
    }


def get_add_user_message():
    if get_task_state('log_supervisor_not_present') == 'success':
        return ""
    if get_task_state('log_supervisor_end_date_in_past') == 'success':
        return 'User Partially Added, Supervisor not added due to end date in past'
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'User Partially Added, Supervisor is disabled in replicon'
    return "User Added"


def get_add_user_severity():
    if get_task_state('log_supervisor_not_present') == 'success' or get_task_state('log_supervisor_end_date_in_past') == 'success' \
            or get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'Exception'
    return 'Success'


def get_supervisor_message(action):
    if get_task_state('log_supervisor_not_created') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + \
            ';Supervisor firstname and lastname not available, Supervisor cannot be created'
    if get_task_state('log_supervisor_end_date_in_past') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + 'Supervisor not added due to end date in past'
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + 'Supervisor is disabled in replicon'
    return "User Added" if action == 'Add' else "User Updated"


def get_supervisor_status():
    if get_task_state('log_supervisor_not_created') == 'success' or get_task_state('log_supervisor_end_date_in_past') == 'success' \
            or get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'Exception'
    return 'Success'


def update_location_grp(locationuri, currentlocationuri, dag_run):
    return {
        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementLocationSchedule": [],
        "updateLocationScheduleOverDateRange": {
            "replacementLocationScheduleEntries": [
                {
                    "location": {
                        "uri": locationuri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effectivedate'])
                }
            ],
            "endDate": null
        }
    } if currentlocationuri != locationuri else null


def update_division_grp(divisionuri, currentdivisionuri, dag_run):
    return {
        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDivisionSchedule": [],
        "updateDivisionScheduleOverDateRange": {
            "replacementDivisionScheduleEntries": [
                {
                    "division": {
                        "uri": divisionuri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effectivedate'])
                }
            ],
            "endDate": null
        }
    } if divisionuri != currentdivisionuri else null


def update_department_grp(departmenturi, currentdepartmenturi, dag_run):
    return {
        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDepartmentGroupSchedule": [],
        "updateDepartmentGroupScheduleOverDateRange": {
            "replacementDepartmentGroupScheduleEntries": [
                {
                    "departmentGroup": {
                        "uri": departmenturi
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effectivedate'])
                }
            ],
            "endDate": null
        }
    } if departmenturi != currentdepartmenturi else null


def update_employeetype_grp(employeetypeuri, currentemployeetypeuri, dag_run):
    return {
        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementEmployeeTypeGroupSchedule": [],
        "updateEmployeeTypeGroupScheduleOverDateRange": {
            "replacementEmployeeTypeGroupScheduleEntries": [
                {
                    "employeeTypeGroup": {
                        "uri": employeetypeuri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effectivedate'])
                }
            ],
            "endDate": null
        }
    } if employeetypeuri != currentemployeetypeuri else null


def update_permission_set(dag_run):
    if not rail.find_first_by_attr_and_get_attr(
            rail.result('get_user_info')['permissionSets'], 'displayText', 'End user with reports', 'displayText'):
        return {
            "permissionSetUrisToAssign": [
                dag_run.conf['enduserwithreportspermissionuri']
            ],
            "policyUrisToRemovePermissionSet": []
        }
    return null


def update_user_details(dag_run):
    user_details = rail.result("get_user_info")['userDetails']
    return {
        "firstName": dag_run.conf['firstname'] if user_details['firstName'] != dag_run.conf['firstname'] else null,
        "lastName": dag_run.conf['lastname'] if user_details['lastName'] != dag_run.conf['lastname'] else null,
        "emailAddress": {
            "emailAddress": dag_run.conf['emailid']
        } if dag_run.conf['emailid'] and (user_details['emailAddress'] != dag_run.conf['emailid']) else null,
        "language": null,
        "employmentDateRange": null,
        "employmentStartDate": {
            "date": get_replicon_date(dag_run.conf['startdate'])
        } if user_details['employmentDateRange']['startDate'] != get_replicon_date(dag_run.conf['startdate']) else null,
        "employmentEndDate": {
            "date": get_replicon_date(dag_run.conf['enddate']) if bool(get_replicon_date(dag_run.conf['enddate'])) else null
        },
        "employeeId":  {
            "employeeId": dag_run.conf['employeeid']
        } if user_details['employeeId'] != dag_run.conf['employeeid'] else null,
        "displayNameParameter": null
    }


def update_payrule_script(dag_run):
    current_payrulescript = rail.result("get_user_info")[
        'payRuleScriptSchedule']

    if not dag_run.conf['payrulescripturi']:
        return null

    if not current_payrulescript:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrulescripturi'],
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        }

    if dag_run.conf['payrulescripturi'] != current_payrulescript[-1]['payRuleScript']['uri']:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrulescripturi'],
                        "name": null
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effectivedate'])
                }
            ]
        }

    return null


def update_schedule(dag_run):
    current_schedule = rail.result('get_user_info')['schedulePolicies']
    if dag_run.conf['schedule_to_assign']:
        if dag_run.conf['schedule_to_assign'] == 'shift':
            if not current_schedule or (
                    current_schedule and current_schedule[-1]['scheduleTypeUri'] != "urn:replicon:schedule-type:shift"):
                return {
                    "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementSchedule": [],
                    "updateScheduleOverDateRange": {
                        "replacementScheduleEntries": [
                            {
                                "schedulePolicy": {
                                    "officeScheduleUri": null,
                                    "name": null,
                                    "officeSchedule": {
                                        "officeScheduleUri": null,
                                        "name": null,
                                    },
                                    "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                                },
                                "effectiveDate": get_replicon_date(dag_run.conf['effectivedate'])
                            }
                        ],
                        "endDate": null
                    }
                }

        elif dag_run.conf['schedule_to_assign'] != 'shift':
            if not current_schedule or (
                    current_schedule and current_schedule[-1]['scheduleTypeUri'] == "urn:replicon:schedule-type:shift"):
                return {
                    "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementSchedule": [],
                    "updateScheduleOverDateRange": {
                        "replacementScheduleEntries": [
                            {
                                "schedulePolicy": {
                                    "officeScheduleUri": null,
                                    "name": dag_run.conf['schedule_to_assign'],
                                    "officeSchedule": {
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['schedule_to_assign'],
                                    },
                                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                                },
                                "effectiveDate": get_replicon_date(dag_run.conf['effectivedate'])
                            }
                        ],
                        "endDate": null
                    }
                }

        return null

    return null


def update_policy_sets(dag_run):
    """Reassign Germany policy templates (timesheet + time-off) only when the
    user's currently assigned templates don't already match. For 'NA' employee
    type only the time-off template applies (PutUser3 policySets logic)."""
    if dag_run.conf['employeetypename'] == 'NA':
        desired_uris = [dag_run.conf['timeofftemplateuri']]
    else:
        desired_uris = [dag_run.conf['timesheettemplateuri'],
                        dag_run.conf['timeofftemplateuri']]
    current_user_info = rail.result('get_user_info')
    current_timesheet_uri = (current_user_info.get(
        'timesheetTemplate') or {}).get('uri')
    current_timeoff_uri = (current_user_info.get(
        'timeOffTemplate') or {}).get('uri')
    current_uris = {uri for uri in (
        current_timesheet_uri, current_timeoff_uri) if uri}
    if set(desired_uris).issubset(current_uris):
        return null
    return {
        "policySetUrisToAssign": desired_uris,
        "policyUrisToRemovePolicySet": [],
        "policySetUrisToRemove": []
    }


def update_timesheet_period(dag_run):
    """Emit a replacement timesheet-period schedule entry only when the current
    period differs from the configured Germany period (Monthly). Effective date
    is the feed's effective date."""
    current_schedule = rail.result('get_user_info').get(
        'timesheetPeriodSchedule') or []
    current_period = current_schedule[-1].get('timesheetPeriod', {}).get(
        'displayText') if current_schedule else null
    if current_period == dag_run.conf['timesheetperiod']:
        return null
    return {
        "userTimesheetPeriodScheduleModificationOptionUri":
            "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": dag_run.conf['timesheetperiod']
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effectivedate'])
                }
            ],
            "endDate": null
        }
    }


def update_timesheet_approval_path(dag_run):
    """Emit only when the user's current timesheet approval path URI differs from
    the configured Germany approval path."""
    current_uri = (rail.result('get_user_info').get(
        'timesheetApprovalPath') or {}).get('uri')
    if current_uri == dag_run.conf['timesheetapprovalpathuri']:
        return null
    return {
        "uri": dag_run.conf['timesheetapprovalpathuri'],
        "name": null
    }


def update_timeoff_approval_path(dag_run):
    """Emit only when the user's current time-off approval path URI differs from
    the configured Germany approval path."""
    current_uri = (rail.result('get_user_info').get(
        'timeOffApprovalPath') or {}).get('uri')
    if current_uri == dag_run.conf['timeoffapprovalpathuri']:
        return null
    return {
        "uri": dag_run.conf['timeoffapprovalpathuri'],
        "name": null
    }


def apply_user_modifications_payload(dag_run, config):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": dag_run.conf['timezoneuri'],
                    "IANAName": null
                }
            },
            "holidayCalendarToApply": {
                "holidayCalendar": {
                    "uri": null,
                    "name": get_holiday_calendar_name(dag_run.conf)
                }
            },
            "workWeekStartToApply": {
                "workWeekStartDayUri": config.DEFAULT_WORK_WEEK
            },
            "policySetsToApply": update_policy_sets(dag_run),
            "timesheetPeriodScheduleToApply": update_timesheet_period(dag_run),
            "timesheetApprovalPathToApply": update_timesheet_approval_path(dag_run),
            "timeOffApprovalPathToApply": update_timeoff_approval_path(dag_run),
            "locationScheduleToApply": update_location_grp(
                dag_run.conf['locationuri'],
                rail.result('get_effective_user_groupmembership', 'location').get('uri', ''), dag_run),
            "divisionScheduleToApply": update_division_grp(
                dag_run.conf['divisionuri'],
                rail.result('get_effective_user_groupmembership', 'division').get('uri', ''), dag_run),
            "departmentGroupScheduleToApply": update_department_grp(
                dag_run.conf['departmenturi'],
                rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), dag_run),
            "employeeTypeGroupScheduleToApply": update_employeetype_grp(
                dag_run.conf['employeetypeuri'],
                rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), dag_run),
            "permissionSetsToApply": update_permission_set(dag_run),
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": update_payrule_script(dag_run),
            "activitiesToApply": _build_activities_modification(dag_run),
            "schedulePolicyToApply": update_schedule(dag_run),
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def is_location_available_in_mapper(dag_run):
    location_in_mapper = list(filter(
        lambda x: x['type'] == 'location' and x['locationname'] == dag_run.conf['locationname']
        and x['locationcode'] == dag_run.conf['locationcode'],
        user_sync_mapper))
    return bool(location_in_mapper)


def is_department_available_in_mapper(dag_run):
    department_in_mapper = list(filter(
        lambda x: x['type'] == 'department' and x['departmentname'] == dag_run.conf['companyname']
        and x['departmentcode'] == dag_run.conf['companycode'],
        user_sync_mapper))
    return bool(department_in_mapper)


def is_employeetype_available_in_mapper(dag_run):
    employeetype_in_mapper = list(filter(
        lambda x: x['type'] == 'employeetype' and x['employeetypename'] == dag_run.conf['employeetypename'], user_sync_mapper))
    return bool(employeetype_in_mapper)


def test_valid_fields_add(dag_run):
    base = bool(
        is_location_available_in_mapper(
            dag_run) and dag_run.conf['locationuri']
        and is_department_available_in_mapper(dag_run) and dag_run.conf['departmenturi']
        and is_employeetype_available_in_mapper(dag_run) and dag_run.conf['employeetypeuri']
    )
    if not base:
        return False
    if not dag_run.conf.get('statutorylimit'):
        return False
    if dag_run.conf['statutorylimit'] not in ('Above Limit', 'Below Limit'):
        return False
    if not get_holiday_calendar_name(dag_run.conf):
        return False
    if not get_payrule_name(dag_run.conf):
        return False
    if not _expected_activity_names(dag_run.conf.get('statutorylimit')):
        return False
    if get_missing_activity_names(dag_run):
        return False
    return True


def get_invalid_fields_message_add(dag_run):
    exception = []
    if not is_location_available_in_mapper(dag_run):
        exception.append('Location not available in mapper')
    if not dag_run.conf['locationuri']:
        exception.append('Location not available in replicon')
    if not is_department_available_in_mapper(dag_run):
        exception.append('Department not available in mapper')
    if not dag_run.conf['departmenturi']:
        exception.append('Department not available in replicon')
    if not is_employeetype_available_in_mapper(dag_run):
        exception.append('Employeetype not available in mapper')
    if not dag_run.conf['employeetypeuri']:
        exception.append('Employeetype not available in replicon')
    if not dag_run.conf.get('statutorylimit'):
        exception.append('Statutory Limit not present in payload')
    elif dag_run.conf['statutorylimit'] not in ('Above Limit', 'Below Limit'):
        exception.append(
            f"Invalid Statutory Limit value: {dag_run.conf['statutorylimit']}")
    if not get_holiday_calendar_name(dag_run.conf):
        exception.append(
            f"Holiday Calendar not available for location code {dag_run.conf.get('locationcode')}")
    if not get_payrule_name(dag_run.conf):
        exception.append(
            f"Payrule is not available for combination of statutorylimit={dag_run.conf.get('statutorylimit')}/"
            f"actualworkinghrs={dag_run.conf.get('actualworkinghrs')}/FTE%={dag_run.conf.get('ftepercent')}"
        )
    expected_activities = _expected_activity_names(
        dag_run.conf.get('statutorylimit'))
    if not expected_activities:
        if dag_run.conf.get('statutorylimit') in ('Above Limit', 'Below Limit'):
            exception.append(
                f"Activity mapping not configured for Statutory Limit value '{dag_run.conf.get('statutorylimit')}'"
            )
    else:
        for missing in get_missing_activity_names(dag_run):
            exception.append(f"Activity '{missing}' not available in replicon")

    return rail.smartjoin_by_delim(exception, ";")


def is_enddate_in_future(dag_run):
    return datetime.strptime(dag_run.conf['enddate'], '%d/%m/%Y') > datetime.strptime(dag_run.conf['todaysdate'], '%d/%m/%Y')


def get_update_user_message():
    if get_task_state('log_supervisor_not_present') == 'success':
        return ""
    if get_task_state('log_supervisor_end_date_in_past') == 'success':
        return 'User Partially Updated, Supervisor not added due to end date in past'
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'User Partially Updated, Supervisor is disabled in replicon'
    return "User Updated"


def get_update_user_severity():
    if get_task_state('log_supervisor_not_present') == 'success' or get_task_state('log_supervisor_end_date_in_past') == 'success' \
            or get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'Exception'
    return 'Success'


def test_valid_fields_update(dag_run):
    base = bool(
        is_location_available_in_mapper(
            dag_run) and dag_run.conf['locationuri']
        and is_department_available_in_mapper(dag_run) and dag_run.conf['departmenturi']
        and is_employeetype_available_in_mapper(dag_run) and dag_run.conf['employeetypeuri']
    )
    if not base:
        return False
    if not dag_run.conf.get('statutorylimit'):
        return False
    if dag_run.conf['statutorylimit'] not in ('Above Limit', 'Below Limit'):
        return False
    if not get_holiday_calendar_name(dag_run.conf):
        return False
    if not get_payrule_name(dag_run.conf):
        return False
    if not _expected_activity_names(dag_run.conf.get('statutorylimit')):
        return False
    if get_missing_activity_names(dag_run):
        return False
    return True


def get_invalid_fields_message_update(dag_run):
    exception = []
    if not is_location_available_in_mapper(dag_run):
        exception.append('Location not available in mapper')
    if not dag_run.conf['locationuri']:
        exception.append('Location not available in replicon')
    if not is_department_available_in_mapper(dag_run):
        exception.append('Department not available in mapper')
    if not dag_run.conf['departmenturi']:
        exception.append('Department not available in replicon')
    if not is_employeetype_available_in_mapper(dag_run):
        exception.append('Employeetype not available in mapper')
    if not dag_run.conf['employeetypeuri']:
        exception.append('Employeetype not available in replicon')
    if not dag_run.conf.get('statutorylimit'):
        exception.append('Statutory Limit not present in payload')
    elif dag_run.conf['statutorylimit'] not in ('Above Limit', 'Below Limit'):
        exception.append(
            f"Invalid Statutory Limit value: {dag_run.conf['statutorylimit']}")
    if not get_holiday_calendar_name(dag_run.conf):
        exception.append(
            f"Holiday Calendar not available for location code {dag_run.conf.get('locationcode')}")
    if not get_payrule_name(dag_run.conf):
        exception.append(
            f"Payrule is not available for combination of statutorylimit={dag_run.conf.get('statutorylimit')}/"
            f"actualworkinghrs={dag_run.conf.get('actualworkinghrs')}/FTE%={dag_run.conf.get('ftepercent')}"
        )
    expected_activities = _expected_activity_names(
        dag_run.conf.get('statutorylimit'))
    if not expected_activities:
        if dag_run.conf.get('statutorylimit') in ('Above Limit', 'Below Limit'):
            exception.append(
                f"Activity mapping not configured for Statutory Limit value '{dag_run.conf.get('statutorylimit')}'"
            )
    else:
        for missing in get_missing_activity_names(dag_run):
            exception.append(f"Activity '{missing}' not available in replicon")

    return rail.smartjoin_by_delim(exception, ";")


def validate_supervisor_changed():
    # PR #10604 review #6: removed dead branch checking `create_spervisor` —
    # this function is only reachable when the supervisor exists in Replicon
    # (search_supervisor_in_replicon non-empty), so create_spervisor is never
    # executed on this path and that branch could never return True.
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
            rail.result('search_supervisor_in_replicon')[0]['loginname'] == \
            rail.result('get_effective_supervisor_of_user')['supervisor']['user']['loginName']:
        return False
    return True


def get_today_date():
    now = datetime.now(timezone.utc)
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day,
    }


def update_employee_endate_and_timesheet_period_payload(dag_run, config):
    enddate_plus_1_day = (datetime.strptime(dag_run.conf['enddate'], '%d/%m/%Y') + timedelta(days=1)).strftime('%d/%m/%Y')
    return {
        "target": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "employmentDateRange": {
                "value": {
                    "startDate": get_replicon_date(dag_run.conf['startdate']),
                    "endDate": get_replicon_date(dag_run.conf['enddate'])
                }
            },
            "timesheetPeriodSchedule": [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(enddate_plus_1_day)
                    },
                    "item": null
                }
            ],
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


def clear_stored_enddate_reverse_term_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "employmentDateRange": {
                "value": {
                    "startDate": get_replicon_date(dag_run.conf['startdate']),
                    "endDate": null
                }
            },
            "timesheetPeriodSchedule": [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(dag_run.conf['effectivedate'])
                    },
                    "item": {
                        "uri": null,
                        "name": "Monthly"
                    }
                }
            ],
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }
