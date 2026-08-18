from datetime import datetime, timedelta
import rail
from uuid import uuid4

from moodys.user_sync.france_v1.mapper.user_sync_mapper import user_sync_mapper

null = None


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
    "jobtitle": "Job Title",
}

INTERN_JOB_TITLES = ('Intern', 'Co-op Student')

PAYRULE_BY_ETYPE = {
    'Intern': 'Moodys - France - Intern',
    'Admin':  'Moodys - France - Admin',
    'ETAM':   'Moodys - France - ETAM',
    'CR':     'Moodys - France - Cadre En Realisation',
    'CA':     'Moodys - France - Cadre Autonome',
}

OVERTIME_ELIGIBLE_TYPES = ('Admin', 'ETAM', 'CR', 'CA')
OVERTIME_TEMPLATE_NAME = 'France OT approval'
OVERTIME_APPROVAL_PATH = 'Supervisor OT/ Complementary hrs'


def get_overtime_template_name(item):
    return OVERTIME_TEMPLATE_NAME if get_employee_type_name(item) in OVERTIME_ELIGIBLE_TYPES else null


def get_overtime_approval_path_name(item):
    return OVERTIME_APPROVAL_PATH if get_employee_type_name(item) in OVERTIME_ELIGIBLE_TYPES else null


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_employee_type_name(item):
    """V2.2 - derive Replicon Employee Type for France.

    Order matters: Intern (Job Title overrides Category), then Admin
    (ETAM + FTE<1), then CR/CA/ETAM passthrough. Fallback returns the raw
    category so edge values like NA/CA213/CA217 still flow through.
    """
    if item.get('jobtitle') in INTERN_JOB_TITLES:
        return 'Intern'
    cat = item.get('employeecategory')
    fte = _to_float(item.get('ftepercent'))
    if cat == 'ETAM' and fte is not None and fte < 1.0:
        return 'Admin'
    return cat


def get_employee_category(item):
    """V2.2 France - derive the Replicon 'Employee Category' dropdown value.

    Per mapper Row 17:
      - Admin: always 'Part time'
      - Intern / ETAM / CR / CA: FTE == 1.0 -> 'Full time', else 'Part time'
      - Anything else (NA / CA213 / CA217 / blank): returns None (no UDF write)

    Returns 'Full time' | 'Part time' | None.
    """
    etype = get_employee_type_name(item)
    if etype == 'Admin':
        return 'Part Time'
    if etype in ('Intern', 'ETAM', 'CR', 'CA'):
        fte = _to_float(item.get('ftepercent'))
        return 'Full Time' if fte == 1.0 else 'Part Time'
    return None


def _expected_activity_names(derived_type):
    if not derived_type:
        return []
    return [e['activityname'] for e in user_sync_mapper
            if e['type'] == 'activity' and e['derivedemployeetype'] == derived_type]


def _resolve_activities(derived_type):
    """Master-DAG-only: join mapper activity names against get_all_activities.

    Returns (missing_names, assigned_entries). rail.result('get_all_activities')
    is only resolvable in the master DAG; child DAGs read the result via
    dag_run.conf['assignedactivities'] / dag_run.conf['missingactivitynames'].
    """
    expected = _expected_activity_names(derived_type)
    if not expected:
        return [], []
    all_activities = rail.result('get_all_activities') or []
    by_name = {a.get('name'): a.get('uri') for a in all_activities}
    missing = [n for n in expected if n not in by_name]
    if missing:
        return missing, []
    return [], [{"uri": by_name[n], "name": n} for n in expected]


def get_assigned_activities(dag_run):
    return list(dag_run.conf.get('assignedactivities') or [])


def get_missing_activity_names(dag_run):
    return list(dag_run.conf.get('missingactivitynames') or [])


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


def get_process_users_conf(item, config):
    def get_location_uri(item):
        location_details = list(filter(lambda x: x['name'] == item['locationname']
                                and x['code'] == item['locationcode'], rail.result('get_all_locations')))
        if location_details:
            return location_details[0]['uri']
        return null

    def get_department_uri(item):
        dept_details = list(filter(lambda x: x['name'] == item['companyname']
                            and x['code'] == item['companycode'], rail.result('get_all_departments')))
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

    def get_ftepercenttype(item):
        if item['ftepercent'] and item['ftepercent'] != 'None':
            if float(item['ftepercent']) >= 1.0:
                return "Full Time"
            return "Part Time"
        return null

    def get_timesheettemplate_name(item):
        """V2.2 - Timesheet template keyed on derived Employee Type."""
        etype = get_employee_type_name(item)
        
        matching_timesheet_template_details_from_mapper = list(filter(
            lambda x: x['type'] == 'timesheet_template' and x['employeetype'] == etype, user_sync_mapper))

        if matching_timesheet_template_details_from_mapper:
            return matching_timesheet_template_details_from_mapper[0]['timesheet_template']
        
        return ""

    def get_timesheet_period(item):
        """V2.2: All categories use 'Monthly'. Termination/rehire handled by update flow."""
        # pylint: disable=unused-argument
        return "Monthly"

    def get_timesheetapprovalpathname(item):
        """V2.2: All categories route to 'Supervisor'."""
        # pylint: disable=unused-argument
        return "Supervisor"

    def get_payrule_name(item):
        if item.get('jobtitle') in INTERN_JOB_TITLES:
            return PAYRULE_BY_ETYPE['Intern']
        cat = item.get('employeecategory')
        fte = _to_float(item.get('ftepercent'))
        hrs = _to_float(item.get('actualworkinghrs'))
        if cat == 'ETAM':
            if hrs is not None and hrs < 35 and fte is not None and fte < 1.0:
                return PAYRULE_BY_ETYPE['Admin']
            if hrs == 37 and fte == 1.0:
                return PAYRULE_BY_ETYPE['ETAM']
            return null
        if cat == 'CR':
            return PAYRULE_BY_ETYPE['CR']
        if cat == 'CA':
            return PAYRULE_BY_ETYPE['CA']
        return null

    missing_activities, assigned_activities = _resolve_activities(
        get_employee_type_name(item))

    return {
        **dict(item.items()),
        **{
            'timezoneuri': get_timezone_uri(item),
            'rehiredefinitionuri': rail.result('get_user_udfs')['rehiredefinitionuri'],
            'regularshiftuserdefinitionuri': rail.result('get_user_udfs')['regularshiftuserdefinitionuri'],
            'actualworkinghrsdefinitionuri': rail.result('get_user_udfs')['actualworkinghrsdefinitionuri'],
            'employeecategorydefinitionuri': rail.result('get_user_udfs')['employeecategorydefinitionuri'],
            'employeecategory2definitionuri':  rail.result('get_user_udfs')['employeecategory2definitionuri'],
            'ftepercentdefinitionuri': rail.result('get_user_udfs')['ftepercentdefinitionuri'],
            'adpfiledefinitionuri': rail.result('get_user_udfs')['adpfiledefinitionuri'],
            'employeecategorydropdownuri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_employeecategory_udf_dropdown_values'), 'name', get_employee_category(item), 'uri') if get_employee_category(item) else null,
            'employeetypeuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_employeetypes'), 'name', get_employee_type_name(item), 'uri'),
            'derivedemployeetype': get_employee_type_name(item),
            'derivedemployeecategory': get_employee_category(item),
            'assignedactivities': assigned_activities,
            'missingactivitynames': missing_activities,
            'divisionuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions'), 'name', item['divisionname'], 'uri'),
            'locationuri': get_location_uri(item),
            'departmenturi': get_department_uri(item),
            'timesheettemplatename': get_timesheettemplate_name(item),
            'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"), 'displayText',
                                                                         get_timesheettemplate_name(item), "uri") if get_timesheettemplate_name(item) else null,
            'timesheetapprovalpathname': get_timesheetapprovalpathname(item),
            'timesheetapprovalpathuri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_timesheet_approval_paths'), 'displayText', get_timesheetapprovalpathname(item), 'uri'),
            'timesheetperiod': get_timesheet_period(item),
            'timeofftemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"), 'displayText', config.TIMEOFF_TEMPLATE, "uri"),
            'timeoffapprovalpathuri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_timeoff_approval_paths'), 'displayText', config.TIMEOFF_APPROVAL_PATH, 'uri'),
            'payrulename': get_payrule_name(item),
            'payrulescripturi': rail.find_first_by_attr_and_get_attr(rail.result(
                "get_all_payrule_scripts"), 'displayText', get_payrule_name(item), "uri") if get_payrule_name(item) else null,
            'supervisorpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_permission_set'), 'displayText', 'Supervisor', 'uri'),
            'enduserwithreportspermissionuri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_permission_set'), 'displayText', 'End user with reports', 'uri'),
            'supervisor_log': rail.result('create_supervisor_log'),
            'get_required_time_off_types': rail.result('get_required_time_off_types'),
            'ftepercenttype': get_ftepercenttype(item),
            'locationudfdefinitionuri': rail.result('get_user_udfs')['locationudfdefinitionuri'],
            'jobtitledefinitionuri': rail.result('get_user_udfs')['jobtitledefinitionuri'],
            'overtimetemplateuri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"), 'displayText',
                get_overtime_template_name(item), "uri") if get_overtime_template_name(item) else null,
            'overtimeapprovalpathuri': rail.find_first_by_attr_and_get_attr(
                rail.result(
                    'get_workauthorization_approval_paths'), 'displayText',
                get_overtime_approval_path_name(item), "uri") if get_overtime_approval_path_name(item) else null,
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
            'todaysdate': (datetime.now()).strftime("%d/%m/%Y")
        }
    }


def get_udfs(userstatus, dag_run):
    # pylint: disable=too-many-branches
    udfs = []

    def add_udf_field_values(definitionuri, dropdownuri=null, textvalue=null, number=null, dropdown_na=False):
        # dropdown_na=True emits the explicit-null dropdown object shape required for
        # 'NA' dropdown writes (e.g. France Regular/Shift User per mapper Row 34):
        #   "dropDownOption": {"uri": null, "name": null}
        # instead of the default behavior where dropdownuri=null collapses dropDownOption to null.
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
            } if (dropdownuri != null or dropdown_na) else null,
            "number": number
        })

    if userstatus == 'adduser':
        # V2.2: Employee Category dropdown is derived (Full time / Part time)
        # from derived Employee Type + FTE% per mapper Row 17, not 1:1 from the feed.
        if dag_run.conf.get('derivedemployeecategory') and dag_run.conf.get('employeecategorydropdownuri'):
            add_udf_field_values(
                definitionuri=dag_run.conf['employeecategorydefinitionuri'], dropdownuri=dag_run.conf['employeecategorydropdownuri'])
        # V2.2: Employee Category 2 mirrors the derived Employee Type per mapper Row 33,
        # not the raw feed 'Employee Category' column.
        if dag_run.conf.get('derivedemployeetype'):
            add_udf_field_values(
                definitionuri=dag_run.conf['employeecategory2definitionuri'], textvalue=dag_run.conf['derivedemployeetype'])
        if dag_run.conf['ftepercent'] and dag_run.conf['ftepercent'] != 'None':
            add_udf_field_values(
                definitionuri=dag_run.conf['ftepercentdefinitionuri'], number=dag_run.conf['ftepercent'])
        if dag_run.conf['adpfile']:
            add_udf_field_values(
                definitionuri=dag_run.conf['adpfiledefinitionuri'], textvalue=dag_run.conf['adpfile'])
        if dag_run.conf['rehire']:
            add_udf_field_values(
                definitionuri=dag_run.conf['rehiredefinitionuri'], textvalue=dag_run.conf['rehire'])
        if dag_run.conf['actualworkinghrs']:
            add_udf_field_values(
                definitionuri=dag_run.conf['actualworkinghrsdefinitionuri'], textvalue=dag_run.conf['actualworkinghrs'])
        if dag_run.conf.get('jobtitle') and dag_run.conf.get('jobtitledefinitionuri'):
            add_udf_field_values(definitionuri=dag_run.conf['jobtitledefinitionuri'],
                                 textvalue=dag_run.conf['jobtitle'])
        # V2.2: Regular/Shift User UDF is NA for France per mapper Row 34.
        # Emit the explicit-null dropdown shape to clear any prior value.
        if dag_run.conf.get('regularshiftuserdefinitionuri'):
            add_udf_field_values(
                definitionuri=dag_run.conf['regularshiftuserdefinitionuri'], dropdown_na=True)

    if userstatus == 'updateuser':
        current_rehire = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'Rehire', 'text')

        current_adpfile = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'ADP File#', 'text')

        current_ftppercent = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'FTE%', 'text')

        current_employeecategory = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'Employee Category', 'text')

        current_actualworkinghrs = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'Actual Working Hours', 'text')

        current_locationudf = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'Location', 'text')

        if current_rehire != dag_run.conf['rehire']:
            add_udf_field_values(
                definitionuri=dag_run.conf['rehiredefinitionuri'], textvalue=dag_run.conf['rehire'])

        if current_adpfile != dag_run.conf['adpfile']:
            add_udf_field_values(
                definitionuri=dag_run.conf['adpfiledefinitionuri'], textvalue=dag_run.conf['adpfile'])

        if (dag_run.conf['ftepercent'] and dag_run.conf['ftepercent'] != 'None') and current_ftppercent != dag_run.conf['ftepercent']:
            add_udf_field_values(
                definitionuri=dag_run.conf['ftepercentdefinitionuri'], number=dag_run.conf['ftepercent'])

        # V2.2: Employee Category dropdown stores derived value (Full time / Part time)
        # per mapper Row 17. Compare current stored dropdown text against the derived target.
        if (current_employeecategory != dag_run.conf.get('derivedemployeecategory')
                and dag_run.conf.get('derivedemployeecategory')
                and dag_run.conf.get('employeecategorydropdownuri')):
            add_udf_field_values(
                definitionuri=dag_run.conf['employeecategorydefinitionuri'], dropdownuri=dag_run.conf['employeecategorydropdownuri'])

        # V2.2: Employee Category 2 mirrors derived Employee Type per mapper Row 33.
        # Compare against the separately-stored EC2 text, not the dropdown.
        current_employeecategory2 = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'Employee Category 2', 'text')
        if (current_employeecategory2 != dag_run.conf.get('derivedemployeetype')
                and dag_run.conf.get('derivedemployeetype')):
            add_udf_field_values(
                definitionuri=dag_run.conf['employeecategory2definitionuri'], textvalue=dag_run.conf['derivedemployeetype'])

        if current_actualworkinghrs != dag_run.conf['actualworkinghrs']:
            add_udf_field_values(
                definitionuri=dag_run.conf['actualworkinghrsdefinitionuri'], textvalue=dag_run.conf['actualworkinghrs'])

        if current_locationudf == 'USA':
            add_udf_field_values(
                definitionuri=dag_run.conf['locationudfdefinitionuri'])

        current_jobtitle = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_current_udf_values'), 'customField.displayText', 'Job Title', 'text')

        if current_jobtitle != dag_run.conf.get('jobtitle') and dag_run.conf.get('jobtitledefinitionuri'):
            add_udf_field_values(definitionuri=dag_run.conf['jobtitledefinitionuri'],
                                 textvalue=dag_run.conf['jobtitle'])

        # V2.2: Regular/Shift User UDF is NA for France per mapper Row 34.
        # Emit the explicit-null dropdown shape on every update to clear any prior value.
        if dag_run.conf.get('regularshiftuserdefinitionuri'):
            add_udf_field_values(
                definitionuri=dag_run.conf['regularshiftuserdefinitionuri'], dropdown_na=True)

    return udfs


def get_login_status(dag_run):
    if dag_run.conf['enddate']:
        return False
    return True


def validate_enddate(dag_run):
    return datetime.strptime(dag_run.conf['enddate'], '%d/%m/%Y') > datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y')


def get_schedule_name(dag_run):
    """V2.2: Schedule keyed on derived Employee Type."""
    etype = dag_run.conf.get('derivedemployeetype')
    if etype in ('Admin', 'Intern'):
        return "France - 35H/WEEK"
    if etype == 'ETAM':
        return "France - 37H/WEEK"
    if etype == 'CR':
        return "France - 38H30/WEEK"
    if etype == 'CA':
        return "France - 1 for each day Monday to Friday"
    return "France - 35H/WEEK"


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
                        "name": get_schedule_name(dag_run),
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": get_schedule_name(dag_run)
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ],
            "workWeekStartDayUri": config.DEFAULT_WORK_WEEK if not dag_run.conf['employeetypename'] == 'NA' else config.EXCEPTION_WORK_WEEK,
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
                "name": config.HOLIDAY_CALENDER
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
            ] + ([{"uri": dag_run.conf['overtimetemplateuri'], "name": null}]
                 if dag_run.conf.get('overtimetemplateuri') else []),
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
            "assignedActivities": get_assigned_activities(dag_run),
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
            "extensionFieldValues": [],
            "workAuthorizationApprovalPath": {
                "uri": dag_run.conf['overtimeapprovalpathuri'],
                "name": null
            } if dag_run.conf.get('overtimeapprovalpathuri') else null,
        }
    }


def put_timeoff_assignment_for_user(dag_run):
    timeofftype_uris = list(map(lambda x: x['timeofftypeuri'], rail.load_all_records(
        dag_run.conf['get_required_time_off_types'])))
    return {
        "userUri": rail.result('add_new_user')['uri'],
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
    if not rail.find_first_by_attr_and_get_attr(rail.result('get_user_info')['permissionSets'],
                                                'displayText', 'End user with reports', 'displayText'):
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
    if not current_payrulescript:
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
    """V2.2: Re-assign Schedule Policy when the derived schedule name differs from
    the user's currently stored schedule. All France V2.2 schedules are
    office-schedule type; only the schedule NAME varies by derived Employee Type.
    Returns null when the schedule name is unchanged.
    """
    new_schedule_name = get_schedule_name(dag_run)
    current_schedule = rail.result(
        'get_user_info').get('schedulePolicies') or []
    current_schedule_name = current_schedule[-1].get(
        'name') if current_schedule else None
    if current_schedule_name == new_schedule_name:
        return null
    return {
        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementSchedule": [],
        "updateScheduleOverDateRange": {
            "replacementScheduleEntries": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": new_schedule_name,
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": new_schedule_name
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effectivedate'])
                }
            ],
            "endDate": null
        }
    }


def _build_activities_modification(dag_run):

    current_etype_group = rail.result(
        'get_effective_user_groupmembership', 'employeetype').get('name', '')
    if current_etype_group == dag_run.conf.get('derivedemployeetype'):
        return null
    new_activities = dag_run.conf.get('assignedactivities')
    if not new_activities:
        return null
    return [{"uri": activity['uri']} for activity in new_activities]


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
                    "name": config.HOLIDAY_CALENDER
                }
            },
            "locationScheduleToApply": update_location_grp(dag_run.conf['locationuri'],
                                                           rail.result('get_effective_user_groupmembership', 'location').get('uri', ''), dag_run),
            "divisionScheduleToApply": update_division_grp(dag_run.conf['divisionuri'],
                                                           rail.result('get_effective_user_groupmembership', 'division').get('uri', ''), dag_run),
            "departmentGroupScheduleToApply": update_department_grp(dag_run.conf['departmenturi'],
                                                                    rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), dag_run),
            "employeeTypeGroupScheduleToApply": update_employeetype_grp(dag_run.conf['employeetypeuri'],
                                                                        rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), dag_run),
            "permissionSetsToApply": update_permission_set(dag_run),
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": update_payrule_script(dag_run),
            "schedulePolicyToApply": update_schedule(dag_run),
            "activitiesToApply": _build_activities_modification(dag_run),
            "workAuthorizationApprovalPathToApply": {
                "uri": dag_run.conf['overtimeapprovalpathuri'],
                "name": null
            } if dag_run.conf.get('overtimeapprovalpathuri') else null,
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def is_location_available_in_mapper(dag_run):
    location_in_mapper = list(filter(lambda x: x['type'] == 'location' and x['locationname'] == dag_run.conf['locationname']
                                     and x['locationcode'] == dag_run.conf['locationcode'], user_sync_mapper))
    return bool(location_in_mapper)


def is_department_available_in_mapper(dag_run):
    department_in_mapper = list(filter(lambda x: x['type'] == 'department' and x['departmentname'] == dag_run.conf['companyname']
                                       and x['departmentcode'] == dag_run.conf['companycode'], user_sync_mapper))
    return bool(department_in_mapper)


def is_employeetype_available_in_mapper(dag_run):
    derived = dag_run.conf.get('derivedemployeetype')
    employeetype_in_mapper = list(filter(
        lambda x: x['type'] == 'employeetype' and x['employeetypename'] == derived, user_sync_mapper))
    return bool(employeetype_in_mapper)


def test_valid_fields_add(dag_run):
    return bool(is_location_available_in_mapper(dag_run) and dag_run.conf['locationuri'] and
                is_department_available_in_mapper(
                    dag_run) and dag_run.conf['departmenturi']
                and is_employeetype_available_in_mapper(dag_run) and dag_run.conf['employeetypeuri'] and (
                    dag_run.conf['ftepercent'] and dag_run.conf['ftepercent'] != "None")
                and dag_run.conf.get('payrulescripturi')
                and not get_missing_activity_names(dag_run))


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
    if not dag_run.conf['ftepercent'] or dag_run.conf['ftepercent'] == "None":
        exception.append('FTE% not available in feed file')
    if not dag_run.conf.get('payrulescripturi'):
        exception.append('Pay Rule could not be derived')
    missing = get_missing_activity_names(dag_run)
    if missing:
        exception.append(
            f"Activities not available in Replicon: {', '.join(missing)}")

    return rail.smartjoin_by_delim(exception, ";")


def is_enddate_in_future(dag_run):
    return datetime.strptime(dag_run.conf['enddate'], '%d/%m/%Y') > datetime.strptime(dag_run.conf['todaysdate'], '%d/%m/%Y')


def get_update_user_message():
    if get_task_state('log_supervisor_not_present') == 'success':
        return ""
    if get_task_state('log_supervisor_end_date_in_past') == 'success':
        return 'User Partially Updated, Supervisor not added due to end date in past'
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'User Partially Added, Supervisor is disabled in replicon'
    return "User Updated"


def get_update_user_severity():
    if get_task_state('log_supervisor_not_present') == 'success' or get_task_state('log_supervisor_end_date_in_past') == 'success' \
            or get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'Exception'
    return 'Success'


def test_valid_fields_update(dag_run):
    return bool(is_location_available_in_mapper(dag_run) and dag_run.conf['locationuri'] and
                is_department_available_in_mapper(
                    dag_run) and dag_run.conf['departmenturi']
                and is_employeetype_available_in_mapper(dag_run) and dag_run.conf['employeetypeuri'] and (dag_run.conf['ftepercent']
                                                                                                          and dag_run.conf['ftepercent'] != "None")
                and dag_run.conf.get('payrulescripturi')
                and not get_missing_activity_names(dag_run))


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
    if not dag_run.conf['ftepercent'] or dag_run.conf['ftepercent'] == "None":
        exception.append('FTE% not available in feed file')
    if not dag_run.conf.get('payrulescripturi'):
        exception.append(
            'Pay Rule could not be derived (no condition matched for: '
            f"employeecategory={dag_run.conf.get('employeecategory')}, "
            f"actualworkinghrs={dag_run.conf.get('actualworkinghrs')}, "
            f"ftepercent={dag_run.conf.get('ftepercent')}, "
            f"jobtitle={dag_run.conf.get('jobtitle')})")
    missing = get_missing_activity_names(dag_run)
    if missing:
        exception.append(
            f"Activities not available in Replicon: {', '.join(missing)}")

    return rail.smartjoin_by_delim(exception, ";")


def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
            rail.result('search_supervisor_in_replicon')[0]['loginname'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['loginName']:
        return False
    if rail.result('create_spervisor') and rail.result('create_spervisor')['loginName']:
        return True
    return True


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def update_employee_endate_and_timesheet_period_payload(dag_run):
    enddate_plus_1_day = (datetime.strptime(
        dag_run.conf['enddate'], '%d/%m/%Y') + timedelta(days=1)).strftime('%d/%m/%Y')
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
