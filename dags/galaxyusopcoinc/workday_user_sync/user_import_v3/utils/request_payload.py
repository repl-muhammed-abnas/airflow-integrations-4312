import datetime
from functools import lru_cache
from json import loads
import pendulum
import rail
from airflow.exceptions import AirflowException
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import custom_methods, response_filter

null = None
true = True
false = False


@lru_cache(maxsize=32)
def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_user_uri():
    return get_conf()['useruri']


def get_created_user_uri():
    return rail.result("create_user")['uri']


def get_timesheet_path(dag_run):
    return {
        "uri": null,
        "name": dag_run.conf['timesheet_approval']
    } if dag_run.conf['timesheet_approval'].lower() != "na" else null


def get_timeoff_path(dag_run):
    return {
        "uri": null,
        "name": dag_run.conf['time_off_approval']
    } if dag_run.conf['time_off_approval'].lower() != "na" else null


def get_date_from_json_date(json_date):
    return datetime.date(day=json_date['day'], month=json_date['month'], year=json_date['year'])


def department_name():
    return get_conf()['JobFamilyGroup'] if get_conf()['Parent'] == "Yes" else get_conf()['JobFamily']


def location_name():
    return get_conf()['Location'] if get_conf()['Parent'] == "No" else get_conf()['Company']


def location_code():
    return get_conf()['LocationType'] if get_conf()['Parent'] == "No" else get_conf()['CompanyCode']

# pylint: disable=too-many-branches


def get_oef_field_values(caller="add"):
    conf = get_conf()
    ia_updated = False
    user_current_assigned_oefs = rail.result("bulk_get_user3")['userDetails']['extensionFieldValues'] if caller != "add" else []
    object_extension_fields = conf['ObjectExtentionfields']
    oef_fields_to_add = []
    if object_extension_fields['Job Profile']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Job Profile'],
            },
            'textValue': conf['jobprofile'],
        })

    if object_extension_fields['Job Profile Code']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Job Profile Code'],
            },
            'textValue': conf['jobprofilecode'],
        })

    if object_extension_fields['Compensation Grade']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Compensation Grade'],
            },
            'textValue': conf['compensationgrade'],
        })

    if object_extension_fields['Country']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Country'],
            },
            'textValue': conf['country'],
        })

    if object_extension_fields['Scheduled Weekly Hours']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Scheduled Weekly Hours'],
            },
            'textValue': conf['scheduledweeklyhours'],
        })

    if object_extension_fields['Default Weekly Hours']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Default Weekly Hours'],
            },
            'textValue': conf['defaultweeklyhours'],
        })

    if object_extension_fields['Contract Type']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Contract Type'],
            },
            'textValue': conf['contracttype'],
        })

    if object_extension_fields['Contract End Date']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Contract End Date'],
            },
            'textValue':  conf['contractenddate'],
        })

    if object_extension_fields['Collective Agreement']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Collective Agreement'],
            },
            'textValue':  conf['collectiveagreement'],
        })

    if object_extension_fields['Position ID']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Position ID'],
            },
            'textValue':  conf['positionid'],
        })

    if object_extension_fields['Exempt']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Exempt'],
            },
            'textValue':  conf['exempt'],
        })

    if object_extension_fields['FTE']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['FTE'],
            },
            'textValue':  conf['fte'],
        })

    if object_extension_fields['Business Title']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Business Title'],
            },
            'textValue':  conf['businesstitle'],
        })

    if object_extension_fields['Additional Job Classification']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Additional Job Classification'],
            },
            'textValue':  conf['additionaljobclassification'],
        })

    if object_extension_fields['Payroll Type']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Payroll Type'],
            },
            'textValue':  conf['payroll_type'],
        })

    if object_extension_fields['Worker Type']:
        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['Worker Type'],
            },
            'textValue':  conf['workertype'],
        })
    
    if object_extension_fields['IA_STA_International_Assignee']:
        current_ia_value = rail.find_first_by_attr_and_get_attr(
                                user_current_assigned_oefs,
                                'definition.displayText',
                                'IA/STA International Assignee',
                                'textValue'
                            )
        if current_ia_value and current_ia_value != conf['ia_sta_international_assignee']:
            if caller != "add":
                rail.set_result(
                    key = "ia_updated",
                    val = True
                )
                ia_updated = True

        oef_fields_to_add.append({
            'definition': {
                'uri': object_extension_fields['IA_STA_International_Assignee'],
            },
            'textValue':  conf['ia_sta_international_assignee'],
        })
        if conf['ia_sta_international_assignee'] == "Y":
            # Employee is on International Assignment - set Host Country to the country from feed
            oef_fields_to_add.append({
                'definition': {
                    'uri': object_extension_fields['Host_Country'],
                },
                'textValue':  conf['country'],
            })
        else:
            # Employee is not on IA (or returned from IA) - clear Host Country
            oef_fields_to_add.append({
                'definition': {
                    'uri': object_extension_fields['Host_Country'],
                },
                'textValue':  ""
            })

    # This will be updated only for ADD
    if conf['action'] == "add" or conf.get('rehire') == "yes":
        oef_fields_to_add.append({
            'definition': {
                'uri': conf['actual_employee_id_details']['uri'],
            },
            'textValue':  conf['employeeid'],
        })

    if caller == "add":
        return oef_fields_to_add

    return oef_fields_to_add, ia_updated


def get_today_date():
    now = pendulum.now('Europe/London')
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def employeeid_toupdate():
    now = pendulum.now('America/Denver')
    date = datetime.datetime.strftime(now, '%Y%m%d')
    return get_conf()['employeeid']+str(date)


def loginname_toupdate():
    now = pendulum.now('America/Denver')
    date = datetime.datetime.strftime(now, '%m%d%y')
    return get_conf()['workemail']+str(date)


def get_replicon_date(date_str):
    if not date_str:
        return None
    # date format in 20060401
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }
    

def get_location_schedule(dag_run):
    return [
        {
            "location": {
                "uri": dag_run.conf['country_location_uri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "effectiveDate": null
        }
    ] if dag_run.conf['country_location_uri'] else null


def get_policy_sets():
    policy_sets = []

    timeoff_policy = get_conf()['time_off_template']
    if timeoff_policy and timeoff_policy.lower() != "na":
        policy_sets.append({
            'uri': null,
            'name': timeoff_policy
        })

    timesheet_policy = get_conf()['timesheet_template']
    if timesheet_policy and timesheet_policy.lower() != "na":
        policy_sets.append({
            'uri': null,
            'name': timesheet_policy
        })

    return policy_sets


def get_holidaycalendar(dag_run):
    data = {
        "uri": null,
        "name": dag_run.conf['holiday_calendar']
    } if dag_run.conf['holiday_calendar'].lower() != "na" else null
    return data


def get_current_schedule(data):
    if not data and len(data) == 0:
        return None
    current_schedule = list(filter(lambda x: datetime.datetime(
        **x['effectiveDate']) if x['effectiveDate'] else datetime.datetime.min <= datetime.datetime(**get_today_date()), data))
    return None if len(current_schedule) == 0 else current_schedule[-1]


def get_timeentry_approval_path(dag_run):
    user = rail.result('create_user')['uri']
    return {
        "userUri": user,
        "approvalPathUri": rail.find_first_by_attr_and_get_attr(dag_run.conf['timeentryapprovalpaths'],
                                                                'name', dag_run.conf['time_entry_approval_path'], 'uri')
    }


def get_costcenter_schedule(dag_run):
    return [
        {
            "costCenter": {
                "uri": dag_run.conf['cost_center_uri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "effectiveDate": null
        }
    ] if dag_run.conf['cost_center_uri'] else null


def get_division_schedule(dag_run):
    return [
        {
            "division": {
                "uri": dag_run.conf['division_uri']
            },
            "effectiveDate": null
        }
    ] if dag_run.conf['division_uri'] else null


def get_dept_group_schedule(dag_run):
    return [
        {
            "departmentGroup": {
                "uri": dag_run.conf['departmentgroupuri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "effectiveDate": null
        }
    ] if dag_run.conf['departmentgroupuri'] else null


def get_emp_group_schedule(dag_run):
    return [
        {
            "employeeTypeGroup": {
                "uri": dag_run.conf['employee_type_uri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "effectiveDate": null
        }
    ] if dag_run.conf['employee_type_uri'] else null


def get_timesheet_period_schedule(dag_run):
    return [
        {
            "timesheetPeriod": {
                "uri": null,
                "name": dag_run.conf['timesheet_period'],
            },
            "effectiveDate": null
        }
    ]


def get_costcenter_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:cost-center-list-filter:effectively-enabled"
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


def get_location_update_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "locationScheduleToApply": {
                "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementLocationSchedule": [],
                "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                        {
                            "location": {
                                "uri": get_conf()['locationuri'],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_search_supervisor_param():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:login-name"
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
                    "text": get_conf()['managerid'],
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


def get_search_user_by_loginname_status_param(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
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
                    "text": dag_run.conf['workemail'],
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


def get_search_user_param():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:login-name"
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
                    "text": "{{ dag_run.conf.employeeid }}",
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


def get_timeoff_payload_update():
    """
    Build payload for updating time off type assignments.
    Merges mapper's time off types with existing non-mapper time offs to retain them.

    - Mapper time offs: Added/updated based on current eligibility
    - Existing non-mapper time offs: Retained (managed by other integrations)
    - TOIL types not in mapper: Handled separately via zero-line policy
    """
    user_uri = get_user_uri()

    # Get mapper's time off URIs
    mapper_timeoffs = rail.result('get_timeoff_toassign')
    mapper_uris = [item['uri'] for item in mapper_timeoffs if item]

    # Get existing non-mapper time off URIs to retain
    # These are time offs NOT managed by this integration (non-TOIL and not in mapper)
    existing_non_mapper_uris = rail.result('get_users_assigned_timeoff', 'existing_non_mapper_timeoff_uris') or []

    # Merge: existing non-mapper time offs + new mapper time offs
    merged_uris = list(set(existing_non_mapper_uris + mapper_uris))

    return {
        "userUri": user_uri,
        "timeOffTypeUris": merged_uris,
    }


def get_timeoff_payload():
    user_uri = get_created_user_uri() if get_created_user_uri() else get_user_uri()
    data = rail.result('get_timeoff_toassign')
    timeoff_uris = [item['uri'] for item in data if item]
    return {
        "userUri": user_uri,
        "timeOffTypeUris": timeoff_uris,
    }


def get_emptype_update_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": get_conf()['employeetypegroupuri'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_update_department_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": get_conf()['departmentgroupuri'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_oef_toupdate():
    oef_payload, _ = get_oef_field_values("update")
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "objectExtensionFieldsToApply": oef_payload
        },
        "userModificationOptionUri":  "urn:replicon:user-modification-option:save"
    }


def get_update_costcenter_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "uri": get_conf()['costcenteruri'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_emp_type_full_path(item):
    if item['WorkerType'] == "Employee":
        return response_filter.get_full_path([item['CompensationGrade'], item['WorkerType'], item['ContractType'], item['EmployeeType']])
    return response_filter.get_full_path([item['WorkerType'], item['ContractType'], item['EmployeeType']])


def get_division_type_full_path(item):
    return response_filter.get_full_path([item['JobCategory'], item['WorkerType'], item['EmployeeType'], item['ManagementLevel']])


def get_work_week_uri(week: str):
    return f"urn:replicon:day-of-week:{week.split(' to ')[0].lower()}"


def get_process_user_conf(item):
    all_ObjectExtensionfields = rail.result('get_all_ObjectExtensionfields')
    return {
        **{k.lower(): v for k, v in item.items()},
        **{
            'employeetypegroupuri': rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_emp_groups'), 'displayText', item['EmployeeType'], 'uri'),
            'ObjectExtentionfields': {
                # pylint: disable=line-too-long
                'Job Profile': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Job Profile', 'uri'),
                'Job Profile Code': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Job Profile Code', 'uri'),
                'Compensation Grade': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Compensation Grade', 'uri'),
                'Country': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Country', 'uri'),
                'Scheduled Weekly Hours': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Scheduled Weekly Hours', 'uri'),
                'Default Weekly Hours': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Default Weekly Hours', 'uri'),
                'Contract Type': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Contract Type', 'uri'),
                'Contract End Date': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Contract End Date', 'uri'),
                'Collective Agreement': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Collective Agreement', 'uri'),
                'Position ID': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Work Assignment Id', 'uri'),
                'Exempt': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Exempt', 'uri'),
                'FTE': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'FTE', 'uri'),
                'Business Title': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Business Title', 'uri'),
                'Worker Type': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Worker Type', 'uri'),
                'Additional Job Classification': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Additional Job Classification', 'uri'),
                'Payroll Type': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Payroll Type', 'uri'),
                'IA_STA_International_Assignee': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'IA/STA International Assignee', 'uri'),
                'Host_Country': rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Host Country', 'uri')
            },
            'permissionseturi':  rail.find_first_by_attr_and_get_attr(rail.result('get_all_permissionset'), 'displayText', 'Project Resource with Reports', 'uri'),
            'departmentgroupuri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_deparments'), 'name', item['JobFamily'], 'uri'),
            'timeentryapprovalpaths': rail.result('get_timeentry_apprroval_paths'),
            'costcenteruri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_costcenter'), 'name', item['CostCenterName'], 'uri'),
            'locationuri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_locations'), 'name', item['Location'], 'uri'),
            'timeofftypes': rail.result('get_all_timeofftypes'),
            'has_loaded_report_users': bool(rail.result('load_all_report_users')),
            'timezonedata': rail.result('get_all_timezones'),
            'holidaycalendar': rail.result('get_all_holiday_calendars'),
            'permissionsets': rail.result('get_all_permissionset'),
            'useruri': rail.find_first_by_attr_and_get_attr(rail.result('load_all_report_users'), 'employeeid', item['EmployeeID'], 'useruri'),
            "starting_balance_set_to_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_timeoff_balance_event_script"), 'displayText', 'Starting Balance Set To'),
            "prevent_balance_overdraw_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_timeoff_balance_validation_script"), 'displayText', 'Prevent balance overdraw')
        },
        **{
            "all_polices": rail.result("get_all_polices")
        },
        **custom_methods.get_derived_values_mapper_row(
            custom_methods.get_data_from_mapper_columns_iterations(item['Country'], item),
            country = item['Country'],
            ajc = item['AdditionalJobClassification']
        ),
        **{
            "employee_type_full_path": get_emp_type_full_path(item),
            "employee_type_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_employee_types_from_replicon'),
                                                                      'full_path', get_emp_type_full_path(item), 'uri'),
            "division_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_division_from_replicon'),
                                                                 'full_path', get_division_type_full_path(item), 'uri'),
            "division_full_path": get_division_type_full_path(item),
            "cost_center_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_costcenter'), 'code', item['CostCenterID'], 'uri'),
            "service_center_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_service_centers'), 'code', item['CompanyCode'], 'uri'),
            "country_location_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_locations'),
                                                                         'name', (item['Location'] if item['Location'] else item['Country']), 'uri')
        },
        **{
            "actual_employee_id_details": rail.find_first_by_attr_and_get_attr(all_ObjectExtensionfields, 'name', 'Actual Employee ID')
        },
        **{
            "new_employee_udf": rail.result("get_user_new_employee_custom_field")
        }
    }


def get_payrule_to_assign(dag_run):
    return [
        {
            "payRuleScript": {
                "name": dag_run.conf['payrule']
            },
            "effectiveDate": null
        }
    ]


def get_service_center_schedule(dag_run):
    return [
        {
            "serviceCenter": {
                "uri": dag_run.conf['service_center_uri']
            },
            "effectiveDate": null
        }
    ] if dag_run.conf['service_center_uri'] else null


def get_default_policy_set_add(dag_run):
    return [{
            'uri': null,
            'name': dag_run.conf['timesheet_template']
            }]


def get_user_schedule_add(dag_run):
    return [
        {
            "schedulePolicy": {
                "officeScheduleUri": null,
                "name": dag_run.conf['default_schedule'],
                "officeSchedule": null,
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": null
        }
    ]


def create_user_payload_using_default_values(dag_run):
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['workemail'],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['legalfirstname'],
            "lastname": dag_run.conf['legallastname'],
            "emailAddress": dag_run.conf['workemail'],
            "employeeId": dag_run.conf['employeeid'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": get_user_schedule_add(dag_run),
            "workWeekStartDayUri": dag_run.conf['work_week_uri'],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['hiredate']),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": true,
                "loginName": dag_run.conf['workemail'],
                "SSOName": dag_run.conf['workemail'],
                "password": null
            },
            "holidayCalendar": null,
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": dag_run.conf['permissionseturi'],
                    "name": null
                }
            ],
            "policySets": get_default_policy_set_add(dag_run),
            "customFieldValues": [
                {
                    "customField": {
                        "uri": dag_run.conf['new_employee_udf']['uri'],
                        "name": null,
                        "groupUri": null
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {
                        "uri": null,
                        "name": "Yes"
                    },
                    "number": null
                }
            ],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": null,
            "expenseApprovalPath": null,
            "timeOffApprovalPath": null,
            "definitionValues": null,
            "assignedActivities": [],
            "timeZone": null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": get_location_schedule(dag_run),
            "divisionSchedule": get_division_schedule(dag_run),
            "costCenterSchedule": get_costcenter_schedule(dag_run),
            "serviceCenterSchedule": get_service_center_schedule(dag_run),
            "departmentGroupSchedule": get_dept_group_schedule(dag_run),
            "employeeTypeGroupSchedule": get_emp_group_schedule(dag_run),
            "timesheetPeriodSchedule": null,
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": get_payrule_to_assign(dag_run),
            "displayNameParameter": null,
            "extensionFieldValues": get_oef_field_values()
        }
    }


def get_create_user_data(dag_run):

    if dag_run.conf['mapper_value_found'] == "No":
        return create_user_payload_using_default_values(dag_run)

    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['workemail'],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['legalfirstname'],
            "lastname": dag_run.conf['legallastname'],
            "emailAddress": dag_run.conf['workemail'],
            "employeeId": dag_run.conf['employeeid'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": get_user_schedule_add(dag_run),
            "workWeekStartDayUri": dag_run.conf['work_week_uri'],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['hiredate']),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": true,
                "loginName": dag_run.conf['workemail'],
                "SSOName": dag_run.conf['workemail'],
                "password": null
            },
            "holidayCalendar": get_holidaycalendar(dag_run),
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": dag_run.conf['permissionseturi'],
                    "name": null
                }
            ],
            "policySets": get_policy_sets(),
            "customFieldValues": [
                {
                    "customField": {
                        "uri": dag_run.conf['new_employee_udf']['uri'],
                        "name": null,
                        "groupUri": null
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {
                        "uri": null,
                        "name": "Yes"
                    },
                    "number": null
                }
            ],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": get_timesheet_path(dag_run),
            "expenseApprovalPath": null,
            "timeOffApprovalPath": get_timeoff_path(dag_run),
            "definitionValues": null,
            "assignedActivities": [],
            "timeZone": get_timezone_payload(dag_run),
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": get_location_schedule(dag_run),
            "divisionSchedule": get_division_schedule(dag_run),
            "costCenterSchedule": get_costcenter_schedule(dag_run),
            "serviceCenterSchedule": get_service_center_schedule(dag_run),
            "departmentGroupSchedule": get_dept_group_schedule(dag_run),
            "employeeTypeGroupSchedule": get_emp_group_schedule(dag_run),
            "timesheetPeriodSchedule": get_timesheet_period_schedule(dag_run),
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": get_payrule_to_assign(dag_run),
            "displayNameParameter": null,
            "extensionFieldValues": get_oef_field_values()
        }
    }


def get_effective_date(dag_run=None, date_format: str = 'json'):
    effective_date = datetime.date.today()
    if effective_date.weekday() != 6:
        effective_date += datetime.timedelta((6-effective_date.weekday()) % 7)
    else:
        effective_date += datetime.timedelta(days=7)
    if date_format != 'json':
        return effective_date.strftime(date_format)
    return {
        "day": effective_date.day,
        "month": effective_date.month,
        "year": effective_date.year
    }


def get_location_update_payload(country_location_uri, currently_assigned_location_uri, effective_date):
    return {
        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementLocationSchedule": [],
        "updateLocationScheduleOverDateRange": {
            "replacementLocationScheduleEntries": [
                {
                    "location": {
                        "uri": country_location_uri
                    },
                    "effectiveDate": effective_date
                }
            ],
            "endDate": null
        }
    } if currently_assigned_location_uri != country_location_uri else null


def get_division_update_payload(division_uri, currently_assigned_division, effective_date):
    return {
        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDivisionSchedule": [],
        "updateDivisionScheduleOverDateRange": {
            "replacementDivisionScheduleEntries": [
                {
                    "division": {
                        "uri": division_uri
                    },
                    "effectiveDate": effective_date
                }
            ],
            "endDate": null
        }
    } if division_uri != currently_assigned_division else null


def get_cost_center_update_payload(cost_center_uri, current_cost_center_uri, effective_date):
    return {
        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementCostCenterSchedule": [],
        "updateCostCenterScheduleOverDateRange": {
            "replacementCostCenterScheduleEntries": [
                {
                    "costCenter": {
                        "uri": cost_center_uri
                    },
                    "effectiveDate": effective_date
                }
            ],
            "endDate": null
        }
    } if cost_center_uri != current_cost_center_uri else null


def get_department_update_payload(department_uri, current_department_uri, effective_date):
    return {
        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDepartmentGroupSchedule": [],
        "updateDepartmentGroupScheduleOverDateRange": {
            "replacementDepartmentGroupScheduleEntries": [
                {
                    "departmentGroup": {
                        "uri": department_uri
                    },
                    "effectiveDate": effective_date
                }
            ],
            "endDate": null
        }
    } if department_uri != current_department_uri else null


def get_employee_type_update_payload(employee_type_uri, current_employee_type, effective_date):
    return {
        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementEmployeeTypeGroupSchedule": [],
        "updateEmployeeTypeGroupScheduleOverDateRange": {
            "replacementEmployeeTypeGroupScheduleEntries": [
                {
                    "employeeTypeGroup": {
                        "uri": employee_type_uri
                    },
                    "effectiveDate": effective_date
                }
            ],
            "endDate": null
        }
    } if employee_type_uri != current_employee_type else null


def get_current_timesheet_period(timesheet_period_data):
    today = pendulum.now('Europe/London').date()
    return list(filter(lambda assigned_ts: True if not assigned_ts['effectiveDate']
                       else get_date_from_json_date(assigned_ts['effectiveDate']) <= today, timesheet_period_data))


def timesheet_period_update_payload(timesheet_period_name, current_timesheet_period, effective_date):
    if not current_timesheet_period:
        current_timesheet_period_name = ""
    else:
        _current_timesheet_period = get_current_timesheet_period(current_timesheet_period)
        current_timesheet_period_name = (_current_timesheet_period[-1].get(
            'timesheetPeriod', {}).get('displayText') if _current_timesheet_period and _current_timesheet_period[-1].get('timesheetPeriod', {}) else "")
    return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "name": timesheet_period_name,
                    },
                    "effectiveDate": effective_date
                }
            ],
            "endDate": null
        }
    } if timesheet_period_name != current_timesheet_period_name else null


def get_service_center_update_payload(service_center_uri, current_service_center, effective_date):
    return {
        "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementServiceCenterSchedule": [],
        "updateServiceCenterScheduleOverDateRange": {
            "replacementServiceCenterScheduleEntries": [
                {
                    "serviceCenter": {
                        "uri": service_center_uri
                    },
                    "effectiveDate": effective_date
                }
            ],
            "endDate": null
        }
    } if service_center_uri != current_service_center else null


def can_update_start_date(dag_run):
    return(get_replicon_date(dag_run.conf['hiredate']) !=
           rail.result('bulk_get_user3')['userDetails']['employmentDateRange']['startDate'])


def can_update_first_name(dag_run):
    return (dag_run.conf['legalfirstname'] and dag_run.conf['legalfirstname'] != rail.result("bulk_get_user3")['userDetails']['firstName'])


def can_update_last_name(dag_run):
    return (dag_run.conf['legallastname'] and dag_run.conf['legallastname'] != rail.result("bulk_get_user3")['userDetails']['lastName'])


def can_update_end_date(dag_run):
    return bool(get_replicon_date(dag_run.conf['terminationdate']))


def should_remove_holiday_calendar(dag_run):
    """
    Check if holiday calendar should be removed (unassigned).
    Returns True when mapper value is 'N/A' and user currently has a holiday calendar assigned.
    """
    if dag_run.conf['holiday_calendar'] and dag_run.conf['holiday_calendar'].strip().lower() == "na":
        current_calendar = rail.result('bulk_get_user3').get('holidayCalendar', {})
        # Only remove if user currently has a calendar assigned
        return bool(current_calendar and current_calendar.get('displayText'))
    return False


def can_update_holiday_calender(dag_run):
    """
    Check if holiday calendar needs to be updated or removed.
    Returns True when:
    - Mapper value is 'N/A' and user has existing calendar (needs removal)
    - Mapper has valid calendar that differs from current assignment
    """
    # Strip the value first
    if dag_run.conf['holiday_calendar']:
        dag_run.conf['holiday_calendar'] = dag_run.conf['holiday_calendar'].strip()

    # Check if we need to remove the calendar (N/A case)
    if should_remove_holiday_calendar(dag_run):
        return True

    # Skip if mapper is N/A but user has no calendar (nothing to remove)
    if dag_run.conf['holiday_calendar'] and dag_run.conf['holiday_calendar'].lower() == "na":
        return False

    # Check if calendar needs to be updated (different from current)
    current_calendar = rail.result('bulk_get_user3').get('holidayCalendar', {})
    current_calendar_name = current_calendar.get('displayText', '') if current_calendar else ''
    return dag_run.conf['holiday_calendar'] and dag_run.conf['holiday_calendar'] != current_calendar_name


def can_update_timezone(dag_run):
    return dag_run.conf['time_zone'] != rail.result('bulk_get_user3')['timeZone']['ianaName']


def is_email_changed(dag_run):
    return dag_run.conf['workemail'] != rail.result("bulk_get_user3")['userDetails']['emailAddress']


def get_holiday_calendar_assignments_payload(dag_run, effective_date, config_ia_holiday_mapper=None, is_ia=False):
    """
    Build the holiday calendar assignment payload with effective date.
    - If mapper = 'N/A' and user has calendar: remove calendar (set to null/None)
    - If mapper has valid calendar: assign new calendar
    - Otherwise: return null (no change)
    """

    if is_ia:
        # IA/STA holiday calendar assignment from IA-specific mapper
        feed_item = {
            'Country': dag_run.conf.get('country', ''),
            'Location': dag_run.conf.get('location', ''),
            'CompensationGrade': dag_run.conf.get('compensationgrade', ''),
            'ContractType': dag_run.conf.get('contracttype', ''),
            'CompanyCode': dag_run.conf.get('companycode', '')
        }

        ia_calendar = custom_methods.get_ia_holiday_calendar(feed_item, config_ia_holiday_mapper)

        if not ia_calendar or ia_calendar.lower() in ('n/a', 'na'):
            return null

        # Check if different from current
        current_calendar = rail.result('bulk_get_user3').get('holidayCalendar', {})
        current_name = current_calendar.get('displayText', '') if current_calendar else ''
        if ia_calendar.strip() == current_name:
            return null

        calendar_uri = rail.find_first_by_attr_and_get_attr(
            dag_run.conf['holidaycalendar'], 'name', ia_calendar, 'uri')
        if not calendar_uri:
            return null

        return {
            "holidayCalendarsAssignmentModificationOptionUri": "urn:replicon:holiday-calendar-assignment-modification-option:update-holiday-calendar-assignments-over-date-range",
            "replacementHolidayCalendarAssignments": [],
            "updateHolidayCalendarAssignmentsOverDateRange": {
                "replacementHolidayCalendarAssignments": [{
                    "holidayCalendar": {
                        "uri": calendar_uri
                    },
                    "effectiveDate": effective_date
                }],
                "endDate": null
            },
            "holidayCalendarAssignmentBehavior": "urn:replicon:holiday-calendar-assignment-behavior:do-not-delete-holiday-bookings-that-no-longer-apply"
        }

    if not can_update_holiday_calender(dag_run):
        return null

    # Check if we need to remove the calendar
    if should_remove_holiday_calendar(dag_run):
        return {
            "holidayCalendarsAssignmentModificationOptionUri": "urn:replicon:holiday-calendar-assignment-modification-option:update-holiday-calendar-assignments-over-date-range",
            "replacementHolidayCalendarAssignments": [],
            "updateHolidayCalendarAssignmentsOverDateRange": {
                "replacementHolidayCalendarAssignments": [
                    {
                        "holidayCalendar": {
                            "uri": null,
                            "name": "None"
                        },
                        "effectiveDate": effective_date
                    }
                ],
                "endDate": null
            },
            "holidayCalendarAssignmentBehavior": null
        }

    # Assign new calendar - use pre-fetched URI from config if available
    calendar_uri = dag_run.conf.get('holiday_calendar_uri') or rail.find_first_by_attr_and_get_attr(
        dag_run.conf['holidaycalendar'], 'name', dag_run.conf['holiday_calendar'], 'uri')

    if not calendar_uri:
        return null

    return {
        "holidayCalendarsAssignmentModificationOptionUri": "urn:replicon:holiday-calendar-assignment-modification-option:update-holiday-calendar-assignments-over-date-range",
        "replacementHolidayCalendarAssignments": [],
        "updateHolidayCalendarAssignmentsOverDateRange": {
            "replacementHolidayCalendarAssignments": [
                {
                    "holidayCalendar": {
                        "uri": calendar_uri
                    },
                    "effectiveDate": effective_date
                }
            ],
            "endDate": null
        },
        "holidayCalendarAssignmentBehavior": null
    }


def get_timezone_payload(dag_run):
    return {
        "uri": rail.find_first_by_attr_and_get_attr(
            dag_run.conf['timezonedata'], 'displayText', dag_run.conf['time_zone'], 'uri')
    } if dag_run.conf['time_zone'].lower() != "na" else null


def can_update_timesheet_template(dag_run):
    """
    Check if timesheet template needs to be updated.
    Returns True if:
    - Mapper has a valid timesheet template (not N/A)
    - User has no template assigned, OR current template differs from mapper template
    """
    if dag_run.conf['ia_sta_international_assignee'].lower() == "y":
        return False
    timesheet_template_name = dag_run.conf.get('timesheet_template', '')
    if not timesheet_template_name or timesheet_template_name.lower() == 'na':
        return False
    user_details = rail.result('bulk_get_user3')
    current_template = user_details.get('timesheetTemplate', {})
    current_template_name = current_template.get('displayText', '') if current_template else ''
    return (not current_template_name) or (current_template_name != timesheet_template_name)


def get_timesheet_template_effective_date(dag_run):
    """
    Get effective date for timesheet template based on payroll type.
    - Weekly payroll: next Sunday
    - Monthly payroll: 1st of next month
    """
    from dateutil.relativedelta import relativedelta
    payroll_type = dag_run.conf.get('payroll_type', 'weekly').lower()
    if payroll_type == "monthly":
        _date = (datetime.date.today() + relativedelta(months=1)).replace(day=1)
    else:
        _date = datetime.date.today()
        if _date.weekday() != 6:
            _date += datetime.timedelta((6 - _date.weekday()) % 7)
        else:
            _date += datetime.timedelta(days=7)
    return {"day": _date.day, "month": _date.month, "year": _date.year}


def get_timesheet_template_update_payload(dag_run):
    """
    Build payload for timesheet template update with effective date.
    Uses policySetsScheduleToApply for effective date support.
    """
    timesheet_template_name = dag_run.conf.get('timesheet_template', '')
    all_policies = dag_run.conf.get('all_polices', [])
    policy_set_uri = rail.find_first_by_attr_and_get_attr(
        all_policies, 'displayText', timesheet_template_name, 'uri')
    if not policy_set_uri:
        raise AirflowException(f"Timesheet template '{timesheet_template_name}' not found in Replicon")
    effective_date = get_timesheet_template_effective_date(dag_run)
    return {
        "user": {
            "uri": dag_run.conf['useruri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "policySetsScheduleToApply": [
                {
                    "policyUri": "urn:replicon:policy:timesheet",
                    "schedule": [{"policySetUri": policy_set_uri, "effectiveDate": effective_date}]
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_updated_fields(dag_run, modifications):
    updated_fields = []
    if can_update_first_name(dag_run):
        updated_fields.append("First Name Updated")
    if can_update_last_name(dag_run):
        updated_fields.append("Last Name Updated")
    if is_email_changed(dag_run):
        updated_fields.append("Email Updated")
    if can_update_start_date(dag_run):
        updated_fields.append("Start Date Updated")
    if can_update_end_date(dag_run):
        updated_fields.append("End Date Updated")
    if dag_run.conf['mapper_value_found'] != "No" and can_update_holiday_calender(dag_run):
        if should_remove_holiday_calendar(dag_run):
            updated_fields.append("Holiday calendar removed")
        else:
            updated_fields.append("Holiday calendar updated")

    if modifications['modifications']['locationScheduleToApply']:
        updated_fields.append("Location Updated")
    if modifications['modifications']['divisionScheduleToApply']:
        updated_fields.append("Management Level Updated")
    if modifications['modifications']['costCenterScheduleToApply']:
        updated_fields.append("Cost Center Updated")
    if modifications['modifications']['departmentGroupScheduleToApply']:
        updated_fields.append("Department Updated")
    if modifications['modifications']['employeeTypeGroupScheduleToApply']:
        updated_fields.append("Employee Type Updated")
    if modifications['modifications']['serviceCenterScheduleToApply']:
        updated_fields.append("Service Center Updated")
    if modifications['modifications']['timesheetPeriodScheduleToApply']:
        updated_fields.append("Timesheet Period Updated")
    return rail.smartjoin_by_delim(updated_fields, ';')


def get_update_user_payload_for_default_values(dag_run, effective_date):
    oef_payload, _ = get_oef_field_values("update")
    current_effective_payrule = get_current_effective_payrule(rail.result("bulk_get_user3")['payRuleScriptSchedule'])

    modifications = {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": null,
            "workWeekStartToApply": null,
            "holidayCalendarToApply": null,
            "schedulePolicyToApply": null,
            "locationScheduleToApply": get_location_update_payload(
                dag_run.conf['country_location_uri'], rail.result('get_effective_user_groupmembership', 'location').get('uri', ''), effective_date),
            "divisionScheduleToApply": get_division_update_payload(
                dag_run.conf['division_uri'], rail.result('get_effective_user_groupmembership', 'division').get('uri', ''), effective_date),
            "costCenterScheduleToApply": get_cost_center_update_payload(
                dag_run.conf['cost_center_uri'], rail.result('get_effective_user_groupmembership', 'costcenter').get('uri', ''), effective_date),
            "departmentGroupScheduleToApply": get_department_update_payload(
                dag_run.conf['departmentgroupuri'], rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), effective_date),
            "employeeTypeGroupScheduleToApply": get_employee_type_update_payload(
                dag_run.conf['employee_type_uri'], rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), effective_date),
            "timesheetPeriodScheduleToApply": null,
            "serviceCenterScheduleToApply": get_service_center_update_payload(
                dag_run.conf['service_center_uri'], rail.result('get_effective_user_groupmembership', 'servicecenter').get('uri', ''), effective_date),
            "timesheetPeriodTypeToApply": null,
            "securitySettingsToApply": {
                "loginName": dag_run.conf['workemail'],
                "ssoName": dag_run.conf['workemail']
            } if is_email_changed(dag_run) else null,
            "timesheetApprovalPathToApply": null,
            "timeEntryRevisionGroupApprovalPathToApply": null,
            "timeOffApprovalPathToApply": null,
            "userDetailsToApply": {
                "firstName": dag_run.conf['legalfirstname'] if can_update_first_name(dag_run) else null,
                "lastName": dag_run.conf['legallastname'] if can_update_last_name(dag_run) else null,
                "emailAddress": {
                    "emailAddress": dag_run.conf['workemail']
                } if is_email_changed(dag_run) else null,
                "employmentStartDate": {
                    "date": get_replicon_date(dag_run.conf['hiredate'])
                } if can_update_start_date(dag_run) else null,
                "employmentEndDate": {
                    "date": get_replicon_date(dag_run.conf['terminationdate'])
                } if can_update_end_date(dag_run) else null
            } if (can_update_first_name(dag_run) or can_update_last_name(dag_run)
                  or can_update_start_date(dag_run) or can_update_end_date(dag_run) or is_email_changed(dag_run)) else null,
            "payRulesScheduleModifications": {
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "name": dag_run.conf['payrule']
                        },
                        "effectiveDate": effective_date
                    }
                ]
            } if current_effective_payrule != dag_run.conf['payrule'] else null,
            "objectExtensionFieldsToApply": oef_payload
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
    rail.set_result(key="updated_fields",
                    val=get_updated_fields(dag_run, modifications))
    return modifications


def get_policies_to_assign_update(dag_run):
    policy_set_uris_to_assign = []
    user_details = rail.result('bulk_get_user3')
    all_polices = dag_run.conf['all_polices']
    timeoff_policy = get_conf()['time_off_template']
    if timeoff_policy and (timeoff_policy.lower() != "na"):
        if (not user_details.get('timeOffTemplate')) or (user_details.get('timeOffTemplate') and (user_details.get('timeOffTemplate', {}).get('displayText') != timeoff_policy)):
            policy_set_uris_to_assign.append(rail.find_first_by_attr_and_get_attr(
                all_polices, 'displayText', timeoff_policy, 'uri'))
    return{
        "policySetUrisToAssign": policy_set_uris_to_assign,
        "policyUrisToRemovePolicySet": []
    } if policy_set_uris_to_assign else null


def get_new_effective_date(dag_run, caller = "not-supervisor"):
    """
    Get effective date based on caller and payroll type.
    - supervisor:
        - If IA (International Assignment): next Sunday (weekly logic)
        - If not IA: tomorrow (next day)
    - timeoff/other: follows payroll type logic (weekly Sunday / monthly 1st)
    """
    from dateutil.relativedelta import relativedelta
    if caller == "supervisor":
        # If on International Assignment, use weekly (next Sunday) effective date
        if dag_run.conf.get('ia_sta_international_assignee', '').lower() == 'y':
            return get_effective_date(dag_run)  # Next Sunday
        # Otherwise use tomorrow
        _date = datetime.date.today() + relativedelta(days=1)
        return {
            'day': _date.day,
            'month': _date.month,
            'year': _date.year
        }

    # For timeoff and other callers, use payroll type based effective date
    if dag_run.conf['payroll_type'].lower() == "monthly":
        _date = (datetime.date.today() + relativedelta(months=1)).replace(day=1)
        return {
            'day': _date.day,
            'month': _date.month,
            'year': _date.year
        }
    if dag_run.conf['payroll_type'].lower() == "weekly":
        return get_effective_date(dag_run)
    raise AirflowException("Payroll type is neither monthly nor weekly")

def get_payroll_field_changes_during_ia(dag_run, effective_date):
    """
    Detect payroll-impacting field changes during International Assignment.
    These fields drive timesheet template and pay rule assignments.
    Changes to these fields are not processed during IA but must be logged as exceptions.

    Reuses existing update payload functions to detect changes.
    """
    payroll_fields_changed = []

    # Build a simulated modifications dict to reuse existing change detection logic
    simulated_modifications = {
        'modifications': {
            'locationScheduleToApply': get_location_update_payload(
                dag_run.conf['country_location_uri'],
                rail.result('get_effective_user_groupmembership', 'location').get('uri', ''),
                effective_date),
            'divisionScheduleToApply': get_division_update_payload(
                dag_run.conf['division_uri'],
                rail.result('get_effective_user_groupmembership', 'division').get('uri', ''),
                effective_date),
            'employeeTypeGroupScheduleToApply': get_employee_type_update_payload(
                dag_run.conf['employee_type_uri'],
                rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''),
                effective_date),
            'costCenterScheduleToApply': None,
            'departmentGroupScheduleToApply': None,
            'serviceCenterScheduleToApply': None
        }
    }

    # Check which payroll-impacting fields would have changed
    if simulated_modifications['modifications']['locationScheduleToApply']:
        payroll_fields_changed.append('Location')
    if simulated_modifications['modifications']['divisionScheduleToApply']:
        payroll_fields_changed.append('ManagementLevel')
    if simulated_modifications['modifications']['employeeTypeGroupScheduleToApply']:
        payroll_fields_changed.append('EmployeeType')

    # Check OEF fields for payroll-impacting changes
    user_current_oefs = rail.result('bulk_get_user3').get('userDetails', {}).get('extensionFieldValues', [])

    # Worker Type
    current_worker_type = rail.find_first_by_attr_and_get_attr(
        user_current_oefs, 'definition.displayText', 'Worker Type', 'textValue') or ''
    if dag_run.conf.get('workertype', '') and dag_run.conf.get('workertype', '') != current_worker_type:
        payroll_fields_changed.append('WorkerType')

    # Compensation Grade
    current_comp_grade = rail.find_first_by_attr_and_get_attr(
        user_current_oefs, 'definition.displayText', 'Compensation Grade', 'textValue') or ''
    if dag_run.conf.get('compensationgrade', '') and dag_run.conf.get('compensationgrade', '') != current_comp_grade:
        payroll_fields_changed.append('CompensationGrade')

    # Contract Type
    current_contract_type = rail.find_first_by_attr_and_get_attr(
        user_current_oefs, 'definition.displayText', 'Contract Type', 'textValue') or ''
    if dag_run.conf.get('contracttype', '') and dag_run.conf.get('contracttype', '') != current_contract_type:
        payroll_fields_changed.append('ContractType')

    # Additional Job Classification
    current_ajc = rail.find_first_by_attr_and_get_attr(
        user_current_oefs, 'definition.displayText', 'Additional Job Classification', 'textValue') or ''
    if dag_run.conf.get('additionaljobclassification', '') and dag_run.conf.get('additionaljobclassification', '') != current_ajc:
        payroll_fields_changed.append('AdditionalJobClassification')

    return payroll_fields_changed


def log_ia_payroll_exceptions(payroll_fields_changed):
    """
    Log payroll-impacting field changes that are ignored during International Assignment.
    """
    if payroll_fields_changed:
        exception_message = f"Payroll-impacting field changes ignored during International Assignment: {', '.join(payroll_fields_changed)}"
        rail.set_result(key="ia_payroll_exceptions", val=exception_message)
        return exception_message
    return None


def get_modifications_for_ia_yes(dag_run, oef_payload, ia_effective_date, config_ia_holiday_mapper):
    modifications = {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": null,
            "workWeekStartToApply": null,
            "holidayCalendarToApply": null,
            "holidayCalendarAssignmentsToApply": get_holiday_calendar_assignments_payload(dag_run, ia_effective_date, config_ia_holiday_mapper, is_ia = True),
            "schedulePolicyToApply": null,
            "policySetsToApply": null,
            "locationScheduleToApply": null,
            "divisionScheduleToApply":null,
            "costCenterScheduleToApply": get_cost_center_update_payload(
                dag_run.conf['cost_center_uri'], rail.result('get_effective_user_groupmembership', 'costcenter').get('uri', ''), ia_effective_date),
            "departmentGroupScheduleToApply": null,
            "employeeTypeGroupScheduleToApply": null,
            # Timesheet period should NOT be updated during International Assignment
            "timesheetPeriodScheduleToApply": null,
            "serviceCenterScheduleToApply": get_service_center_update_payload(
                dag_run.conf['service_center_uri'], rail.result('get_effective_user_groupmembership', 'servicecenter').get('uri', ''), ia_effective_date),
            "timesheetPeriodTypeToApply": null,
            "securitySettingsToApply": {
                "loginName": dag_run.conf['workemail'],
                "ssoName": dag_run.conf['workemail']
            } if is_email_changed(dag_run) else null,
            "timesheetApprovalPathToApply": null,
            "timeEntryRevisionGroupApprovalPathToApply": null,
            "timeOffApprovalPathToApply": null,
            "userDetailsToApply": {
                "firstName": dag_run.conf['legalfirstname'] if can_update_first_name(dag_run) else null,
                "lastName": dag_run.conf['legallastname'] if can_update_last_name(dag_run) else null,
                "emailAddress": {
                    "emailAddress": dag_run.conf['workemail']
                } if is_email_changed(dag_run) else null,
                "employmentStartDate": null,
                "employmentEndDate": null
            } if (
                    can_update_first_name(dag_run) or can_update_last_name(dag_run) or is_email_changed(dag_run)
                ) else null,
            "payRulesScheduleModifications": null,
            "objectExtensionFieldsToApply": oef_payload
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
    rail.set_result(key="updated_fields",
                    val=get_updated_fields(dag_run, modifications))
    return modifications

def get_current_effective_payrule(pay_rule_script_schedule):
    today = datetime.date.today()
    current_payrule = None
    current_effective_date = None

    for entry in pay_rule_script_schedule:
        effective_date = entry.get('effectiveDate')
        if effective_date is None:
            if current_effective_date is None:
                current_payrule = entry['payRuleScript']['displayText']
        else:
            entry_date = datetime.date(
                day=effective_date['day'],
                month=effective_date['month'],
                year=effective_date['year']
            )
            if entry_date > today:
                continue
            if current_effective_date is None or entry_date > current_effective_date:
                current_effective_date = entry_date
                current_payrule = entry['payRuleScript']['displayText']

    return current_payrule

def get_update_user_payload(dag_run, config_ia_holiday_mapper):
    ia_effective_date = get_effective_date(dag_run)
    effective_date = new_effective_date = get_new_effective_date(dag_run)

    oef_payload, ia_updated = get_oef_field_values("update")

    current_effective_payrule = get_current_effective_payrule(rail.result("bulk_get_user3")['payRuleScriptSchedule'])

    if dag_run.conf['ia_sta_international_assignee'].lower() == "y":
        # Check for payroll-impacting field changes and log as exceptions
        payroll_fields_changed = get_payroll_field_changes_during_ia(dag_run, new_effective_date)
        log_ia_payroll_exceptions(payroll_fields_changed)
        return get_modifications_for_ia_yes(dag_run, oef_payload, ia_effective_date, config_ia_holiday_mapper)

    if not dag_run.conf['ia_sta_international_assignee'] and ia_updated:
        # When an employee returns from an international assignment, 
        # an updated record will be sent in the feed to revert values back to the home-country data, following the same weekly logic.
        # to support this the condition is added 
        # Returned from IA is true when (IA value sent in feed file is blank and IA is updated ("Yes" => ""))
        effective_date = new_effective_date = ia_effective_date

    if dag_run.conf['mapper_value_found'] == "No":
        return get_update_user_payload_for_default_values(dag_run, effective_date)

    modifications = {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": get_timezone_payload(dag_run) if can_update_timezone(dag_run) else null
            },
            "workWeekStartToApply": {
                "workWeekStartDayUri": dag_run.conf['work_week_uri'] if dag_run.conf['work_week_uri'] else null
            },
            "holidayCalendarToApply": null,
            "holidayCalendarAssignmentsToApply": get_holiday_calendar_assignments_payload(dag_run, new_effective_date),
            "schedulePolicyToApply": null,
            "policySetsToApply": get_policies_to_assign_update(dag_run),
            "locationScheduleToApply": get_location_update_payload(
                dag_run.conf['country_location_uri'], rail.result('get_effective_user_groupmembership', 'location').get('uri', ''), new_effective_date),
            "divisionScheduleToApply": get_division_update_payload(
                dag_run.conf['division_uri'], rail.result('get_effective_user_groupmembership', 'division').get('uri', ''), effective_date),
            "costCenterScheduleToApply": get_cost_center_update_payload(
                dag_run.conf['cost_center_uri'], rail.result('get_effective_user_groupmembership', 'costcenter').get('uri', ''), new_effective_date),
            "departmentGroupScheduleToApply": get_department_update_payload(
                dag_run.conf['departmentgroupuri'], rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), new_effective_date),
            "employeeTypeGroupScheduleToApply": get_employee_type_update_payload(
                dag_run.conf['employee_type_uri'], rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), new_effective_date),
            # Needs to be checked
            # as the timesheet period update can be in the past
            "timesheetPeriodScheduleToApply": timesheet_period_update_payload(dag_run.conf['timesheet_period'],
                                                                              rail.result("bulk_get_user3")['timesheetPeriodSchedule'], effective_date),
            "serviceCenterScheduleToApply": get_service_center_update_payload(
                dag_run.conf['service_center_uri'], rail.result('get_effective_user_groupmembership', 'servicecenter').get('uri', ''), new_effective_date),
            "timesheetPeriodTypeToApply": null,
            "securitySettingsToApply": {

                "loginName": dag_run.conf['workemail'],
                "ssoName": dag_run.conf['workemail']
            } if is_email_changed(dag_run) else null,
            "timesheetApprovalPathToApply": {
                "name": dag_run.conf["timesheet_approval"]
            } if rail.result("bulk_get_user3").get('timesheetApprovalPath', {}).get('displayText', "") != dag_run.conf["time_entry_approval_path"] else null,
            "timeEntryRevisionGroupApprovalPathToApply": {
                "name": dag_run.conf["time_entry_approval_path"]
            },
            "timeOffApprovalPathToApply": {
                "name": dag_run.conf['time_off_approval']
            } if dag_run.conf['time_off_approval'].lower() != "na" else null,
            "userDetailsToApply": {
                "firstName": dag_run.conf['legalfirstname'] if can_update_first_name(dag_run) else null,
                "lastName": dag_run.conf['legallastname'] if can_update_last_name(dag_run) else null,
                "emailAddress": {
                    "emailAddress": dag_run.conf['workemail']
                } if is_email_changed(dag_run) else null,
                "employmentStartDate": {
                    "date": get_replicon_date(dag_run.conf['hiredate'])
                } if can_update_start_date(dag_run) else null,
                "employmentEndDate": {
                    "date": get_replicon_date(dag_run.conf['terminationdate'])
                } if can_update_end_date(dag_run) else null
            } if (can_update_first_name(dag_run) or can_update_last_name(dag_run)
                  or can_update_start_date(dag_run) or can_update_end_date(dag_run) or is_email_changed(dag_run)) else null,
            "payRulesScheduleModifications": {
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "name": dag_run.conf['payrule']
                        },
                        "effectiveDate": new_effective_date
                    }
                ]
            } if current_effective_payrule != dag_run.conf['payrule'] else null,
            "objectExtensionFieldsToApply": oef_payload
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
    rail.set_result(key="updated_fields",
                    val=get_updated_fields(dag_run, modifications))
    return modifications


def get_timeoff_scripts(details: list):
    if not details:
        return []

    def get_template(additional_parameters, script):
        return {
            "scriptTarget": {
                "uri": script['uri']
            },
            "additionalParameters": additional_parameters
        }
    return list(map(lambda item: get_template(item['additionalParameters'], item['script']), details))


def get_zero_line_policy_effective_date(dag_run):
    """
    Get the effective date for zero line policy based on payroll type.
    - Weekly payroll: effective from next Sunday
    - Monthly payroll: effective from 1st of next month
    """
    from dateutil.relativedelta import relativedelta
    payroll_type = dag_run.conf.get('payroll_type', 'weekly').lower()

    if payroll_type == "monthly":
        _date = (datetime.date.today() + relativedelta(months=1)).replace(day=1)
    else:
        # Weekly - use next Sunday logic
        _date = datetime.date.today()
        if _date.weekday() != 6:
            _date += datetime.timedelta((6 - _date.weekday()) % 7)
        else:
            _date += datetime.timedelta(days=7)

    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }, _date.strftime('%d-%m-%Y')


def get_zero_line_policy(dag_run):
    """
    Creates a zero line policy that:
    1. Sets TOIL balance to zero
    2. Disables TOIL booking (prevents overdraw)
    Effective date follows payroll type logic (weekly/monthly)
    """
    effective_date, effective_date_str = get_zero_line_policy_effective_date(dag_run)
    return {
        "description": f"TOIL Disabled - Effective On {effective_date_str}",
        "effectiveDate": effective_date,
        "policySet": {
            "timeOffBalanceEventScripts": [
                {
                    "additionalParameters": [
                        {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "number": 0
                            }
                        },
                        {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                                "number": 10
                            }
                        }
                    ],
                    "scriptTarget": {
                        "uri": dag_run.conf['starting_balance_set_to_uri'],
                    }
                }
            ],
            "timeOffValidationScripts": [
                {
                    "additionalParameters": [
                        {
                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                            "value": {
                                "number": 0
                            }
                        }
                    ],
                    "scriptTarget": {
                        "uri": dag_run.conf['prevent_balance_overdraw_uri'],
                    }
                }
            ]
        }}


def get_put_timeoff_zero_line_policy_payload(dag_run):
    """
    Build the payload to set TOIL balance to zero and disable booking.
    Uses payroll type based effective date for consistency.
    """
    current_policies = dag_run.conf['policy']
    exiting_policies_list = []
    # Use payroll type based effective date for comparison
    effective_date_json, _ = get_zero_line_policy_effective_date(dag_run)
    effective_date_date = get_date_from_json_date(effective_date_json)
    for policy in current_policies:
        if get_date_from_json_date(policy['effectiveDate']) < effective_date_date:
            exiting_policies_list.append({
                "effectiveDate": policy['effectiveDate'],
                "description": policy['description'],
                "policySet": {
                    "timeOffBalanceEventScripts": get_timeoff_scripts(policy['policySet']['timeOffBalanceEventScripts']),
                    "timeOffValidationScripts": get_timeoff_scripts(policy['policySet']['timeOffValidationScripts'])
                }
            }
            )
    exiting_policies_list.append(get_zero_line_policy(dag_run))
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['user_uri'],
            "timeOffTypeUri": dag_run.conf['uri']
        },
        "policySetScheduleEntries": exiting_policies_list
    }


def get_default_timeoff_policy_schedule_payload(dag_run):
    return {
        "timeOffTypeUri": dag_run.conf['timeoff_to_process']['uri']
    }


def get_user_timeoff_policy_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['user_uri'],
            "timeOffTypeUri": dag_run.conf['timeoff_to_process']['uri']
        },
        "policySetScheduleEntries": loads(rail.result('policy_to_assign'))
    }


def effective_dateformat_payload(effective_date):
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }
