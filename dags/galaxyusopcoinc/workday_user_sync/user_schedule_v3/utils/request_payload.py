import datetime
import rail
from galaxyusopcoinc.workday_user_sync.user_schedule_v3.utils.python_callable_method import schedule_all_blank_zero, schedule_is_blank
null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_process_each_user_schedule_records_conf(item):
    return {
        'employee_id': item['EmployeeId'],
        'effective_date': item['EffectiveDate'] if item['EffectiveDate'] else "",
        'monday_hours': float(item['MondayHours']) if item['MondayHours'] else "",
        'tuesday_hours': float(item['TuesdayHours']) if item['TuesdayHours'] else "",
        'wednesday_hours': float(item['WednesdayHours']) if item['WednesdayHours'] else "",
        'thursday_hours': float(item['ThursdayHours']) if item['ThursdayHours'] else "",
        'friday_hours': float(item['FridayHours']) if item['FridayHours'] else "",
        'saturday_hours': float(item['SaturdayHours']) if item['SaturdayHours'] else "",
        'sunday_hours': float(item['SundayHours']) if item['SundayHours'] else "",
        'replicon_schedule_type': item['replicon_schedule_name']
    }


def get_user_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
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
                    "text": dag_run.conf['employee_id'],
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

def get_user_details3_payload(dag_run):
    return {
        "users": [
            {
                "uri":null,
                "loginName": null,
                "employeeId":  dag_run.conf['employee_id'],
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_effective_date(date_format:str='json'):
    effective_date = datetime.date.today()
    if effective_date.weekday()!=6:
        effective_date += datetime.timedelta((6-effective_date.weekday()) % 7)
    else:
        effective_date += datetime.timedelta(days=7)
    if date_format!='json':
        return effective_date.strftime(date_format)
    return {
        "day": effective_date.day,
        "month": effective_date.month,
        "year": effective_date.year
    }

def apply_user_modification_payload(dag_run, sch_type):
    schedule = get_schedule_to_apply(dag_run)
    return {
        "user": {
            "uri": rail.result('get_user_info_from_user_service')[0]['userDetails']['uri'],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "schedulePolicyToApply": {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": schedule,
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": schedule
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null if sch_type else get_effective_date()
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_schedule_to_apply(dag_run):
    schedule = dag_run.conf['replicon_schedule_type']
    if schedule_all_blank_zero(schedule):
        return "0|0|0|0|0|0|0"
    if schedule_is_blank(schedule):
        return "8|8|8|8|8|0|0"
    return schedule

def get_update_schedule_as_initial(dag_run):

    schedule = get_schedule_to_apply(dag_run)

    return {
        "user": {
            "uri": rail.result('get_user_info_from_user_service')[0]['userDetails']['uri'],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "schedulePolicyToApply": {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": schedule,
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": schedule
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_update_new_employee_as_no():
    return {
        "user": {
            "uri": rail.result('get_user_info_from_user_service')[0]['userDetails']['uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "customFieldValuesToApply": [
            {
                "customField": {
                    "uri": rail.find_first_by_attr_and_get_attr(
                        rail.result("get_user_info_from_user_service")[0]['userDetails']['customFieldValues'],
                        "customField.displayText", "New Employee" , "customField.uri"),
                    "name": null,
                    "groupUri": null
                },
                "text": null,
                "date": null,
                "dropDownOption": {
                    "uri": null,
                    "name": "No"
                },
                "number": null
            }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
