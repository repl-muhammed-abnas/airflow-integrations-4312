import rail
from galaxyusopcoinc.workday_user_sync.user_schedule.utils import python_callable_method

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


def get_user_payload():
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
                    "text": get_dag_run_conf()['employee_id'],
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


def apply_user_modification_payload(sch_type):
    effective_date = python_callable_method.get_replicon_date(get_dag_run_conf()['effective_date']) if bool(
        get_dag_run_conf()['effective_date']) else python_callable_method.get_today_date()
    return {
        "user": {
            "uri": rail.result('get_user_info_from_user_service')[0]['cells'][0]['uri'],
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
                                "name": "8 hours/day; Mon-Fri" if sch_type else get_dag_run_conf()['replicon_schedule_type'],
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": "8 hours/day; Mon-Fri" if sch_type else get_dag_run_conf()['replicon_schedule_type']
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null if sch_type else effective_date
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
