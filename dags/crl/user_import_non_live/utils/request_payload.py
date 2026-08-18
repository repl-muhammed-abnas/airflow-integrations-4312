from datetime import datetime, timedelta
import json
import rail

null = None
DATE_FORMAT = "%m/%d/%Y"

def get_replicon_date(date_str):
    if not date_str:
        return None

    date = datetime.strptime(date_str, DATE_FORMAT)
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }

# pylint: disable=too-many-arguments
def get_process_other_users_conf(item, config):
    get_all_permission_sets = rail.result("get_all_permission_set")
    get_user_udfs = rail.result('get_user_udfs')

    def get_holiday_calender_name():
        if item['holiday_calendar']:
            return item['holiday_calendar']
        return null

    def get_employee_status():
        if item['emp_status'] in config.ACTIVE_STATUS:
            return 'Active'
        return 'Terminated'

    return {
        **item,
        **{
            "replicon_employee_status": get_employee_status(),
            'title_def_uri': get_user_udfs['title_def_uri'],
            'functional_segment_def_uri': get_user_udfs['functional_segment_def_uri'],
            'std_hrs_def_uri': get_user_udfs['std_hrs_def_uri'],
            'adjusted_hiredate_def_uri': get_user_udfs['adjusted_hiredate_def_uri'],
            'adjusted_hiredate_accrual_def_uri': get_user_udfs['adjusted_hiredate_accrual_def_uri'],
            'job_code_def_uri': get_user_udfs['job_code_def_uri'],
            'pay_grp_def_uri': get_user_udfs['pay_grp_def_uri'],
            'us_flsa_status_def_uri': get_user_udfs['us_flsa_status_def_uri'],
            'profit_center_def_uri': get_user_udfs['profit_center_def_uri'],
            'project_user_def_uri': get_user_udfs['project_user_def_uri'],
            'us_vacation_exception_def_uri': get_user_udfs['us_vacation_exception_def_uri'],
            'us_veterans_day_def_uri': get_user_udfs['us_veterans_day_def_uri'],
            'emp_status_def_uri': get_user_udfs['emp_status_def_uri'],
            'buisness_segment_def_uri': get_user_udfs['buisness_segment_def_uri'],
            'buisness_unit_def_uri': get_user_udfs['buisness_unit_def_uri'],
            'reg_temp_def_uri': get_user_udfs['reg_temp_def_uri'],
            'full_part_def_uri': get_user_udfs['full_part_def_uri'],
            'is_hrbp_def_uri': get_user_udfs['is_hrbp_def_uri'],
            'pay_type_def_uri': get_user_udfs['pay_type_def_uri'],
            'remote_worker_def_uri': get_user_udfs['remote_worker_def_uri'],
            'change_effective_date_def_uri': get_user_udfs['change_effective_date_def_uri'],
            'event_def_uri': get_user_udfs['event_def_uri'],
            'event_reason_def_uri': get_user_udfs['event_reason_def_uri'],
            "default_activity_def_uri": get_user_udfs['default_activity_def_uri'],
            "holiday_calendar_def_uri": get_user_udfs['holiday_calendar_def_uri'],

            'us_flsa_status_drop_uri': rail.find_first_by_attr_and_get_attr
                (rail.result("get_us_flsa_status_dropdown_values"),'name', item['us_flsa_status'],'uri')
                if item['us_flsa_status'] else null,
            'project_user_drop_uri': rail.find_first_by_attr_and_get_attr
                (rail.result("get_project_user_dropdown_values"),'name',"Yes" if item['activity_type'] else "No",'uri'),
            'us_veterans_drop_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_us_veterans_day_dropdown_values"),
                'name',item['us_veterans_status'],'uri') if item['us_veterans_status'] else null,

            'location_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_location_grps'), 'full_path', item['location_full_path'], 'uri'),

            "holiday_calendar": get_holiday_calender_name(),
            "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calenders'),
                'displayText', get_holiday_calender_name(), 'uri') if get_holiday_calender_name() else null,

            'payrule_name': "Placeholder_Payrule",
            'payrule_script_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_payrule_scripts"),'displayText', "Placeholder_Payrule","uri"),

            "supervisor_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Supervisor','uri'),
            "report_user_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Report User','uri'),
            "admin_hrpb_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'View Only Admin HRPB','uri'),
            "ts_hrpb_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'View Only TS HRBP','uri'),

            'starting_balance_script_uri': rail.result('get_timeoff_balance_event_script_uri')['starting_balance_script_uri'],
            'prevent_balance_overdraw_uri': rail.result('get_timeoff_balance_validation_script')['prevent_balance_overdraw_uri'],

            'office_schedule_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),'displayText',item['work_schedule'],"uri")
                if item['work_schedule'] else null,
        }
    }

def get_process_new_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log' : rail.result('create_user_log')
        }
    }

def get_process_update_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log': rail.result('create_user_log'),
            'useruri': rail.result('get_user_data')[0]['uri'],
            'todays_date': (datetime.now()).strftime(DATE_FORMAT)
        }
    }

def get_date_from_replicon_date(replicon_date):
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def get_udfs(user_status, dag_run):
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    udfs = []
    def add_udf_field_values(definitionuri, dropdownuri = null, textvalue = null , number = null, date = null):
        udfs.append({
        "customField": {
          "uri": definitionuri,
          "name": null,
          "groupUri": null
        },
        "text": textvalue,
        "date": get_replicon_date(date) if date else null,
        "dropDownOption": {
          "uri": dropdownuri,
          "name": null
        } if dropdownuri != null else null,
        "number": number
      })


    if user_status =='adduser':
        add_udf_field_values(definitionuri = dag_run.conf['project_user_def_uri'], dropdownuri= dag_run.conf['project_user_drop_uri'])
        if dag_run.conf['title']:
            add_udf_field_values(definitionuri = dag_run.conf['title_def_uri'], textvalue= dag_run.conf['title'])
        if dag_run.conf['functional_segment']:
            add_udf_field_values(definitionuri = dag_run.conf['functional_segment_def_uri'], textvalue= dag_run.conf['functional_segment'])
        if dag_run.conf['std_hrs']:
            add_udf_field_values(definitionuri = dag_run.conf['std_hrs_def_uri'], number= dag_run.conf['std_hrs'])
        if dag_run.conf['adjusted_start_date']:
            add_udf_field_values(definitionuri = dag_run.conf['adjusted_hiredate_def_uri'], date= dag_run.conf['adjusted_start_date'])
        if dag_run.conf['job_code']:
            add_udf_field_values(definitionuri = dag_run.conf['job_code_def_uri'], textvalue= dag_run.conf['job_code'])
        if dag_run.conf['pay_grp']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_grp_def_uri'], textvalue= dag_run.conf['pay_grp'])
        if dag_run.conf['us_flsa_status']:
            add_udf_field_values(definitionuri = dag_run.conf['us_flsa_status_def_uri'], dropdownuri= dag_run.conf['us_flsa_status_drop_uri'])
        if dag_run.conf['profit_center']:
            add_udf_field_values(definitionuri = dag_run.conf['profit_center_def_uri'], textvalue= dag_run.conf['profit_center'])
        if dag_run.conf['us_vacation_exception']:
            add_udf_field_values(definitionuri = dag_run.conf['us_vacation_exception_def_uri'], textvalue= dag_run.conf['us_vacation_exception'])
        if dag_run.conf['us_veterans_status']:
            add_udf_field_values(definitionuri = dag_run.conf['us_veterans_day_def_uri'], dropdownuri= dag_run.conf['us_veterans_drop_uri'])
        if dag_run.conf['holiday_calendar']:
            add_udf_field_values(definitionuri = dag_run.conf['holiday_calendar_def_uri'], textvalue= dag_run.conf['holiday_calendar'])

        add_udf_field_values(definitionuri = dag_run.conf['emp_status_def_uri'], textvalue= dag_run.conf['emp_status'])
        if dag_run.conf['buisness_unit_full_path']:
            add_udf_field_values(definitionuri = dag_run.conf['buisness_segment_def_uri'], textvalue= (dag_run.conf['buisness_unit_full_path']).split('|')[0])
            add_udf_field_values(definitionuri = dag_run.conf['buisness_unit_def_uri'], textvalue= (dag_run.conf['buisness_unit_full_path']).split('|')[1])
        if dag_run.conf['full_part']:
            add_udf_field_values(definitionuri = dag_run.conf['full_part_def_uri'], textvalue= dag_run.conf['full_part'])
        if dag_run.conf['reg_temp']:
            add_udf_field_values(definitionuri = dag_run.conf['reg_temp_def_uri'], textvalue= dag_run.conf['reg_temp'])
        if dag_run.conf['pay_type']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_type_def_uri'], textvalue= dag_run.conf['pay_type'])
        if dag_run.conf['is_hrbp']:
            add_udf_field_values(definitionuri = dag_run.conf['is_hrbp_def_uri'], textvalue= dag_run.conf['is_hrbp'])
        if dag_run.conf['remote_worker']:
            add_udf_field_values(definitionuri = dag_run.conf['remote_worker_def_uri'], textvalue= dag_run.conf['remote_worker'])
        if dag_run.conf['event']:
            add_udf_field_values(definitionuri = dag_run.conf['event_def_uri'], textvalue= dag_run.conf['event'])
        if dag_run.conf['event_reason_code']:
            add_udf_field_values(definitionuri = dag_run.conf['event_reason_def_uri'], textvalue= dag_run.conf['event_reason_code'])
        if dag_run.conf['activity_type']:
            add_udf_field_values(definitionuri = dag_run.conf['default_activity_def_uri'], textvalue= dag_run.conf['activity_type'])

    if user_status =='updateuser':
        add_udf_field_values(definitionuri = dag_run.conf['project_user_def_uri'], dropdownuri= dag_run.conf['project_user_drop_uri'])

        current_title = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Title', 'text')
        if dag_run.conf['title'] and current_title != dag_run.conf['title']:
            add_udf_field_values(definitionuri = dag_run.conf['title_def_uri'], textvalue= dag_run.conf['title'])

        current_functional_segment = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Functional Segment', 'text')
        if dag_run.conf['functional_segment'] and current_functional_segment != dag_run.conf['functional_segment']:
            add_udf_field_values(definitionuri = dag_run.conf['functional_segment_def_uri'], textvalue= dag_run.conf['functional_segment'])

        current_std_hrs = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Standard Hours', 'text')
        if dag_run.conf['std_hrs'] and (float(current_std_hrs) if current_std_hrs else current_std_hrs) != float(dag_run.conf['std_hrs']):
            add_udf_field_values(definitionuri = dag_run.conf['std_hrs_def_uri'], number= dag_run.conf['std_hrs'])

        current_adjusted_start_date = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Adjusted Hire Date', 'date')
        if dag_run.conf['adjusted_start_date'] and get_date_from_replicon_date(current_adjusted_start_date
                )!= get_date_from_replicon_date(get_replicon_date(dag_run.conf['adjusted_start_date'])):
            add_udf_field_values(definitionuri = dag_run.conf['adjusted_hiredate_def_uri'], date= dag_run.conf['adjusted_start_date'])

        current_job_code= rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Job Code', 'text')
        if dag_run.conf['job_code'] and current_job_code != dag_run.conf['job_code']:
            add_udf_field_values(definitionuri = dag_run.conf['job_code_def_uri'], textvalue= dag_run.conf['job_code'])

        current_pay_grp = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'US Pay Group', 'text')
        if dag_run.conf['pay_grp'] and current_pay_grp != dag_run.conf['pay_grp']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_grp_def_uri'], textvalue= dag_run.conf['pay_grp'])

        current_profit_center = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Profit Center', 'text')
        if dag_run.conf['profit_center'] and current_profit_center != dag_run.conf['profit_center']:
            add_udf_field_values(definitionuri = dag_run.conf['profit_center_def_uri'], textvalue= dag_run.conf['profit_center'])

        current_us_vacation_exception = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Vacation Exception', 'text')
        if dag_run.conf['us_vacation_exception'] and current_us_vacation_exception != dag_run.conf['us_vacation_exception']:
            add_udf_field_values(definitionuri = dag_run.conf['profit_center_def_uri'], textvalue= dag_run.conf['us_vacation_exception'])

        current_us_flsa_status = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'US FLSA Status', 'text')
        if dag_run.conf['us_flsa_status'] and current_us_flsa_status != dag_run.conf['us_flsa_status']:
            add_udf_field_values(definitionuri = dag_run.conf['us_flsa_status_def_uri'], dropdownuri= dag_run.conf['us_flsa_status_drop_uri'])

        current_us_veterans_status = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'US Veterans Day', 'text')
        if dag_run.conf['us_veterans_status'] and current_us_veterans_status != dag_run.conf['us_veterans_status']:
            add_udf_field_values(definitionuri = dag_run.conf['us_veterans_day_def_uri'], dropdownuri= dag_run.conf['us_veterans_drop_uri'])

        current_emp_status = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Employee Status', 'text')
        if dag_run.conf['emp_status'] and current_emp_status != dag_run.conf['emp_status']:
            add_udf_field_values(definitionuri = dag_run.conf['emp_status_def_uri'], textvalue= dag_run.conf['emp_status'])

        current_buisness_segment = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Business Segment', 'text')
        if dag_run.conf['buisness_unit_full_path'] and current_buisness_segment != (dag_run.conf['buisness_unit_full_path']).split('|')[0]:
            add_udf_field_values(definitionuri = dag_run.conf['buisness_segment_def_uri'], textvalue= (dag_run.conf['buisness_unit_full_path']).split('|')[0])

        current_buisness_segment = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Business Unit', 'text')
        if dag_run.conf['buisness_unit_full_path'] and current_buisness_segment != (dag_run.conf['buisness_unit_full_path']).split('|')[1]:
            add_udf_field_values(definitionuri = dag_run.conf['buisness_unit_def_uri'], textvalue= (dag_run.conf['buisness_unit_full_path']).split('|')[1])

        current_full_part = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Full/Part', 'text')
        if dag_run.conf['full_part'] and current_full_part != dag_run.conf['full_part']:
            add_udf_field_values(definitionuri = dag_run.conf['full_part_def_uri'], textvalue= dag_run.conf['full_part'])

        current_reg_temp = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Reg/Temp', 'text')
        if dag_run.conf['reg_temp'] and current_reg_temp != dag_run.conf['reg_temp']:
            add_udf_field_values(definitionuri = dag_run.conf['reg_temp_def_uri'], textvalue= dag_run.conf['reg_temp'])

        current_pay_type = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Pay Type', 'text')
        if dag_run.conf['pay_type'] and current_pay_type != dag_run.conf['pay_type']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_type_def_uri'], textvalue= dag_run.conf['pay_type'])

        current_is_hrbp = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')
        if dag_run.conf['is_hrbp'] and current_is_hrbp != dag_run.conf['is_hrbp']:
            add_udf_field_values(definitionuri = dag_run.conf['is_hrbp_def_uri'], textvalue= dag_run.conf['is_hrbp'])

        current_remote_worker = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Remote Worker', 'text')
        if dag_run.conf['remote_worker'] and current_remote_worker != dag_run.conf['remote_worker']:
            add_udf_field_values(definitionuri = dag_run.conf['remote_worker_def_uri'], textvalue= dag_run.conf['remote_worker'])

        current_change_effective_date = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Change Effective Date', 'date')
        if dag_run.conf['change_effective_date'] and \
            (get_date_from_replicon_date(current_change_effective_date).strftime(DATE_FORMAT) if current_change_effective_date else null) \
            != datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT).strftime(DATE_FORMAT):
            add_udf_field_values(definitionuri = dag_run.conf['change_effective_date_def_uri'], date= dag_run.conf['change_effective_date'])

        current_event = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Event', 'text')
        if dag_run.conf['event'] and current_event != dag_run.conf['event']:
            add_udf_field_values(definitionuri = dag_run.conf['event_def_uri'], textvalue= dag_run.conf['event'])

        current_event_reason = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Event Reason', 'text')
        if dag_run.conf['event_reason_code'] and current_event_reason != dag_run.conf['event_reason_code']:
            add_udf_field_values(definitionuri = dag_run.conf['event_reason_def_uri'], textvalue= dag_run.conf['event_reason_code'])

        current_activity_type = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Default Activity', 'text')
        if current_activity_type != dag_run.conf['activity_type']:
            add_udf_field_values(definitionuri = dag_run.conf['default_activity_def_uri'], textvalue= dag_run.conf['activity_type'])

        current_holiday_calendar = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Holiday Calendar Code', 'text')
        if current_holiday_calendar != dag_run.conf['activity_type']:
            add_udf_field_values(definitionuri = dag_run.conf['holiday_calendar_def_uri'], textvalue= dag_run.conf['holiday_calendar'])

    return udfs
def get_put_user_payload(dag_run):
    def validate_holiday_calender():
        if not dag_run.conf['holiday_calendar']:
            return False
        if dag_run.conf['holiday_calendar'] and not dag_run.conf['holiday_calendar_uri']:
            return False
        return True

    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['login_name'],
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "emailAddress": dag_run.conf['email'],
            "employeeId": dag_run.conf['emp_id'],
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule":
            [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['work_schedule'],
                        "officeSchedule": {
                            "officeScheduleUri":null,
                            "name": dag_run.conf['work_schedule']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['work_schedule'] and dag_run.conf['office_schedule_uri'] else
            [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": "M-F 8 hours/day",
                        "officeSchedule": {
                            "officeScheduleUri":null,
                            "name": "M-F 8 hours/day"
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['start_date']),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                   "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "false",
                "loginName": dag_run.conf['login_name'],
                "SSOName": dag_run.conf['login_name'],
            },
            "holidayCalendar": {
                "uri": null,
                "name": dag_run.conf['holiday_calendar']
            } if validate_holiday_calender() else null,
            "permissionSets": [
                {
                    "uri": dag_run.conf['report_user_permission_uri'],
                    "name": null
                }
            ]if dag_run.conf['is_hrbp'] != 'Y' else
            [
                {
                    "uri": dag_run.conf['report_user_permission_uri'],
                    "name": null
                },
                {
                    "uri": dag_run.conf['admin_hrpb_permission_uri'],
                    "name": null
                },
                {
                    "uri": dag_run.conf['ts_hrpb_permission_uri'],
                    "name": null
                }
            ],
            "customFieldValues": get_udfs('adduser', dag_run),
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf['location_uri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]if dag_run.conf['location_uri'] else null,
        }
    }

def assign_policyDataAccessScopes_to_projectmanager():
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "policyDataAccessScopes": [
            {
            "policyUri": "urn:replicon:policy:administration",
            "locations": [],
            "divisions": [
                {
                "division": null,
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                }
            ],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": [],
            "scopeObjectTypeUri": null
            },
            {
            "policyUri": "urn:replicon:policy:payroll-management",
            "locations": [],
            "divisions": [
                {
                "division": null,
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                }
            ],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": [],
            "scopeObjectTypeUri": null
            }
        ]
        }

def update_holiday_calendar(dag_run):
    if not dag_run.conf['holiday_calendar']:
        return null
    if dag_run.conf['holiday_calendar'] and not dag_run.conf['holiday_calendar_uri']:
        return null
    current_holiday_calendar = rail.result("get_user_info")['holidayCalendar']
    if current_holiday_calendar and current_holiday_calendar['displayText'] == dag_run.conf['holiday_calendar']:
        return null
    return{
        "holidayCalendar": {
            "uri": null,
            "name": dag_run.conf['holiday_calendar']
        }
    }


def update_schedule(dag_run, log):
    current_schedule = rail.result("get_user_info")['schedulePolicies']
    if dag_run.conf['office_schedule_uri']:
        if not current_schedule or (dag_run.conf['work_schedule'] !=
            (current_schedule[-1]['officeSchedule']['displayText'] if current_schedule[-1]['scheduleTypeUri']==
            'urn:replicon:schedule-type:office-schedule' else null)):
            return {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                    {
                        "schedulePolicy": {
                        "officeScheduleUri": dag_run.conf['office_schedule_uri'],
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": dag_run.conf['office_schedule_uri'],
                            "name": null,
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                    }
                    ],
                    "endDate": null
                }
                }
        return null

    log.append("Work Schedule not available in Replicon")
    return null

def update_location_grp(location_uri, current_location_uri, dag_run,log):
    if not location_uri:
        log.append("Location not updated, not available in replicon")
        return null
    return {
        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementLocationSchedule": [],
        "updateLocationScheduleOverDateRange": {
            "replacementLocationScheduleEntries": [
                {
                    "location": {
                        "uri": location_uri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ],
            "endDate": null
        }
    } if current_location_uri != location_uri else null

def update_permission_set(dag_run):
    previous_is_hrbp_value = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')

    if dag_run.conf['is_hrbp']=="Y" and dag_run.conf['is_hrbp']!= previous_is_hrbp_value:
        return {
            "permissionSetUrisToAssign": [dag_run.conf['ts_hrpb_permission_uri'],dag_run.conf['admin_hrpb_permission_uri']],
            "policyUrisToRemovePermissionSet": []
        }
    return null

def update_permission__hrbp_access_scope(dag_run):
    previous_is_hrbp_value = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')
    if dag_run.conf['is_hrbp']!= previous_is_hrbp_value and dag_run.conf['is_hrbp']=="Y":
        return {
            "policyDataAccessScopes": [
                {
                "policyUri": "urn:replicon:policy:administration",
                "locations": [],
                "divisions": [
                    {
                    "division": null,
                    "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                    "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                    }
                ],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": [],
                "scopeObjectTypeUri": null
                },
                {
                "policyUri": "urn:replicon:policy:payroll-management",
                "locations": [],
                "divisions": [
                    {
                    "division": null,
                    "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                    "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                    }
                ],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": [],
                "scopeObjectTypeUri": null
                }
            ]
            }
    return null

def update_policy_set():
    # pylint: disable=too-many-boolean-expressions
    policy_set_to_remove = []

    assigned_timesheet_template = rail.result("get_user_info")['timesheetTemplate']

    if assigned_timesheet_template:
        policy_set_to_remove.append("urn:replicon:policy:timesheet")

    if policy_set_to_remove:
        return {
            "policySetUrisToAssign": [],
            "policyUrisToRemovePolicySet": policy_set_to_remove
        }
    return null

def update_user_details(dag_run):
    user_details = rail.result("get_user_info")['userDetails']

    return {
      "firstName": dag_run.conf['first_name'] if user_details['firstName'] != dag_run.conf['first_name'] else null,
      "lastName": dag_run.conf['last_name'] if user_details['lastName'] != dag_run.conf['last_name'] else null,
      "emailAddress": {
        "emailAddress": dag_run.conf['email']
      } if user_details['emailAddress'] != dag_run.conf['email'] else null,
      "language": null,
      "employmentDateRange": null,
      "employmentStartDate": null,
       "employmentEndDate": {
         "date": null
       } if not dag_run.conf['end_date'] else ({
         "date": get_replicon_date(dag_run.conf['end_date'])
       }if dag_run.conf['end_date'] else null),
    }

def update_payrule_script(dag_run):
    current_payrulescript = rail.result("get_user_info")['payRuleScriptSchedule']
    if not current_payrulescript and dag_run.conf['payrule_name']:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_script_uri'],
                        "name": null
                    },
                    "effectiveDate":   get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ]
        }

    if dag_run.conf['payrule_name'] and (dag_run.conf['payrule_name'] != current_payrulescript[-1]['payRuleScript']['displayText']):
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_script_uri'],
                        "name": null
                    },
                    "effectiveDate":  get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ]
        }

    return null

def update_security_settings(dag_run):
    if rail.result('get_direct_reports_for_user'):
        return null
    return {
        "loginEnabled": "false",
        "forcePasswordChange": null,
        "loginName": dag_run.conf['login_name'],
        "ssoName": dag_run.conf['login_name'],
        "password": null,
        "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
        "emailMFAResendVerificationEmail": "false",
        "emailMFATryAddMethodFromUsersEmail": "false",
        "clearIsLockedOut": "false"
        }

def apply_user_modifications_payload(dag_run):
    log=[]
    user_update_payload = {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "holidayCalendarToApply": update_holiday_calendar(dag_run),
            "schedulePolicyToApply": update_schedule(dag_run, log),
            "locationScheduleToApply": update_location_grp(dag_run.conf['location_uri'],
                rail.result('get_effective_user_groupmembership','location').get('uri', ''), dag_run,log),
            "permissionSetsToApply": update_permission_set(dag_run),
            "policyDataAccessScopesToApply2": update_permission__hrbp_access_scope(dag_run),
            "policySetsToApply": update_policy_set(),
            "securitySettingsToApply": update_security_settings(dag_run),
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": update_payrule_script(dag_run)
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    rail.set_result(key="exception_logs",val= log)

    return user_update_payload

def validate_is_remove_hrbp_permossion_set(dag_run):
    previous_is_hrbp_value = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')
    return bool(dag_run.conf['is_hrbp']=="N" and dag_run.conf['is_hrbp']!= previous_is_hrbp_value)

def validate_enddate(dag_run):
    if dag_run.conf['end_date']:
        return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)
    return datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)

def is_enddate_in_future(dag_run):
    if dag_run.conf['end_date']:
        return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) >= datetime.strptime(dag_run.conf['todays_date'], DATE_FORMAT)
    return datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT) >= datetime.strptime(dag_run.conf['todays_date'], DATE_FORMAT)

def get_udfs_disable_user(dag_run):
    current_udf_values  = rail.result("get_current_udf_values")
    udfs = []
    def add_udf_field_values(definitionuri, dropdownuri = null, textvalue = null , number = null, date = null):
        udfs.append({
        "customField": {
          "uri": definitionuri,
          "name": null,
          "groupUri": null
        },
        "text": textvalue,
        "date": get_replicon_date(date) if date else null,
        "dropDownOption": {
          "uri": dropdownuri,
          "name": null
        } if dropdownuri != null else null,
        "number": number
      })
    current_emp_status = rail.find_first_by_attr_and_get_attr(current_udf_values,
        'customField.displayText', 'Employee Status', 'text')
    if dag_run.conf['emp_status'] and current_emp_status != dag_run.conf['emp_status']:
        add_udf_field_values(definitionuri = dag_run.conf['emp_status_def_uri'], textvalue= dag_run.conf['emp_status'])

    current_change_effective_date = rail.find_first_by_attr_and_get_attr(current_udf_values,
        'customField.displayText', 'Change Effective Date', 'date')
    if dag_run.conf['change_effective_date'] and \
        (get_date_from_replicon_date(current_change_effective_date).strftime(DATE_FORMAT) if current_change_effective_date else null)\
            != datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT).strftime(DATE_FORMAT):
        add_udf_field_values(definitionuri = dag_run.conf['change_effective_date_def_uri'], date= dag_run.conf['change_effective_date'])

    current_event = rail.find_first_by_attr_and_get_attr(current_udf_values,
        'customField.displayText', 'Event', 'text')
    if dag_run.conf['event'] and current_event != dag_run.conf['event']:
        add_udf_field_values(definitionuri = dag_run.conf['event_def_uri'], textvalue= dag_run.conf['event'])

    current_event_reason = rail.find_first_by_attr_and_get_attr(current_udf_values,
        'customField.displayText', 'Event Reason', 'text')
    if dag_run.conf['event_reason_code'] and current_event_reason != dag_run.conf['event_reason_code']:
        add_udf_field_values(definitionuri = dag_run.conf['event_reason_def_uri'], textvalue= dag_run.conf['event_reason_code'])

    return udfs

def update_end_date_payload(dag_run):
    return {
            "user": {
                "uri": dag_run.conf['useruri']
            },
            "modifications": {
                "userDetailsToApply":   {
                    "employmentEndDate": {
                        "date": get_replicon_date(dag_run.conf['end_date']) if dag_run.conf['end_date'] else \
                            get_replicon_date((datetime.strptime(
                        dag_run.conf['change_effective_date'],DATE_FORMAT)-timedelta(days=1)).strftime(DATE_FORMAT))
                    },
                },
                "customFieldValuesToApply": get_udfs_disable_user(dag_run),
                },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_disable_message():
    if get_task_state('log_end_date_future') =='success':
        return "User End date in Future, Enddate updated but Profile will be disabled on end date"
    return "User disabled Successfully"

def get_disable_status():
    if get_task_state('log_end_date_future') =='success':
        return "Exception"
    return "Success"

def get_update_user_message():
    exception_logs = rail.result('apply_user_modifications', 'exception_logs')

    if exception_logs:
        return "User Partially Updated;"+ rail.smartjoin_by_delim(exception_logs, ";")
    return "User Updated Successfully"

def get_update_user_severity():
    exception_logs = rail.result('apply_user_modifications', 'exception_logs')
    if  exception_logs:
        return 'Exception'
    return 'Success'

def put_user_timeoff_policy_schedule_blank_policy(dag_run):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('for_each_time_off_type_no_accural')['timeoff_type_uri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign_for_disable_user'))
    }
