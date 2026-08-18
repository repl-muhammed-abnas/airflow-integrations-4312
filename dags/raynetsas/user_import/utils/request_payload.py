import rail

MANDATORY_FIELDS = {
    "country":"Country",
    "first_name":"First_Name",
    "last_name": "Last_Name",
    "email": "Email"
}

def get_missing_field_message(item):
    missing_fields = []
    for key, log_value in MANDATORY_FIELDS.items():
        if not item[key]:
            missing_fields.append(f"{log_value} not present in the input")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_put_user_payload(dag_run):
    def get_policy_sets(dag_run):
        policy_set=[]
        policy_set.append({
                "uri": dag_run.conf['timesheet_template_uri']
            })
        policy_set.append({
                "uri": dag_run.conf['timeoff_template_uri']
            })
        return policy_set

    return {
            "user": {
                "target": {
                "loginName": dag_run.conf['email']
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "emailAddress": dag_run.conf['email'],
            "schedulePolicySchedule":
            [
                {
                    "schedulePolicy": {
                        "name": dag_run.conf['work_schedule'],
                        "officeSchedule": {
                            "name": dag_run.conf['work_schedule']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    }
                }
            ],
            "workWeekStartDayUri": dag_run.conf['work_week'],
            "employmentDateRange": {
                "startDate": None,
                "endDate": None
            },
            "securityConfiguration": {
                "isLoginEnabled": "1",
                "loginName": dag_run.conf['email'],
                "SSOName": dag_run.conf['email'],
            },
            "holidayCalendar": {
                "name": dag_run.conf['holiday_calendar']
            } if dag_run.conf['holiday_calendar'] else None,
            "permissionSets": [
                {
                    "uri": dag_run.conf['permission_set_uri']
                }
            ],
            "policySets": get_policy_sets(dag_run),
            "timesheetApprovalPath": {
                "name": dag_run.conf['timesheet_approval_path']
            },
            "timeOffApprovalPath": {
                "name": dag_run.conf['timeoff_approval_path']
            },
            "timeZone": {
                "uri": dag_run.conf['timezone_uri']
            },
            "locationSchedule": [
                {
                    "location": {
                    "uri": dag_run.conf['location_uri']
                    }
                }
            ] if dag_run.conf['location_uri'] else None,
            "costCenterSchedule": [
                {
                    "costCenter": {
                    "uri": dag_run.conf['cost_center_uri']
                    },
                }
            ] if dag_run.conf['cost_center_uri'] else None,
            "departmentGroupSchedule": [],
            "serviceCenterSchedule": [
                {
                    "serviceCenter": {
                    "uri": dag_run.conf['service_center_uri']
                    }
                }
            ] if dag_run.conf['service_center_uri'] else None,
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                    "name": dag_run.conf['timesheet_period']
                    }
                }
            ]
        }
    }

def get_child_conf(item,config):
    user_details_from_mapper = list(filter(lambda x:
            x['Country'] == item['country'],
        config.country_code_mapper))
    USER_DEFAULT_FIELDS = config.user_default_fields
    return {
        **item,
        **{
            'user_log' : rail.result('create_log'),
            'location_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_all_locations'),
                'displayText', user_details_from_mapper[0]['Location'], 'uri') if user_details_from_mapper else None,
            'service_center_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_all_service_centers'),
                'displayText', user_details_from_mapper[0]['Business Unit'], 'uri') if user_details_from_mapper else None,
            'cost_center_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_all_cost_centers'),
                'displayText', item['cost_center'], 'uri'),
            "holiday_calendar": user_details_from_mapper[0]['Holiday'] if user_details_from_mapper else None,
            "timesheet_period": USER_DEFAULT_FIELDS['timesheet_period'],
            "timesheet_template_name": USER_DEFAULT_FIELDS['timesheet_template'],
            "timesheet_template_uri": rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',USER_DEFAULT_FIELDS['timesheet_template'],"uri"),
            "timesheet_approval_path":USER_DEFAULT_FIELDS['timesheet_approval_path'],
            "timeoff_template_name": USER_DEFAULT_FIELDS['timeoff_template'],
            "timeoff_template_uri": rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',USER_DEFAULT_FIELDS['timeoff_template'],"uri"),
            "timeoff_approval_path": USER_DEFAULT_FIELDS['timeoff_approval_path'],
            "work_week": f"urn:replicon:day-of-week:{(USER_DEFAULT_FIELDS['work_week'].split(' ', maxsplit=1)[0].lower())}",
            "work_schedule": USER_DEFAULT_FIELDS['work_schedule'],
            'timezone_uri': USER_DEFAULT_FIELDS['timezone_uri'],
            'timeoff_types': rail.result("get_all_time_off_types"),
            'license_uris': rail.result("get_all_licenses"),
            "permission_set_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_set"),
                    'name', USER_DEFAULT_FIELDS['permission_set'],'uri'),
            "process_user": 'yes' if user_details_from_mapper else 'no'
        }
    }

def put_timeoff_assignment_for_user(dag_run):
    return {
        "userUri": rail.result("add_new_user")['uri'],
        "timeOffTypeUris": dag_run.conf['timeoff_types']
    }
