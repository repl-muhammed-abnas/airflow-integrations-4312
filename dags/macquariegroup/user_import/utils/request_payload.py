from datetime import datetime, date, timedelta
import json
import uuid
import rail
from airflow.exceptions import AirflowException
from macquariegroup.user_import.mapper.recovery_field_mapper import recovery_field_mapper
null = None


def add_log_message(fields_updated_list, field, value, message="updated"):
    if value:
        fields_updated_list.append(
            f"{field} {message} successfully to '{value}'")


def get_create_add_location_payload():
    location_to_be_added = rail.load_all_records(
        rail.result('query_locations_not_present_in_replicon'))
    current_dorp_down_details = rail.result(
        "get_all_drop_down_options_from_replicon")

    customFieldDropDownOptionUris = []

    for item in current_dorp_down_details:
        customFieldDropDownOptionUris.append({
            "target": {
                "uri": item['uri'],
                "name": null
            },
            "name": item['name'],
            "isEnabled": item['enabled']
        })

    for item in location_to_be_added:
        customFieldDropDownOptionUris.append({
            "target": null,
            "name": item['location'],
            "isEnabled": 1
        })

    if not customFieldDropDownOptionUris:
        raise Exception("values missing")

    return customFieldDropDownOptionUris


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def update_employment_daterange_user(dag_run):

    start_date = datetime.strptime(
        dag_run.conf['startdate'], '%b %d, %Y') if dag_run.conf['startdate'] else null

    return {
        'userUri': dag_run.conf['user_uri'],
        'dateRange': {
            'startDate': {
                'year': start_date.year,
                'month': start_date.month,
                'day': start_date.day
            } if start_date else null,
            'endDate': rail.result("get_users_current_timesheet_end_date")
        }
    }


def get_custom_field_update_schema(uri, text=null, date_value=null, dropdown=null, number=null):
    return {
        "customField": {
            "uri": uri,
        },
        "text": text,
        "date": date_value,
        "dropDownOption": {
            "name": dropdown
        } if dropdown else null,
        "number": number
    }


def create_custom_fields_payload(dag_run):
    payload = []
    is_office_updated = False
    if dag_run.conf['fte']:
        payload.append(get_custom_field_update_schema(
            uri=dag_run.conf['fte_udf']['uri'], number=dag_run.conf['fte']))

    if dag_run.conf['office']:
        is_office_updated = True
        payload.append(get_custom_field_update_schema(
            uri=dag_run.conf['employee_location_udf']['uri'], dropdown=dag_run.conf['office']))

    if dag_run.conf['grade']:
        payload.append(get_custom_field_update_schema(
            uri=dag_run.conf['grade_udf']['uri'], text=dag_run.conf['grade']))

    if dag_run.conf['region']:
        payload.append(get_custom_field_update_schema(
            uri=dag_run.conf['region_udf']['uri'], text=dag_run.conf['region']))

    if dag_run.conf['business_title']:
        payload.append(get_custom_field_update_schema(
            uri=dag_run.conf['title_udf']['uri'], text=dag_run.conf['business_title']))
    return payload, is_office_updated


def get_create_user_payload(dag_run):
    fields_updated_list = []

    def get_current_month_first_day():
        now = datetime.now().replace(day=1)
        return {
            "day": now.day,
            "month": now.month,
            "year": now.year
        }

    def get_permission_sets():
        permission_set = [
            {
                "name": dag_run.conf['default_user_permission']
            }
        ]
        if dag_run.conf['is_user_supervisor'] == 'true':
            permission_set.append(
                {
                    "name": "Gen3 Supervisor"
                }
            )
        return permission_set

    custom_field_payload, is_office_updated = create_custom_fields_payload(
        dag_run)
    if dag_run.conf['cost_center']:
        add_log_message(fields_updated_list, "Cost Center",
                        dag_run.conf['cost_center'], message="added")
    if dag_run.conf['department_to_assign']:
        add_log_message(fields_updated_list, "Department",
                        dag_run.conf['department_to_assign']['name'], message="added")
    if dag_run.conf['employee_type_to_assign']:
        add_log_message(fields_updated_list, "Employee Type",
                        dag_run.conf['employee_type_to_assign'].get('name'), message="added")
    if is_office_updated:
        add_log_message(fields_updated_list, "Office",
                        dag_run.conf['office'], message="added")
    rail.set_result(key="updated_fields", val=fields_updated_list)

    return {
        "user": {
            "target": {
                "loginName": dag_run.conf['login_name']
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "emailAddress": dag_run.conf['email'],
            "employeeId": dag_run.conf['emp_id'],
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeSchedule": {
                            "name": dag_run.conf['default_office_schedule']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    }
                }
            ],
            "workWeekStartDayUri": "urn:replicon:day-of-week:sunday",
            "employmentDateRange": {
                "startDate": get_current_month_first_day(),
                "endDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "1",
                "loginName": dag_run.conf['login_name'],
                "SSOName": dag_run.conf['login_name']
            },
            "permissionSets": get_permission_sets(),
            "policySets": [
                {
                    "name": dag_run.conf['default_timesheet_template']
                }
            ],
            "customFieldValues": custom_field_payload,
            "timesheetApprovalPath": {
                "name": dag_run.conf['default_timesheet_approval_path']
            } if dag_run.conf['employee_type'] else null,
            "costCenterSchedule": [
                {
                    "costCenter": {
                        "name": dag_run.conf['cost_center']
                    }
                }
            ] if dag_run.conf['cost_center'] else null,
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['department_to_assign']['uri']
                    }
                }
            ] if dag_run.conf['department_to_assign'] else null,
            "serviceCenterSchedule": [
                {
                    "serviceCenter": {
                        "name": "Yes" if dag_run.conf['timesheet_period'] else "No"
                    }
                }
            ],
            "divisionSchedule":  [{
                "division": {
                    "name": dag_run.conf['groups']
                }
            }],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "name": dag_run.conf['employee_type_to_assign']['name']
                    }
                }
            ] if dag_run.conf['employee_type_to_assign'] else null,
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "name": dag_run.conf['timesheet_period']
                    }
                }
            ] if dag_run.conf['timesheet_period'] else null,
            "displayNameParameter": {
                "displayName": dag_run.conf['display_name']
            },
            "extensionFieldValues": [
                {
                    "definition": {
                        "name": "Recovery Override"
                    },
                    "tag": {
                        "slug": "no",
                    }
                },
                {
                    "definition": {
                        "uri": dag_run.conf['ea_login_name']['uri']
                        },
                    "textValue": ("euser" if dag_run.conf['groups'].lower() == "risk management group" else ""),
                }
            ]
        }
    }


def get_add_cost_center_payload(item):
    return {
        "costCenter": null,
        "modifications": {
            "name": item['cost_center'],
            "codeToApply": null,
            "descriptionToApply": null,
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_add_department_payload(dag_run):
    return {
        "departmentGroup": {
            "parent": {
                "uri": rail.result("get_parent_department_details")[0]['uri']
            },
        },
        "modifications": {
            "name": dag_run.conf['name'],
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def can_update_first_name(dag_run, user_details):
    return user_details['firstName'] != dag_run.conf['first_name']


def can_update_last_name(dag_run, user_details):
    return user_details['lastName'] != dag_run.conf['last_name']


def can_update_email(dag_run, user_details):
    return user_details['emailAddress'] != dag_run.conf['email']


def can_update_display_text(dag_run, user_details):
    return user_details['displayText'] != dag_run.conf['display_name']


def get_updated_user_details_payload(dag_run, user_details):
    return {
        "firstName": dag_run.conf['first_name'] if can_update_first_name(dag_run, user_details) else null,
        "lastName": dag_run.conf['last_name'] if can_update_last_name(dag_run, user_details) else null,
        "emailAddress": {
            "emailAddress": dag_run.conf['email']
        } if can_update_email(dag_run, user_details) else null,
        "displayNameParameter": {
            "displayName": dag_run.conf['display_name']
        } if can_update_display_text(dag_run, user_details) else null
    }


def get_user_udf_update_payload(dag_run, current_users_udf_values):
    updated_user_udf_values = []
    is_office_update = False
    if rail.find_first_by_attr_and_get_attr(current_users_udf_values, 'customField.displayText', 'Title', 'text') != dag_run.conf['business_title']:
        updated_user_udf_values.append(get_custom_field_update_schema(
            uri=dag_run.conf['title_udf']['uri'], text=dag_run.conf['business_title']))

    if rail.find_first_by_attr_and_get_attr(current_users_udf_values, 'customField.displayText', 'Region', 'text') != dag_run.conf['region']:
        updated_user_udf_values.append(get_custom_field_update_schema(
            uri=dag_run.conf['region_udf']['uri'], text=dag_run.conf['region']))

    if rail.find_first_by_attr_and_get_attr(current_users_udf_values, 'customField.displayText', 'Grade', 'text') != dag_run.conf['grade']:
        updated_user_udf_values.append(get_custom_field_update_schema(
            uri=dag_run.conf['grade_udf']['uri'], text=dag_run.conf['grade']))

    if rail.find_first_by_attr_and_get_attr(current_users_udf_values, 'customField.displayText', 'Employee Location', 'text') != dag_run.conf['office']:
        updated_user_udf_values.append(get_custom_field_update_schema(
            uri=dag_run.conf['employee_location_udf']['uri'], dropdown=dag_run.conf['office']))
        is_office_update = True

    if str(rail.find_first_by_attr_and_get_attr(current_users_udf_values, 'customField.displayText', 'FTE', 'number')) != dag_run.conf['fte']:
        updated_user_udf_values.append(get_custom_field_update_schema(
            uri=dag_run.conf['fte_udf']['uri'], number=dag_run.conf['fte']))

    return updated_user_udf_values, is_office_update


def get_update_payload(dag_run):

    fields_updated_list = []
    # user details update payload preparation
    user_details = rail.result('get_user_details')[0]['userDetails']
    update_user_details_payload = get_updated_user_details_payload(dag_run, user_details) if (can_update_display_text(dag_run, user_details)
                                            or can_update_email(dag_run, user_details) or can_update_first_name(dag_run, user_details)
                                            or can_update_last_name(dag_run, user_details)) else null

    # payload preparation for updating users udf
    current_users_udf_values = user_details['customFieldValues']
    updated_user_udf_values, is_office_update = get_user_udf_update_payload(
        dag_run, current_users_udf_values)

    # update users group payload preparation
    current_effective_groups = rail.result("get_effectivegroup_membership")

    def get_update_group_uri(to_assign_key, current_effective_group_key, pluck_key='uri'):
        if dag_run.conf[to_assign_key]:
            if current_effective_groups.get(current_effective_group_key, None):
                if current_effective_groups.get(current_effective_group_key)[pluck_key] != dag_run.conf[to_assign_key].get(pluck_key):
                    return dag_run.conf[to_assign_key].get(pluck_key)
            else:
                return dag_run.conf[to_assign_key].get(pluck_key)
        return null

    cost_center_to_assign = get_update_group_uri(
        to_assign_key='cost_center_to_assign', current_effective_group_key="cost_center")
    department_to_assign = get_update_group_uri(
        to_assign_key='department_to_assign', current_effective_group_key="department")
    employee_type_to_assign = get_update_group_uri(
        to_assign_key='employee_type_to_assign', current_effective_group_key="employee_type", pluck_key="name")
    division_to_assign = dag_run.conf['groups'] \
        if rail.result("get_effectivegroup_membership").get("division", {}).get("name", "") != dag_run.conf['groups'] else None
    update_recovery_enable_flag = "Yes" if rail.result('get_effectivegroup_membership').get(
        'service_center', {}).get("name", "").lower() != "yes" else null

    json_today_date = get_today_date()

    def get_timesheet_period_update_payload(dag_run):
        effective_date = list(filter(
            lambda item: item['employee_type'] == dag_run.conf['employee_type'], recovery_field_mapper))
        if dag_run.conf['recovery_enabled'].lower() == "no" or (employee_type_to_assign and dag_run.conf['timesheet_period']):
            return {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {
                            "timesheetPeriod": {
                                "name": dag_run.conf['timesheet_period']
                            },
                            "effectiveDate": effective_date[0]['timesheet_period_assignment']
                        }
                    ],
                    "endDate": null
                }
            }
        return null

    if department_to_assign:
        add_log_message(fields_updated_list, "Department",
                        dag_run.conf['department_to_assign']['name'])
    if cost_center_to_assign:
        add_log_message(fields_updated_list, "Cost Center",
                        dag_run.conf['cost_center_to_assign']['name'])
    if is_office_update:
        add_log_message(fields_updated_list, "Office", dag_run.conf['office'])
    if employee_type_to_assign:
        add_log_message(fields_updated_list, "Employee Type",
                        dag_run.conf['employee_type_to_assign'].get('name'))

    rail.set_result(key="updated_fields", val=fields_updated_list)

    # main return
    return {
        "user": {
            "uri": dag_run.conf['user_uri']
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "uri": cost_center_to_assign
                            },
                            "effectiveDate": json_today_date
                        }
                    ],
                    "endDate": null
                }
            } if cost_center_to_assign else null,
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": department_to_assign,
                            },
                            "effectiveDate": json_today_date
                        }
                    ],
                    "endDate": null
                }
            } if department_to_assign else null,
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "name": dag_run.conf['employee_type']
                            },
                            "effectiveDate": json_today_date
                        }
                    ],
                    "endDate": null
                }
            } if employee_type_to_assign else null,
            "serviceCenterScheduleToApply": {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementServiceCenterSchedule": [],
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                        {
                            "serviceCenter": {
                                "name": "Yes"
                            },
                            "effectiveDate": json_today_date
                        }
                    ],
                    "endDate": null
                }
            } if update_recovery_enable_flag else null,
            "divisionScheduleToApply": {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [
                        {
                            "division": {
                                "name": dag_run.conf['groups']
                            },
                            "effectiveDate": json_today_date
                        }
                    ],
                    "endDate": null
                }
            } if division_to_assign else null,
            "securitySettingsToApply": {
                "loginName": dag_run.conf['login_name'],
                "ssoName": dag_run.conf['login_name'],
            } if bool(rail.result("is_different_user_found")) else null,
            "timesheetPeriodScheduleToApply": get_timesheet_period_update_payload(dag_run),
            "customFieldValuesToApply": updated_user_udf_values,
            "userDetailsToApply": update_user_details_payload,
            "objectExtensionFieldsToApply": [{
                    "definition": {
                        "uri": dag_run.conf['ea_login_name']['uri']
                        },
                    "textValue": ("euser" if dag_run.conf['groups'].lower() == "risk management group" else ""),
                }]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_search_supervisor_payload(dag_run):
    return{
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['supervisor'],
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }


def get_supervisor_assignment_start_date(current_time_sheet_end_date):
    if not current_time_sheet_end_date:
        return get_today_date()

    date_value = date(year=current_time_sheet_end_date['year'],
                      month=current_time_sheet_end_date['month'],
                      day=current_time_sheet_end_date['day']) + timedelta(days=1)

    return {"day": date_value.day, "month": date_value.month, "year": date_value.year}

def is_supervisor_enabled(dag_run):
    if rail.result('search_supervisor_in_replicon'):
        return rail.result('search_supervisor_in_replicon')[0]['enabled']
    return dag_run.conf.get('supervisor_status', "False") == "True"

def get_supervisor_uri(dag_run):
    if dag_run.conf['can_assign_default'] not in ["No", False, 'False']:
        return dag_run.conf['default_supervisor_uri']
    if dag_run.conf.get('supervisor_status', None) in ['False', False]:
        return dag_run.conf['default_supervisor_uri']
    if not dag_run.conf['supervisor_uri']:
        return rail.result('search_supervisor_in_replicon')[0]['useruri'] if is_supervisor_enabled(dag_run) else dag_run.conf['default_supervisor_uri']
    return dag_run.conf['supervisor_uri'] if is_supervisor_enabled(dag_run) else dag_run.conf['default_supervisor_uri']

def get_update_supervisor_payload(dag_run):
    supervisor_uri = get_supervisor_uri(dag_run)
    return {
        "userUri": dag_run.conf['user_uri'],
        "supervisorUri": supervisor_uri,
        "dateRange":  None if dag_run.conf['action'].lower() == 'add' else {
            "startDate": get_supervisor_assignment_start_date(rail.result('get_users_current_timesheet_end_date'))
        }
    }


def get_final_payload_sendemail(useruri, html_body, subject_line):
    if not useruri:
        raise AirflowException("User uri is blank")

    final_payload = {"email": {
        "to": [
            {
                "user": {
                    "uri": useruri,
                    "loginName": null
                },
                "email": null
            }
        ],
        "cc": [],
        "bcc": [],
        "replyTo": null,
        "fromDisplayName": "Do-Not-Reply@deltek.com",
        "subject": subject_line,
        "htmlBody": html_body,
        "textBody": null,
        "attachments": []
    }}
    return json.dumps(final_payload)
