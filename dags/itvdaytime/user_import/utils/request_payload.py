import os
import rail
from itvdaytime.user_import.utils import custom_methods
null = None

mandatory_fields = ['employee_number', 'first_name', 'last_name', 'start_date',
                    'assignment_number', 'department', 'user_person_type', 'location', 'contract_type']

fields_not_to_update = ['employee_number']


def get_invalid_logs_property_conf(item):
    def get_missing_field():
        not_present_fields = []
        for field in mandatory_fields:
            if item[field] in [None, '']:
                not_present_fields.append(field)
        not_present_fields = list(filter(None, not_present_fields))
        return ";".join(not_present_fields)
    return {
        "employee_number": item['employee_number'],
        "loginname": item['first_name'] + '.' + item['last_name'],
        "status": "Skipped",
        "action": "Validation",
        "details": get_missing_field() + " not present in feed file",
        "user_uri": "",
        "allowed_for_supervisor_processing": "No",
        "line_manager": item['line_manager']
    }


def is_user_manager(item):
    return "true" if rail.find_first_by_attr_and_get_attr(
        custom_methods.get_data_from_document(rail.result("query_valid_records")), "line_manager", item["employee_number"]) else 'false'


def get_process_user_records_conf(item):
    return{
        **{"file_name": os.path.split(rail.result("new_file_sensor"))[1]},
        **{k: v if v is not None else '' for k, v in item.items()},
        **{
            "service_centers": rail.result('get_all_service_centers'),
            "permission_sets": rail.result("get_required_permission_sets"),
            "custom_fields": rail.result('get_user_custom_fields'),
            "is_user_manager": is_user_manager(item),
            "log": rail.result('create_supervisor_log'),
            "time_off_details_collection": rail.result('create_timeoff_details_collection')
        }
    }


def get_search_user_payload(dag_run, is_supervisor=False):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:start-date",
            "urn:replicon:user-list-column:end-date",
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
                    "text": dag_run.conf['line_manager'] if is_supervisor else dag_run.conf['employee_number']
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_custom_fields(dag_run):
    custom_fields = []
    if dag_run.conf['assignment_number']:
        custom_fields.append(custom_methods.get_custom_field(
            uri=rail.find_first_by_attr_and_get_attr(
                dag_run.conf['custom_fields'], "name", "Assignment ID", 'uri'),
            text=dag_run.conf['assignment_number']
        ))
    if dag_run.conf['phone_number']:
        custom_fields.append(custom_methods.get_custom_field(
            uri=rail.find_first_by_attr_and_get_attr(
                dag_run.conf['custom_fields'], "name", "Primary Contact Number", 'uri'),
            text=dag_run.conf['phone_number']
        ))
    if dag_run.conf['department']:
        custom_fields.append(custom_methods.get_custom_field(
            uri=rail.find_first_by_attr_and_get_attr(
                dag_run.conf['custom_fields'], "name", "Fusion Department Name", 'uri'),
            text=dag_run.conf['department']
        ))
    if dag_run.conf['job_role']:
        custom_fields.append(custom_methods.get_custom_field(
            uri=rail.find_first_by_attr_and_get_attr(
                dag_run.conf['custom_fields'], "name", "Job Title", 'uri'),
            text=dag_run.conf['job_role']
        ))
    if dag_run.conf['assignment_start_date']:
        custom_fields.append(custom_methods.get_custom_field(
            uri=rail.find_first_by_attr_and_get_attr(
                dag_run.conf['custom_fields'], "name", "Assignment ID Enter Date", 'uri'),
            date_value=dag_run.conf['assignment_start_date']
        ))

    if dag_run.conf['location']:
        custom_fields.append(custom_methods.get_custom_field(
            uri=rail.find_first_by_attr_and_get_attr(
                dag_run.conf['custom_fields'], "name", "Fusion Location", 'uri'),
            text=dag_run.conf['location']
        ))

    if dag_run.conf['contract_type']:
        custom_fields.append(custom_methods.get_custom_field(
            uri=rail.find_first_by_attr_and_get_attr(
                dag_run.conf['custom_fields'], "name", "Contract Type", 'uri'),
            dropdown=dag_run.conf['contract_type']
        ))

    return custom_fields


def get_create_user_payload(dag_run):
    def get_employment_dates():
        if not dag_run.conf['start_date'] and not dag_run.conf['end_date']:
            return None

        return {
            "startDate": custom_methods.get_replicon_date(dag_run.conf['start_date']),
            "endDate": custom_methods.get_replicon_date(dag_run.conf['termination_date']),
        }

    login_name = dag_run.conf['first_name'] + '.' + dag_run.conf['last_name']
    return {
        "user": {
            "target": {
                "loginName": login_name,
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "employeeId": dag_run.conf['employee_number'],
            "department": {
                "name": "Company"
            },
            "employmentDateRange": get_employment_dates(),
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": "Shift Schedule"
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                    },
                    "effectiveDate": null
                }
            ],
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "1",
                "loginName": login_name,
                "SSOName": login_name,
                "password": null
            },
            "policySets": [
                {
                    "name": "Time Off - User"
                }
            ],
            "employeeType": {
                "uri": null,
                "name": dag_run.conf['user_person_type']
            },
            "permissionSets": [
                {
                    "uri": rail.find_first_by_attr_and_get_attr(
                        dag_run.conf['permission_sets'], 'name', custom_methods.get_permission_set_name(dag_run.conf['user_person_type']), 'uri')
                }
            ],

            "timeOffApprovalPath": {
                "uri": null,
                "name": "Supervisor"
            },
            "customFieldValues": get_custom_fields(dag_run),
            "serviceCenterSchedule": [
                {
                    "serviceCenter": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                            dag_run.conf['service_centers'], 'displayText', dag_run.conf['user_person_type'], 'uri')
                    },
                    "effectiveDate": custom_methods.get_replicon_date(dag_run.conf['start_date'])
                }
            ],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": []
        }
    }


def get_timeOffPolicies_by_time_off_type(dag_run, timeoff_name):

    effective_date, balance = custom_methods.get_effective_date_balance(
        dag_run, timeoff_name)
    if not effective_date or not balance:
        return None
    time_off_details = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_timeoffs'), 'name', timeoff_name)
    is_hours = "hours" == time_off_details['is_day_or_hour']

    return {
        "timeOffType": {
            "uri": time_off_details['uri']
        },
        "isTimeOffAllowedAgainstThisTimeOffType": "1",
        "policySchedule": [
            {
                "effectiveDate": custom_methods.get_replicon_date(effective_date),
                "initialBalancePolicy": {
                    "initialBalanceOption": "urn:replicon:time-off-policy-initial-balance-option:reset-balance-to-specific-value",
                    "initialBalanceDuration": {
                        "durationInCalendarDays": custom_methods.get_duration_calendar_days(balance) if is_hours else null,
                        "durationInWorkdays": {
                            "workdays": "0",
                            "hours": "0",
                            "minutes": "0",
                            "decimalWorkdays": balance
                        } if not is_hours else null
                    }
                }
            }
        ]
    }


def get_put_timeoff_payload(dag_run):
    timeoffs_to_assign = rail.result("get_timeoffs_to_assign_from_mapper")

    timeoff_policies = []
    for timeoff_type in timeoffs_to_assign:
        timeoff_policies.append(
            get_timeOffPolicies_by_time_off_type(dag_run, timeoff_type))

    timeoff_policies = list(filter(None, timeoff_policies))
    return {
        "userUri": rail.result('create_user')['uri'],
        "policy": {
            "bankedTimePolicy": {
                "isAllowedToBankTime": "0",
                "bankedTimePolicySchedule": []
            },
            "timeOffPoliciesByTimeOffType": timeoff_policies
        }
    }


def get_assign_timeoff_payload(dag_run, is_create=True):
    timeoffs_to_assign = rail.result("get_timeoffs_to_assign_from_mapper") or rail.result(
        'get_new_timeoff_types_to_assign')
    return {
        "userUri": rail.result("create_user")['uri'] if is_create else dag_run.conf['user_uri'],
        "timeOffTypeUris": list(map(lambda timeoff: timeoff['uri'],
                                    filter(
                                        lambda timeoff_type: timeoff_type['name'] in timeoffs_to_assign, rail.result('get_all_timeoffs'))
                                    )
                                )
    }


def get_create_add_contract_type_payload():
    contact_type_to_be_added = rail.load_all_records(
        rail.result('query_contract_types_not_present_in_replicon'))
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

    for item in contact_type_to_be_added:
        customFieldDropDownOptionUris.append({
            "target": null,
            "name": item['contract_type'],
            "isEnabled": 1
        })

    if not customFieldDropDownOptionUris:
        raise Exception("values missing")

    return customFieldDropDownOptionUris
