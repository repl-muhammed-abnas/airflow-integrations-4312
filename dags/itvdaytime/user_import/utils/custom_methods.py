from datetime import date, datetime
import ast
import json
import os
import rail
from airflow.models import Variable

DATE_FORMAT = "%Y/%m/%d"


def get_today_date():
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def do_has_file_content():
    with rail.existing_artifact(rail.result('decrypt_file')) as artifact:
        return os.path.getsize(artifact.local_filename) > 0


def get_replicon_date(date_value, date_value_format=DATE_FORMAT):
    if not date_value:
        return None

    date_value = datetime.strptime(date_value, date_value_format)
    return{
        "year": date_value.year,
        "month": date_value.month,
        "day": date_value.day
    }


def get_permission_set_name(key):
    mapping = {
        "Freelance": "Standard User - Freelance",
        "Staff": "Standard User - Staff"
    }
    return mapping.get(key)


def get_job_role_timeoff_mapper(config):
    mapper = Variable.get(key=config.job_role_timeoff_mapper, default_var=None)

    if not mapper:
        raise Exception(
            "No mapper table ``job_role_timeoff_mapper`` found under variables")

    parsed_mapper = ast.literal_eval(mapper)

    if not isinstance(parsed_mapper, dict):
        raise Exception(f" Excepted a dict class got {type(parsed_mapper)}")

    if not parsed_mapper:
        raise Exception("No value found in the job_role_timeoff_mapper mapper")

    return parsed_mapper


def get_custom_field_value(data, search_value, pluck_key="text"):
    if not data:
        return ""
    return rail.find_first_by_attr_and_get_attr(data, "customField.displayText", search_value, pluck_key)


def get_timeoffs_to_assign_from_mapper(config, dag_run):
    job_role = dag_run.conf['job_role'] if dag_run.conf['job_role'] else "NONE"
    return get_job_role_timeoff_mapper(config).get(job_role, [])


def get_date(date_value, date_value_format, required_format='%Y/%m/%d'):
    if not date_value:
        return ""
    if not date_value_format:
        raise Exception("format is Required")
    if date_value_format == "json":
        return date(date_value['year'], date_value['month'], date_value['day']).strftime(required_format)
    ret_date = datetime.strptime(date_value, date_value_format)
    if required_format:
        return ret_date.strftime(required_format)
    return ret_date


null = None


def get_custom_field(uri, text=null, dropdown=null, date_value=null):
    return {
        "customField": {
            "uri": uri
        },
        "text": text,
        "dropDownOption": {
            "name": dropdown
        } if dropdown else null,
        "date": get_replicon_date(date_value)
    }


def get_update_user_payload(dag_run):

    user_details = rail.result("get_user_details")[0]
    custom_field_to_apply = []
    changed_fields = []

    user_custom_field_values = user_details['userDetails'].get(
        "customFieldValues")
    if dag_run.conf['assignment_number'] != get_custom_field_value(user_custom_field_values,
                                                                   "Assignment ID"):
        custom_field_to_apply.append(get_custom_field(uri=get_custom_field_value(user_custom_field_values,
                                                                                 "Assignment ID", pluck_key='customField.uri'),
                                                      text=dag_run.conf['assignment_number']))
        changed_fields.append("Assignment Number updated")

    if dag_run.conf['phone_number'] and (dag_run.conf['phone_number'] != get_custom_field_value(user_custom_field_values,
                                                                                                "Primary Contact Number")):
        custom_field_to_apply.append(get_custom_field(uri=get_custom_field_value(user_custom_field_values,
                                                                                 "Primary Contact Number", pluck_key='customField.uri'),
                                                      text=dag_run.conf['phone_number']))
        changed_fields.append("Phone Number updated")

    if dag_run.conf['department'] != get_custom_field_value(user_custom_field_values, "Fusion Department Name"):
        custom_field_to_apply.append(get_custom_field(uri=get_custom_field_value(user_custom_field_values,
                                                                                 "Fusion Department Name", pluck_key='customField.uri'),
                                                      text=dag_run.conf['department']))
        changed_fields.append("Department updated")

    if dag_run.conf['assignment_start_date'] and dag_run.conf['assignment_start_date'] != get_date(
            date_value=get_custom_field_value(user_details['userDetails'].get(
                "customFieldValues"), "Assignment ID Enter Date", pluck_key='date'),
            date_value_format='json'):
        custom_field_to_apply.append(get_custom_field(uri=get_custom_field_value(user_custom_field_values,
                                                                                 "Assignment ID Enter Date", pluck_key='customField.uri'),
                                                      date_value=dag_run.conf['assignment_start_date']))
        changed_fields.append("Assignment Start Date updated")

    if dag_run.conf['job_role'] != get_custom_field_value(user_details['userDetails'].get(
            "customFieldValues"), "Job Tittle"):
        custom_field_to_apply.append(get_custom_field(uri=get_custom_field_value(user_custom_field_values,
                                                                                 "Job Title", pluck_key='customField.uri'),
                                                      text=dag_run.conf['job_role']))
        changed_fields.append("Job Title updated")

    if dag_run.conf['location'] != get_custom_field_value(user_details['userDetails'].get(
            "customFieldValues"), "Fusion Location"):
        custom_field_to_apply.append(get_custom_field(
            uri=get_custom_field_value(user_custom_field_values,
                                                                "Fusion Location", pluck_key='customField.uri'),
            text=dag_run.conf['location']
        ))

        changed_fields.append("Location updated")
    if dag_run.conf['contract_type'] != get_custom_field_value(user_details['userDetails'].get(
            "customFieldValues"), "Contract Type"):
        custom_field_to_apply.append(get_custom_field(
            uri=get_custom_field_value(user_custom_field_values,
                                                                "Contract Type", pluck_key='customField.uri'),
            dropdown=dag_run.conf['contract_type']
        ))

        changed_fields.append("Contact Type updated")

    def is_first_name_changed():
        if user_details['userDetails']['firstName'] != dag_run.conf['first_name']:
            return True
        return False

    def is_last_name_changed():
        if user_details['userDetails']['lastName'] != dag_run.conf['last_name']:
            return True
        return False

    def is_start_date_changed():
        if dag_run.conf['start_date'] != get_date(user_details['userDetails']['employmentDateRange'].get('startDate'), date_value_format='json'):
            return True
        return False

    def can_update_service_center():
        current_service_center = rail.result('get_effective_groups')['service_center']
        if not current_service_center or (current_service_center['displayText'] != dag_run.conf['user_person_type']):
            return True
        return False

    def can_update_user_details():
        if is_first_name_changed() or is_last_name_changed() or is_start_date_changed():
            return True
        return False
    payload = {
        "user": {
            "uri": dag_run.conf['user_uri'],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "customFieldValuesToApply": custom_field_to_apply,
            "employeeTypeToApply": {
                "name": dag_run.conf['user_person_type']
            },
            "permissionSetsToApply": {
                "permissionSetUrisToAssign": [
                    rail.find_first_by_attr_and_get_attr(
                        dag_run.conf['permission_sets'], 'name', get_permission_set_name(dag_run.conf['user_person_type']), 'uri')
                ],
                "policyUrisToRemovePermissionSet": []
            } if can_update_service_center() else null,
            "serviceCenterScheduleToApply": {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementServiceCenterSchedule": [],
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                        {
                            "serviceCenter": {
                                "uri": rail.find_first_by_attr_and_get_attr(dag_run.conf['service_centers'], 'displayText',
                                                                            dag_run.conf['user_person_type'], 'uri'),
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            } if can_update_service_center() else null,
            "securitySettingsToApply": {
                "loginName": dag_run.conf['first_name']+'.'+dag_run.conf['last_name'],
                "ssoName": dag_run.conf['first_name']+'.'+dag_run.conf['last_name'],
            } if is_last_name_changed() or is_first_name_changed() else null,
            "userDetailsToApply": {
                "firstName": dag_run.conf['first_name'] if is_first_name_changed() else null,
                "lastName": dag_run.conf['last_name'] if is_last_name_changed() else null,
                "employmentDateRange": null,
                "employmentStartDate": {
                    "date": get_replicon_date(dag_run.conf['start_date'])
                } if is_start_date_changed() else null,
                "employmentEndDate": null,
                "displayNameParameter": {
                    "displayName": dag_run.conf['first_name']+'.'+dag_run.conf['last_name']
                } if (is_first_name_changed() or is_last_name_changed()) else null
            } if can_update_user_details() else null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    if is_first_name_changed():
        changed_fields.append("First Name updated")
    if is_last_name_changed():
        changed_fields.append("Last Name updated")
    if is_start_date_changed():
        changed_fields.append("Start Date updated")
    if can_update_service_center():
        changed_fields.append("Type of person updated")

    return {
        "custom_field_to_apply": custom_field_to_apply,
        "changed_fields": ";".join(changed_fields),
        "payload": payload
    }


def get_effective_date_balance(dag_run, timeoff_name):
    effective_date = None
    balance = None
    if 'Standard' in timeoff_name:
        effective_date = dag_run.conf['ale_effective_date']
        balance = dag_run.conf['annual_leave_entitlement']
    elif 'Carryover' in timeoff_name:
        effective_date = dag_run.conf['carry_forward_effective_date']
        balance = dag_run.conf['carry_forward']
    elif 'Relish' in timeoff_name:
        effective_date = dag_run.conf['relish_start_date']
        balance = dag_run.conf['relish_purchased_holiday']
    else:
        pass
    return effective_date, balance


def get_policy_payload(effective_date, balance, is_timeoff_for_hours, initial_balance_uri, previous=False):
    return {
        "effectiveDate": effective_date,
        "initialBalancePolicy": {
            "initialBalanceOption": initial_balance_uri,
            "initialBalanceDuration": {
                "durationInCalendarDays": (balance if previous else get_duration_calendar_days(balance)) if is_timeoff_for_hours else null,
                "durationInWorkdays": {
                    "workdays": 0,
                    "hours": 0,
                    "minutes": 0,
                    "decimalWorkdays": balance
                } if not is_timeoff_for_hours else null
            }
        },
        "accrualPolicy": null,
        "resetPolicy": null,
        "balanceLimitPolicy": null
    }


def get_duration_calendar_days(balance):
    return {
        "hours": "0",
        "minutes": "0",
        "seconds": int(float(balance)*3600),
        "milliseconds": "0",
        "microseconds": "0"
    }


def is_timeoff_unit_hour(dag_run):
    if dag_run.conf['timeoff_type_details']['is_day_or_hour'] == 'hours':
        return True
    return False


def get_put_policy_payload(dag_run):

    get_current_policies = rail.result('get_specific_timeoff_policy_for_user')[
        'policySchedule']
    policies = []

    def get_balance(policy):
        if is_timeoff_unit_hour(dag_run):
            return policy['initialBalancePolicy']['initialBalanceDuration'].get("calendarDayDuration")

        return policy['initialBalancePolicy']['initialBalanceDuration'].get(
            'decimalWorkdays')

    if get_current_policies:
        for policy in get_current_policies:
            policies.append(get_policy_payload(
                effective_date=policy['effectiveDate'],
                balance=get_balance(policy),
                is_timeoff_for_hours=is_timeoff_unit_hour(dag_run),
                initial_balance_uri=policy['initialBalancePolicy'].get(
                    'initialBalanceOption'),
                previous=True
            )
            )

    policies.append(get_policy_payload(
        effective_date=get_replicon_date(dag_run.conf['effective_date']),
        balance=dag_run.conf["balance"],
        is_timeoff_for_hours=is_timeoff_unit_hour(dag_run),
        initial_balance_uri="urn:replicon:time-off-policy-initial-balance-option:reset-balance-to-specific-value")
    )

    return {
        "userUri": dag_run.conf['user_uri'],
        "policy": {
            "bankedTimePolicy": {
                "isAllowedToBankTime": "0",
                "bankedTimePolicySchedule": []
            },
            "timeOffPoliciesByTimeOffType": [
                {
                    "timeOffType": {
                        "uri": dag_run.conf['timeoff_uri']
                    },
                    "isTimeOffAllowedAgainstThisTimeOffType": "1",
                    "policySchedule": policies
                }
            ]
        }
    }


def do_format_logs():
    def can_filter_record(log):
        if log['status'].lower() == "error" and log['action'].lower() == "add" and log['user_uri']:
            return True
        if log['status'].lower() in ['success', 'exception'] and log['action'].lower() == "add":
            return True
        return False

    def get_filtered_records(logs, status):
        return list(filter(lambda log: log['status'].lower() == status, logs))

    def get_record_summary(logs):
        return {
            "success": len(get_filtered_records(logs, 'success')),
            "failed":  len(get_filtered_records(logs, 'error')),
            "exception": len(get_filtered_records(logs, "exception")),
            "new_users_added": len(list(filter(can_filter_record, logs))),
            "users_updated": len(list(filter(lambda log: log['status'].lower() in ['success', 'exception', 'error']
                                             and log['action'].lower() == "update", logs)))
        }

    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"

    master_log = json.loads(rail.result('load_master_log'))
    users = list(
        set(map(lambda x: x['properties'].get('employee_number', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for employeeid in users:
        if not employeeid:
            continue
        user_logs = list(
            filter(lambda x: x['properties'].get('employee_number', '') == employeeid and x['properties'].get('details', ''), master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                'employee_number': employeeid,
                'loginname': first['properties'].get('loginname'),
                'status': get_status(user_logs),
                'action': first['properties'].get('action'),
                'details': ",".join(list(map(lambda x: x['properties'].get('details'), user_logs))),
                'jobid': first['ecid'],
                'user_uri': first['properties']['user_uri']
            })

    return {
        "get_record_summary": get_record_summary(logs),
        "final_logs": json.dumps(logs, ensure_ascii=False)
    }
