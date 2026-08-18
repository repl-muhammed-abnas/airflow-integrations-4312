from datetime import datetime
from pendulum import now
from dateutil.relativedelta import relativedelta
from functools import lru_cache
import itertools
import json
import re
import rail

null = None


def dict_date_to_datetime(dict_date):
    return datetime.strptime(str(dict_date['day']) + "/" + str(dict_date['month']) + "/" + str(dict_date['year']), "%d/%m/%Y").date()


def get_all_service_centers_list(response):
    if not (response['rows']):
        return []

    return list(map(
        lambda item: {
            'servicecentrename': item['cells'][0]['textValue'],
            'uri': item['cells'][0]['uri'],
            'fullpath': '/'.join([x['textValue'] for x in item['cells'][2]['cellCollection']]) if item['cells'][2]['cellCollection'] else '',
            'length': len(item['cells'][2]['cellCollection']) if item['cells'][2]['cellCollection'] else 0,
            'status': item['cells'][1].get('textValue', ''),
        }, response['rows'])
    )


def get_validation_log_details(item):
    log_detail = []
    if not (item['OHRID']):
        log_detail.append("OHR ID not present in feed file")
    if not (item['LegalEntity']):
        log_detail.append("Legal Entity not present in feed file")
    if not (item['LegacyPayrollID']):
        log_detail.append("Legacy Payroll ID not present in feed file")
    log_detail = ";".join(log_detail)
    return log_detail


def get_required_customfield_values(customfield_values):
    return {
        "job_position_title": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Job/Position Title', 'text', ''),
        "hrm_sso_id": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'HRM SSO ID', 'text', ''),
        "hrm_name": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'HRM Name', 'text', ''),
        "payroll": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Payroll', 'text', ''),
        "contract_type": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Contract #', 'text', ''),
        "previous_experience": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Previous Experience', 'text', ''),
        "contract_id": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Contract ID', 'text', ''),
        "education_level": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Education Level', 'text', ''),
        "work_location": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Work Location', 'text', ''),
        "overtime_eligibility": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Overtime Eligibility', 'text', ''),
        "overwrite_policy": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'overwrite policy', 'text', ''),
        "contract_start_date": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Contract Start Date', 'text', ''),
        "contract_end_date": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Contract End Date', 'text', ''),
        "suspend_assignment_catagory": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Suspend Assignment Category', 'text', ''),
    }


def get_required_customfield_uris(customfield_values):
    return {
        "job_position_title_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Job/Position Title', 'customField.uri', ''),
        "hrm_sso_id_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'HRM SSO ID', 'customField.uri', ''),
        "hrm_name_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'HRM Name', 'customField.uri', ''),
        "payroll_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Payroll', 'customField.uri', ''),
        "contract_type_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Contract #', 'customField.uri', ''),
        "previous_experience_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Previous Experience', 'customField.uri', ''),
        "contract_id_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Contract ID', 'customField.uri', ''),
        "education_level_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Education Level', 'customField.uri', ''),
        "work_location_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Work Location', 'customField.uri', ''),
        "overtime_eligibility_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Overtime Eligibility', 'customField.uri', ''),
        "overwrite_policy_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'overwrite policy', 'customField.uri', ''),
        "contract_start_date_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Contract Start Date', 'customField.uri', ''),
        "contract_end_date_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Contract End Date', 'customField.uri', ''),
        "suspend_assignment_catagory_field_uri": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Suspend Assignment Category', 'customField.uri', ''),
    }


def get_required_custom_field_uris(response):
    return {
        "hrm_sso_id_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "HRM SSO ID", 'uri'),
        "hrm_name_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "HRM Name", 'uri'),
        "payroll_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Payroll", 'uri'),
        "contract_type_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Contract #", 'uri'),
        "contract_id_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Contract ID", 'uri'),
        "education_level_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Education Level", 'uri'),
        "work_location_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Work Location", 'uri'),
        "overwrite_policy_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "overwrite policy", 'uri'),
        "contract_start_date_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Contract Start Date", 'uri'),
        "contract_end_date_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Contract End Date", 'uri'),
        "job_position_title_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Job/Position Title", 'uri'),
        "previous_experience_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Previous Experience", 'uri'),
        "suspend_assignment_catagory_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Suspend Assignment Category", 'uri'),
        "overtime_eligibility_field_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Overtime Eligibility", 'uri'),
    }


def get_number_of_working_days(schedule_to_assign, mapper_search_result, dag_run):
    number_of_working_days = 0
    matching_schedule_in_mapper = rail.find_first_by_attr_and_get_attr(
        mapper_search_result, 'type', 'Default Schedule', 'value', '')
    if schedule_to_assign and matching_schedule_in_mapper and schedule_to_assign != matching_schedule_in_mapper:
        number_of_working_days = 5
    else:
        if dag_run.conf['DWSMonday']:
            number_of_working_days += int(dag_run.conf['DWSMonday'])
        if dag_run.conf['DWSTuesday']:
            number_of_working_days += int(dag_run.conf['DWSTuesday'])
        if dag_run.conf['DWSWednesday']:
            number_of_working_days += int(dag_run.conf['DWSWednesday'])
        if dag_run.conf['DWSThursday']:
            number_of_working_days += int(dag_run.conf['DWSThursday'])
        if dag_run.conf['DWSFriday']:
            number_of_working_days += int(dag_run.conf['DWSFriday'])
        if dag_run.conf['DWSSaturday']:
            number_of_working_days += int(dag_run.conf['DWSSaturday'])
        if dag_run.conf['DWSSunday']:
            number_of_working_days += int(dag_run.conf['DWSSunday'])

    return number_of_working_days


def check_name_email_changes(dag_run, current_firstname, current_lastname, current_email):
    userDetailsToApply_payload = {}
    log_message = []
    if dag_run.conf['EmployeeFirstName'] and current_firstname.lower() != dag_run.conf['EmployeeFirstName'].lower():
        userDetailsToApply_payload.update({
            "firstName": dag_run.conf['EmployeeFirstName']
        })
        log_message.append('First name updated')

    if dag_run.conf['EmployeeLastName'] and current_lastname.lower() != dag_run.conf['EmployeeLastName'].lower():
        userDetailsToApply_payload.update({
            "lastName": dag_run.conf['EmployeeLastName']
        })
        log_message.append('Last name updated')

    if dag_run.conf['EmployeeEmailAddress'] and current_email != dag_run.conf['EmployeeEmailAddress']:
        userDetailsToApply_payload.update({
            "emailAddress": {"emailAddress": dag_run.conf['EmployeeEmailAddress']}
        })
        log_message.append('Email updated')

    return {
        "applyusermodifications_payload": {
            "user": {
                "uri": dag_run.conf['useruri']
            },
            "modifications": {
                "userDetailsToApply": userDetailsToApply_payload
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        },
        "log_message": ';'.join(log_message)
    }


def get_customfield_values_change_payload_logs(field_uris, existing_customfield_values, default_date_format, dag_run):
    user_customfield_modifications_payload = []
    log_message = []
    time_off_trigger = 'no'
    time_off_trigger_type = 'update'
    overwrite_policy = 'no'

    if dag_run.conf['JobPositionTitle'] and existing_customfield_values['job_position_title'] != dag_run.conf['JobPositionTitle']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['job_position_title_field_uri']
            },
            "text": dag_run.conf['JobPositionTitle']
        })
        log_message.append("Job/Position Title updated")

    if dag_run.conf['HRMSSOID'] and existing_customfield_values['hrm_sso_id'] != dag_run.conf['HRMSSOID']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['hrm_sso_id_field_uri']
            },
            "text": dag_run.conf['HRMSSOID']
        })
        log_message.append("HRMSSOID updated")

    if dag_run.conf['HRMName'] and existing_customfield_values['hrm_name'] != dag_run.conf['HRMName']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['hrm_name_field_uri']
            },
            "text": dag_run.conf['HRMName']
        })
        log_message.append("HRM Name updated")

    if dag_run.conf['Payroll'] and existing_customfield_values['payroll'] != dag_run.conf['Payroll']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['payroll_field_uri']
            },
            "text": dag_run.conf['Payroll']
        })
        log_message.append("Payroll UDF updated")

    if dag_run.conf['ContractType'] and existing_customfield_values['contract_type'] != dag_run.conf['ContractType']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['contract_type_field_uri']
            },
            "text": dag_run.conf['ContractType']
        })
        log_message.append("Contract type UDF updated")
        time_off_trigger = "yes"

    if dag_run.conf['PreviousExperience'] and existing_customfield_values['previous_experience'] != dag_run.conf['PreviousExperience']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['previous_experience_field_uri']
            },
            "text": dag_run.conf['PreviousExperience']
        })
        log_message.append("Previous Experience UDF updated")
        time_off_trigger = "yes"
        if not (existing_customfield_values['previous_experience']):
            time_off_trigger_type = 'add'

    if dag_run.conf['ContractID'] and existing_customfield_values['contract_id'] != dag_run.conf['ContractID']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['contract_id_field_uri']
            },
            "text": dag_run.conf['ContractID']
        })
        log_message.append("Contract ID UDF updated")

    if dag_run.conf['EducationLevel'] and existing_customfield_values['education_level'] != dag_run.conf['EducationLevel']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['education_level_field_uri']
            },
            "text": dag_run.conf['EducationLevel']
        })
        log_message.append("Education Level UDF updated")
        time_off_trigger = "yes"

    if dag_run.conf['LocationName'] and existing_customfield_values['work_location'] != dag_run.conf['LocationName']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['work_location_field_uri']
            },
            "text": dag_run.conf['LocationName']
        })
        log_message.append("Work Location UDF updated")

    if not (dag_run.conf['PreviousExperience']):
        overwrite_policy = 'yes'

    if existing_customfield_values['overwrite_policy'].lower() != overwrite_policy:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['overwrite_policy_field_uri']
            },
            "text": overwrite_policy
        })
        log_message.append("Overwrite Policy UDF updated")

    if dag_run.conf['RadiationFlag'] and (existing_customfield_values['contract_start_date'] or '') != dag_run.conf['RadiationFlag']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['contract_start_date_field_uri']
            },
            "date": rail.parse_date(dag_run.conf['RadiationFlag'], default_date_format)
        })
        log_message.append("Contract start date updated")
        time_off_trigger = "yes"

    if dag_run.conf['PositionCapacity'] and (existing_customfield_values['contract_end_date'] or '') != dag_run.conf['PositionCapacity']:
        user_customfield_modifications_payload.append({
            "customField": {
                "uri": field_uris['contract_end_date_field_uri']
            },
            "date": rail.parse_date(dag_run.conf['PositionCapacity'], default_date_format)
        })
        log_message.append("Contract end date updated")
        time_off_trigger = "yes"
        time_off_trigger_type = 'add'

    return {
        'user_modifications_payload': {
            "user": {
                "uri": dag_run.conf['useruri']
            },
            "modifications": {
                "customFieldValuesToApply": user_customfield_modifications_payload
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        },
        'log_message': log_message,
        'time_off_trigger': time_off_trigger,
        'time_off_trigger_type': time_off_trigger_type,
        'overwrite_policy': overwrite_policy,
    }


def get_user_custom_fields_values_payload_and_exceptions(custom_field_uris, default_date_format, dag_run):
    user_customfield_values_add_payload = []
    exceptions = []
    overwrite_policy = 'no'

    if dag_run.conf['HRMSSOID']:
        if not (custom_field_uris['hrm_sso_id_field_uri']):
            exceptions.append('HRM SSO ID udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['hrm_sso_id_field_uri']
                },
                "text": dag_run.conf['HRMSSOID']
            })

    if dag_run.conf['HRMName']:
        if not (custom_field_uris['hrm_name_field_uri']):
            exceptions.append('HRM Name udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['hrm_name_field_uri']
                },
                "text": dag_run.conf['HRMName']
            })

    if dag_run.conf['Payroll']:
        if not (custom_field_uris['payroll_field_uri']):
            exceptions.append('Payroll udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['payroll_field_uri']
                },
                "text": dag_run.conf['Payroll']
            })

    if dag_run.conf['ContractType']:
        if not (custom_field_uris['contract_type_field_uri']):
            exceptions.append('Contract # udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['contract_type_field_uri']
                },
                "text": dag_run.conf['ContractType']
            })

    if dag_run.conf['ContractID']:
        if not (custom_field_uris['contract_id_field_uri']):
            exceptions.append('Contract ID udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['contract_id_field_uri']
                },
                "text": dag_run.conf['ContractID']
            })

    if dag_run.conf['EducationLevel']:
        if not (custom_field_uris['education_level_field_uri']):
            exceptions.append('Education Level udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['education_level_field_uri']
                },
                "text": dag_run.conf['EducationLevel']
            })

    if dag_run.conf['LocationName']:
        if not (custom_field_uris['work_location_field_uri']):
            exceptions.append('Work Location udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['work_location_field_uri']
                },
                "text": dag_run.conf['LocationName']
            })

    if not (dag_run.conf['EducationLevel']) and not (dag_run.conf['previousemployment']):
        overwrite_policy = 'yes'

    if dag_run.conf['EducationLevel'] and not (dag_run.conf['previousemployment']):
        overwrite_policy = 'yes'

    if not (custom_field_uris['overwrite_policy_field_uri']):
        exceptions.append('overwrite policy UDF is not available')
    else:
        user_customfield_values_add_payload.append({
            "customField": {
                "uri": custom_field_uris['overwrite_policy_field_uri']
            },
            "text": overwrite_policy
        })

    if dag_run.conf['RadiationFlag']:
        if not (custom_field_uris['contract_start_date_field_uri']):
            exceptions.append(
                'Contract Start Date udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['contract_start_date_field_uri']
                },
                "date": rail.parse_date(dag_run.conf['RadiationFlag'], default_date_format)
            })

    if dag_run.conf['PositionCapacity']:
        if not (custom_field_uris['contract_end_date_field_uri']):
            exceptions.append(
                'Contract End Date udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['contract_end_date_field_uri']
                },
                "date": rail.parse_date(dag_run.conf['PositionCapacity'], default_date_format)
            })

    if dag_run.conf['JobPositionTitle']:
        if not (custom_field_uris['job_position_title_field_uri']):
            exceptions.append(
                'Job/Position Title udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['job_position_title_field_uri']
                },
                "text": dag_run.conf['JobPositionTitle']
            })

    if dag_run.conf['previousemployment']:
        if not (custom_field_uris['previous_experience_field_uri']):
            exceptions.append(
                'Previous Experience udf is not available')
        else:
            user_customfield_values_add_payload.append({
                "customField": {
                    "uri": custom_field_uris['previous_experience_field_uri']
                },
                "text": dag_run.conf['previousemployment']
            })

    return {
        'custom_field_value_add_payload': user_customfield_values_add_payload,
        'exceptions': exceptions
    }


def get_missing_user_permissions(permissions_to_assign_from_mapper, response):
    final_user_permissions_to_assign = []

    for permission_uri in permissions_to_assign_from_mapper:
        if not (response) or not rail.find_first_by_attr_and_get_attr(response, 'permissionSet.uri', permission_uri):
            final_user_permissions_to_assign.append(permission_uri)

    return final_user_permissions_to_assign


def check_schedule_change(dag_run):
    if int(dag_run.conf['DWSMonday']) == 0 and int(
        dag_run.conf['DWSTuesday']) == 0 and int(
            dag_run.conf['DWSWednesday']) == 0 and int(
                dag_run.conf['DWSThursday']) == 0 and int(
                    dag_run.conf['DWSFriday']) == 0 and int(
                        dag_run.conf['DWSSaturday']) == 0 and int(
                            dag_run.conf['DWSSunday']) == 0:
        return False

    if not (dag_run.conf['DWSMonday']) or not (
        dag_run.conf['DWSTuesday']) or not (
            dag_run.conf['DWSWednesday']) or not (
                dag_run.conf['DWSThursday']) or not (
                    dag_run.conf['DWSFriday']) or not (
                        dag_run.conf['DWSSaturday']) or not (
                            dag_run.conf['DWSSunday']):
        return False

    return True


def get_previous_experience(dag_run):
    years = 0
    months = 0
    days = 0
    if dag_run.conf['PreviousExperience']:
        if dag_run.conf['PreviousExperience'].split(","):
            years = re.sub(
                r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[0]).strip()
            months = re.sub(
                r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[1]).strip()
            days = re.sub(
                r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[2]).strip()
            if years:
                years += int(years)
            if months:
                months += int(months)
            if years:
                days += int(days)
    return {
        "years": years,
        "months": months,
        "days": days
    }


def get_current_officescedule_name(user_office_schedule, date_format, dag_run):
    current_schedule = null
    initial_schedule = null
    current_min_day_diff = "*"
    if 'urn' in json.dumps(user_office_schedule):
        for item in user_office_schedule:

            if not item['effectiveDate']:
                initial_schedule = item
                continue

            daydiff = (datetime.strptime(
                dag_run.conf['DWSStartDate'] or dag_run.conf['integration_run_date'], date_format).date()) - dict_date_to_datetime(item['effectiveDate'])

            # ignore the future ones
            if daydiff.days < 0:
                continue

            if current_min_day_diff == "*":
                current_schedule = item
                current_min_day_diff = daydiff
                continue

            if current_min_day_diff > daydiff:
                current_min_day_diff = daydiff
                current_schedule = item

    return {
        "current_schedule_name": current_schedule['officeSchedule']['displayText'] if current_schedule else (
            initial_schedule['officeSchedule']['displayText'] if initial_schedule else ''),
        "current_schedule_uri": current_schedule['officeSchedule']['uri'] if current_schedule else (
            initial_schedule['officeSchedule']['uri'] if initial_schedule else ''),
    }


def get_mapper_entry(dag_run, POLAND_MASTER_MAPPER):
    licences = list(filter(
        lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "License", POLAND_MASTER_MAPPER))
    required_supervisor_permission = list(filter(
        lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Permission" and (
            x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == "Supervisor"), POLAND_MASTER_MAPPER))

    return {
        "timesheetperiod": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timesheet Period", POLAND_MASTER_MAPPER)), {}).get(
                'default__uri', ''),
        "timeofftemplate": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timeoff Template", POLAND_MASTER_MAPPER)), {}).get(
                'value', ''),
        "timeoffapprovalpath": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timeoff Approval Path", POLAND_MASTER_MAPPER)), {}).get(
                'value', ''),
        "timesheetapprovalpath": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timesheet Approval Path", POLAND_MASTER_MAPPER)), {}).get(
                'value', ''),
        "payrulename": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Payrule", POLAND_MASTER_MAPPER)), {}).get(
                'value', ''),
        "holidaycalendar": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Holiday Calendar", POLAND_MASTER_MAPPER)), {}).get(
                'value', ''),
        "workweek": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Work Week", POLAND_MASTER_MAPPER)), {}).get(
                'default__uri', ''),
        "authenticationtype": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Authentication type", POLAND_MASTER_MAPPER)), {}).get(
                'default__uri', ''),
        "permissionsets": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Permission" and (
                x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == "User"), POLAND_MASTER_MAPPER)), {}).get('value', ''),
        "legalentityname": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Legal Entity", POLAND_MASTER_MAPPER)), {}).get(
                'value', ''),
        "location": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Location", POLAND_MASTER_MAPPER)), {}).get(
                'value', ''),
        "timesheettemplate": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timesheet Template", POLAND_MASTER_MAPPER)), {}).get(
                'value', ''),
        "licence": [license['default__uri'] for license in licences],
        "language": next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Language", POLAND_MASTER_MAPPER)), {}).get(
                'default__uri', ''),
        "required_supervisor_permission": required_supervisor_permission if required_supervisor_permission else []
    }


def get_create_user_payload(dag_run, DATE_DEFAULT_FORMAT):
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['OHRID'],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['EmployeeFirstName'],
            "lastname": dag_run.conf['EmployeeLastName'],
            "emailAddress": dag_run.conf['EmployeeEmailAddress'],
            "employeeId": dag_run.conf['OHRID'],
            "department": {
                "uri": dag_run.conf['Departmenturi'],
                "name": null,
                "parent": null,
                "parameterCorrelationId": null
            },
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": rail.result('log_required_mapper_entries_19_29')['workweek'],
            "employmentDateRange": {
                "startDate": rail.parse_date(dag_run.conf['LegalEntityHireDate'] or dag_run.conf['HireEffectiveDate'], DATE_DEFAULT_FORMAT),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    rail.result('log_required_mapper_entries_19_29')[
                        'authenticationtype']
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['OHRID'],
                "SSOName": dag_run.conf['OHRID'],
                "password": null
            },
            "holidayCalendar": {
                "uri": null,
                "name": rail.result('log_required_mapper_entries_19_29')['holidaycalendar']
            },
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": null,
                    "name": rail.result('log_required_mapper_entries_19_29')['permissionsets']
                }
            ],
            "policySets": [
                {
                    "uri": null,
                    "name": rail.result('log_required_mapper_entries_19_29')['timeofftemplate']
                }
            ],
            "employeeType": {
                "uri": null,
                "name": rail.result('log_employee_type_name_from_mapper_12')
            },
            "timesheetPeriodTypeUri": rail.result('log_required_mapper_entries_19_29')['timesheetperiod'],
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {
                "uri": null,
                "name": rail.result('log_required_mapper_entries_19_29')['timesheetapprovalpath']
            },
            "expenseApprovalPath": null,
            "timeOffApprovalPath": {
                "uri": null,
                "name": rail.result('log_required_mapper_entries_19_29')['timeoffapprovalpath']
            },
            "customFieldValues": [],
            "assignedActivities": [{
                "uri": null,
                "name": "On-Duty"
            }],
            "timeZone": null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": null,
                        "parentUri": null,
                        "name": rail.result('log_required_mapper_entries_19_29')['location']
                    },
                    "effectiveDate": null
                }
            ],
            "divisionSchedule": [],
            "costCenterSchedule": [
                {
                    "costCenter": {
                        "uri": null,
                        "parentUri": null,
                        "name": rail.result('log_required_mapper_entries_19_29')['legalentityname']
                    },
                    "effectiveDate": null
                }
            ],
            "serviceCenterSchedule": [
                {
                    "serviceCenter": {
                        "uri": dag_run.conf['servicecenteruri_hrmssoid'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['servicecenteruri_hrmssoid'] else [],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": [],
            "timesheetPeriodSchedule": [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [
                {
                    "payRuleScript": {
                        "uri": null,
                        "name": rail.result('log_required_mapper_entries_19_29')['payrulename']
                    },
                    "effectiveDate": null
                }
            ] if rail.result('log_required_mapper_entries_19_29')['payrulename'] else []
        }
    }


def weekly_work_hours_and_schedule_to_assign(schedule_to_assign_var, matching_mapper_entries, dag_run):
    schedule_to_assign = schedule_to_assign_var
    weekly_work_hours = 0

    if int(dag_run.conf['DWSMonday']) == 0 and int(
        dag_run.conf['DWSTuesday']) == 0 and int(
            dag_run.conf['DWSWednesday']) == 0 and int(
                dag_run.conf['DWSThursday']) == 0 and int(
                    dag_run.conf['DWSFriday']) == 0 and int(
                        dag_run.conf['DWSSaturday']) == 0 and int(
                            dag_run.conf['DWSSunday']) == 0:
        schedule_to_assign = rail.find_first_by_attr_and_get_attr(
            matching_mapper_entries, 'type', 'Default Schedule', 'value', '')
        weekly_work_hours = 40

    if not (dag_run.conf['DWSMonday']) or not (
        dag_run.conf['DWSTuesday']) or not (
            dag_run.conf['DWSWednesday']) or not (
                dag_run.conf['DWSThursday']) or not (
                    dag_run.conf['DWSFriday']) or not (
                        dag_run.conf['DWSSaturday']) or not (
                            dag_run.conf['DWSSunday']):
        schedule_to_assign = rail.find_first_by_attr_and_get_attr(
            matching_mapper_entries, 'type', 'Default Schedule', 'value', '')
        weekly_work_hours = 40

    if int(dag_run.conf['DWSMonday']) > 0:
        weekly_work_hours += int(dag_run.conf['DWSMonday'])
    if int(dag_run.conf['DWSTuesday']) > 0:
        weekly_work_hours += int(dag_run.conf['DWSTuesday'])
    if int(dag_run.conf['DWSWednesday']) > 0:
        weekly_work_hours += int(dag_run.conf['DWSWednesday'])
    if int(dag_run.conf['DWSThursday']) > 0:
        weekly_work_hours += int(dag_run.conf['DWSThursday'])
    if int(dag_run.conf['DWSFriday']) > 0:
        weekly_work_hours += int(dag_run.conf['DWSFriday'])
    if int(dag_run.conf['DWSSaturday']) > 0:
        weekly_work_hours += int(dag_run.conf['DWSSaturday'])
    if int(dag_run.conf['DWSSunday']) > 0:
        weekly_work_hours += int(dag_run.conf['DWSSunday'])

    return {
        'schedule_to_assign': schedule_to_assign,
        'weekly_work_hours': weekly_work_hours
    }


def get_status_type_and_details_for_logs(exception_log, dag_run):
    exception_entries = rail.load_all_records(exception_log)
    final_exceptions = [entry['properties']['details']
                        for entry in exception_entries]
    if final_exceptions:
        if dag_run.conf['type'] == 'add':
            return {
                'status': "Exception",
                'details': "User (New) partially created, " + ", ".join(final_exceptions)
            }
        if dag_run.conf['type'] == 'rehire':
            return {
                'status': "Exception",
                'details': "User (Rehire) partially created, " + ", ".join(final_exceptions)
            }

    else:
        if dag_run.conf['type'] == 'rehire':
            return {
                'status': "Success",
                'details': "User (Rehire) successfully created"
            }

    return {
        'status': "Success",
        'details': "User (New) successfully created"
    }


def get_supervisor_details(dag_run):
    return {
        'first_name': dag_run.conf['supervisorname'].split(" ")[0],
        'last_name': " ".join(dag_run.conf['supervisorname'].split(' ')[1:]).strip(),
        'email': dag_run.conf['supervisorloginname'] + "@mail.ad.ge.com"
    }


def get_all_values_to_create_foreign_supervisors(mapper_search_result, dag_run):
    permission_sets_mapper_search = list(filter(
        lambda x: x["type"] == "Permission" and x["identifier__1__(_legal_entity_code/_type/_timeoff_type)"] == 'Supervisor', mapper_search_result))
    permission_set_to_apply = []
    for entry in permission_sets_mapper_search:
        permission_set_to_apply.append({
            'uri': null,
            'name': entry['value']
        })
    return {
        'location': rail.find_first_by_attr_and_get_attr(mapper_search_result, 'type', 'Location', 'value', ''),
        'authentication_type': rail.find_first_by_attr_and_get_attr(mapper_search_result, 'type', 'Authentication Type', 'default__uri', ''),
        'language': rail.find_first_by_attr_and_get_attr(mapper_search_result, 'type', 'Language', 'default__uri', ''),
        'employee_type': rail.find_first_by_attr_and_get_attr(mapper_search_result, 'type', 'Employee Type', 'value', ''),
        'required_licences': [entry['default__uri'] for entry in filter(lambda x: x['type'] == 'License', mapper_search_result)],
        'required_office_default_schedule': rail.find_first_by_attr_and_get_attr(mapper_search_result, 'type', 'Default Schedule', 'value', ''),
        'permission_set_to_apply': permission_set_to_apply
    }


def create_foreign_supervisor_conf(dag_run):
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['supervisorloginname'],
                "parameterCorrelationId": null
            },
            "firstname": rail.result('log_supervisor_details_6_8')['first_name'],
            "lastname": rail.result('log_supervisor_details_6_8')['last_name'],
            "emailAddress": rail.result('log_supervisor_details_6_8')['email'],
            "employeeId": dag_run.conf['supervisorloginname'],
            "department": {
                "uri": dag_run.conf['foreignsupervisordepartmenturi'],
                "name": null,
                "parent": null,
                "parameterCorrelationId": null
            },
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": rail.result('log_get_all_values_to_create_foreign_supervisors_10_20')['required_office_default_schedule'],
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": rail.result('log_get_all_values_to_create_foreign_supervisors_10_20')['required_office_default_schedule']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ],
            "workWeekStartDayUri": null,
            "employmentDateRange": null,
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    rail.result('log_get_all_values_to_create_foreign_supervisors_10_20')[
                        'authentication_type']
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['supervisorloginname'],
                "SSOName": dag_run.conf['supervisorloginname'],
                "password": null
            },
            "holidayCalendar": null,
            "timeOffPolicy": null,
            "permissionSets": rail.result('log_get_all_values_to_create_foreign_supervisors_10_20')['permission_set_to_apply'],
            "policySets": [],
            "employeeType": {
                "uri": null,
                "name": rail.result('log_get_all_values_to_create_foreign_supervisors_10_20')['employee_type']
            },
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": null,
            "expenseApprovalPath": null,
            "timeOffApprovalPath": null,
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": null,
                        "parentUri": null,
                        "name": rail.result('log_get_all_values_to_create_foreign_supervisors_10_20')['location']
                    },
                    "effectiveDate": null
                }
            ],
            "divisionSchedule": [],
            "costCenterSchedule": [],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": [],
            "timesheetPeriodSchedule": [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": []
        }
    }


def get_supervisor_status(status, dag_run):
    log_supervisor_not_present = rail.result('search_users_3') != []
    log_supervisor_disabled = rail.result(
        'log_errorwhensupervisorisdisabled_12')
    log_supervisorname_equals_userloginname = dag_run.conf[
        'supervisorloginname'] == dag_run.conf['loginname']

    if status == 'Error':
        return 'Error'

    if status == 'Skipped' or status == 'Exception':
        if bool(log_supervisorname_equals_userloginname):
            return 'Error'
        if bool(log_supervisor_not_present) or bool(log_supervisor_disabled):
            return 'Exception'

    return status


def get_log_details_post_supervisor_assignment(details, dag_run):
    log_supervisor_not_present = rail.result('search_users_3') != []
    log_supervisor_disabled = rail.result(
        'log_errorwhensupervisorisdisabled_12')
    log_supervisorname_equals_userloginname = dag_run.conf[
        'supervisorloginname'] == dag_run.conf['loginname']

    if bool(log_supervisor_not_present):
        return details + rail.render_template(';Supervisor "{{ dag_run.conf.supervisorloginname }}" not available')

    if bool(log_supervisorname_equals_userloginname):
        return details + ';Supervisor not assigned since the user and supervisor SSO ID are same'

    if bool(log_supervisor_disabled):
        return details + str(log_supervisor_disabled)

    return details + ';Supervisor added/updated successfully'


def sort_updates_exceptions_logs(exception_log, update_log):
    exception_entries = rail.load_all_records(exception_log)
    update_entries = rail.load_all_records(update_log)
    final_exceptions = [entry['properties']['details']
                        for entry in exception_entries]
    final_updates = [entry['properties']['details']
                     for entry in update_entries]
    if final_exceptions:
        return {
            'status': "Exception",
            'details': "Partially updated " + ";".join(final_exceptions)
        }
    if final_updates:
        return {
            'status': "Success",
            'details': "Successfully updated"
        }
    return {
        'status': "Skipped",
        'details': "No change to the user record in Replicon"
    }


@lru_cache(maxsize=32)
def load_all_input_records():
    return rail.load_all_records(rail.result('query_inputfilerawdata_for_records'))


def get_schedules_to_assign(all_office_schedules, mapper_search_result):
    input_records = load_all_input_records()
    schedules_to_assign_list = []
    for item in input_records:
        schedule_to_assign = str(item['DWSMonday'] + '|' + item['DWSTuesday'] + '|' + item['DWSWednesday'] + '|' +
                                 item['DWSThursday'] + '|' + item['DWSFriday'] + '|' + item['DWSSaturday'] + '|' + item['DWSSunday'])

        if not (item['DWSMonday']) or not (
            item['DWSTuesday']) or not (
                item['DWSWednesday']) or not (
                    item['DWSThursday']) or not (
                        item['DWSFriday']) or not (
                            item['DWSSaturday']) or not (
                                item['DWSSunday']):
            schedule_to_assign = rail.find_first_by_attr_and_get_attr(
                mapper_search_result, 'type', 'Default Schedule', 'value', '')

        elif int(float(item['DWSMonday'])) == 0 and int(float(
            item['DWSTuesday'])) == 0 and int(float(
                item['DWSWednesday'])) == 0 and int(float(
                    item['DWSThursday'])) == 0 and int(float(
                        item['DWSFriday'])) == 0 and int(float(
                            item['DWSSaturday'])) == 0 and int(float(
                                item['DWSSunday'])) == 0:
            schedule_to_assign = rail.find_first_by_attr_and_get_attr(
                mapper_search_result, 'type', 'Default Schedule', 'value', '')
        schedules_to_assign_list.append({
            'schedulename': schedule_to_assign,
            'scheduleuri': rail.find_first_by_attr_and_get_attr(all_office_schedules, 'displayText', schedule_to_assign, 'uri', '')
        })

    return schedules_to_assign_list


def get_minutes_for_weekdays(dag_run):
    minutes_for_monday = (int(float(dag_run.conf['monday']))*60) + (
        (int(dag_run.conf['monday'].split('.')[1]))*6 if len(dag_run.conf['monday'].split('.')) > 1 else 0)
    minutes_for_tuesday = (int(float(dag_run.conf['tuesday']))*60) + (
        (int(dag_run.conf['tuesday'].split('.')[1]))*6 if len(dag_run.conf['tuesday'].split('.')) > 1 else 0)
    minutes_for_wednesday = (int(float(dag_run.conf['wednesday']))*60) + (
        (int(dag_run.conf['wednesday'].split('.')[1]))*6 if len(dag_run.conf['wednesday'].split('.')) > 1 else 0)
    minutes_for_thursday = (int(float(dag_run.conf['thursday']))*60) + (
        (int(dag_run.conf['thursday'].split('.')[1]))*6 if len(dag_run.conf['thursday'].split('.')) > 1 else 0)
    minutes_for_friday = (int(float(dag_run.conf['friday']))*60) + (
        (int(dag_run.conf['friday'].split('.')[1]))*6 if len(dag_run.conf['friday'].split('.')) > 1 else 0)
    minutes_for_saturday = (int(float(dag_run.conf['saturday']))*60) + (
        (int(dag_run.conf['saturday'].split('.')[1]))*6 if len(dag_run.conf['saturday'].split('.')) > 1 else 0)
    minutes_for_sunday = (int(float(dag_run.conf['sunday']))*60) + (
        (int(dag_run.conf['sunday'].split('.')[1]))*6 if len(dag_run.conf['sunday'].split('.')) > 1 else 0)
    return {
        'minutes_for_monday': minutes_for_monday,
        'minutes_for_tuesday': minutes_for_tuesday,
        'minutes_for_wednesday': minutes_for_wednesday,
        'minutes_for_thursday': minutes_for_thursday,
        'minutes_for_friday': minutes_for_friday,
        'minutes_for_saturday': minutes_for_saturday,
        'minutes_for_sunday': minutes_for_sunday
    }


def get_required_value_from_user_policyset(policyset, scipt_name, key_uri, value_type):
    for x in policyset['timeOffBalanceEventScripts']:
        if x['script']['name'] == scipt_name:
            for y in x['additionalParameters']:
                if y['keyUri'] == key_uri:
                    return y['value'][value_type]
    return null


def ordinalize(n):
    suffixes = {1: 'st', 2: 'nd', 3: 'rd'}
    if 10 <= n % 100 <= 20:  # Special case for 10<n<20
        return str(n) + 'th'
    return str(n) + suffixes.get(n % 10, 'th')


def get_required_scripts_balances(timeoff_policy_schedule_for_user, date_format, dag_run):
    policyset = timeoff_policy_schedule_for_user[0]['policySet']

    existing_annual_accrual_amount = get_required_value_from_user_policyset(
        policyset, 'Yearly Accrual', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'number')
    existing_annual_accrual_param_to_gsub = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": existing_annual_accrual_amount}})

    existing_accrual_month_uri = get_required_value_from_user_policyset(
        policyset, 'Yearly Accrual', 'urn:replicon:script-key:parameter:accrue-on-month', 'uri')
    existing_accrual_month_param_to_gsub = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": existing_accrual_month_uri}})
    required_accrual_month_uri = "urn:replicon:month:" + str(
        datetime.strptime(dag_run.conf['effective_date_10_years'], date_format).strftime("%B")).lower()
    required_accrual_month_param_for_gsub = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": required_accrual_month_uri}})

    existing_accrual_day_uri = get_required_value_from_user_policyset(
        policyset, 'Yearly Accrual', 'urn:replicon:script-key:parameter:accrue-on-day-of-month', 'uri')
    existing_accrual_day_param_to_gsub = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": existing_accrual_day_uri}})
    required_accual_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + ordinalize(int(
        datetime.strptime(dag_run.conf['effective_date_10_years'], date_format).strftime("%d")))
    required_accual_day_param_for_gsub = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": required_accual_day_uri}})

    existing_starting_balance = get_required_value_from_user_policyset(
        policyset, 'Starting Balance Set To', 'urn:replicon:script-key:parameter:amount', 'number')
    existing_starting_balance_param_to_gsub = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": existing_starting_balance}})

    required_accrual_in_hours = round(
        (float(dag_run.conf['scheduledweeklyhours'])/40)*(float(dag_run.conf['exp'])))*8
    required_accrual_param_for_gsub = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": required_accrual_in_hours}})

    return {
        'existing_annual_accrual_amount': existing_annual_accrual_amount,
        'existing_annual_accrual_param_to_gsub': existing_annual_accrual_param_to_gsub,
        'required_accrual_in_hours': required_accrual_in_hours,
        'required_accrual_param_for_gsub': required_accrual_param_for_gsub,
        'existing_accrual_month_uri': existing_accrual_month_uri,
        'existing_accrual_month_param_to_gsub': existing_accrual_month_param_to_gsub,
        'required_accrual_month_uri': required_accrual_month_uri,
        'required_accrual_month_param_for_gsub': required_accrual_month_param_for_gsub,
        'existing_accrual_day_uri': existing_accrual_day_uri,
        'existing_accrual_day_param_to_gsub': existing_accrual_day_param_to_gsub,
        'required_accual_day_uri': required_accual_day_uri,
        'required_accual_day_param_for_gsub': required_accual_day_param_for_gsub,
        'existing_starting_balance': existing_starting_balance,
        'existing_starting_balance_param_to_gsub': existing_starting_balance_param_to_gsub
    }


def compare_dates(date1_str, comparison_type, date2_str, date_format):
    if comparison_type == 'less_than':
        if datetime.strptime(date1_str, date_format) < datetime.strptime(date2_str, date_format):
            return True
        return False
    if comparison_type == 'greater_than':
        if datetime.strptime(date1_str, date_format) > datetime.strptime(date2_str, date_format):
            return True
        return False
    return null


def get_timeoffbalanceeventscript_to_gsub(policy_set_line, scipt_desciption):
    for item in policy_set_line['timeOffBalanceEventScripts']:
        if item['script']['description'] == scipt_desciption:
            return json.dumps(item)
    return ''


def get_timeoff_policy_to_assign(user_policyset, gsub_params, monthly_accrual_script_uri, date_format, dag_run):
    timeoff_policy_list = []
    user_policyset_first_line = user_policyset[0]['policySet']
    gsub_to_get_rid_of_starting_balance = get_timeoffbalanceeventscript_to_gsub(
        user_policyset_first_line, 'Set initial balance for the first day of a policy')
    scheduled_weekly_hours = float(
        dag_run.conf['scheduledweeklyhours'])/40

    if dag_run.conf['monthlyaccrual'] == 'no':

        required_starting_balance = (round(scheduled_weekly_hours * float(rail.result('log_required_value_to_calculate_starting_balance_8')))*8) if (datetime.strptime(
            dag_run.conf['startdate'], date_format) > datetime.strptime(f"01/01/{dag_run.conf['startdate'].split('/')[-1]}", date_format)) else 0
        required_starting_balance_param_for_gsub = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": required_starting_balance}})
        required_timeoff_policyset = json.loads(json.dumps(user_policyset_first_line, ensure_ascii=False).replace(
            gsub_params['existing_starting_balance_param_to_gsub'], required_starting_balance_param_for_gsub).replace(
                gsub_params['existing_annual_accrual_param_to_gsub'], gsub_params['required_accrual_param_for_gsub']).replace(
                    'null', '"effective"').replace('"script"', '"scriptTarget"'))
        timeoff_policy_list.append({
            'description': "Effective on" + dag_run.conf['startdate'],
            'effectiveDate': rail.parse_date(dag_run.conf['startdate'], date_format),
            'policySet': required_timeoff_policyset
        })

        if int(dag_run.conf['exp']) == 20:
            assign_10_yr_policy = 'yes'
            if dag_run.conf['ContractType'] != 'UN00' and compare_dates(
                    dag_run.conf['effective_date_10_years'], 'greater_than', dag_run.conf['contract_end_date'], date_format):
                assign_10_yr_policy = 'no'

            if assign_10_yr_policy == 'yes':
                required_accrual_in_hours_6_days = round(
                    scheduled_weekly_hours*6)*8
                required_annual_accrual_param_for_gsub = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": required_accrual_in_hours_6_days}})

                required_timeoff_policy_set = json.loads(json.dumps(user_policyset_first_line, ensure_ascii=False).replace(
                    gsub_params['existing_annual_accrual_param_to_gsub'], required_annual_accrual_param_for_gsub).replace(
                    gsub_params['existing_accrual_month_param_to_gsub'], gsub_params['required_accrual_month_param_for_gsub']).replace(
                    gsub_params['existing_accrual_day_param_to_gsub'], gsub_params['required_accual_day_param_for_gsub']).replace(
                    gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace(
                    'null', '"effective"').replace('"script"', '"scriptTarget"'))
                timeoff_policy_list.append({
                    'description': "Effective on" + dag_run.conf['effective_date_10_years'],
                    'effectiveDate': rail.parse_date(dag_run.conf['effective_date_10_years'], date_format),
                    'policySet': required_timeoff_policy_set
                })

                effective_date_10_yr_next_year = (datetime.strptime(
                    dag_run.conf['effective_date_10_years'], date_format) + relativedelta(months=12)).strftime('%Y')
                required_contract_end_date = dag_run.conf['contract_end_date'] if dag_run.conf[
                    'contract_end_date'] else f'02/01/{effective_date_10_yr_next_year}'

                if compare_dates(f'01/01/{effective_date_10_yr_next_year}', 'less_than', required_contract_end_date, date_format):
                    required_accrual_in_hours = round(
                        scheduled_weekly_hours*26)*8
                    req_annual_accrual_param_for_gsub = json.dumps(
                        {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": required_accrual_in_hours}})
                    timeoff_policyset_line_to_add = json.loads(json.dumps(user_policyset_first_line, ensure_ascii=False).replace(
                        gsub_params['existing_annual_accrual_param_to_gsub'], req_annual_accrual_param_for_gsub).replace(
                            gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace(
                                'null', '"effective"').replace('"script"', '"scriptTarget"'))
                    timeoff_policy_list.append({
                        'description': "Effective on" + dag_run.conf['effective_date_10_years'],
                        'effectiveDate': {
                            'day': 1,
                            'month': 1,
                            'year': int(dag_run.conf['effective_date_10_years'].split('/')[-1]) + 1
                        },
                        'policySet': timeoff_policyset_line_to_add
                    })

        if dag_run.conf['ContractType'] != 'UN00':
            timeoff_policy_list.append({
                'description': "Effective on" + dag_run.conf['contract_end_date'],
                'effectiveDate':  rail.parse_date(dag_run.conf['contract_end_date'], date_format),
                'policySet': {"timeOffBalanceEventScripts": [], "timeOffValidationScripts": []}
            })

    if dag_run.conf['monthlyaccrual'] == 'yes':
        actual_effective_on_uri = "urn:replicon:monthly-frequency-start-day-option:last-day-of-month" if int(
            dag_run.conf['startdate'].split('/')[0]) == 1 else "urn:replicon:monthly-frequency-start-day-option:1st"
        policy_line = json.dumps({"timeOffBalanceEventScripts": [{"scriptTarget": {"uri": monthly_accrual_script_uri}, "additionalParameters": [{
            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": "160.32"}}, {
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": actual_effective_on_uri}}, {
                    "keyUri": "urn:replicon:script-key:parameter:proration-option", "value": {
                        "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"}}, {
                            "keyUri": "urn:replicon:script-key:parameter:precedence", "value": {"number": "30"}}]}], "timeOffValidationScripts": []})
        timeoff_policy_list.append({
            'description': "Effective on" + dag_run.conf['startdate'],
            'effectiveDate': rail.parse_date(dag_run.conf['startdate'], date_format),
            'policySet': json.loads(policy_line)
        })

        required_accrual_in_hours_20_days_per_year = round(
            scheduled_weekly_hours*20)*8
        required_accrual_param_gsub = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": required_accrual_in_hours_20_days_per_year}})
        timeoff_policyset = json.loads(json.dumps(user_policyset_first_line, ensure_ascii=False).replace(
            gsub_params['existing_annual_accrual_param_to_gsub'], required_accrual_param_gsub).replace(
                gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace(
                    'null', '"effective"').replace('"script"', '"scriptTarget"'))
        next_year_from_startdate = (datetime.strptime(
            dag_run.conf['startdate'], date_format) + relativedelta(months=12)).strftime('%Y')
        timeoff_policy_list.append({
            'description': "Effective on 01/01/" + next_year_from_startdate,
            'effectiveDate': {
                'day': 1,
                'month': 1,
                'year': int(next_year_from_startdate)
            },
            'policySet': timeoff_policyset
        })

    final_policyset_to_apply = json.loads(json.dumps(timeoff_policy_list, ensure_ascii=False).replace(
        gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace(
            'null', '"effective"').replace('"script"', '"scriptTarget"'))

    return final_policyset_to_apply


def get_max_date_from_policy_line(user_policysetschedule, date_format):
    if not (user_policysetschedule):
        return now().strftime(date_format)

    last_policy_line_effective_date = dict_date_to_datetime(
        user_policysetschedule[0]['effectiveDate'])
    for policy_line in user_policysetschedule:
        effective_date = dict_date_to_datetime(policy_line['effectiveDate'])
        if effective_date > last_policy_line_effective_date and effective_date < now().date():
            last_policy_line_effective_date = effective_date

    return last_policy_line_effective_date.strftime(date_format)


def get_timeoff_policy_lines_prior_to_last_line(user_policysetschedule, eff_date_last_policy_line, date_format):
    timeoff_policy_list = []
    for policy_line in user_policysetschedule:
        effective_date = dict_date_to_datetime(policy_line['effectiveDate'])
        if effective_date < datetime.strptime(eff_date_last_policy_line, date_format):
            timeoff_policy_list.append({
                'description': policy_line['description'],
                'effectiveDate': policy_line['effectiveDate'],
                'policySet': policy_line['policySet']
            })

    return timeoff_policy_list


def get_new_policy_lines_to_update(past_policy_lines, user_policyset, gsub_params, max_date_existing_policy_lines, master_mapper, date_format, dag_run):
    user_policyset_first_line = user_policyset[0]['policySet']
    prev_exp_years = int(re.sub(
        r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[0]).strip()) if dag_run.conf['PreviousExperience'] else 0
    prev_exp_months = int(re.sub(
        r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[1]).strip()) if dag_run.conf['PreviousExperience'] else 0
    prev_exp_days = int(re.sub(
        r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[2]).strip()) if dag_run.conf['PreviousExperience'] else 0

    if dag_run.conf['education_level']:
        current_year_mapper_search = next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['legal_entity'] and x['type'] == "educationlevel" and (
                x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == (
                    dag_run.conf['education_level_old'])), master_mapper)), {}).get('value', '')

    total_year_value_for_exp = (int(current_year_mapper_search) + int(
        prev_exp_years)) if current_year_mapper_search else int(prev_exp_years)

    effective_date_10_years = (((datetime.strptime(dag_run.conf['startdate'], date_format) - relativedelta(
        years=total_year_value_for_exp)) - relativedelta(months=prev_exp_months)) - relativedelta(days=prev_exp_days)) + relativedelta(
            years=10)

    starting_balance_mapper_search = next(iter(filter(
        lambda x: x['legal_entity'] == dag_run.conf['legal_entity'] and x['type'] == "balance" and (
            x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == str((int(now().strftime('%m'))-int(
                max_date_existing_policy_lines.split('/')[1])) + 1)) and x['identifier__2__(_legal_entity_name/_start_date_month)'] == str(
            20 if (effective_date_10_years.date() > datetime.strptime(
                dag_run.conf['startdate'], date_format).date()) else 26), master_mapper)), {}).get('value', '')

    required_accrual_month_uri = "urn:replicon:month:" + \
        effective_date_10_years.strftime("%B").lower()
    required_accrual_month_param_for_gsub = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": required_accrual_month_uri}})

    required_accual_day_uri = "urn:replicon:monthly-frequency-start-day-option:" + ordinalize(int(
        effective_date_10_years.strftime("%d")))
    required_accual_day_param_for_gsub = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": required_accual_day_uri}})

    required_number_of_days_for_proration_calculation = 20 if (effective_date_10_years.date() > datetime.strptime(
        dag_run.conf['startdate'], date_format).date()) else 26
    required_starting_balance_in_hours = (round((float(
        dag_run.conf['old_scheduled_weekly_hrs'])/40) * float(starting_balance_mapper_search)))*8
    required_starting_balance_json = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": required_starting_balance_in_hours}})

    gsub_to_get_rid_of_starting_balance = get_timeoffbalanceeventscript_to_gsub(
        user_policyset_first_line, 'Set initial balance for the first day of a policy')
    gsub_to_get_rid_of_yearly_accrual_script = get_timeoffbalanceeventscript_to_gsub(
        user_policyset_first_line, 'Accrues time once per year.')
    gsub_to_get_rid_of_reset_balance_script = get_timeoffbalanceeventscript_to_gsub(
        user_policyset_first_line, 'Carry over balance and expire if not used')

    timeoff_policyset_final = json.loads(json.dumps(user_policyset_first_line, ensure_ascii=False).replace(
        gsub_params['existing_annual_accrual_param_to_gsub'], required_starting_balance_json).replace(
        gsub_to_get_rid_of_yearly_accrual_script, "").replace(gsub_to_get_rid_of_reset_balance_script, "").replace(
        ", ]", "]").replace("[,", "[").replace(", ,", ",").replace('null', '"effective"').replace('"script"', '"scriptTarget"'))

    past_policy_lines.append({
        'description': "Effective on" + "01/01/" + now().strftime("%Y"),
        'effectiveDate':  {
            'day': 1,
            'month': 1,
            'year': int(now().strftime("%Y"))
        },
        'policySet': timeoff_policyset_final
    })

    if int((now().date() + relativedelta(months=1)).strftime("%m")) != 1:
        mapper_search_new_policy_current = next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['legal_entity'] and x['type'] == "balance" and (
                x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == str(13 - int(
                    (now().date() + relativedelta(months=1)).strftime("%m"))) and x['identifier__2__(_legal_entity_name/_start_date_month)'] == str(int(
                        dag_run.conf['exp']))), master_mapper)), {}).get('value', '')

        required_accrual_balance = round((float(
            dag_run.conf['scheduledweeklyhours'])/40) * float(mapper_search_new_policy_current)) * 8
        required_accrual_json = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": required_accrual_balance}})

        required_accrualmonth_uri = "urn:replicon:month:" + \
            (now().date() + relativedelta(months=1)).strftime("%B").lower()
        required_accrualmonth_param_for_gsub = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrue-on-month", "value": {"uri": required_accrualmonth_uri}})

        required_accrual_date_param_for_gsub = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month", "value": {"uri": "urn:replicon:monthly-frequency-start-day-option:1st"}})

        timeoff_policy_to_append = json.loads(json.dumps(user_policyset_first_line, ensure_ascii=False).replace(
            gsub_params['existing_annual_accrual_param_to_gsub'], required_accrual_json).replace(
            gsub_params['existing_accrual_month_param_to_gsub'], required_accrualmonth_param_for_gsub).replace(
            gsub_params['existing_accrual_day_param_to_gsub'], required_accrual_date_param_for_gsub).replace(
            gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace(
            'null', '"effective"').replace('"script"', '"scriptTarget"'))

        past_policy_lines.append({
            'description': "Effective on" + ((now().date() + relativedelta(months=1)).replace(day=1)).strftime(date_format),
            'effectiveDate':  rail.parse_date(((now().date() + relativedelta(months=1)).replace(day=1)).strftime(date_format), date_format),
            'policySet': timeoff_policy_to_append
        })

    if datetime.strptime(dag_run.conf['effective_date_10_years'], date_format).date() > datetime.strptime(
            dag_run.conf['startdate'], date_format).date():
        assign_10_yr_policy = 'yes'
        if dag_run.conf['ContractType'] != 'UN00' and compare_dates(
                dag_run.conf['effective_date_10_years'], 'greater_than', dag_run.conf['contract_end_date'], date_format):
            assign_10_yr_policy = 'no'

        if assign_10_yr_policy == 'yes':
            required_accrual_in_hours_6_days = round(
                (float(dag_run.conf['scheduledweeklyhours'])/40)*6)*8
            required_annual_accrual_param_for_gsub = json.dumps(
                {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": required_accrual_in_hours_6_days}})

            time_off_policy_to_append = json.loads(json.dumps(user_policyset_first_line, ensure_ascii=False).replace(
                gsub_params['existing_annual_accrual_param_to_gsub'], required_annual_accrual_param_for_gsub).replace(
                gsub_params['existing_accrual_month_param_to_gsub'], required_accrual_month_param_for_gsub).replace(
                gsub_params['existing_accrual_day_param_to_gsub'], required_accual_day_param_for_gsub).replace(
                gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace(
                'null', '"effective"').replace('"script"', '"scriptTarget"'))

            past_policy_lines.append({
                'description': "Effective on" + effective_date_10_years.strftime(date_format),
                'effectiveDate':  {
                    'day': effective_date_10_years.day,
                    'month': effective_date_10_years.month,
                    'year': effective_date_10_years.year
                },
                'policySet': time_off_policy_to_append
            })

            if ((datetime.strptime(dag_run.conf['effective_date_10_years'], date_format) + relativedelta(
                months=12)).replace(day=1).replace(month=1)).date() < (datetime.strptime(
                    dag_run.conf['contract_end_date'], date_format).date() if dag_run.conf['contract_end_date'] else ((
                        datetime.strptime(dag_run.conf['effective_date_10_years'], date_format) + relativedelta(
                            months=12)).replace(day=2).replace(month=1)).date()):

                required_accrual_in_hours_26_days = round(
                    (float(dag_run.conf['scheduledweeklyhours'])/40)*26)*8
                required_annual_accrual_for_gsub = json.dumps(
                    {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": required_accrual_in_hours_26_days}})

                time_off_policy_for_append = json.loads(json.dumps(user_policyset_first_line, ensure_ascii=False).replace(
                    gsub_params['existing_annual_accrual_param_to_gsub'], required_annual_accrual_for_gsub).replace(
                    gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace(
                    'null', '"effective"').replace('"script"', '"scriptTarget"'))

                past_policy_lines.append({
                    'description': "Effective on" + "01/01/" + str(int(effective_date_10_years.strftime('%Y')) + 1),
                    'effectiveDate':  {
                        'day': 1,
                        'month': 1,
                        'year': int(effective_date_10_years.strftime('%Y')) + 1
                    },
                    'policySet': time_off_policy_for_append
                })

    if ((now() + relativedelta(months=12)).replace(day=1).replace(month=1)).date() < (datetime.strptime(
            dag_run.conf['contract_end_date'], date_format).date() if dag_run.conf['contract_end_date'] else ((
                now() + relativedelta(months=12)).replace(day=2).replace(month=1)).date()):

        new_policy_current_year_mapper_search = next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['legal_entity'] and x['type'] == "balance" and (
                x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == "12") and x['identifier__2__(_legal_entity_name/_start_date_month)'] == str(int(
                    dag_run.conf['exp'])), master_mapper)), {}).get('value', '')
        required_accrual = round(
            (float(dag_run.conf['scheduledweeklyhours'])/40)*new_policy_current_year_mapper_search)*8
        required_accrual_json_param = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": required_accrual}})

        timeoff_policy_for_append = json.loads(json.dumps(user_policyset_first_line, ensure_ascii=False).replace(
            gsub_params['existing_annual_accrual_param_to_gsub'], required_accrual_json_param).replace(
            gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace(
            'null', '"effective"').replace('"script"', '"scriptTarget"'))

        past_policy_lines.append({
            'description': "Effective on" + "01/01/" + (now() + relativedelta(months=12)).strftime('%Y'),
            'effectiveDate':  {
                'day': 1,
                'month': 1,
                'year': int((now() + relativedelta(months=12)).strftime('%Y'))
            },
            'policySet': timeoff_policy_for_append
        })

    if dag_run.conf['ContractType'] != 'UN00':
        past_policy_lines.append({
            'description': "Effective on" + dag_run.conf['contract_end_date'],
            'effectiveDate':  rail.parse_date(dag_run.conf['contract_end_date'], date_format),
            'policySet': {"timeOffBalanceEventScripts": [], "timeOffValidationScripts": []}
        })

    return past_policy_lines


def get_required_value_to_calculate_termination_accrual(master_mapper, date_format, dag_run):
    prev_exp_years = int(re.sub(
        r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[0]).strip()) if dag_run.conf['PreviousExperience'] else 0
    prev_exp_months = int(re.sub(
        r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[1]).strip()) if dag_run.conf['PreviousExperience'] else 0
    prev_exp_days = int(re.sub(
        r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[2]).strip()) if dag_run.conf['PreviousExperience'] else 0

    if dag_run.conf['education_level']:
        current_year_mapper_search = next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['legal_entity'] and x['type'] == "educationlevel" and (
                x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == (
                    dag_run.conf['education_level'])), master_mapper)), {}).get('value', '')

    total_year_value_for_exp = (int(current_year_mapper_search) + int(
        prev_exp_years)) if current_year_mapper_search else int(prev_exp_years)

    eff_date_to_consider = (dag_run.conf['legal_entity_hire_date'] if dag_run.conf['ContractType'] == "UN00" else dag_run.conf['radiation_flag']) if (
        dag_run.conf['ContractType']) else dag_run.conf['legal_entity_hire_date']

    effective_date_10_years = (((datetime.strptime(eff_date_to_consider, date_format) - relativedelta(
        years=total_year_value_for_exp)) - relativedelta(months=prev_exp_months)) - relativedelta(days=prev_exp_days)) + relativedelta(
            years=10)

    exp_to_consider = 26 if (datetime.strptime(
        eff_date_to_consider, date_format).date() > effective_date_10_years.date()) else 20

    mapper_search_for_balance = next(iter(filter(
        lambda x: x['legal_entity'] == dag_run.conf['legal_entity'] and x['type'] == "balance" and (
            x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == str(int(
                dag_run.conf['disabledate'].split('/')[1]))) and (
                    x['identifier__2__(_legal_entity_name/_start_date_month)'] == str(exp_to_consider)), master_mapper)), {}).get('value', '')
    print(f'mapper_search:{mapper_search_for_balance}')

    required_value_to_calculate_termination_accrual = int(
        mapper_search_for_balance) * 8

    return required_value_to_calculate_termination_accrual


def get_past_policy_lines_and_date_for_balance_daterange(user_timeoff_policies_by_timeoff_type, date_format, dag_run):
    time_off_policy_lines = []
    required_timeoff_policysetschedule = []
    for item in user_timeoff_policies_by_timeoff_type:
        if item['timeOffType']['name'] == dag_run.conf['timeofftype']:
            required_timeoff_policysetschedule = item['policySetSchedule']

    for policy_line in required_timeoff_policysetschedule:
        if dict_date_to_datetime(policy_line['effectiveDate']) < datetime.strptime(dag_run.conf['disabledate'], date_format).date():
            time_off_policy_lines.append({
                'description': policy_line['description'],
                'effectiveDate': policy_line['effectiveDate'],
                'policySet': policy_line['policySet']
            })
            required_policyset_for_timeoffbalanceeventscripts = policy_line['policySet']

    existing_annual_accrual_amount = get_required_value_from_user_policyset(
        required_policyset_for_timeoffbalanceeventscripts, 'Yearly Accrual', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'number')

    existing_accrual_day_uri = get_required_value_from_user_policyset(
        required_policyset_for_timeoffbalanceeventscripts, 'Yearly Accrual', 'urn:replicon:script-key:parameter:accrue-on-day-of-month', 'uri')
    existing_accrual_day = re.sub(
        r"[a-zA-Z]", "", existing_accrual_day_uri.split(':')[-1]).strip()

    existing_accrual_month_uri = get_required_value_from_user_policyset(
        required_policyset_for_timeoffbalanceeventscripts, 'Yearly Accrual', 'urn:replicon:script-key:parameter:accrue-on-month', 'uri')
    existing_accrual_month = existing_accrual_month_uri.split(':')[-1]

    accrual_date = existing_accrual_day + "/" + existing_accrual_month + \
        "/" + dag_run.conf['disabledate'].split('/')[-1]

    required_days_for_proration_calculation = (datetime.strptime(dag_run.conf['disabledate'], date_format).date() - datetime.strptime(
        accrual_date, '%d/%B/%Y').date()).days

    return {
        'past_policy_lines': time_off_policy_lines,
        'accrual_date': accrual_date,
        'required_days_for_proration_calculation': required_days_for_proration_calculation
    }


def get_sum_timeoff_booking_hours(response):
    sum = 0
    if not (response['rows']):
        return sum
    for row in response['rows']:
        for cell in row['cells']:
            sum += cell['numberValue']

    return sum


def final_policy_lines_with_disable_user_policy_line(
        past_policy_lines, value_to_calculate_termination_accrual, total_booking_hours, starting_balance_script_uri, date_default_format, dag_run):
    required_termination_proration_balance = round(
        float(value_to_calculate_termination_accrual)) - float(total_booking_hours)
    disable_user_policyset = {
        'timeOffBalanceEventScripts': [{
            "additionalParameters": [
                {
                    "keyUri": "urn:replicon:script-key:parameter:amount",
                    "value": {
                        "number": required_termination_proration_balance
                    }
                }
            ],
            "script": {
                "description": "Starting Balance Set To",
                "name": "Starting Balance Set To",
                "uri": starting_balance_script_uri
            }
        }]
    }

    disable_date = datetime.strptime(dag_run.conf['disabledate'], date_default_format)
    past_policy_lines.append({
        'description': f"Effective on {disable_date.day}/{disable_date.month}/{disable_date.year}",
        'effectiveDate': rail.parse_date(dag_run.conf['disabledate'], date_default_format),
        'policySet': disable_user_policyset
    })

    past_policy_lines_to_put = json.loads(json.dumps(past_policy_lines, ensure_ascii=False).replace(
        '"script"', '"scriptTarget"'))

    return past_policy_lines_to_put


def get_final_dropdown_options_list():
    final_dropdown_options_list = []
    existing_values = rail.result(
        'get_all_custom_field_drop_down_options_suspend_assignment_category_10')
    new_values_to_add = rail.load_all_records(rail.result(
        'query_to_get_new_dropdown_values_to_add_14'))

    for item in existing_values:
        final_dropdown_options_list.append({
            "target": {
                "uri": item['uri'],
                "name": item['displayText']
            },
            "name": item['displayText'],
            "isEnabled": item['isEnabled']
        })

    for item in new_values_to_add:
        final_dropdown_options_list.append({
            "target": {
                "uri": null,
                "name": null
            },
            "name": item['SuspendAssignmentCategory'],
            "isEnabled": True
        })

    return final_dropdown_options_list


def get_total_experience_including_education_level(master_mapper, date_default_format, dag_run):
    prev_exp_years = int(re.sub(
        r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[0]).strip()) if dag_run.conf['PreviousExperience'] else 0
    prev_exp_months = int(re.sub(
        r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[1]).strip()) if dag_run.conf['PreviousExperience'] else 0
    prev_exp_days = int(re.sub(
        r"[a-zA-Z]", "", dag_run.conf['PreviousExperience'].split(",")[2]).strip()) if dag_run.conf['PreviousExperience'] else 0

    previous_exp_in_days = (prev_exp_years * 365) + \
        (prev_exp_months * 31) + prev_exp_days

    current_year_mapper_search = ''

    if dag_run.conf['educationlevel']:
        current_year_mapper_search = next(iter(filter(
            lambda x: x['legal_entity'] == dag_run.conf['legalentity'] and x['type'] == "educationlevel" and (
                x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == (
                    dag_run.conf['educationlevel'])), master_mapper)), {}).get('value', '')

    total_years_of_exp_with_education_level = (int(current_year_mapper_search) + int(
        prev_exp_years)) if current_year_mapper_search else int(prev_exp_years)

    effective_date_10_years = (((datetime.strptime(dag_run.conf['startdate'], date_default_format) - relativedelta(
        years=total_years_of_exp_with_education_level)) - relativedelta(months=prev_exp_months)) - relativedelta(days=prev_exp_days)) + relativedelta(
            years=10)

    return {
        'prev_exp_days': prev_exp_days,
        'prev_exp_months': prev_exp_months,
        'total_years_of_exp_with_education_level': total_years_of_exp_with_education_level,
        'previous_exp_in_days': previous_exp_in_days,
        'effective_date_10_years': effective_date_10_years
    }


def page_handler(request, result):
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return None


def compose_user_details(response, loginname):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
        'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
        'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
        'useruri': row['cells'][1]['uri'],
        'employeetype': row['cells'][4]['textValue'] if 'textValue' in row['cells'][4] else None,
    }, flaten_rows)))
    return users_info[0] if users_info else None


def get_final_timeoff_policy_line(default_policyset, required_starting_value):
    first_entry = default_policyset[0]
    policyset_first_line = first_entry['policySet']

    existing_duration_value = get_required_value_from_user_policyset(
        policyset_first_line, 'Poland Alstom Compensatory Rule', 'urn:replicon:script-key:parameter:period-duration', 'text')
    existing_duration_value_json_param = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:period-duration", "value": {"text": existing_duration_value}})

    required_duration_value_json_param = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:period-duration", "value": {"text": str(required_starting_value)}})

    final_policy_set = json.loads(json.dumps(policyset_first_line, ensure_ascii=False).replace(
        existing_duration_value_json_param, required_duration_value_json_param).replace(
        'null', '"effective"').replace('"script"', '"scriptTarget"'))

    return [{
        'description': first_entry.get('description') or 'effective',
        'effectiveDate': first_entry.get('effectiveDate'),
        'policySet': final_policy_set
    }]


def do_format_logs():

    log_artifacts = []
    log_records = []

    userlogs = rail.result("gather_user_logs")
    otherlogs = rail.result("user_import_log_master")

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    rail.set_result(key="skipped_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Skipped', final_log_records))))
    rail.set_result(key="total_record_count", val=rail.result(
        "create_collection_from_csv", "length"))

    return final_log_records


def update_user_log():
    formatted_logs = rail.result('format_logs')
    user_logs_from_supervisor_dag = rail.load_all_records(
        rail.result('create_supervisor_user_temp_logs'))

    for item in user_logs_from_supervisor_dag:
        for entry in formatted_logs:
            if item['properties']['childjobid'] == entry['childjobid']:
                if "Exception" in item['properties']['entry_type']:
                    entry['status'] = "Error" if "Error" in entry['status'] else "Exception"
                    entry['details'] = item['properties']['details'] if "No change to the user record in Replicon" in entry['details'] else (
                        rail.smartjoin_by_delim((str(entry['details']) + ',' + item['properties']['details']).split(','), ";"))
                    break
                if "Error" in item['properties']['entry_type']:
                    entry['status'] = "Error"
                    entry['details'] = entry['details'] + \
                        ";" + item['properties']['details']
                    break
                if "Processed" in item['properties']['entry_type']:
                    entry['status'] = "Error" if "Error" in entry['status'] else (
                        "Exception" if "Exception" in entry['status'] else item['properties']['status'])
                    entry['details'] = entry['details'] + \
                        ";" + item['properties']['details']
                    break

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', formatted_logs))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', formatted_logs))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', formatted_logs))))
    rail.set_result(key="skipped_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Skipped', formatted_logs))))
    rail.set_result(key="total_record_count", val=rail.result(
        "create_collection_from_csv", "length"))

    return formatted_logs
