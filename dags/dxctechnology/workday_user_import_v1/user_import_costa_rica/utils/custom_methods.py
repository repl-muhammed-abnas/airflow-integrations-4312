from datetime import datetime
import rail

from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import convert_json_date_to_date

null = None

INPUT_DATE_FORMAT = "%Y-%d-%m"

def get_replicon_date(date_str, return_format= "dict", _date_format= INPUT_DATE_FORMAT):
    # Return None if date_str is empty or None
    if not date_str:
        return None
    _date = datetime.strptime(date_str, _date_format)
    if return_format == "date":
        return _date
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def is_profile_enabled(dag_run):
    return dag_run.conf['mapper_data']['profile_status'].lower() == 'enabled'

def timeoff_to_assign():
    replicon_timeoffs = rail.result("get_all_timeoffs")
    mapper_timeoffs = rail.result('get_mapper_timeoff_data')

    timeoff_list =  list(map(lambda timeoff: {
            "name": timeoff['Value'],
            "uri": rail.find_first_by_attr_and_get_attr(
                replicon_timeoffs, 'name', timeoff['Value'].strip(), 'uri'),
            "policy_type": timeoff['URI'] if timeoff['URI'] else null
        }, mapper_timeoffs))
    
    filtered_timeoff_list = list(filter(lambda x: bool(x['uri']), timeoff_list))
    
    timeoff_unique_uri_list_to_assign = list(set(map(lambda record: record['uri'], filtered_timeoff_list)))

    rail.set_result(key = "timeoff_list_mapped_as_per_replicon", val = timeoff_list)

    rail.set_result(key = "timeoff_list_to_assign", val = filtered_timeoff_list )

    rail.set_result(key = "formatted_timeoff_uri_list_to_assign", val = [{"timeoff_uri": item } for item in timeoff_unique_uri_list_to_assign])

    return timeoff_unique_uri_list_to_assign


def get_effective_grp_membership_data_handler(response):
    return_data = {}
    return_data['costCenter'] = response['costCenters'][0]['costCenter']['costCenter'] if response['costCenters'] else {}
    return_data['department'] = response['departments'][0]['department']['department'] if response['departments'] else {}
    return_data['division'] = response['divisions'][0]['division']['division'] if response['divisions'] else {}
    return_data['employeeType'] = response['employeeTypes'][0]['employeeType']['employeeType'] if response['employeeTypes'] else {}
    return_data['location'] = response['locations'][0]['location']['location'] if response['locations'] else {}
    return_data['serviceCenter'] = response['serviceCenters'][0]['serviceCenter']['serviceCenter'] if response['serviceCenters'] else {}
    return_data['parent_location'] = response['locations'][0]['location']['parent'] if response['locations'] else {}
    return_data['parent_division'] = response['divisions'][0]['division']['parent'] if response['divisions'] else {}

    rail.set_result(key="response", val=response)

    return return_data

def is_user_disabled_for_non_go_live_country(dag_run, user_details_task_id):
    user_details = rail.result(user_details_task_id)
    return user_details['userDetails']['isEnabled'] is True \
        and dag_run.conf['mapper_data']['profile_status'] != "enabled"

def can_update_user_end_date_test(dag_run):
    return (bool(dag_run.conf['file_data']['term_date'])
            and not bool(rail.result("get_user_details")['userDetails']['employmentDateRange'].get('endDate', False)))

def user_does_not_have_admin_and_payroll_permission_test():
    return (not bool(rail.find_first_by_attr_and_get_attr(
        rail.result("get_assigned_permission_for_user"), "policyUri", "urn:replicon:policy:administration"
    ))) and ( not bool(rail.find_first_by_attr_and_get_attr(
        rail.result("get_assigned_permission_for_user"), "policyUri", "urn:replicon:policy:payroll-management"
    )))

def is_user_already_disabled_41_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is not True and dag_run.conf['replicon_field'] in [False , 'false']

def is_user_rehire_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is False \
        and dag_run.conf['replicon_field'] in ['true', True] \
            and dag_run.conf['mapper_data']['profile_status'] == "enabled"

def can_update_user_start_date_test(dag_run):
    user_start_date = rail.result("get_user_details")['userDetails']['employmentDateRange'].get('startDate', False)
    if not user_start_date:
        return True
    return dag_run.conf['file_data']['hire_date'] != f"{user_start_date['year']}-{user_start_date['day']}-{user_start_date['month']}"


def should_disabled_user_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is True and\
        dag_run.conf['replicon_field'] in [False , 'false']

def is_end_date_less_than_today_test(dag_run):
    return get_replicon_date(dag_run.conf['file_data']['term_date'], "date").date() < convert_json_date_to_date(get_replicon_date(dag_run.conf['todays_date']))


def get_current_assigned_udf_values(custom_field_values):
    return {
        "perner_id": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "IA PERNER ID", "text"),
        "gender": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Gender", "text"),
        "service_date": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Continious Service Date", "text"),
        "on_leave": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "On Leave", "text"),
        "personnal_area_code": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Personnel Area Code", "text"),
        "personnal_area_description": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Personnel Area Description", "text"),
        "job_activity_type": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Job Activity Type", "text"),
        "fte": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "FTE", "text"),
        "ftepct": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "FTE %", "text"),
        "is_ia": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "International Assignee", "text"),
        "ia_start_date": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "International assignee start date", "text"),
        "ia_end_date": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "International assignee end date", "text"),
        "rut": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "RUT", "text"),
        "middle_name": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Middle Name", "text"),
        "time_type": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Time Type", "text"),
        "dob": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Date of Birth", "text"),
        "employee_type_udf": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Employee Group", "text"),
        "mgmt_lvl": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Management Level", "text"),
        "assignment_type": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "assignment_type", "text"),
    }

def get_filtered_user_timeoff_policy(response):
    if not response:
        return None
    return list(filter(lambda x:x['enabled'] in ["true", True], map(lambda item: {
        "name": item['timeOffType']['displayText'],
        "enabled":item['isTimeOffAllowedAgainstThisTimeOffType'],
        "uri": item['timeOffType']['uri'],
        "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
    },response['policiesByTimeOffType'])))
