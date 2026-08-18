from datetime import datetime
import rail
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import convert_json_date_to_date
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_todays_date_in_json
from airflow.exceptions import AirflowException

nil = None

INPUT_DATE_FORMAT = "%Y-%d-%m"


def get_replicon_date(date_str, return_format= "dict", _date_format= INPUT_DATE_FORMAT):
    _date = datetime.strptime(date_str, _date_format)
    if return_format == "date":
        return _date
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }


def map_mapper_replicon_timeoffs(dag_run):
    replicon_timeoffs = rail.result("get_all_timeoffs")
    mapper_timeoffs = rail.load_all_records(rail.result('query_timeoff_data'))
    
    mapped_timeoff_data =  list(map(lambda timeoff: {
            "name": timeoff['Value'],
            "uri": rail.find_first_by_attr_and_get_attr(
                replicon_timeoffs, 'name', timeoff['Value'].strip(), 'uri'),
            "policy_type": timeoff['URI'] if timeoff['URI'] else nil
        }, mapper_timeoffs))
    
    rail.set_result(key = "mapped_timeoff_data", val = mapped_timeoff_data)

    return list(set(map(lambda record: record['uri'], filter(lambda to: bool(to['uri']), mapped_timeoff_data))))

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


def user_has_no_project_management_permission_test(dag_run):
    return not bool(rail.find_first_by_attr_and_get_attr(
        rail.result("get_assigned_permission_for_user"), "policyUri", "urn:replicon:policy:project-management"
    ))

def is_division_gsap_test():
    if rail.result("get_effective_group_membership")['parent_division']:
        return rail.result("get_effective_group_membership")['parent_division']['division']['displayText'] == "GSAP"
    return False

def user_does_not_have_admin_and_payroll_permission_test(dag_run):
        return (not bool(rail.find_first_by_attr_and_get_attr(
            rail.result("get_assigned_permission_for_user"), "policyUri", "urn:replicon:policy:administration"
        ))) and ( not bool(rail.find_first_by_attr_and_get_attr(
            rail.result("get_assigned_permission_for_user"), "policyUri", "urn:replicon:policy:payroll-management"
        )))

def can_update_user_end_date_test(dag_run):
    return (bool(dag_run.conf['file_data']['term_date'])
            and bool(rail.result("get_user_details")['userDetails']['employmentDateRange'].get('endDate', False)))

def is_user_on_leave_test(dag_run):
    return dag_run.conf['file_data']['on_leave'] in [1, '1']

def is_user_for_long_leave_disable_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is True \
        and is_user_on_leave_test(dag_run) and \
            user_has_no_project_management_permission_test(dag_run)


def should_disabled_user_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is True and\
        dag_run.conf['replicon_field'] in [False , 'false']

def is_end_date_less_than_today_test(dag_run):
    return get_replicon_date(dag_run.conf['file_data']['term_date'], "date").date() < convert_json_date_to_date(get_todays_date_in_json())

def is_user_disabled_and_replicon_field_false_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is False \
            and dag_run.conf['replicon_field'] in ['false', False]

def get_disable_user_log_message(dag_run):
    if is_user_disabled_for_non_go_live_country(dag_run, 'get_user_details'):
        if user_has_no_project_management_permission_test(dag_run):
            if not bool(rail.result('get_direct_reports_for_user')):
                if is_division_gsap_test():
                    if dag_run.conf['file_data']['term_date']:
                        return {
                            "Jobid": "",
                            "Userid": dag_run.conf['file_data']["emp_id"],
                            "Email": dag_run.conf['file_data']["email_id"],
                            "Action": 'Update',
                            "Status": "Success",
                            "Details": '''User disabled in Replicon as the required user's company code and country not in allowed status. User's company code is GSAP. User has an end date in the feed file'''
                        }
                    else:
                        if user_does_not_have_admin_and_payroll_permission_test(dag_run):
                            return {
                            "Jobid": "",
                            "Userid": dag_run.conf['file_data']["emp_id"],
                            "Email": dag_run.conf['file_data']["email_id"],
                            "Action": 'Update',
                            "Status": "Success",
                            "Details": '''User disabled in Replicon as the required user's company code and country not in allowed status. User's company code is GSAP. User does not have payroll or admin permission'''
                        }
                else:
                    return {
                            "Jobid": "",
                            "Userid": dag_run.conf['file_data']["emp_id"],
                            "Email": dag_run.conf['file_data']["email_id"],
                            "Action": 'Update',
                            "Status": "Success",
                            "Details": '''User disabled in Replicon as the required user's company code and country not in allowed status'''
                        }
    
    if is_user_for_long_leave_disable_test(dag_run):
        return  {
            "Jobid": "",
            "Userid": dag_run.conf['file_data']["emp_id"],
            "Email": dag_run.conf['file_data']["email_id"],
            "Action": 'Update',
            "Status": "Success",
            "Details": '''User disabled in Replicon as "On Leave" is set to 1 for user in feed file'''
        }
    if should_disabled_user_test(dag_run) and is_end_date_less_than_today_test(dag_run):
        return {
            "Jobid": "",
            "Userid": dag_run.conf['file_data']["emp_id"],
            "Email": dag_run.conf['file_data']["email_id"],
            "Action": 'Update',
            "Status": "Success",
            "Details": '''User disabled in Replicon as "status" is set to 0 for user in feed file'''
        }
    raise AirflowException("Disabled log Task was executed without passing any validation")

def get_error_message_for_long_leave_or_user_disabled_with_replicon_field_false(dag_run):
    if not is_user_disabled_and_replicon_field_false_test(dag_run):
        return {
            "Jobid": "",
            "Userid": dag_run.conf['file_data']["emp_id"],
            "Email": dag_run.conf['file_data']["email_id"],
            "Action": 'Update',
            "Status": "Skipped",
            "Details": 'User not enabled in Replicon as "On Leave" is set to 1 for user in feed file'
        }
    return {
        "Jobid": "",
        "Userid": dag_run.conf['file_data']["emp_id"],
        "Email": dag_run.conf['file_data']["email_id"],
        "Action": 'Update',
        "Status": "Skipped",
        "Details": 'User already disabled in Replicon'
    }

def is_user_rehire_test(dag_run, return_type="bool"):
    user_details = rail.result('get_user_details')
    return_value = user_details['userDetails']['isEnabled'] is False \
            and dag_run.conf['replicon_field'] in ['true', True] \
                and dag_run.conf['mapper_data']['profile_status'] == "enabled"
    if return_type == "bool":
        return return_value 
    return "Yes" if return_value else "No"

def can_update_user_start_date_test(dag_run):
    user_start_date = rail.result("get_user_details")['userDetails']['employmentDateRange'].get('startDate', False)
    if not user_start_date:
        return True #bool(dag_run.conf['file_data']['hire_date'])
    return dag_run.conf['file_data']['hire_date'] != f"{user_start_date['year']}-{user_start_date['day']}-{user_start_date['month']}"

