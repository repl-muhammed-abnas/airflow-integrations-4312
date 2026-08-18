import json
from datetime import datetime, timedelta
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None


def split_date_string(date_str, split_type='string'):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    if split_type == 'datetime':
        return {
            'day': date.day,
            'month': date.month,
            'year': date.year
        }
    if split_type == 'int':
        return {
            'day': int(date.strftime("%d")),
            'month': int(date.strftime("%m")),
            'year': int(date.strftime("%Y"))
        }

    return {
        'day': date.strftime("%d"),
        'month': date.strftime("%m"),
        'year': date.strftime("%Y")
    }


def round_off_accrual_value(decimal_value):
    decimal_left = str(decimal_value.split(".")[0])
    decimal_right = str(decimal_value.split(".")[-1])
    decimal_right_len = len(decimal_right)

    if decimal_right_len == 1:
        decimal_right = decimal_right + "0"

    if decimal_right == "00" or decimal_right == "0":
        return str(decimal_left + ".0")

    if decimal_right == '99':
        return str(int(decimal_left) + 1)

    if int(decimal_right) > 0 and int(decimal_right) < 26:
        return str(decimal_left + ".0")

    if int(decimal_right) > 25 and int(decimal_right) < 73:
        return str(decimal_left + ".5")

    if int(decimal_right) > 72 and int(decimal_right) < 99:
        return str(int(decimal_left) + 1) + ".0"

    return decimal_left


def validate_hiredate_startdate(dag_run):
    return bool(datetime.strptime(str(rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['year']) + '-' + str(
        rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['month']) + '-' + str(
        rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['day']), "%Y-%m-%d") == datetime.strptime(
        dag_run.conf['hiredate'], "%Y-%m-%d"))


def validate_terminationdate_enddate(dag_run):
    enddate = datetime.strptime('2099-01-01', "%Y-%m-%d")
    userend_date = rail.result('get_user_data')[
        0]['userDetails']['employmentDateRange']['endDate']
    if dag_run.conf['terminationdate']:
        if userend_date and 'day' in userend_date:
            enddate = datetime.strptime(
                str(userend_date['year']) + '-' + str(userend_date['month']) + '-' + str(userend_date['day']), "%Y-%m-%d")
        if enddate == datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d"):
            return True
    return False


def get_day_of_the_year(date_value):
    if str(type(date_value)) == "<class 'str'>":
        date = datetime.strptime(date_value, "%Y-%m-%d")
    else:
        date = date_value
    start_date_of_year = datetime.strptime(
        str(date.year) + "-01-01", "%Y-%m-%d")
    return float((date - start_date_of_year).days + 1)


def get_number_of_days_to_be_considered_for_accrual(dag_run):
    day_of_year = get_day_of_the_year(datetime.now())
    last_day_of_year = datetime.now().strftime("%Y") + "-12-31"

    if dag_run.conf['rehire'] == 'rehire':
        day_of_year = get_day_of_the_year(
            dag_run.conf['hiredate']) if dag_run.conf['hiredate'] else ''
        last_day_of_year = str(rail.result("get_split_dates")[
                               'hire_date']['year']) + "-12-31"

    number_of_days_in_year = get_day_of_the_year(last_day_of_year)
    no_of_days_for_accrual = int(number_of_days_in_year - day_of_year) + 1

    return {
        "number_of_days_in_year": number_of_days_in_year,
        "day_of_year": day_of_year,
        "no_of_days_for_accrual": no_of_days_for_accrual
    }


def decimal_number_split(decimal_number):
    decimal_number = str(decimal_number)
    return {
        'integer_part': decimal_number.split(".")[0],
        'decimal_part': decimal_number.split(".")[-1],
        "count_of_decimal": len(decimal_number.split(".")[-1])
    }


def get_input_validationlog(dag_run):
    exception_list = []
    if not dag_run.conf['userid']:
        exception_list.append('Login name not present')
    if not dag_run.conf['firstname']:
        exception_list.append('First_Name not present')
    if not dag_run.conf['lastname']:
        exception_list.append('Last_Name not present')
    if not dag_run.conf['hiredate']:
        exception_list.append('Hire date not present')
    if not dag_run.conf['emailaddress']:
        exception_list.append('Email_Address not present')
    if not dag_run.conf['exemptionstatus']:
        exception_list.append('Excemption Status not present')
    if not dag_run.conf['workertype']:
        exception_list.append('Worker type not present')
    if not dag_run.conf['location']:
        exception_list.append('Department (location) not present')
    if not dag_run.conf['active']:
        exception_list.append('Employee status not present')
    if not dag_run.conf['managerid']:
        exception_list.append('Manager ID not present')
    if not dag_run.conf['country']:
        exception_list.append('Country not present')

    if len(exception_list) > 0:
        return {
            'exc_present': True,
            'exc_value': ','.join(exception_list)
        }
    return {
        'exc_present': False,
        'exc_value': ''
    }


def get_udf_values_from_userdetails():
    user_customfield = rail.result('get_user_data')[
        0]['userDetails']['customFieldValues']
    return {
        'dob': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Date of Birth', 'text', ''),
        'title': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Title', 'text', ''),
        'worker_subType': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Worker Sub Type', 'text', ''),
        'yearsofservice': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Years of Service', 'text', ''),
        'hrm': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'HRM', 'text', ''),
        'cont_yearsofservice': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Continuous Years of Service - YOS', 'text', ''),
        'timeoffservcdate': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Time off Service Date - YOSS', 'text', ''),
        'gender': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Gender', 'text', ''),
        'function': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Function', 'text', ''),
        'work_shift': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Work Shift', 'text', ''),
        'dob_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Date of Birth', 'customField.uri', ''),
        'title_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Title', 'customField.uri', ''),
        'workersubtype_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Worker Sub Type', 'customField.uri', ''),
        'yearsofservice_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Years of Service', 'customField.uri', ''),
        'hrm_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'HRM', 'customField.uri', ''),
        'cont_yearsofservice_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Continuous Years of Service - YOS', 'customField.uri', ''),
        'timeoffservcdate_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Time off Service Date - YOSS', 'customField.uri', ''),
        'gender_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Gender', 'customField.uri', ''),
        'function_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Function', 'customField.uri', ''),
        'workshift_uri': rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Work Shift', 'customField.uri', ''),
    }


def get_details_for_employeetype_and_departmentygrpuri_not_exist(dag_run):
    details = ''
    if not (rail.result('get_required_employeetype_uri')):
        details = details + ";" + \
            'User not created, since Employee type group does not exist in Replicon or is disabled'
    if not (dag_run.conf['departmentgroupuri']):
        details = details + ";" + \
            'User not created, since Department (location)  does not exist in Replicon or is disabled'
    return details


def get_userdata_list_for_managerid(response, dag_run):
    if list(filter(lambda x: 'textValue' in x['cells'][0] and x['cells'][0]['textValue'] == dag_run.conf['managerid'], response['rows'])):
        return list(filter(lambda x: 'textValue' in x['managerid_txt'] and x['managerid_txt']['textValue'] == dag_run.conf['managerid'], list(map(
            lambda d: {
                'uri': d['cells'][1]['uri'],
                'loginname': d['cells'][1]['textValue'],
                'managerid_txt': d['cells'][0]
            }, response['rows']))))
    return []


def compare_dates_to_today(dag_run):
    exemptioneff_date = False
    cflrvbusinesstitlechange_effectivedate = False
    effectivedateof_workertype = False
    cflrvlocationchange_effectivedate = False

    if dag_run.conf['exemption_eff_date']:
        cf_lrv_job_exempt_eff_date = datetime.strptime(
            dag_run.conf['exemption_eff_date'], "%Y-%m-%d")
        if cf_lrv_job_exempt_eff_date.date() == datetime.now().date():
            exemptioneff_date = True

    if dag_run.conf['CF_LRV_Business_Title_Change_Eff_Date']:
        work_shift_change_effective_date = datetime.strptime(
            dag_run.conf['CF_LRV_Business_Title_Change_Eff_Date'], "%Y-%m-%d")
        if work_shift_change_effective_date.date() == datetime.now().date():
            cflrvbusinesstitlechange_effectivedate = True

    if dag_run.conf['effective_date_of_worker_type']:
        effective_date_of_worker_type = datetime.strptime(
            dag_run.conf['effective_date_of_worker_type'], "%Y-%m-%d")
        if effective_date_of_worker_type.date() == datetime.now().date():
            effectivedateof_workertype = True

    if dag_run.conf['CF_LRV_Location_Change_Effective_Date']:
        location_change_eff_date = datetime.strptime(
            dag_run.conf['CF_LRV_Location_Change_Effective_Date'], "%Y-%m-%d")
        if location_change_eff_date.date() == datetime.now().date():
            cflrvlocationchange_effectivedate = True

    return {
        'exemption_eff_date': exemptioneff_date,
        'cf_lrv_businesstitle_change_effective_date': cflrvbusinesstitlechange_effectivedate,
        'effective_date_of_workertype': effectivedateof_workertype,
        'cf_lrv_location_change_effective_date': cflrvlocationchange_effectivedate
    }


def get_startday_of_nexttimesheet():
    if 'day' in rail.result('get_timesheet_details')['dateRange']['endDate']:
        return (datetime.strptime(
            str(rail.result('get_timesheet_details')['dateRange']['endDate']['year']) + '-' + str(
                rail.result('get_timesheet_details')['dateRange']['endDate']['month']) + '-' + str(
                    rail.result('get_timesheet_details')['dateRange']['endDate']['day']), "%Y-%m-%d") + timedelta(days=1)).date().strftime("%Y-%m-%d")
    return datetime.now().date().strftime("%Y-%m-%d")


def get_status_and_details_for_update(dag_run):
    message = "Success"
    details = "No field updates received"
    has_log_entries = ','.join(list(
        map(lambda v: v['properties']['value'], rail.load_all_records(rail.result('log_entries')))))
    if has_log_entries:
        details = has_log_entries
    has_exception_message = ','.join(list(map(
        lambda v: v['properties']['value'], rail.load_all_records(rail.result('exception_log')))))
    if has_exception_message:
        message = "Exception"
        details = has_exception_message
        if has_log_entries:
            details = has_exception_message + ',' + has_log_entries
    return {
        "jobid": dag_run.conf['parentjobid'],
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": "Update",
        "status": message,
        'details': details,
        "childjobid": get_dagrun_ecid(dag_run),
    }


def get_exceptions():
    return ("Supervisor not assigned sincemultiple users found with same EMP id" if len(rail.result('search_for_user_with_empid')) > 1 else '') + (
        rail.result('log_supervisor_disabled') if rail.result('log_supervisor_disabled') else '') + (
            rail.result('log_foreign_supervisor_not_received') if rail.result('log_foreign_supervisor_not_received') else '')


def modify_previous_policy(policysetschedule):
    required_policysetschedules = [{
        "description": item["description"],
        "effectiveDate": item["effectiveDate"],
        "policySet": item['policySet']
    }for item in policysetschedule if datetime.strptime((str(item["effectiveDate"]['day']) + "/" + str(item["effectiveDate"]['month']) + "/" + str(item["effectiveDate"]['year'])), "%d/%m/%Y") < datetime.now()]

    modified_previouspolicies = json.loads(json.dumps(required_policysetschedules).replace(
        'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))

    return modified_previouspolicies


def final_policy_starting_balance_modified(default_policyset, new_balance):
    gsubbed_policyset_with_starting_balance_modified = json.loads(json.dumps(default_policyset).replace(
        'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
    for item in gsubbed_policyset_with_starting_balance_modified['timeOffBalanceEventScripts']:
        if item['scriptTarget']['name'] == 'Starting Balance Set To':
            for entry in item['additionalParameters']:
                if entry['keyUri'] == 'urn:replicon:script-key:parameter:amount':
                    entry['value']['number'] = float(new_balance)

    return gsubbed_policyset_with_starting_balance_modified


def new_old_policyset_append(new_policyset, previous_policyset, effective_date):
    if 'urn' in json.dumps(new_policyset):
        previous_policyset.append({
            "description": "Effective from " + str(effective_date['day']) + "/" + str(effective_date['month']) + "/" + str(effective_date['year']),
            "effectiveDate": effective_date,
            "policySet": new_policyset
        })

    return previous_policyset


def modify_new_policy(default_policyset, previous_policyset, rehire_status):
    gsubbed_policyset = json.loads(json.dumps(default_policyset).replace(
        'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
    if rehire_status == 'rehire':
        modified_policysetschedule = {
            "description": "Effective from " + str(rail.result("get_split_dates")['hire_date']['day']) + "/" + str(rail.result("get_split_dates")['hire_date']['month']) + "/" + str(rail.result("get_split_dates")['hire_date']['year']),
            "effectiveDate": rail.result("get_split_dates")['hire_date'],
            "policySet": gsubbed_policyset
        }
    else:
        modified_policysetschedule = {
            "description": "Effective from " + str(rail.result("get_split_dates")['today']['day']) + "/" + str(rail.result("get_split_dates")['today']['month']) + "/" + str(rail.result("get_split_dates")['today']['year']),
            "effectiveDate": rail.result("get_split_dates")['today'],
            "policySet": gsubbed_policyset
        }
    if 'urn' in json.dumps(modified_policysetschedule):
        previous_policyset.append(modified_policysetschedule)
    return previous_policyset


def get_required_value_from_policy_line(user_default_policy_set_schedule, scipt_name, key_uri):
    for item in user_default_policy_set_schedule:
        for x in item['policySet']['timeOffBalanceEventScripts']:
            if x['script']['name'] == scipt_name:
                for y in x['additionalParameters']:
                    if y['keyUri'] == key_uri:
                        return y['value']['number']
    return null


def get_variable_accrual_amount(final_value_after_decimal, decimal_point_value_split):
    variable_accrual_amount = ''
    if final_value_after_decimal == '00' or final_value_after_decimal == '0':
        variable_accrual_amount = decimal_point_value_split['integer_part'] + ".0"
    elif final_value_after_decimal == '99':
        variable_accrual_amount = int(
            decimal_point_value_split['integer_part']) + 1
    elif int(final_value_after_decimal) > 0 and int(final_value_after_decimal) < 26:
        variable_accrual_amount = decimal_point_value_split['integer_part'] + ".0"
    elif int(final_value_after_decimal) > 25 and int(final_value_after_decimal) < 73:
        variable_accrual_amount = decimal_point_value_split['integer_part'] + ".5"
    elif int(final_value_after_decimal) > 72 and int(final_value_after_decimal) < 99:
        variable_accrual_amount = str(
            int(decimal_point_value_split['integer_part']) + 1) + ".0"

    return variable_accrual_amount


def modify_required_value_in_policy_set(policy_set_to_modify, script_description, key_uri, value_to_set):
    for x in policy_set_to_modify['timeOffBalanceEventScripts']:
        if x['script']['description'] == script_description:
            for y in x['additionalParameters']:
                if y['keyUri'] == key_uri:
                    y['value']['number'] = value_to_set
    return null


def get_modified_policyset_schedule(default_policyset_schedule, calculated_starting_balance, effective_date_dict):

    default_policyset_for_modification = default_policyset_schedule[0]['policySet']

    modify_required_value_in_policy_set(
        default_policyset_for_modification, "Set initial balance for the first day of a policy", "urn:replicon:script-key:parameter:amount", calculated_starting_balance)

    final_modified_policyset = json.loads(json.dumps(
        default_policyset_for_modification, ensure_ascii=False).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

    final_modified_policyset_schedule = [{
        "description": "Effective from" + effective_date_dict['day'] + "/" + effective_date_dict['month'] + "/" + effective_date_dict['year'],
        "effectiveDate": effective_date_dict,
        "policySet": final_modified_policyset
    }]

    return final_modified_policyset_schedule


def initial_date_tasks(dag_run):
    start_date_split = split_date_string(
        dag_run.conf['startdate'])
    day_of_the_year = get_day_of_the_year(
        dag_run.conf['startdate'])
    number_of_days_in_the_year = get_day_of_the_year(
        str(start_date_split['year']) + "-12-31")
    number_of_days_to_be_considered_for_accrual = float(
        number_of_days_in_the_year - day_of_the_year) + 1
    india_casual_leave_accrual = float(
        10.0 / number_of_days_in_the_year)
    return {
        'start_date_split': start_date_split,
        'log_numberofdaystobeconsideredforaccrual_7': number_of_days_to_be_considered_for_accrual,
        'log_1_i_n_d_privilege_leaveaccrualcalculation_8': float(20.0 / number_of_days_in_the_year),
        'log_2_ind_casual_leave_accrual_calculation_9': india_casual_leave_accrual,
        'log_daystobeaccruedstartingbalance_10': round(round(float(
            india_casual_leave_accrual * number_of_days_to_be_considered_for_accrual), 3), 2)
    }
