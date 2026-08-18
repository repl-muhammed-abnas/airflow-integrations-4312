from datetime import datetime, timedelta
import ast
import json
from dateutil.relativedelta import relativedelta
import rail
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_south_korea.utils.request_payload import effective_dateformat_payload
from momentive.user_import_south_korea.mappers.momentive_mapper_othercountries import mapper_othercountries

def get_current_date_time():
    return datetime.now().strftime("%Y_%m_%d%H_%M_%S")

def validate_terminationdate(dag_run):
    if 'terminationdate' in dag_run.conf:
        if datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d").date() <= datetime.now().date():
            return True
    return False

def validate_hiredate(dag_run):
    if 'hiredate' in dag_run.conf:
        if datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").date() <= datetime.now().date():
            return True
    return False

def construct_policyschedule(date_arg):
    date_val = datetime.strptime(date_arg, "%Y-%m-%d")
    policy_set_schedule = rail.result('get_existingpolicy_schedule_for_timeoff')
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                if effective_datetime and effective_datetime.date() < date_val.date():
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)
    return policy_schedule_entries

def get_final_policysets(response, dag_run):
    final_policy_sets = rail.get_dag_run_var('create_policyset')['name'] if rail.get_dag_run_var('create_policyset')['name'] else []
    if response:
        start_date = datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d")
        for item in response:
            if int(item['startOffset']['offsetValue']) == 0:
                description = "Policy" + dag_run.conf['startdate']
                effectiveDate = effective_dateformat_payload(start_date)
            if int(item['startOffset']['offsetValue']) == 1:
                start_date_added = start_date + relativedelta(months=12)
                begining_start_year = start_date_added.replace(month=1, day=1)
                description = "Policy" + dag_run.conf['startdate']
                effectiveDate = effective_dateformat_payload(begining_start_year)
            if int(item['startOffset']['offsetValue']) == 5:
                start_date_added = start_date + relativedelta(months=60)
                begining_start_year = start_date_added.replace(month=1, day=1)
                description = "Added for rehire" + dag_run.conf['startdate']
                effectiveDate = effective_dateformat_payload(begining_start_year)
            if int(item['startOffset']['offsetValue']) == 10:
                start_date_added = start_date + relativedelta(months=120)
                begining_start_year = start_date_added.replace(month=1, day=1)
                description = "Policy" + dag_run.conf['startdate']
                effectiveDate = effective_dateformat_payload(begining_start_year)
            final_policy_sets.append({
                'description': description,
                'effectiveDate': effectiveDate,
                'policySet': json.loads(json.dumps(
                    item['policySet'], ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"'))
            })

    return final_policy_sets

def get_policy_to_assign(response):
    if not response:
        return None
    res = list(map(lambda item: {
        'description': 'effective',
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, response))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))

def get_number_of_days_proration(dag_run):
    start_date = datetime.strptime(dag_run.conf["startdate"], "%Y-%m-%d")
    begining_year = start_date + relativedelta(months=12)
    start_of_year = begining_year.replace(month=1, day=1)
    return (start_of_year.timestamp() - start_date.timestamp()) / 86400

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

    if len(exception_list) > 0 :
        return {
            'exc_present' : True,
            'exc_value' : ','.join(exception_list)
        }
    return {
        'exc_present' : False,
        'exc_value' : ''
    }

# pylint: disable=too-many-boolean-expressions
def search_in_mapper_for_employeetype(dag_run):
    for data in mapper_othercountries:
        if (data['type'] == 'Employee Type') and \
            (data['Workertype'] == dag_run.conf["workertype"]) and \
            (data['Location'] == rail.result('get_location_lookup_variable')['value']) and \
            (data['Exemptstatus'] == ('Yes' if '1' in dag_run.conf['exemptionstatus'] else 'No')) and \
            (data['Shift'] == 'Any') and \
            (data['Workersubtype_legalentity'] == rail.result('get_workersubshift_lookup_variable')['value']) and \
            (data['Gender'] == 'Any') and \
            (data['Country'] == rail.result('get_country_lookup_variable')['value']) :
            return {
                'value': data['Value']
            }
    return{
        'value': None
    }

def get_details_for_employeetype_and_departmentygrpuri_not_exist(dag_run):
    details = ''
    if (not rail.result('search_entry_in_mapper_for_employeetype_37')['value']) or (
        rail.result('search_entry_in_mapper_for_employeetype_37')['value'] and not rail.result('get_required_employeetype_uri')):
        details = details + 'User not created, since Employee type group does not exist in Replicon or is disabled'
    if not dag_run.conf['departmentgroupuri']:
        details = details + 'User not created, since Department (location)  does not exist in Replicon or is disabled'
    return details

def search_momentivemapper_workertype_country(dag_run):
    output = []
    for data in mapper_othercountries:
        if data['Workertype'] == dag_run.conf["workertype"] and data['Country'] == rail.result('get_country_lookup_variable')['value'] :
            output.append(data)
    return output

# pylint: disable=too-many-branches
def user_mappings_mapper(workertype, exemptionstatus, gender, arg):
    timesheet = ''
    timesheetapprovalpath = ''
    payrule = ''
    schedule = ''
    activities = ''
    punchentrypolicy = ''
    timezone = ''
    holidaycalendar = ''
    timeoffs = ''
    language = ''
    timeoffapprovalpath = 'Supervisor'
    if arg == 'add':
        timesheetperiod = None
        workweek = ''

    # pylint: disable=too-many-nested-blocks, too-many-boolean-expressions
    for data in rail.result('search_momentive_mapper_values'):
        if data['type']:
            if (data['Workertype'] == workertype) and \
                (data['Location'] == rail.result('get_location_lookup_variable')['value']) and \
                (data['Exemptstatus'] == ('Yes' if '1' in exemptionstatus else 'No')) and \
                (data['Workersubtype_legalentity'] == rail.result('get_workersubshift_lookup_variable')['value']) and \
                (data['Country'] == rail.result('get_country_lookup_variable')['value']) :

                if data['Shift'] == rail.result('get_shift_lookup_variable')['value']:
                    if data['Gender'] == 'Any':
                        if data['type'] == 'Timesheet Template':
                            timesheet = data['Value']
                        if data['type'] == 'Timesheet approval path':
                            timesheetapprovalpath = data['Value']
                        if data['type'] == 'Payrule':
                            payrule = data['Value']
                        if data['type'] == 'Schedule':
                            schedule = data['Value']
                        if data['type'] == 'Activity':
                            activities = data['Value']
                        if data['type'] == 'Punch entry policy':
                            punchentrypolicy = data['Value']

                if data['Shift'] == 'Any':
                    if data['Gender'] == 'Any':
                        if data['type'] == 'Holiday Calendar':
                            holidaycalendar = data['Value']
                        if data['type'] == 'Time zone':
                            timezone = data['Value']
                        if arg == 'add':
                            if data['type'] == 'Work week':
                                workweek = data['Value']

                    if data['Gender'] == gender:
                        if data['type'] == 'Time off types':
                            timeoffs = data['Value']

            # pylint: disable=too-many-boolean-expressions
            if (data['Workertype'] == workertype) and \
                (data['Workersubtype_legalentity'] == rail.result('get_workersubshift_lookup_variable')['value']) and \
                (data['Country'] == rail.result('get_country_lookup_variable')['value']) and \
                (data['Location'] == 'Any') and (data['Exemptstatus'] == 'Any') and (data['Shift'] == 'Any') and (data['Gender'] == 'Any'):
                if data['type'] == 'Language':
                    language = data['Value']
    return {
        'timesheet' : timesheet,
        'timesheetapprovalpath' : timesheetapprovalpath,
        'payrule' : payrule,
        'schedule' : schedule,
        'activities' : activities,
        'punchentrypolicy' : punchentrypolicy,
        'timezone' : timezone,
        'holidaycalendar' : holidaycalendar,
        'workweek' : workweek,
        'timeoffs' : timeoffs,
        'language' : language,
        'timeoffapprovalpath' : timeoffapprovalpath,
        'timesheetperiod' : timesheetperiod
    } if arg == 'add' else {
        'timesheet' : timesheet,
        'timesheetapprovalpath' : timesheetapprovalpath,
        'payrule' : payrule,
        'schedule' : schedule,
        'activities' : activities,
        'punchentrypolicy' : punchentrypolicy,
        'timezone' : timezone,
        'holidaycalendar' : holidaycalendar,
        'timeoffs' : timeoffs,
        'language' : language,
        'timeoffapprovalpath' : timeoffapprovalpath,
    }

def get_userdata_list_for_managerid(response, dag_run):
    if list(filter(lambda x: 'textValue' in x['cells'][0] and x['cells'][0]['textValue'] == dag_run.conf['managerid'], response['rows'])):
        return list(filter(lambda x: 'textValue' in x['managerid_txt'] and x['managerid_txt']['textValue'] == dag_run.conf['managerid'], list(map(
            lambda d:{
                'uri' : d['cells'][1]['uri'],
                'loginname': d['cells'][1]['textValue'],
                'managerid_txt' : d['cells'][0]
                } , response['rows']))))
    return []

def get_status_and_details_for_add(dag_run):
    message = "Success"
    details = "User created successfully"
    has_exception_message = ','.join(list(map(lambda v: v['value'] , rail.load_all_records(rail.result('write_log_user_import')))))
    if has_exception_message:
        message = "Exception"
        details = "User created with exception" + ' ' + has_exception_message
    return {
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": "Add",
        "status": message,
        'details': details,
        "country": rail.result('get_country_lookup_variable')['value']
    }

def get_status_and_details_for_update(dag_run):
    message = "Success"
    details = "No field updates received"
    has_log_entries = ','.join(list(map(lambda v: v['value'] , rail.load_all_records(rail.result('write_log_entry')))))
    if has_log_entries:
        details = "User updated successfully" + ' ' + has_log_entries
    has_exception_message = ','.join(list(map(lambda v: v['value'] , rail.load_all_records(rail.result('write_log_exception')))))
    if has_exception_message:
        message = "Exception"
        details = has_exception_message
        if has_log_entries :
            details = has_exception_message + ' ' + has_log_entries
    return {
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": "Update",
        "status": message,
        'details': details,
        "country": rail.result('get_country_lookup_variable')['value']
    }

def validate_hiredate_startdate(dag_run):
    if datetime.strptime(str(rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['year']) + '-' +  str(
            rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['month']) + '-' + str(
                rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['day']) , "%Y-%m-%d") == datetime.strptime(
                    dag_run.conf['hiredate'], "%Y-%m-%d"):
        return True
    return False

def validate_terminationdate_enddate(dag_run):
    enddate = datetime.strptime('2099-01-01' , "%Y-%m-%d")
    userend_date = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['endDate']
    if userend_date and 'day' in userend_date:
        enddate = datetime.strptime(
        str(userend_date['year']) + '-' +  str(userend_date['month']) + '-' + str(userend_date['day']) , "%Y-%m-%d")
        if enddate == datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d"):
            return True
    return False

def get_udf_values_from_userdetails():
    user_customfield = rail.result('get_user_data')[0]['userDetails']['customFieldValues']

    return {
        'dob' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Date of Birth', 'text', ''),
        'title' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Title', 'text', ''),
        'worker_subType' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Worker Sub Type', 'text', ''),
        'yearsofservice' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Years of Service', 'text', ''),
        'hrm' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'HRM', 'text', ''),
        'cont_yearsofservice' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Continuous Years of Service - YOS', 'text', ''),
        'timeoffservcdate' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Time off Service Date - YOSS', 'text', ''),
        'gender' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Gender', 'text', ''),
        'function' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Function', 'text', ''),
        'work_shift' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Work Shift', 'text', ''),

        'dob_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Date of Birth', 'customField.uri', ''),
        'title_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Title', 'customField.uri', ''),
        'workersubtype_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Worker Sub Type', 'customField.uri', ''),
        'yearsofservice_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Years of Service', 'customField.uri', ''),
        'hrm_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'HRM', 'customField.uri', ''),
        'cont_yearsofservice_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Continuous Years of Service - YOS', 'customField.uri', ''),
        'timeoffservcdate_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Time off Service Date - YOSS', 'customField.uri', ''),
        'gender_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Gender', 'customField.uri', ''),
        'function_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Function', 'customField.uri', ''),
        'workshift_uri' : rail.find_first_by_attr_and_get_attr(
            user_customfield, 'customField.displayText', 'Work Shift', 'customField.uri', ''),
    }

def compare_dates_to_today(dag_run):
    exemptioneff_date = False
    workshiftchangeeffective_date = False
    effectivedateof_workertype = False
    cflrvlocationchange_effectivedate = False

    if dag_run.conf['cf_lrv_job_exempt_eff_date']:
        cf_lrv_job_exempt_eff_date = datetime.strptime(dag_run.conf['cf_lrv_job_exempt_eff_date'], "%Y-%m-%d")
        if cf_lrv_job_exempt_eff_date.date() == datetime.now().date():
            exemptioneff_date = True

    if dag_run.conf['work_shift_change_effective_date']:
        work_shift_change_effective_date = datetime.strptime(dag_run.conf['work_shift_change_effective_date'], "%Y-%m-%d")
        if work_shift_change_effective_date.date() == datetime.now().date():
            workshiftchangeeffective_date = True

    if dag_run.conf['effective_date_of_worker_type']:
        effective_date_of_worker_type = datetime.strptime(dag_run.conf['effective_date_of_worker_type'], "%Y-%m-%d")
        if effective_date_of_worker_type.date() == datetime.now().date():
            effectivedateof_workertype = True

    if dag_run.conf['location_change_eff_date']:
        location_change_eff_date = datetime.strptime(dag_run.conf['location_change_eff_date'], "%Y-%m-%d")
        if location_change_eff_date.date() == datetime.now().date():
            cflrvlocationchange_effectivedate = True

    return {
        'exemption_eff_date':exemptioneff_date,
        'workshift_change_effective_date': workshiftchangeeffective_date,
        'effective_date_of_workertype': effectivedateof_workertype,
        'cf_lrv_location_change_effective_date': cflrvlocationchange_effectivedate
    }

def get_startday_of_nexttimesheet():
    if 'day' in rail.result('get_timesheet_details')['dateRange']['endDate']:
        return str((datetime.strptime(
            str(rail.result('get_timesheet_details')['dateRange']['endDate']['year']) + '-' +  str(
                rail.result('get_timesheet_details')['dateRange']['endDate']['month']) + '-' + str(
                    rail.result('get_timesheet_details')['dateRange']['endDate']['day']) , "%Y-%m-%d") + timedelta(days=1)).date())
    return str(datetime.now().date())

def get_current_data(arg1,arg2):
    data_dict = {}
    data = rail.result('get_user_data')[0][arg1]
    emplpoyment_daterange_data = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']
    for p_data in data:
        if p_data['effectiveDate']:
            effective_date = str(p_data['effectiveDate']['month']) + "/" + str(p_data['effectiveDate']['day']) \
                + "/" + str(p_data['effectiveDate']['year'])
        else:
            effective_date = str(emplpoyment_daterange_data['month']) + "/" + str(emplpoyment_daterange_data['day']) \
                + "/" + str(emplpoyment_daterange_data['year'])
        date_diff = (datetime.strptime(datetime.now().strftime("%m/%d/%Y"), "%m/%d/%Y") - datetime.strptime(effective_date, "%m/%d/%Y")).days
        data_dict[p_data[arg2]['uri']] = date_diff

    return {
        'uri' : min(data_dict.keys(), key = lambda k: data_dict[k]),
        'text': rail.find_first_by_attr_and_get_attr(data, 'payRuleScript.uri', min(
            data_dict.keys(), key = lambda k: data_dict[k]), 'payRuleScript.displayText', '') if (arg2 == 'payRuleScript') else \
            rail.find_first_by_attr_and_get_attr(data, 'officeSchedule.uri', min(
            data_dict.keys(), key = lambda k: data_dict[k]), 'officeSchedule.displayText', '')
    }

def get_exceptions():
    return (rail.result('log_multiple_user_for_same_managerid') if rail.result(
        'log_multiple_user_for_same_managerid') else '') + (rail.result('log_supervisor_disabled') if rail.result(
            'log_supervisor_disabled') else '') + (rail.result('log_foreign_supervisor_not_received') if rail.result(
                'log_foreign_supervisor_not_received') else '')

def get_previoustimeoff_list():
    return list(set(map(lambda d: d['uri'], rail.result(
        'get_assigned_timeofftypes')[0]['timeOffTypeAssignmentsDetails']['timeOffTypes'])))

def get_final_timeoff(dag_run):
    final_timeoff_set = list(filter(lambda x: x['displayText'] in dag_run.conf['timeofftypes'].split('|'), list(map(
            lambda d:{
                'name' : d['displayText'],
                'uri':d['uri']
                } , rail.result('get_alltimeoff_types') ))))

    final_timeoff_assign_val = list(set(map(lambda x: x['uri'] , final_timeoff_set )))

    timeoff_previously_assigned_to_be_notassigned = list(set(filter(lambda x: x not in final_timeoff_assign_val, rail.result('get_previoustimeofflist') )))

    timeoff_not_previously_assigned = list(filter(lambda x: x['uri'] not in rail.result('get_previoustimeofflist'), list(map(
            lambda d:{
                'name' : d['name'],
                'uri':d['uri']
                } , final_timeoff_set ))))

    timeoff_add_rehire = []
    annual_leave_policy_rehire = []

    for item in final_timeoff_set:
        if (item['uri'] not in list(set(map(lambda x: x['uri'] , timeoff_not_previously_assigned )))) and \
            (datetime.strptime(dag_run.conf['oldstartdate'], '%Y-%m-%d') != datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')):
            if (item['name'] == 'KOR_Annual Leave 연차휴가') or (item['name'] == 'KOR_Monthly Leave 월차휴가'):
                timeoff_add_rehire.append(item)
            if (item['name'] != 'KOR_Annual Leave 연차휴가') and \
                (item['name'] != 'KOR_Monthly Leave 월차휴가') and \
                (item['name'].startswith('[BEL]')) and \
                (item['name'] != 'UK_Holiday Paid'):
                annual_leave_policy_rehire.append(item)

    return {
        'final_timeoff_assign_val': ','.join(final_timeoff_assign_val),
        'final_timeoff_list': final_timeoff_set,
        'timeoff_previously_assigned_to_be_notassigned': timeoff_previously_assigned_to_be_notassigned,
        'timeoff_not_previously_assigned' : timeoff_not_previously_assigned,
        'timeoff_add_rehire': timeoff_add_rehire,
        'annual_leave_policy_rehire': annual_leave_policy_rehire
    }

def update_yearlyentitilement_val_30(dag_run):
    start_date = datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d")
    begining_year = start_date + relativedelta(months=12)
    end_of_year = begining_year.replace(month=1, day=1) - timedelta(days=1)
    return round((float(rail.get_dag_run_var(
        rail.result('create_yearlyentitilement')['name']))/int(end_of_year.strftime('%j'))) * int(
            rail.result('log_numberofdaysforproration_for_yearly')), 2)

def accurals_rounded_val():
    yearly_entitlement = str(rail.get_dag_run_var(rail.result('create_yearlyentitilement')['name']))
    if (float(yearly_entitlement) % 0.5) == 0:
        return float(yearly_entitlement)
    if len(yearly_entitlement.split('.')[1]) < 2:
        if int(yearly_entitlement.split('.')[1]) * 10 > 50:
            return float(int(yearly_entitlement) + 1)
        return float(str(int(yearly_entitlement)) + '.50')
    if int(yearly_entitlement.split('.')[1]) > 50:
        return float(int(yearly_entitlement) + 1)
    return float(str(int(yearly_entitlement)) + '.50')


def get_final_timeoff_newuser(dag_run):
    all_timeoff_data = rail.result('get_alltimeoff_types')
    timeofflist = list(set(map(lambda x:x, dag_run.conf['timeofftypes'].split('|'))))
    final_timeoff_list = []
    for item in timeofflist:
        if ((int(rail.result('get_years_of_service')) < 2) and (item == 'KOR_Monthly Leave 월차휴가')) or (
            item == 'KOR_Annual Leave 연차휴가') or (item not in ['KOR_Monthly Leave 월차휴가','KOR_Annual Leave 연차휴가']):
            timeoffdict = {}
            timeoffdict['name'] = rail.find_first_by_attr_and_get_attr(all_timeoff_data, 'displayText', item, 'displayText', '')
            timeoffdict['uri'] = rail.find_first_by_attr_and_get_attr(all_timeoff_data, 'displayText', item, 'uri', '')
        final_timeoff_list.append(timeoffdict)

    return {
        'final_timeoff_list' : json.dumps(final_timeoff_list),
        'final_timeoff_assign_val': list(filter(None, (list(set(map(lambda x: x['uri'] , final_timeoff_list ))))))
    }

def get_req_start_bal(dag_run):
    start_date = datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d")
    begining_year = start_date + relativedelta(months=12)
    end_of_year = begining_year.replace(month=1, day=1) - timedelta(days=1)
    days = float(end_of_year.strftime('%j'))

    if start_date > start_date.replace(month=1, day=1):
        return round((float(rail.result('log_existing_accrual_balance_48')) / days) * float(
            rail.result('log_numberofdaysforproration_54')), 2)
    return 0

def get_department_group_list(response):
    return list(map(lambda x:{
        'departmentgroupname': x['cells'][0]['textValue'],
        'departmentgroupuri': x['cells'][0]['uri'],
        'fullpath': '/'.join(list(map(lambda c:c['textValue'], x['cells'][2]['cellCollection'])))
    }, response['rows']))

def get_user_data(response):
    return list(map(lambda x:{
        'username': x['cells'][0]['textValue'],
        'useruri': x['cells'][0]['uri'],
        'status': x['cells'][3]['textValue'],
        'enddate': x['cells'][1]['dateValue'] if 'dateValue' in x['cells'][1]  else None,
        'startdate': x['cells'][2]['dateValue'] if 'dateValue' in x['cells'][2]  else None,
    }, response['rows']))

def get_req_uris(dag_run):
    useruri = ''
    enddate = ''
    startdate = ''
    status = ''
    if rail.result('search_user'):
        useruri = rail.find_first_by_attr_and_get_attr(
            rail.result('search_user'), 'username', dag_run.conf['userid'], 'useruri', '')
        end_date = rail.find_first_by_attr_and_get_attr(
            rail.result('search_user'), 'username', dag_run.conf['userid'], 'enddate', '')
        if end_date:
            enddate = str(end_date['year']) + '-' + str(end_date['month']) + '-' + str(end_date['day'])
        start_date = rail.find_first_by_attr_and_get_attr(
            rail.result('search_user'), 'username', dag_run.conf['userid'], 'startdate', '')
        if startdate:
            startdate = str(start_date['year']) + '-' + str(start_date['month']) + '-' + str(start_date['day'])
        status = rail.find_first_by_attr_and_get_attr(
            rail.result('search_user'), 'username', dag_run.conf['userid'], 'status', '')

    departmentgroupuri = ''
    if dag_run.conf['departmentgroup']:
        departmentgroupuri = rail.find_first_by_attr_and_get_attr(
            dag_run.conf['departmentgroup'], 'departmentgroupname', dag_run.conf['location'], 'departmentgroupuri', '')

    legalentityuri = ''
    if dag_run.conf['legalentity'] and dag_run.conf['enableddivisions']:
        legalentityuri = rail.find_first_by_attr_and_get_attr(
            dag_run.conf['enableddivisions'], 'displayText', dag_run.conf['legalentity'], 'uri', '')

    paygroupuri = ''
    if dag_run.conf['paygroup'] and dag_run.conf['servc_centre']:
        paygroupuri = rail.find_first_by_attr_and_get_attr(
            dag_run.conf['servc_centre'], 'displayText', dag_run.conf['paygroup'], 'uri', '')

    costcenteruri = ''
    if dag_run.conf['cost_center'] and dag_run.conf['enabledcostcentre']:
        costcenteruri = rail.find_first_by_attr_and_get_attr(
            dag_run.conf['enabledcostcentre'], 'displayText', dag_run.conf['cost_center'], 'uri', '')

    return {
        'useruri' : useruri,
        'enddate' : enddate,
        'startdate' : startdate,
        'status' : status,
        'departmentgroupuri' : departmentgroupuri,
        'legalentityuri' : legalentityuri,
        'paygroupuri' : paygroupuri,
        'costcenteruri' : costcenteruri
    }

def do_format_logs():

    def get_filtered_records(logs, status):
        return list(filter(lambda log: log['status'].lower() == status, logs))

    def get_record_summary(logs):
        return {
            "success": len(get_filtered_records(logs, 'success')),
            "failed":  len(get_filtered_records(logs, 'error')),
            "exception": len(get_filtered_records(logs, "exception")),
            "skipped": len(get_filtered_records(logs, "skipped"))
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

    gather_logs = rail.result('gather_user_logs') if rail.result('gather_user_logs') else []

    for log in gather_logs:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)

    users = list(
        set(map(lambda x: x['properties'].get('userid', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for user in users:
        if not user:
            continue
        user_logs = list(
            filter(lambda x: x['properties'].get('userid', '') == user and x['properties'].get('details', ''), master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                'userid': user,
                'username': first['properties'].get('username'),
                'action': first['properties'].get('action'),
                'status': get_status(user_logs),
                'details': ",".join(list(map(lambda x: x['properties'].get('details'), user_logs))),
                'country': first['properties'].get('country'),
                'ecid': first['ecid']
            })

    return {
        "get_record_summary": get_record_summary(logs),
        "final_logs": json.dumps(logs, ensure_ascii=False)
    }

def get_iniial_country_lookup_value(dag_run):
    return "South Korea" if "Korea, Republic of" in dag_run.conf['country'] else "UAE" if "United Arab Emirates" in dag_run.conf['country'] else \
        "Belgium" if "Belgium" in dag_run.conf['country'] else "France" if "France" in dag_run.conf['country'] else \
            "United Kingdom" if "United Kingdom" in dag_run.conf['country'] else "Null"
