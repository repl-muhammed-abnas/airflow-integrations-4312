import json
from hashlib import md5
from ast import literal_eval
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pendulum import now
from rail import find_first_by_attr_and_get_attr, result, load_all_records
from impervainc.user_sync.mapper.imperva_mapper_table import imperva_mapper_table
from impervainc.user_sync.mapper.imperva_timezone_mapper import imperva_timezone_mapper


def dag_trigger_time():
    time = now()
    return {
        "y_m_d_h_m_s": time.strftime("%Y_%m_%d%H_%M_%S"),
        "d_m_y_T_h_m_s": time.strftime("%d%m%YT%H%M%S")
    }

def get_current_date_time():
    return {
        "year": now().year,
        "month": now().month,
        "day": now().day
    }

def get_originalhiredate(dag_run):
    hire_date = dag_run.conf['Original_Hire_Date'].split('T')[0]
    hire_date = datetime.strptime(hire_date, '%Y-%m-%d').date()
    return {
            "year":hire_date.year,
            "month":hire_date.month,
            "day":hire_date.day
        }

def get_termination_date(dag_run):
    termination_date = dag_run.conf['termination_date'].split('T')[0]
    termination_date = datetime.strptime(termination_date, '%Y-%m-%d').date()
    return {
        "m_d_y":f"{termination_date.month}/{termination_date.day}/{termination_date.year}",
        "terminationdate":{
            "year":termination_date.year,
            "month":termination_date.month,
            "day":termination_date.day
        }
    }

def get_if_username_present(username):
    reference_data = load_all_records(result('load_user_reference_report_data'))
    return bool(find_first_by_attr_and_get_attr(reference_data, 'Login Name', username, 'uri'))

def get_statecode_for_payrule(dag_run):
    statecode = "any"
    if dag_run.conf['Country_ISO_Code'].find('USA') >= 0:
        statecode = "CA" if dag_run.conf['State_ISO_Code'].find('CA') >= 0 else "notca"
    return statecode

def payrule_name_derived(dag_run, derived_name):
    countrycode = 'USA' if dag_run.conf['Country_ISO_Code'].find('USA') >= 0 else ''
    statecode = get_statecode_for_payrule(dag_run)
    payrate = dag_run.conf['Pay_Rate_Type'] if dag_run.conf['Pay_Rate_Type'].find('Hourly') >= 0 else "Salary"
    payrule_value = list(filter(lambda x: x['type'] == 'payrule' and x[
        'Countrycode'] == countrycode and x[
            'Impervaorg/statecodeforpayrule'] == statecode and x[
            'Payratetype'] == payrate, 
        imperva_mapper_table))
    if payrule_value:
        derived_name = payrule_value[0]['Value']
    return derived_name

def payrule_schedule_list():
    user_data = result('get_user_details')[0]
    response = []
    today_plus_1 = (now()+timedelta(days=1)).date()
    for rec in user_data['payRuleScriptSchedule']:
        if rec['effectiveDate'] and rec['effectiveDate'].get('day', ''):
            date = f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}"
            effective_date = datetime.strptime(date, '%d/%m/%Y').date()
            if effective_date < today_plus_1:
                response.append({
                    "name": rec['payRuleScript']['displayText'],
                    "uri": rec['payRuleScript']['uri'],
                    "effectivedate": effective_date.strftime("%d/%m/%Y")
                })
        else:
            date = user_data['userDetails']['employmentDateRange']['startDate']
            date = f"{date['day']}/{date['month']}/{date['year']}"
            response.append({
                "name": rec['payRuleScript']['displayText'],
                "uri": rec['payRuleScript']['uri'],
                "effectivedate": date
            })
    return response

def max_payrule_schedule_from_list():
    dates = result('payrule_schedule_list')
    dates = [datetime.strptime(rec['effectivedate'], '%d/%m/%Y').date() for rec in dates]
    max_date = max(dates) if dates else None
    return max_date.strftime("%d/%m/%Y") if max_date else None

def get_max_date(response, key, max_date):
    max_date = datetime.strptime(max_date, '%d/%m/%Y').date()
    resp = list(filter(lambda x: datetime.strptime(x[key], '%d/%m/%Y').date() == max_date, response))
    return resp

def payrule_name():
    resp = get_max_date(
        response = result('payrule_schedule_list'),
        key = 'effectivedate',
        max_date = result('max_payrule_schedule_from_list')
    )
    return resp[0]['name'] if resp else None

def create_payrule_list():
    user_data = result('get_user_details')[0]
    timesheet_details = result('get_timesheet_details')['dateRange']['startDate']
    timesheet_date = f"{timesheet_details['day']}/{timesheet_details['month']}/{timesheet_details['year']}"
    response = []
    for rec in user_data['payRuleScriptSchedule']:
        if rec['effectiveDate'] and rec['effectiveDate'].get('day', ''):
            date = f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}"
            if date != timesheet_date:
                response.append({
                    "payRuleScript": {
                        "uri": rec['payRuleScript']['uri'],
                        "name": rec['payRuleScript']['displayText'],
                    },
                    "effectivedate": {
                        "year":rec['effectiveDate']['year'],
                        "month":rec['effectiveDate']['month'],
                        "day":rec['effectiveDate']['day']
                    }
                })
        else:
            response.append({
                "payRuleScript": {
                    "uri": rec['payRuleScript']['uri'],
                    "name": rec['payRuleScript']['displayText'],
                }
            })
    response.append({
            "payRuleScript": {
                "uri": find_first_by_attr_and_get_attr(result('get_required_payrule_script'),
                    'displayText', result('payrule_name_derived'), 'uri'),
                "name": None
            },
            "effectivedate": {
                "year":timesheet_details['year'] if timesheet_details.get('year','') else now().year,
                "month":timesheet_details['month'] if timesheet_details.get('month','') else now().month,
                "day":timesheet_details['day'] if timesheet_details.get('day','') else now().day
            }
    })
    return response

def create_holiday_calendar_list(holiday_data):
    response = []
    for rec in holiday_data:
        response.append({
            "name":rec['displayText'],
            "compare":rec['displayText'].split('-')[0].strip(),
            "uri":rec['uri']
        })
    return response

def get_country_code_to_compare(dag_run):
    iso_code = "any"
    if dag_run.conf['Country_ISO_Code'].find('USA') >= 0:
        iso_code = "USA"
    elif dag_run.conf['Country_ISO_Code'].find('ISR') >= 0:
        iso_code = "ISR"
    elif dag_run.conf['Country_ISO_Code'].find('ARE') >= 0:
        iso_code = "ARE"
    return iso_code

def get_statecodeforpayrule_to_compare(dag_run):
    iso_code = "any"
    if dag_run.conf['Country_ISO_Code'].find('ISR') >= 0 and dag_run.conf['Imperva_Organization'].find('Sales') >= 0:
        iso_code = dag_run.conf['Imperva_Organization']
    return iso_code

def get_time_type_to_compare(dag_run):
    iso_code = "any"
    if dag_run.conf['Country_ISO_Code'].find('USA') >= 0 and dag_run.conf['Time_Type'].find('Part time') >= 0:
        iso_code = dag_run.conf['Time_Type']
    elif dag_run.conf['Country_ISO_Code'].find('ISR') >= 0 and \
        (dag_run.conf['Time_Type'].find('Part time') >= 0 or dag_run.conf['Time_Type'].find('Full time') >= 0):
        iso_code = dag_run.conf['Time_Type']
    return iso_code

def get_payrate_type_to_compare(dag_run):
    iso_code = "any"
    if dag_run.conf['Country_ISO_Code'].find('USA') >= 0 and \
        (dag_run.conf['Pay_Rate_Type'].find('Salary') >= 0 or dag_run.conf['Pay_Rate_Type'].find('Hourly') >= 0):
        iso_code = dag_run.conf['Pay_Rate_Type']
    return iso_code

def create_schedule_list_to_compare():
    schedule_policy = result('get_existing_schedule_types')
    user_data = result('get_user_details')[0]['userDetails']['employmentDateRange']['startDate']
    response = []
    today_plus_1 = (now()+timedelta(days=1)).date()
    for rec in schedule_policy:
        date = f"{user_data['day']}/{user_data['month']}/{user_data['year']}"
        if rec['effectiveDate'] and rec['effectiveDate'].get('day', ''):
            effective_date = f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}"
            effective_date = datetime.strptime(effective_date, '%d/%m/%Y').date()
            if effective_date < today_plus_1:
                date = f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}"
        response.append({
                "date": date,
                "name": rec['officeSchedule']['displayText'],
                "uri": rec['officeSchedule']['uri']
            })
    return response

def get_max_effective_date_from_schedule_list():
    dates = result('create_schedule_list_to_compare')
    dates = [datetime.strptime(rec['date'], '%d/%m/%Y').date() for rec in dates]
    max_date = max(dates) if dates else None
    return max_date.strftime("%d/%m/%Y") if dates else ''

def current_schedule_name():
    resp = get_max_date(
        response = result('create_schedule_list_to_compare'),
        key = 'date',
        max_date = result('get_max_effective_date_from_schedule_list')
    )
    return resp[0]['name'] if resp else ''

def create_schedule_list_134():
    existing_schedule_type = result('get_existing_schedule_types')
    response = []
    today_plus_1 = (now()+timedelta(days=1)).date()
    for rec in existing_schedule_type:
        if rec['effectiveDate'] and rec['effectiveDate'].get('day', ''):
            effective_date = f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}"
            effective_date = datetime.strptime(effective_date, '%d/%m/%Y').date()
            if not effective_date != today_plus_1:
                response.append({
                        "schedulePolicy":{
                            "officeScheduleUri": rec['officeSchedule']['uri'],
                            "scheduleTypeUri": rec['scheduleTypeUri']
                        },
                        "effectiveDate":{
                            "year":rec['effectiveDate']['year'],
                            "month":rec['effectiveDate']['month'],
                            "day":rec['effectiveDate']['day']
                        }
                    })
        else:
            response.append({
                    "schedulePolicy":{
                        "officeScheduleUri": rec['officeSchedule']['uri'],
                        "scheduleTypeUri": rec['scheduleTypeUri']
                    }
                })
    return response

def add_item_to_schedule_list_144():
    response = result('create_schedule_list_134') or []
    response.append({
            "schedulePolicy":{
                "officeScheduleUri": result('get_new_office_schedule_uri'),
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate":{
                "year":now().year,
                "month":now().month,
                "day":now().day
            }
        })
    return response

def search_entries_in_imperva_mapper_table(dag_run, type_val):
    countrycode = get_country_code_to_compare(dag_run)
    statecode = get_statecodeforpayrule_to_compare(dag_run)
    timetype = get_time_type_to_compare(dag_run)
    payrate = get_payrate_type_to_compare(dag_run)
    found_value = list(filter(lambda x: x['type'] == type_val and x[
        'Countrycode'] == countrycode and x[
            'Impervaorg/statecodeforpayrule'] == statecode and x[
            'Timetype'] == timetype and x[
            'Payratetype'] == payrate, 
        imperva_mapper_table))
    return found_value[0]['Value'] if found_value else None

def get_permission_sets_from_user_details(config):
    permission_sets = result('get_user_details')[0]['permissionSets']
    return {
        "end_user_with_report_accesss":find_first_by_attr_and_get_attr(permission_sets, 'displayText', 
            config.imperva_end_user_with_report_access, 'uri') if permission_sets[0]['name'] else '',
        "imperva_supervisor":find_first_by_attr_and_get_attr(permission_sets, 'displayText', 
            config.imperva_supervisor, 'uri') if permission_sets[0]['name'] else ''
    }

def get_permission_sets_from_permissionsets(config):
    permission_sets = result('get_all_permissionsets')
    return {
        "end_user_with_report_accesss":find_first_by_attr_and_get_attr(permission_sets, 'name', 
            config.imperva_end_user_with_report_access, 'uri') if permission_sets[0]['uri'] else '',
        "imperva_supervisor":find_first_by_attr_and_get_attr(permission_sets, 'name', 
            config.imperva_supervisor, 'uri') if permission_sets[0]['uri'] else ''
    }

def create_payrule_list_225():
    user_data = result('get_user_details')[0]
    timesheet_details = result('get_timesheet_details_223')['dateRange']['startDate'] if result('get_timesheet_details_223') else {}
    timesheet_date = f"{timesheet_details['day']}/{timesheet_details['month']}/{timesheet_details['year']}" if timesheet_details else ""
    response = []
    for rec in user_data['payRuleScriptSchedule']:
        if rec['effectiveDate'] and rec['effectiveDate'].get('day', ''):
            date = f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}"
            if date != timesheet_date:
                response.append({
                    "payRuleScript": {
                        "uri": rec['payRuleScript']['uri'],
                        "name": rec['payRuleScript']['displayText'],
                    },
                    "effectiveDate": {
                        "year":rec['effectiveDate']['year'],
                        "month":rec['effectiveDate']['month'],
                        "day":rec['effectiveDate']['day']
                    }
                })
        else:
            response.append({
                "payRuleScript": {
                    "uri": rec['payRuleScript']['uri'],
                    "name": rec['payRuleScript']['displayText'],
                }
            })
    response.append({
        "payRuleScript": {
            "uri": find_first_by_attr_and_get_attr(result('get_all_payrule_scripts'),
                'displayText', result('payrule_name_derived'), 'uri'),
            "name": None
        },
        "effectiveDate": {
            "year":timesheet_details['year'] if timesheet_details.get('year','') else now().year,
            "month":timesheet_details['month'] if timesheet_details.get('month','') else now().month,
            "day":timesheet_details['day'] if timesheet_details.get('day','') else now().day
        }
    })
    return response

def primary_workemail_does_not_equal(dag_run):
    email = result('get_user_details')[0]['userDetails']['emailAddress']
    return bool(dag_run.conf['primaryWorkEmail'] and dag_run.conf['primaryWorkEmail'] != email \
                and dag_run.conf['primaryWorkEmail'].find("@") > 0)

def firstname_does_not_equal(dag_run):
    firstname = result('get_user_details')[0]['userDetails']['firstName']
    return bool(dag_run.conf['Legal_First_Name'] and dag_run.conf['Legal_First_Name'] != firstname)

def lastname_does_not_equal(dag_run):
    lastname = result('get_user_details')[0]['userDetails']['lastName']
    return bool(dag_run.conf['Legal_Last_Name'] and dag_run.conf['Legal_Last_Name'] != lastname)

def hourly_rate_schedule_list():
    user_data = result('get_user_details')[0]
    response = []
    for rec in user_data['payrollRateSchedule']:
        if rec['effectiveDate'] and rec['effectiveDate'].get('day', ''):
            date = f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}"
            effective_date = datetime.strptime(date, '%d/%m/%Y').date()
            if effective_date != now().date():
                response.append({
                    "amount": rec['hourlyRate']['amount'],
                    "date": effective_date.strftime("%d/%m/%Y"),
                    "currency": rec['hourlyRate']['currency']['symbol']
                })
        else:
            date = user_data['userDetails']['employmentDateRange']['startDate']
            date = f"{date['day']}/{date['month']}/{date['year']}"
            response.append({
                    "amount": rec['hourlyRate']['amount'],
                    "date": date,
                    "currency": rec['hourlyRate']['currency']['symbol']
                })
    return response

def max_hourly_rate_schedule_list():
    dates = result('hourly_rate_schedule_list')
    dates = [datetime.strptime(rec['date'], '%d/%m/%Y').date() for rec in dates]
    max_date = max(dates) if dates else None
    return max_date.strftime("%d/%m/%Y") if max_date else None

def hourly_rate_amount_currency():
    resp = get_max_date(
        response = result('hourly_rate_schedule_list'),
        key = 'date',
        max_date = result('get_max_hourly_rate_schedule_list')
    )
    return {
        "amount": resp[0]['amount'] if resp else None,
        "currency":resp[0]['currency'] if resp else None
    }

def amount_or_currency_does_not_equal(dag_run):
    hourly_rate = result('get_hourly_rate_amount_currency')
    return bool((hourly_rate['amount'] != dag_run.conf['Hourly_Pay']) or (hourly_rate['currency'] != dag_run.conf['Currency']))

def original_hire_date_uri():
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "Original Hire Date", "uri", "")

def get_current_value_from_customfield(text):
    custom_field_values = result('get_user_details')[0]['userDetails']['customFieldValues']
    return find_first_by_attr_and_get_attr(custom_field_values, "customField.displayText", text, "text", '')

def costcenter_doest_equal_and_customfield_uri(dag_run):
    costcenter = ''
    if dag_run.conf['Cost_Center_ID'] and result('get_current_value_for_costcenter') != dag_run.conf['Cost_Center_ID']:
        costcenter = result('get_current_value_for_costcenter')
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "Cost Center - ID", "uri", "") if costcenter else ''

def jobcode_present(dag_run):
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "Job Code", "uri", "") if dag_run.conf['Job_Code'] else ''

def workertype_does_not_equal_and_customfield_uri_present(dag_run):
    workertype = ''
    if result('get_current_value_for_workertype') != dag_run.conf['Imperva_Worker_Type']:
        workertype = result('get_current_value_for_workertype')
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "Imperva Worker Type", "uri", "") if workertype else ''

def employeetype_does_not_equal_and_customfield_uri_present(dag_run):
    employeetype = ''
    if result('get_current_value_for_employeetype') != dag_run.conf['Imperva_Employee_Type']:
        employeetype = result('get_current_value_for_employeetype')
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "Imperva Employee Type", "uri", "") if employeetype else ''

def workcountry_does_not_equal_and_customfield_uri_present(dag_run):
    workcountry = ''
    if result('get_current_value_for_workcountry') != dag_run.conf['Work_Address_Country']:
        workcountry = result('get_current_value_for_workcountry')
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "Work Country", "uri", "") if workcountry else ''

def workstate_does_not_equal_and_customfield_uri_present(dag_run):
    workstate = ''
    if result('get_current_value_for_workstate') != dag_run.conf['Work_Address_State_Province']:
        workstate = result('get_current_value_for_workstate')
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "Work State", "uri", "") if workstate else ''

def state_isocode_does_not_equal_and_customfield_uri_present(dag_run):
    state_isocode = ''
    if result('get_current_value_for_state_iso_code') != dag_run.conf['State_ISO_Code']:
        state_isocode = result('get_current_value_for_state_iso_code')
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "State ISO Code", "uri", "") if state_isocode else ''

def exemptstatus_does_not_equal_and_customfield_uri_present(dag_run):
    exempt_status = ''
    if result('get_current_value_for_exempt_status') != dag_run.conf['Exempt_Status']:
        exempt_status = result('get_current_value_for_exempt_status')
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "Exempt Status", "uri", "") if exempt_status else ''

def final_timezone_uri_to_assign(dag_run):
    timezone = dag_run.conf['timezone']
    timezone_uri = list(filter(lambda x: x['workday_time_zone'] == timezone,
        imperva_timezone_mapper))
    timezones = result('get_all_timezones')
    if timezone_uri:
        return find_first_by_attr_and_get_attr(timezones, "displayText", timezone_uri[0]['replicon_time_zone'].strip(), "uri")
    return find_first_by_attr_and_get_attr(timezones, "displayText", "(UTC-8:00) Pacific Standard Time", "uri")

def manager_does_not_equal_and_custom_uri_present(dag_run):
    ismanager = ''
    is_manager = "Yes" if dag_run.conf['isManager'].find("1") >= 0 else "-"
    if result('get_current_value_for_manager') != is_manager:
        ismanager = result('get_current_value_for_manager')
    custom_fields = result('get_required_user_customfields')
    return find_first_by_attr_and_get_attr(custom_fields, "displayText", "Is Manager", "uri", "") if ismanager else ''

def create_supervisor_schedule_list():
    user_data = result('get_user_details')[0]
    response = []
    today_plus_1 = (now()+timedelta(days=1)).date()
    for rec in user_data['supervisorAssignmentSchedule']:
        if not (rec['effectiveDate'] and rec['effectiveDate'].get('day', '')):
            date = user_data['userDetails']['employmentDateRange']['startDate']
            date = f"{date['day']}/{date['month']}/{date['year']}"
            response.append({
                "loginname": rec['supervisor']['user']['loginName'],
                "uri": rec['supervisor']['user']['uri'],
                "effectivedate": date,
                "name":rec['supervisor']['user']['displayText']
            })
        else:
            date = f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}"
            effective_date = datetime.strptime(date, '%d/%m/%Y').date()
            if effective_date < today_plus_1:
                response.append({
                    "loginname": rec['supervisor']['user']['loginName'],
                    "uri": rec['supervisor']['user']['uri'],
                    "effectivedate": effective_date.strftime("%d/%m/%Y"),
                    "name":rec['supervisor']['user']['displayText']
                })
    return response

def max_effectivedate_and_supervisor_from_list():
    response = result('create_supervisor_schedule_list')
    dates = [datetime.strptime(rec['effectivedate'], '%d/%m/%Y').date() for rec in response]
    maxdate = max(dates) if dates else None
    resp = get_max_date(
        response = response,
        key = 'effectivedate',
        max_date = datetime.strftime(maxdate, '%d/%m/%Y')
    )
    return {
        "max_date": maxdate.strftime("%d/%m/%Y"),
        "current_supervisor":resp[0]['loginname'].lower() if resp else ''
    } if dates else {"max_date": "", "current_supervisor":""}

def check_supervisoruri_and_status():
    response = result('search_user_in_replicon')
    return bool(response and response[0]['uri'] and response[0]['status'])

def check_current_supervisor(dag_run):
    response = result('max_effectivedate_and_supervisor_from_list')
    return bool(not response['current_supervisor'] or (response['current_supervisor'] != dag_run.conf['Manager'].lower()))

def create_cost_center_list():
    user_data = result('get_user_details')[0]
    response = []
    today_plus_1 = (now()+timedelta(days=1)).date()
    for rec in user_data['costCenterSchedule']:
        if rec['effectiveDate'] and rec['effectiveDate'].get('day', ''):
            date = f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}"
            effective_date = datetime.strptime(date, '%d/%m/%Y').date()
            if effective_date < today_plus_1:
                response.append({
                    "name": rec['costCenter']['displayText'],
                    "uri": rec['costCenter']['uri'],
                    "date": effective_date.strftime("%d/%m/%Y"),
                })
        else:
            date = user_data['userDetails']['employmentDateRange']['startDate']
            date = f"{date['day']}/{date['month']}/{date['year']}"
            response.append({
                "name": rec['costCenter']['displayText'],
                "uri": rec['costCenter']['uri'],
                "date": date
            })
    return response

def uri_and_name_from_cost_center_list():
    response = result('create_cost_center_list')
    dates = [datetime.strptime(rec['date'], '%d/%m/%Y').date() for rec in response]
    max_date = max(dates) if dates else None
    resp = list(filter(lambda x: datetime.strptime(x['date'], '%d/%m/%Y').date() == max_date, response))
    return {
        "uri": resp[0]['uri'] if resp else "",
        "name":resp[0]['name'] if resp else ""
    } if dates else {"uri": "", "name":""}

def create_cost_center_list_at_398(dag_run):
    user_data = result('get_user_details')[0]
    costcenter_uri = result('uri_and_name_from_cost_center_list')['uri']
    response = []
    if costcenter_uri:
        for rec in user_data['costCenterSchedule']:
            if rec['effectiveDate'] and rec['effectiveDate'].get('day', ''):
                response.append({
                    "costCenter": {
                        "uri": rec['costCenter']['uri']
                    },
                    "effectiveDate": {
                        "year": rec['effectiveDate']['year'],
                        "month": rec['effectiveDate']['month'],
                        "day": rec['effectiveDate']['day']
                    }
                })
            else:
                response.append({
                        "costCenter": {
                            "uri": rec['costCenter']['uri']
                        }
                    })
            response.append({
                "costCenter": {
                    "uri": result('get_all_costcenters')
                },
                "effectiveDate": get_originalhiredate(dag_run)
            })
    else:
        response.append({
            "costCenter": {
                "uri": result('get_all_costcenters')
            }
        })
    return response

def get_timezone_type_to_assign(dag_run):
    timezone = dag_run.conf['timezone']
    timezone_type = list(filter(lambda x: x['workday_time_zone'] == timezone,
        imperva_timezone_mapper))
    return timezone_type[0] if timezone_type else {"workday_time_zone":"","replicon_time_zone":""}

def search_employee_type_value(dag_run):
    countrycode = get_country_code_to_compare(dag_run)
    statecode = get_statecodeforpayrule_to_compare(dag_run)
    timetype = get_time_type_to_compare(dag_run)
    payrate = get_payrate_type_to_compare(dag_run)
    schedule_value = list(filter(lambda x: x['type'] == 'Employee Type' and x[
        'Countrycode'] == countrycode and x[
            'Impervaorg/statecodeforpayrule'] == statecode and x[
            'Timetype'] == timetype and x[
            'Payratetype'] == payrate, 
        imperva_mapper_table))
    return schedule_value[0]['Value']

def create_permissionset_list(dag_run):
    permissionsets = result('get_all_permissionsets_12')
    resp = []
    if dag_run.conf['isManager'] and dag_run.conf['isManager']==1:
        resp.append({
            "uri":find_first_by_attr_and_get_attr(permissionsets, "name", "**Imperva - End User with Report access", "uri") if permissionsets else None,
            "name":None
        })
        resp.append({
            "uri":find_first_by_attr_and_get_attr(permissionsets, "name", "**Imperva - Supervisor", "uri") if permissionsets else None,
            "name":None
        })
    else:
        resp.append({
            "uri":find_first_by_attr_and_get_attr(permissionsets, "name", "**Imperva - End User", "uri") if permissionsets else None,
            "name":None
        })
    return resp

def get_timezone_uri_to_assign():
    timezones = result('get_all_timezones_13')
    timezone_types = result('search_timezone_type_value')
    response = find_first_by_attr_and_get_attr(timezones, "displayText", "(UTC-8:00) Pacific Standard Time", "uri")
    if timezone_types:
        response = find_first_by_attr_and_get_attr(timezones, "displayText", timezone_types['replicon_time_zone'].strip(), "uri")
    return response

def create_timeoffuriall_list_203(response):
    uris = [rec['uri'] for rec in response]
    return uris

def create_timeoff_list_208_223(timeoff_types, response, dag_run):
    timeoff_list = []
    for rec in response:
        if rec['description'] == dag_run.conf['Country_ISO_Code'] \
            and rec['displayText'] not in ["US/Temporary", "US/Comp Day", "Canada/Family Day"]:
            if rec['displayText'].find("US/PTO") >= 0 and dag_run.conf['Country_ISO_Code'] == "USA":
                if dag_run.conf['Exempt_Status'] == "Exempt" and rec['displayText'] == "US/PTO NEW POLICY" or \
                    dag_run.conf['Exempt_Status'] == "Non-Exempt" and rec['displayText'] == "US/PTO":
                    timeoff_list.append({
                        "uri": rec['uri'],
                        "name": rec['displayText']
                    })
            else:
                timeoff_list.append({
                        "uri": rec['uri'],
                        "name": rec['displayText']
                    })
    countrycode = dag_run.conf['Country_ISO_Code']
    holiday_timeoff_type = list(filter(lambda x: x['type'] == 'Time Off' and x[
        'Countrycode'] == countrycode, 
        imperva_mapper_table))
    if holiday_timeoff_type:
        timeoff_list.append({
                "uri": find_first_by_attr_and_get_attr(timeoff_types, "displayText", holiday_timeoff_type[0]['Value'], "uri"),
                "name": holiday_timeoff_type[0]['Value']
            })
    if dag_run.conf['Country_ISO_Code'].find("USA") < 0:
        timeoff_list.append({
                "uri": find_first_by_attr_and_get_attr(timeoff_types, "displayText", "Volunteer Time Off Intl.", "uri"),
                "name": "Volunteer Time Off Intl."
            })
    return {
        "uris": [rec['uri'] for rec in timeoff_list],
        "timeoff_list": timeoff_list
    }

def create_department_details_list():
    response = result('get_all_department_hierarchy')['childDepartments']
    dept = []
    for rec in response:
        dept.append({
            "name":rec['department']['displayText'],
            "status":rec['department']['isEnabled'],
            "uri":rec['department']['uri']
        })
    return dept

def get_md5(item):
    return md5((str(item.get('Status', '')) + "," +str(item.get('Employee_ID', '')) + "," + str(item.get('Legal_First_Name', '')) + "," +
    str(item.get('Legal_Last_Name', '')) + "," + str(item.get('primaryWorkEmail', '')) + "," + str(item.get('Username', '')) + "," +
    str(item.get('Authentication_ID', '')) + "," + str(item.get('Hire_Date', '')) + "," + str(item.get('Original_Hire_Date', '')) + "," +
    str(item.get('termination_date', '')) + "," + str(item.get('Manager', '')) + "," + str(item.get('Imperva_Worker_Type', '')) + "," +
    str(item.get('Imperva_Employee_Type', '')) + "," + str(item.get('Time_Type', '')) + "," + str(item.get('Pay_Rate_Type', '')) + "," +
    str(item.get('Hourly_Pay', '')) + "," + str(item.get('Currency', '')) + "," + str(item.get('Job_Code', '')) + "," +
    str(item.get('Cost_Center_ID', '')) + "," + str(item.get('Cost_Center_Name', '')) + "," + str(item.get('Imperva_Organization', '')) + "," +
    str(item.get('Time_Zone_of_Location_of_Worker_s_Primary_Position', '')) + "," +str(item.get('Work_Address_Country', '')) + "," +
    str(item.get('Country_ISO_Code', '')) + "," + str(item.get('Work_Address_State_Province', '')) + "," + str(item.get('State_ISO_Code', '')) + "," +
    str(item.get('Exempt_Status', '')) + "," + str(item.get('isManager', ''))).encode('utf-8')).hexdigest()

def create_imperva_list_schema(dropdown_values, imperva_collection_values, coll_name):
    impervaemployeetypelist = []
    if dropdown_values and isinstance(dropdown_values, str):
        dropdown_values = literal_eval(dropdown_values)
    for rec in dropdown_values:
        impervaemployeetypelist.append({
            "target":{
                "uri":rec['uri'],
                "name":rec['displayText']
            },
            "name":rec['displayText'],
            "isEnabled":rec['isEnabled']
        })
    for rec in load_all_records(imperva_collection_values):
        impervaemployeetypelist.append({
            "target":{
                "uri":None,
                "name":None
            },
            "name":rec[coll_name],
            "isEnabled":True
        })
    return impervaemployeetypelist

def construct_policyschedule(policy_set_schedule, termination_date):
    termination_date = datetime.strptime(termination_date, '%m/%d/%Y').date()
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                if effective_datetime and effective_datetime.date() < termination_date:
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)
    return policy_schedule_entries

def create_timeoff_names_with_uri(timeoff_type_details, timeoff_types, country_iso_code):
    resp = []
    for rec in timeoff_type_details:
        if rec['description'] == country_iso_code:
            resp.append({
                "name":rec['displayText'],
                "uri":rec['uri']
            })
    search_value = list(filter(lambda x: x['type'] == 'Time Off' and x[
        'Countrycode'] == country_iso_code, 
        imperva_mapper_table))
    if search_value:
        resp.append({
            "name":search_value[0]['Value'],
            "uri":find_first_by_attr_and_get_attr(timeoff_types, "displayText", search_value[0]['Value'], "uri")
        })
    if country_iso_code != "USA":
        resp.append({
            "name":"Volunteer Time Off Intl.",
            "uri":find_first_by_attr_and_get_attr(timeoff_types, "displayText", "Volunteer Time Off Intl.", "uri")
        })
    return {
        "nameswithuri":resp,
        "finaluris":[rec['uri'] for rec in resp]
    }

def existing_policies(timeoffuri, policiesByTimeOffType):
    policySetSchedule = find_first_by_attr_and_get_attr(policiesByTimeOffType,
                'timeOffType.uri',timeoffuri,'policySetSchedule')
    policySetSchedule = json.loads(json.dumps(
                        policySetSchedule, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"'))
    return policySetSchedule

def create_pto_policy_list(existing_policy, effective_date):
    effective_date = datetime.strptime(effective_date, '%Y-%m-%d').date()
    policy_list = []
    for rec in existing_policy:
        effective_datetime = datetime.strptime(
            f"{rec['effectiveDate']['day']}/{rec['effectiveDate']['month']}/{rec['effectiveDate']['year']}",
            '%d/%m/%Y')
        if effective_datetime and effective_datetime.date() < effective_date:
            policy_list.append({
                "description": rec['description'],
                "effectiveDate": {
                    "day": rec['effectiveDate']['day'],
                    "month": rec['effectiveDate']['month'],
                    "year": rec['effectiveDate']['year']
                },
                "policySet": rec['policySet']
            })
    return policy_list

def update_pto_policy_list_and_create_policy_list(dag_run, effective_date, default_timeoff_policyset, pto_policy_list):
    effective_date = datetime.strptime(effective_date, '%Y-%m-%d').date()
    policy_list = []
    today = now()
    pto_policy_list = pto_policy_list or []
    for rec in default_timeoff_policyset:
        num_of_month_to_add = rec['startOffset']['offsetValue'] * 12
        if dag_run.conf['rehire_update'] == 'update':
            hire_date = dag_run.conf['Original_Hire_Date'].split('T')[0]
            hire_date = datetime.strptime(hire_date, '%Y-%m-%d')
            tenure = ((today.timestamp() - hire_date.timestamp())/86400)/365
            hire_date = hire_date.date()
            start_date = hire_date + relativedelta(months=num_of_month_to_add)
            start_date = f"{start_date.strftime('%m')}/{start_date.strftime('%d')}/{start_date.strftime('%Y')}"
            if rec['startOffset']['offsetValue'] >= tenure:
                effective_date = hire_date + relativedelta(months=num_of_month_to_add)
                pto_policy_list.append({
                    "description": "Assigned by RIT",
                    "effectiveDate": {
                        "day": int(effective_date.strftime("%d")),
                        "month": int(effective_date.strftime("%m")),
                        "year": int(effective_date.strftime("%Y"))
                    },
                    "policySet": rec['policySet']
                })
            if rec['startOffset']['offsetValue'] < tenure:
                effectivedate = today + relativedelta(months=num_of_month_to_add)
                policy_list.append({
                    "policy": rec['policySet'],
                    "offset": rec['startOffset']['offsetValue'],
                    "effectivedate": {
                        "day": int(effectivedate.strftime("%d")),
                        "month": int(effectivedate.strftime("%m")),
                        "year": int(effectivedate.strftime("%Y"))
                    },
                    "diff": tenure - rec['startOffset']['offsetValue']
                })
        else:
            effective_date = effective_date + relativedelta(months=num_of_month_to_add)
            pto_policy_list.append({
                "description": "Assigned by RIT",
                "effectiveDate": {
                    "day": int(effective_date.strftime("%d")),
                    "month": int(effective_date.strftime("%m")),
                    "year": int(effective_date.strftime("%Y"))
                },
                "policySet": rec['policySet']
            })
    last_policy = [item['diff'] for item in policy_list] if policy_list else None
    if last_policy:
        pto_policy_list.append({
            "description": "Assigned by RIT",
            "effectiveDate": {
                "day": int(today.strftime("%d")),
                "month": int(today.strftime("%m")),
                "year": int(today.strftime("%Y"))
            },
            "policySet": find_first_by_attr_and_get_attr(policy_list, "diff", min(last_policy), "policy")
        })
    return {
        "policy_list":policy_list,
        "pto_policy_list":pto_policy_list
    }

def reason_4(dag_run):
    resp = []
    if not dag_run.conf.get('Username'):
        resp.append("Login name not present")
    if not dag_run.conf.get('Legal_First_Name'):
        resp.append("First Name not present")
    if not dag_run.conf.get('Hire_Date'):
        resp.append("Start Date not present")
    if not dag_run.conf.get('Legal_Last_Name'):
        resp.append("Last Name not present")
    if not dag_run.conf.get('Cost_Center_Name'):
        resp.append("Department not present")
    if not dag_run.conf.get('status'):
        resp.append("Status not present")
    return ", ".join(resp)
