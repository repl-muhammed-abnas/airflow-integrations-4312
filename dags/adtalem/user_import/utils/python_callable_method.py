from datetime import datetime
import json
from dateutil.relativedelta import relativedelta
from rail import get_current_context, load_all_records, result, find_first_by_attr_and_get_attr, set_result
from adtalem.user_import.mappers.adtalem_caribbean_job_code_and_pay_group_mapper import adtalem_caribbean_job_code_and_pay_group_mapper
from adtalem.user_import.mappers.adtalem_paygroup_and_job_code_mapper import adtalem_paygroup_and_job_code_mapper
from adtalem.user_import.mappers.adtalem_mapper_file_old import adtalem_mapper_file_old
from adtalem.user_import.mappers.adtalem_mapper_file import adtalem_mapper_file
from adtalem.user_import.mappers.adtalem_timezone_mapper import adtalem_timezone_mapper
from adtalem.user_import.mappers.adtalem_time_off_jobcode_mapper_old import adtalem_time_off_jobcode_mapper_old
from adtalem.user_import.mappers.adtalem_timeoff_policy_mapper_vacation_old import adtalem_timeoff_policy_mapper_vacation_old
from adtalem.user_import.mappers.adtalem_caribbean_master_mapper import adtalem_caribbean_master_mapper
from adtalem.user_import.mappers.adtalem_us_timeoff_policy_mapper_fto import adtalem_us_timeoff_policy_mapper_fto
from adtalem.user_import.mappers.adtalem_time_off_jobcode_mapper_ch import adtalem_time_off_jobcode_mapper_ch
from adtalem.user_import.mappers.adtalem_timeoff_policy_mapper_pto import adtalem_timeoff_policy_mapper_pto
from adtalem.user_import.utils.request_payload import get_datetime_obj, get_today_date


null = None


def get_task_state(task_id):
    task_instance = get_current_context()['dag_run'].get_task_instance(task_id)
    return task_instance.current_state() if task_instance else null


def get_number(val):
    number = null
    if val.isdigit():
        return int(val)
    try:
        number = float(val)
    except ValueError:
        number = null
    return number


def get_mapper_paygroup_jobcode(process_userdag_caller):

    dag_run_conf = get_current_context()['dag_run'].conf

    if process_userdag_caller:
        if 'caribbean' in process_userdag_caller:
            paygroup_jobcode_mapper = adtalem_caribbean_job_code_and_pay_group_mapper
            if process_userdag_caller == 'caribbean_supervisor':
                return {
                    'paygroup_jobcode_mapper': paygroup_jobcode_mapper,
                    'paygroup': result('get_new_supervisor')['paygroup'],
                    'jobcode': result('get_new_supervisor')['jobcode']
                }

            return {
                'paygroup_jobcode_mapper': paygroup_jobcode_mapper,
                'paygroup': dag_run_conf['paygroup'],
                'jobcode': dag_run_conf['jobcode']
            }

    paygroup_jobcode_mapper = adtalem_paygroup_and_job_code_mapper
    return {
        'paygroup_jobcode_mapper': paygroup_jobcode_mapper,
        'paygroup': dag_run_conf['paygroup'],
        'jobcode': dag_run_conf['jobcode']
    }


def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_logs():

    dag_run_conf = get_current_context()['dag_run'].conf

    log_records = []
    log_artifacts = dag_run_conf['user_logs'] if dag_run_conf['user_logs'] else None

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    return list(map(lambda x: {
        **{
            'jobid': x['ecid']
        },
        **dict(x['properties'].items()),
    }, log_records)) if log_records else []


def is_us_user_based_on_paygroup(pay_group):

    non_us_paygroups = ('ACAFR', 'ACADE', 'ACAAU', 'ACAUK', 'ACACH', 'HK',
                        'ACAIN', 'ACAJP', 'ACAPA', 'PORTO', 'ACASG', 'ACATW', 'ACAMX', 'ACAAS')
    if any(pay in pay_group for pay in non_us_paygroups):
        return 'no'
    return 'yes'


def load_supervisor_from_collection():

    fields_to_consider = ('lastname', 'firstname', 'jobcode', 'jobtitle', 'managerindicator',
                          'paygroup', 'division', 'salaryhourly', 'regulartemp', 'fullparttime',
                          'flsastatus', 'filenumber', 'departmentnumber', 'effectivedate', 'supervisor_log')

    collection_data = load_all_records(
        result('query_newuser_from_managerdnumber'))

    new_supervisor = list(map(lambda item: {
        **{
            k: v.strip() if v else null for k, v in item.items() if k in fields_to_consider
        },
        **{
            'employeeid': item['employeenumber'],
            'loginname': item['dnumber'],
            'activeleavestatus': item['employeestatus'],
            'supervisor': item['managerdnumber'],
            'emailaddress': item['businessemailaddress'],
            'homestate': item['state'],
            'standardhours': item['standardhours'],
            'startdate': item['hiredate'].replace('-', '/'),
            'rehiredate': item['rehiredate'].replace('-', '/'),
            'servicedate': item['servicedate'].replace('-', '/'),
            'terminationdate': item['terminationdate'].replace('-', '/'),
            'colleaguednumber': item['dnumber'],
            'worklocation': item['worklocationname'],
            'jobfunction': item['jobfunctionname']
        }
    }, collection_data)) if collection_data else []

    return new_supervisor[0] if new_supervisor else ''


# pylint: disable=too-many-boolean-expressions
# pylint: disable=too-many-return-statements
# pylint: disable=too-many-branches
# pylint: disable=chained-comparison
# pylint: disable=too-many-statements
def get_ususer_mappercombination_from_dagrunconf():

    mapper_combination = ''
    dag_run_conf = get_current_context()['dag_run'].conf

    salary_hourly = dag_run_conf['salaryhourly']
    regular_temp = dag_run_conf['regulartemp']
    fullparttime = dag_run_conf['fullparttime']
    homestate = dag_run_conf['homestate']
    standardhours = get_number(dag_run_conf['standardhours'])
    division = dag_run_conf['division']
    jobcode = dag_run_conf['jobcode']

    if salary_hourly == 'S' and regular_temp == 'R' and fullparttime == 'F':
        mapper_combination = 'S/R/F/Null/Null/All/Null'
    if salary_hourly == 'S' and regular_temp == 'R' and fullparttime == 'P' and homestate != 'CA':
        mapper_combination = 'S/R/P/All State except CA/Null/All/Null'
    if salary_hourly == 'S' and regular_temp == 'R' and fullparttime == 'P' and homestate == 'CA':
        mapper_combination = 'S/R/P/CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'F' and homestate != 'CA':
        mapper_combination = 'H/R/F/All State except CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'F' and homestate == 'CA':
        mapper_combination = 'H/R/F/CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'P' and homestate != 'CA' and (
            59 < standardhours < 79 if standardhours else False):
        if division in ('CCG', 'CCN'):
            mapper_combination = 'H/R/P/All State except CA/Null/Carrington College/60-78'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'P' and homestate != 'CA':
        if division not in ('CCG', 'CCN'):
            mapper_combination = 'H/R/P/All State except CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'P' and homestate == 'CA' and (
            59 < standardhours < 79 if standardhours else False):
        if division in ('CCG', 'CCN'):
            mapper_combination = 'H/R/P/CA/Null/Carrington College/60-78'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'P' and homestate == 'CA':
        if division not in ('CCG', 'CCN'):
            mapper_combination = 'H/R/P/CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'F' and homestate != 'CA':
        mapper_combination = 'H/T/F/All State except CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'F' and homestate == 'CA':
        mapper_combination = 'H/T/F/CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate != 'CA':
        if not jobcode.startswith(('FW', 'SW')):
            mapper_combination = 'H/T/P/All State except CA/not in FW,SW/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate == 'CA':
        if not jobcode.startswith(('FW', 'SW')):
            mapper_combination = 'H/T/P/CA/not in FW,SW/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate != 'CA' and jobcode == 'FW0008':
        mapper_combination = 'H/T/P/All State except CA/FW0008/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate == 'CA':
        if jobcode.startswith(('FW', 'SW')):
            mapper_combination = 'H/T/P/CA/SW/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P':
        if homestate in ('GA', 'MO', 'NJ'):
            if jobcode.startswith(('FW', 'SW')):
                if jobcode != 'FW0008':
                    mapper_combination = 'H/T/P/GA/SW0002/All/Null'
        if homestate not in ('GA', 'MO', 'NJ', 'CA'):
            if jobcode.startswith(('FW', 'SW')):
                if jobcode != 'FW0008':
                    mapper_combination = 'H/T/P/All State except CA, GA, MO, NJ/SW0002/All/Null'
    if get_task_state('process_adduser_cr2021') == 'success' or get_task_state('process_updateuser_cr2021') == 'success':
        if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate != 'CA' and jobcode == 'FW0010':
            mapper_combination = 'H/T/P/All State except CA/FW0010/All/Null'
        if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate == 'CA' and jobcode == 'FW0010':
            mapper_combination = 'H/T/P/CA/FW0010/All/Null'
    return mapper_combination


def get_mapper_entries_from_adtalem_mapperfile(identifier, mapper='old'):
    if mapper == 'old':
        return [x for x in adtalem_mapper_file_old if x[
            'identifier(salary_hourly/reg_temp/full_part_time/state/job_code/division/standard_hours)'] == identifier]
    return [x for x in adtalem_mapper_file if x[
        'identifier(salary_hourly/reg_temp/full_part_time/state/job_code/division/standard_hours)'] == identifier]


def get_mapper_entry_value(mapper_type):
    return next(iter(filter(lambda x: x['mapper_type'] == mapper_type, result('get_mapper_entries'))), {}).get('value', '')


def get_required_policysets(response):
    policysets_uris = []
    timesheet_template = get_mapper_entry_value(
        'Timesheet Template')
    timeoff_template = get_mapper_entry_value(
        'Time Off Template')
    punchentrypolicy_template = get_mapper_entry_value(
        'Punch Entry Policy')

    if timesheet_template:
        timesheet_template_uri = find_first_by_attr_and_get_attr(
            response, 'displayText', timesheet_template, 'uri', '')
        if timesheet_template_uri:
            set_result(timesheet_template_uri,
                       'timesheet_template_uri')
            policysets_uris.append(timesheet_template_uri)

    if timeoff_template:
        timeoff_template_uri = find_first_by_attr_and_get_attr(
            response, 'displayText', timeoff_template, 'uri', '')
        if timeoff_template_uri:
            set_result(timeoff_template_uri,
                       'timeoff_template_uri')
            policysets_uris.append(timeoff_template_uri)

    if punchentrypolicy_template:
        punchentrypolicy_uri = find_first_by_attr_and_get_attr(
            response, 'displayText', punchentrypolicy_template, 'uri', '')
        if punchentrypolicy_uri:
            policysets_uris.append(punchentrypolicy_uri)
    return policysets_uris


def get_timezone_mapper_entry(home_state):
    return find_first_by_attr_and_get_attr(adtalem_timezone_mapper, 'home_state', home_state, 'uri', '')


def get_policy_schedule_entries(adtalem_sicktime_timeoffpolicy_schedule_mapper_old):
    return next(iter(filter(lambda x: x["timeofftype"] == result(
        'get_timeofftype_uris_to_assign', 'sick_timeoff_name'), adtalem_sicktime_timeoffpolicy_schedule_mapper_old)), '')


def get_timeofftype_uris():

    timeoff_type_uris_to_assign = []
    required_timeofftypes = [x.strip() for x in result(
        'get_timeofftypes_from_mapper').split('|')]
    set_result(required_timeofftypes, 'required_timeofftypes')
    dag_run_conf = get_current_context()['dag_run'].conf
    ususer = dag_run_conf['ususer']
    fullparttime = dag_run_conf['fullparttime']
    homestate = dag_run_conf['homestate']
    worklocation = dag_run_conf['worklocation']
    replicon_timeoff_types = result('get_alltimeoff_types')

    if ususer == 'yes':
        for timeoff in required_timeofftypes:
            if 'sick' in timeoff.lower():
                sick_timeoff_name = ''
                if 'F' in fullparttime:
                    sick_timeoff_name = 'SICK - PERSONAL'
                else:
                    if homestate == 'CA':
                        sick_timeoff_name = 'Sick Leave'
                    elif homestate == 'MA':
                        sick_timeoff_name = 'Sick Leave - MA'
                    elif homestate == 'OR':
                        sick_timeoff_name = 'Sick Leave - OR'
                    elif homestate == 'NJ' and worklocation == 'North Brunswick':
                        sick_timeoff_name = 'Sick Leave - NBR'
                if sick_timeoff_name:
                    set_result(sick_timeoff_name, 'sick_timeoff_name')
                    sick_timeoffname_uri = find_first_by_attr_and_get_attr(
                        replicon_timeoff_types, 'displayText', sick_timeoff_name, 'uri', '')
                    if sick_timeoffname_uri:
                        set_result(sick_timeoffname_uri,
                                   'sick_timeoffname_uri')
                        timeoff_type_uris_to_assign.append(
                            sick_timeoffname_uri)
            else:
                timeoff_name_uri = find_first_by_attr_and_get_attr(
                    replicon_timeoff_types, 'displayText', timeoff, 'uri', '')
                if timeoff_name_uri:
                    timeoff_type_uris_to_assign.append(timeoff_name_uri)
    else:
        for timeoff in required_timeofftypes:
            timeoff_name_uri = find_first_by_attr_and_get_attr(
                replicon_timeoff_types, 'displayText', timeoff, 'uri', '')
            if timeoff_name_uri:
                timeoff_type_uris_to_assign.append(timeoff_name_uri)

    return timeoff_type_uris_to_assign


def get_ptotimeofftype_uri():
    replicon_timeoff_types = result('get_alltimeoff_types')
    pto_timeofftype_uri = find_first_by_attr_and_get_attr(
        replicon_timeoff_types, 'displayText', 'Paid Time Off', 'uri', '')
    set_result(pto_timeofftype_uri, 'pto_timeofftype_uri')
    return pto_timeofftype_uri


def get_vacationtimeoff_to_assign():
    required_timeofftypes = result(
        'get_timeofftype_uris_to_assign', 'required_timeofftypes')
    return next(iter(filter(lambda x: x == 'Vacation', required_timeofftypes)), '')


def get_jobcode_timeoff_jobcode_mapper(jobcode):
    return next(iter(filter(lambda x: x["jobcode"] == jobcode, adtalem_time_off_jobcode_mapper_old)), '')


def get_final_policy_mapper(mapper_type='old'):

    final_jobcode_lookup = result('get_required_timeoff_jobcode_mapper')['jobcode'] if result(
        'get_required_timeoff_jobcode_mapper') and result(
            'get_required_timeoff_jobcode_mapper')['jobcode'] else 'Null'

    dag_run_conf = get_current_context()['dag_run'].conf

    final_policy_lookup = 'CA' if dag_run_conf['homestate'] == 'CA' else 'Null'
    final_state_parttime_policy_lookup = f"{final_policy_lookup}/{dag_run_conf['fullparttime']}"
    final_division_policy_lookup = 'Carrington College' if dag_run_conf['division'] in (
        'CCN', 'CCG') else 'Null'

    final_standardhours_policy_lookup = 'Null'
    if dag_run_conf['standardhours'] and dag_run_conf['standardhours'].lower() != 'null':
        standardhours = get_number(dag_run_conf['standardhours'])
        if mapper_type == 'old':
            if final_division_policy_lookup == 'Null':
                if 59 < standardhours < 79:
                    final_standardhours_policy_lookup = '60-78'
            else:
                if 39 < standardhours < 79:
                    final_standardhours_policy_lookup = '40-78'
                elif standardhours < 40 or standardhours > 79:
                    final_standardhours_policy_lookup = 'Null'
        else:
            if 39 < standardhours < 79:
                final_standardhours_policy_lookup = '40-78'
            elif standardhours < 40 or standardhours > 79:
                final_standardhours_policy_lookup = 'Null'

    lookup_values = {
        'jobcode': final_jobcode_lookup,
        'state_partfull': final_state_parttime_policy_lookup,
        'standardhours': final_standardhours_policy_lookup,
        'division': final_division_policy_lookup
    }
    policy_mapper = next(iter(filter(lambda item: all((
        item[k] == v for (k, v) in lookup_values.items())), (
        adtalem_timeoff_policy_mapper_vacation_old if mapper_type == 'old' else adtalem_timeoff_policy_mapper_pto))), {}).get('policy', '')

    final_policy_mapper = policy_mapper

    if policy_mapper.startswith(('S/R/F')) or policy_mapper.startswith(('H/R/F')):
        final_policy_mapper = 'RFT'
        if dag_run_conf['homestate'] == 'CA':
            final_policy_mapper = 'RFT-CA'

    return final_policy_mapper


def get_vacation_timeoff_policyschedule(adtalem_vacation_timeoffpolicy_schedule_mapper_old, policyname):
    return next(iter(filter(
        lambda x: x['schedulename'] == policyname, adtalem_vacation_timeoffpolicy_schedule_mapper_old)), '')


# pylint: disable=too-many-boolean-expressions
# pylint: disable=too-many-return-statements
# pylint: disable=too-many-branches
# pylint: disable=chained-comparison
# pylint: disable=too-many-statements
def get_replicon_mappercombination_from_report():

    mapper_combination = ''
    user_reportdata = result('parse_csv_user_data')

    salary_hourly = user_reportdata['Salary/Hourly']
    regular_temp = user_reportdata['Regular/Temp']
    fullparttime = user_reportdata['Full/Part Time']
    homestate = user_reportdata['Home State']
    standardhours = get_number(user_reportdata['Standard Hours'])
    division = user_reportdata['Division']
    jobcode = user_reportdata['Job Code']

    if salary_hourly == 'S' and regular_temp == 'R' and fullparttime == 'F':
        mapper_combination = 'S/R/F/Null/Null/All/Null'
    if salary_hourly == 'S' and regular_temp == 'R' and fullparttime == 'P' and homestate != 'CA':
        mapper_combination = 'S/R/P/All State except CA/Null/All/Null'
    if salary_hourly == 'S' and regular_temp == 'R' and fullparttime == 'P' and homestate == 'CA':
        mapper_combination = 'S/R/P/CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'F' and homestate != 'CA':
        mapper_combination = 'H/R/F/All State except CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'F' and homestate == 'CA':
        mapper_combination = 'H/R/F/CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'P' and homestate != 'CA' and (
            59 < standardhours < 79 if standardhours else False):
        if division in ('CCG', 'CCN'):
            mapper_combination = 'H/R/P/All State except CA/Null/Carrington College/60-78'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'P' and homestate != 'CA':
        if division not in ('CCG', 'CCN'):
            mapper_combination = 'H/R/P/All State except CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'P' and homestate == 'CA' and (
            59 < standardhours < 79 if standardhours else False):
        if division in ('CCG', 'CCN'):
            mapper_combination = 'H/R/P/CA/Null/Carrington College/60-78'
    if salary_hourly == 'H' and regular_temp == 'R' and fullparttime == 'P' and homestate == 'CA':
        if division not in ('CCG', 'CCN'):
            mapper_combination = 'H/R/P/CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'F' and homestate != 'CA':
        mapper_combination = 'H/T/F/All State except CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'F' and homestate == 'CA':
        mapper_combination = 'H/T/F/CA/Null/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate != 'CA':
        if not jobcode.startswith(('FW', 'SW')):
            mapper_combination = 'H/T/P/All State except CA/not in FW,SW/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate == 'CA':
        if not jobcode.startswith(('FW', 'SW')):
            mapper_combination = 'H/T/P/CA/not in FW,SW/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate != 'CA' and jobcode == 'FW0008':
        mapper_combination = 'H/T/P/All State except CA/FW0008/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate == 'CA':
        if jobcode.startswith(('FW', 'SW')):
            mapper_combination = 'H/T/P/CA/SW/All/Null'
    if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P':
        if homestate in ('GA', 'MO', 'NJ'):
            if jobcode.startswith(('FW', 'SW')):
                if jobcode != 'FW0008':
                    mapper_combination = 'H/T/P/GA/SW0002/All/Null'
        if homestate not in ('GA', 'MO', 'NJ', 'CA'):
            if jobcode.startswith(('FW', 'SW')):
                if jobcode != 'FW0008':
                    mapper_combination = 'H/T/P/All State except CA, GA, MO, NJ/SW0002/All/Null'
    if get_task_state('process_updateuser_cr2021') == 'success':
        if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate != 'CA' and jobcode == 'FW0010':
            mapper_combination = 'H/T/P/All State except CA/FW0010/All/Null'
        if salary_hourly == 'H' and regular_temp == 'T' and fullparttime == 'P' and homestate == 'CA' and jobcode == 'FW0010':
            mapper_combination = 'H/T/P/CA/FW0010/All/Null'
    return mapper_combination


def get_replicon_mapper_entries_from_adtalem_mapperfile():
    mapper = 'new' if result('process_updateuser_cr2021') else 'old'
    identifier = result('get_replicon_mappercombination')
    user_reportdata = result('parse_csv_user_data')
    cocode = user_reportdata['CoCode']
    if cocode and cocode in ('ACAFR', 'ACADE', 'ACAAU', 'ACAUK'):
        identifier = cocode
    if mapper == 'old':
        return [x for x in adtalem_mapper_file_old if x[
            'identifier(salary_hourly/reg_temp/full_part_time/state/job_code/division/standard_hours)'] == identifier]
    return [x for x in adtalem_mapper_file if x[
            'identifier(salary_hourly/reg_temp/full_part_time/state/job_code/division/standard_hours)'] == identifier]


def get_required_approvalpaths_updateuser(response, mapper_key, csv_result_value):
    entry_val = result('specific_paygrade_timeoff') if result(
        'specific_paygrade_timeoff') else get_mapper_entry_value(mapper_key)
    if entry_val and entry_val != csv_result_value:
        return find_first_by_attr_and_get_attr(
            response, 'displayText', entry_val, 'uri', '')
    return ''


def get_timeoff_trigger_var():
    timeoff_trigger = 'no'
    dag_run_conf = get_current_context()['dag_run'].conf
    final_mapper_combination = result(
        'get_ususer_mappercombination') if dag_run_conf['ususer'] == 'yes' else dag_run_conf['paygroup']
    replicon_mapper_combination = result('get_replicon_mappercombination')
    if final_mapper_combination != replicon_mapper_combination:
        timeoff_trigger = 'yes'
    if result('parse_csv_user_data')['User Status'] == 'Disabled' and 'T' not in dag_run_conf['activeleavestatus']:
        timeoff_trigger = 'yes'
    if result('parse_csv_user_data')['Regular/Temp'] == 'T' and dag_run_conf['regulartemp'] == 'R':
        timeoff_trigger = 'yes'

    return timeoff_trigger


def get_temp_to_regular_var():
    temptoregular = 'no'
    dag_run_conf = get_current_context()['dag_run'].conf
    replicon_job_code = result('parse_csv_user_data')['Job Code']
    replicon_user_status = result('parse_csv_user_data')['User Status']
    if result('update_jobcode_udf') or result('update_jobcode_udf2'):
        if replicon_job_code in ('CH0324', 'CH0367', 'FA0115', 'FA0116', 'FA0117', 'CA1011') and dag_run_conf[
                'jobcode'] not in ('CH0324', 'CH0367', 'FA0115', 'FA0116', 'FA0117', 'CA1011') and replicon_user_status == 'Disabled':
            temptoregular = 'yes'

    return temptoregular


def get_final_timeoffs_to_assign():

    timeoff_uris = []
    if result('get_vacationbuyup_timeoff_uri'):
        timeoff_uris.append(result('get_vacationbuyup_timeoff_uri'))
    dag_run_conf = get_current_context()['dag_run'].conf
    homestate = dag_run_conf['homestate']
    worklocation = dag_run_conf['worklocation']
    sick_timeoff_name = ''
    if homestate == 'CA':
        sick_timeoff_name = 'Sick Leave'
    elif homestate == 'MA':
        sick_timeoff_name = 'Sick Leave - MA'
    elif homestate == 'OR':
        sick_timeoff_name = 'Sick Leave - OR'
    elif homestate == 'NJ' and worklocation == 'North Brunswick':
        sick_timeoff_name = 'Sick Leave - NBR'

    set_result(sick_timeoff_name, 'sick_timeoff_name')
    sick_timeoff_uri = find_first_by_attr_and_get_attr(result(
        'get_enabled_replicon_timeoffs'), 'displayText', sick_timeoff_name, 'uri', '')
    set_result(sick_timeoff_uri, 'sick_timeoff_uri')
    if sick_timeoff_uri:
        timeoff_uris.append(sick_timeoff_uri)
    return timeoff_uris


def get_mapper_lookup():
    dag_run_conf = get_current_context()['dag_run'].conf
    new_mapper_lookup = dag_run_conf.get('newmapperlookup', '')
    if dag_run_conf.get('ususer') and dag_run_conf['ususer'] != 'yes':
        if dag_run_conf['regulartemp'] == 'R' and dag_run_conf['fullparttime'] == 'F':
            new_mapper_lookup = f"{dag_run_conf.get('newmapperlookup', '')}/RF"
    return new_mapper_lookup


def get_timeoffs_not_in_newset(key='return_value'):
    timeoff_uris_not_in_newset = []
    uris_previously_assigned = [x['uri'] for x in result(
        'get_assigned_timeofftypes')['timeOffTypeAssignmentsDetails']['timeOffTypes']]

    add_timeoff_typeuris = result(
        'get_timeofftype_uris_to_assign', key)

    for uri in uris_previously_assigned:
        uri_to_assign = [x for x in add_timeoff_typeuris if x == uri]
        if not uri_to_assign:
            timeoff_uris_not_in_newset.append(uri)

    return timeoff_uris_not_in_newset


def get_timeoffs_not_in_oldset(key='return_value'):
    timeoff_uris_not_in_oldset = []
    uris_previously_assigned = [x['uri'] for x in result(
        'get_assigned_timeofftypes')['timeOffTypeAssignmentsDetails']['timeOffTypes']]

    add_timeoff_typeuris = result(
        'get_timeofftype_uris_to_assign', key)

    for uri in add_timeoff_typeuris:
        uri_to_assign = [x for x in uris_previously_assigned if x == uri]
        if not uri_to_assign:
            timeoff_uris_not_in_oldset.append(uri)

    return timeoff_uris_not_in_oldset


def construct_policyschedule():
    policy_set_schedule = result('get_existingpolicy_schedule_for_timeoff')
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                if effective_datetime and effective_datetime.date() < datetime.now().date():
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)
    return policy_schedule_entries


def get_salary_details(paygroup, salaryhourly):
    if paygroup in ('STKITT', 'STKLOC'):
        return 'S'
    if paygroup in ('BRBEXP', 'BRBLOC'):
        return salaryhourly
    return ''


def get_employeetype_details(salaryhourly):
    if salaryhourly == 'S':
        return 'Exempt'
    return 'Non-Exempt'


def get_mapper_entries_from_adtalem_caribbean_mapperfile(paygroup, jobcode):
    identifier = jobcode
    if paygroup in ('BRBEXP', 'BRBLOC'):
        identifier = paygroup
    if result('get_salary'):
        return [x for x in adtalem_caribbean_master_mapper if x['salary_hourly'] == result('get_salary') and x[
            'identifier'] == identifier]
    return [x for x in adtalem_caribbean_master_mapper if x['identifier'] == identifier]


def get_timeofftype_uris_caribbean():
    timeoff_type_uris_to_assign = []
    required_timeofftypes = [x.strip() for x in result(
        'get_timeofftypes_from_mapper').split('|')]
    set_result(required_timeofftypes, 'required_timeofftypes')
    replicon_timeoff_types = result('get_alltimeoff_types')

    for timeoff in required_timeofftypes:
        timeoff_name_uri = find_first_by_attr_and_get_attr(
            replicon_timeoff_types, 'displayText', timeoff, 'uri', '')
        if timeoff_name_uri:
            timeoff_type_uris_to_assign.append(timeoff_name_uri)

    return timeoff_type_uris_to_assign


def get_salary_timeoff():
    dag_run_conf = get_current_context()['dag_run'].conf
    return dag_run_conf['salaryhourly'] if 'BB' in dag_run_conf['jobcode'] else ''


def get_us_timeoff_policy_mapper(pay_grade, salary_hourly):
    return next(iter(filter(lambda x: x['paygrade'] == pay_grade and x[
        'salary_hourly'] == salary_hourly, adtalem_us_timeoff_policy_mapper_fto)), '')


def get_specific_paygrade_timeoff(salary_hourly):
    us_timeoff_policy_mapper_fto = result('get_us_timeoff_policy_mapper_fto')
    if us_timeoff_policy_mapper_fto and us_timeoff_policy_mapper_fto['paygrade'] and salary_hourly == 'S':
        return 'FTO - Supervisor'
    return ''


def get_flex_existing_new_vars(replicon_salaryhourly, paygroup, replicon_paygrade):
    flex_existing_var = 'no'
    flex_new_var = 'no'

    get_us_timeoff_policy_mapper_fto_s_existing = result(
        'get_us_timeoff_policy_mapper_fto_s_existing')
    if get_us_timeoff_policy_mapper_fto_s_existing and replicon_salaryhourly == 'S' and paygroup != 'ACA' and replicon_paygrade:
        flex_existing_var = 'yes'

    get_us_timeoff_policy_mapper_fto_any_existing = result(
        'get_us_timeoff_policy_mapper_fto_any_existing')
    if get_us_timeoff_policy_mapper_fto_any_existing and replicon_paygrade:
        flex_existing_var = 'yes'

    get_us_timeoff_policy_mapper_fto_s_new = result(
        'get_us_timeoff_policy_mapper_fto_s_new')
    if get_us_timeoff_policy_mapper_fto_s_new and replicon_salaryhourly == 'S' and paygroup != 'ACA' and replicon_paygrade:
        flex_new_var = 'yes'

    get_us_timeoff_policy_mapper_fto_any_new = result(
        'get_us_timeoff_policy_mapper_fto_any_new')
    if get_us_timeoff_policy_mapper_fto_any_new and replicon_paygrade:
        flex_new_var = 'yes'
    return {
        'flex_existing_var': flex_existing_var,
        'flex_new_var': flex_new_var
    }


def search_chamberlain_jobcode_mapper(jobcode):
    return next(iter(filter(lambda x: x["jobcode"] == jobcode, adtalem_time_off_jobcode_mapper_ch)), '')


def get_trigger_pto_policyupdate():

    trigger_pto_policyupdate = 'no'

    search_chamberlain_jobcode_mapper_newjobcode = result(
        'search_chamberlain_jobcode_mapper_newjobcode')
    search_chamberlain_jobcode_mapper_oldjobcode = result(
        'search_chamberlain_jobcode_mapper_oldjobcode')
    if search_chamberlain_jobcode_mapper_newjobcode and not search_chamberlain_jobcode_mapper_oldjobcode:
        trigger_pto_policyupdate = 'yes'
    if not search_chamberlain_jobcode_mapper_newjobcode and search_chamberlain_jobcode_mapper_oldjobcode:
        trigger_pto_policyupdate = 'yes'

    return trigger_pto_policyupdate


def get_rft_rpt_policysets(effective_date_0_years, effective_date_5_years, effective_date_10_years, existing_global_policysets):
    policy_sets = []

    for item in existing_global_policysets:
        if item['startOffset']['offsetValue'] == 0:
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': item['policySet']
            })
        if item['startOffset']['offsetValue'] == 5:
            policy_sets.append({
                'description': f"Effective on {effective_date_5_years.month}/{effective_date_5_years.day}/{effective_date_5_years.year}",
                'effectiveDate': {
                    'day': effective_date_5_years.day,
                    'month': effective_date_5_years.month,
                    'year': effective_date_5_years.year
                },
                'policySet': item['policySet']
            })
        if item['startOffset']['offsetValue'] == 10:
            policy_sets.append({
                'description': f"Effective on {effective_date_10_years.month}/{effective_date_10_years.day}/{effective_date_10_years.year}",
                'effectiveDate': {
                    'day': effective_date_10_years.day,
                    'month': effective_date_10_years.month,
                    'year': effective_date_10_years.year
                },
                'policySet': item['policySet']
            })
    return policy_sets


def get_rftca_rptca_policysets(effective_date_0_years, effective_date_5_years, effective_date_10_years, existing_global_policysets):
    policy_sets = []

    for idx, item in enumerate(existing_global_policysets):
        if idx == 0:
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': item['policySet']
            })
        if idx not in (0, len(existing_global_policysets) - 1):
            policy_sets.append({
                'description': f"Effective on {effective_date_5_years.month}/{effective_date_5_years.day}/{effective_date_5_years.year}",
                'effectiveDate': {
                    'day': effective_date_5_years.day,
                    'month': effective_date_5_years.month,
                    'year': effective_date_5_years.year
                },
                'policySet': item['policySet']
            })
        if idx == len(existing_global_policysets) - 1:
            policy_sets.append({
                'description': f"Effective on {effective_date_10_years.month}/{effective_date_10_years.day}/{effective_date_10_years.year}",
                'effectiveDate': {
                    'day': effective_date_10_years.day,
                    'month': effective_date_10_years.month,
                    'year': effective_date_10_years.year
                },
                'policySet': item['policySet']
            })
    return policy_sets


# pylint:disable=too-many-nested-blocks
def get_rftch_rptch_policysets(effective_date_0_years, existing_global_policysets, policyname):
    policy_sets = []

    existing_policy_sets = [x['policySet']
                            for x in existing_global_policysets][0]
    for idx, x in enumerate(existing_policy_sets['timeOffBalanceEventScripts']):
        if x['scriptTarget']['name'] == 'Yearly Reset':
            yearly_reset_policy_set = x
            if yearly_reset_policy_set['additionalParameters']:
                for i, item in enumerate(yearly_reset_policy_set['additionalParameters']):
                    if item["keyUri"] == "urn:replicon:script-key:parameter:periodic-reset-option":
                        yearly_reset_policy_set['additionalParameters'][i] = {
                            "keyUri": "urn:replicon:script-key:parameter:periodic-reset-option",
                            "value": {
                                "uri": "urn:replicon:time-off-policy-reset-option:reset-balance-to-specific-value"
                            }
                        }
                    if item['keyUri'] == 'urn:replicon:script-key:parameter:reset-balance-amount':
                        yearly_reset_policy_set['additionalParameters'][i] = {
                            "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                            "value": {
                                "number": 0.0
                            }
                        }
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = yearly_reset_policy_set
        if x['scriptTarget']['name'] == 'Monthly Accrual':
            monthly_accrual_policy_set = x
            if monthly_accrual_policy_set['additionalParameters']:
                for i2, item2 in enumerate(monthly_accrual_policy_set['additionalParameters']):
                    if item2['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                        if policyname == 'RPT-CH':
                            monthly_accrual_policy_set['additionalParameters'][i2] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": 40.0
                                }
                            }
                        else:
                            monthly_accrual_policy_set['additionalParameters'][i2] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": 80.0
                                }
                            }
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = monthly_accrual_policy_set

    policy_sets.append({
        'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
        'effectiveDate': {
            'day': effective_date_0_years.day,
            'month': effective_date_0_years.month,
            'year': effective_date_0_years.year
        },
        'policySet': existing_policy_sets
    })
    return policy_sets


def get_rftchca_rptchca_policysets(effective_date_0_years, existing_global_policysets):
    policy_sets = []

    existing_policy_sets = [x['policySet']
                            for x in existing_global_policysets][0]
    for idx, x in enumerate(existing_policy_sets['timeOffBalanceEventScripts']):
        if x['scriptTarget']['name'] == 'Yearly Reset':
            yearly_reset_policy_set = x
            if yearly_reset_policy_set['additionalParameters']:
                yearly_reset_policy_set['additionalParameters'] = [{
                    "keyUri": 'urn:replicon:script-key:parameter:daily-maximum-balance-amount',
                    "value": {
                        "number": 160
                    }
                }]
                yearly_reset_policy_set['scriptTarget']['uri'] = result(
                    'get_maxbal_script_uri')
                yearly_reset_policy_set['scriptTarget']['name'] = 'Max Balance Limit'
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = yearly_reset_policy_set
        if x['scriptTarget']['name'] == 'Monthly Accrual':
            monthy_accrual_policy_set = x
            if monthy_accrual_policy_set['additionalParameters']:
                for i, item in enumerate(monthy_accrual_policy_set['additionalParameters']):
                    if item['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                        monthy_accrual_policy_set['additionalParameters'][i] = {
                            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                            "value": {
                                "number": 80.0
                            }
                        }
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = monthy_accrual_policy_set

    policy_sets.append({
        'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
        'effectiveDate': {
            'day': effective_date_0_years.day,
            'month': effective_date_0_years.month,
            'year': effective_date_0_years.year
        },
        'policySet': existing_policy_sets
    })
    return policy_sets


def get_pto_policy_assignments(type_policy):
    dag_run_conf = get_current_context()['dag_run'].conf

    effective_date_0_years = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y')

    effective_date_5_years = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y') + relativedelta(months=+60)

    effective_date_10_years = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y') + relativedelta(months=+120)

    existing_global_policysets = result('get_pto_policyset')

    if type_policy == 'rft_rpt':
        return get_rft_rpt_policysets(effective_date_0_years, effective_date_5_years, effective_date_10_years, existing_global_policysets)
    if type_policy == 'rftca_rptca':
        return get_rftca_rptca_policysets(effective_date_0_years, effective_date_5_years, effective_date_10_years, existing_global_policysets)
    if type_policy == 'rftch_rptch':
        return get_rftch_rptch_policysets(effective_date_0_years, existing_global_policysets, dag_run_conf['policyname'])
    if type_policy == 'rftchca_rptchca':
        return get_rftchca_rptchca_policysets(effective_date_0_years, existing_global_policysets)
    return ''


def get_rft_rpt_policysets_rehire(effective_date_0_years, effective_date_5_years, effective_date_10_years, existing_global_policysets):
    policy_sets = []

    for item in existing_global_policysets:
        if item['startOffset']['offsetValue'] == 0:
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': item['policySet']
            })
        if item['startOffset']['offsetValue'] == 5:
            policy_sets.append({
                'description': f"Effective on {effective_date_5_years.month}/{effective_date_5_years.day}/{effective_date_5_years.year}",
                'effectiveDate': {
                    'day': effective_date_5_years.day,
                    'month': effective_date_5_years.month,
                    'year': effective_date_5_years.year
                },
                'policySet': item['policySet']
            })
        if item['startOffset']['offsetValue'] == 10:
            policy_sets.append({
                'description': f"Effective on {effective_date_10_years.month}/{effective_date_10_years.day}/{effective_date_10_years.year}",
                'effectiveDate': {
                    'day': effective_date_10_years.day,
                    'month': effective_date_10_years.month,
                    'year': effective_date_10_years.year
                },
                'policySet': item['policySet']
            })
    return policy_sets


def get_rftca_rptca_policysets_rehire(effective_date_0_years, effective_date_5_years, effective_date_10_years, existing_global_policysets):
    policy_sets = []

    for idx, item in enumerate(existing_global_policysets):
        timeoff_balance_event_scripts = item['timeOffBalanceEventScripts']
        monthly_accrual_policyset = json.loads(json.dumps(
            [x for x in timeoff_balance_event_scripts if x[
                'scriptTarget']['name'] == 'Monthly Accrual']).replace(
            '[{"additionalParameters"', '{"additionalParameters"').replace("}}]", "}}").replace(
                    '}},"scriptTarget', '}}],"scriptTarget'))
        max_balance = float(find_first_by_attr_and_get_attr(monthly_accrual_policyset, 'keyUri',
                            'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')) * 1.5
        for idx2, item2 in timeoff_balance_event_scripts:
            if item2['scriptTarget']['name'] == 'Yearly Reset':
                existing_global_policysets[idx2] = {
                    'additionalParameters': [{
                        'keyUri': 'urn:replicon:script-key:parameter:daily-maximum-balance-amount',
                        'value': {
                            'number': max_balance
                        }
                    }],
                    'scriptTarget': {
                        'uri': result('get_maxbal_script_uri')
                    }
                }
        if idx == 0:
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': item['policySet']
            })
        if idx not in (0, len(existing_global_policysets) - 1):
            policy_sets.append({
                'description': f"Effective on {effective_date_5_years.month}/{effective_date_5_years.day}/{effective_date_5_years.year}",
                'effectiveDate': {
                    'day': effective_date_5_years.day,
                    'month': effective_date_5_years.month,
                    'year': effective_date_5_years.year
                },
                'policySet': item['policySet']
            })
        if idx == len(existing_global_policysets) - 1:
            policy_sets.append({
                'description': f"Effective on {effective_date_10_years.month}/{effective_date_10_years.day}/{effective_date_10_years.year}",
                'effectiveDate': {
                    'day': effective_date_10_years.day,
                    'month': effective_date_10_years.month,
                    'year': effective_date_10_years.year
                },
                'policySet': item['policySet']
            })
    return policy_sets


def get_rftch_rptch_policysets_rehire(effective_date_0_years, existing_global_policysets, policyname):
    policy_sets = []

    existing_policy_sets = [x['policySet']
                            for x in existing_global_policysets][0]
    for idx, x in enumerate(existing_policy_sets['timeOffBalanceEventScripts']):
        if x['scriptTarget']['name'] == 'Yearly Reset':
            yearly_reset_policy_set = x
            if yearly_reset_policy_set['additionalParameters']:
                for i, item in enumerate(yearly_reset_policy_set['additionalParameters']):
                    if item['keyUri'] == 'urn:replicon:script-key:parameter:reset-balance-amount':
                        yearly_reset_policy_set['additionalParameters'][i] = {
                            "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                            "value": {
                                "number": 0.0
                            }
                        }
                    elif item['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                        if policyname == 'RPT-CH':
                            yearly_reset_policy_set['additionalParameters'][i] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": 40.0
                                }
                            }
                        else:
                            yearly_reset_policy_set['additionalParameters'][i] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": 80.0
                                }
                            }
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = yearly_reset_policy_set

    policy_sets.append({
        'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
        'effectiveDate': {
            'day': effective_date_0_years.day,
            'month': effective_date_0_years.month,
            'year': effective_date_0_years.year
        },
        'policySet': existing_policy_sets
    })
    return policy_sets


def get_rftcha_rptcha_policysets_rehire(effective_date_0_years, existing_global_policysets):
    policy_sets = []

    existing_policy_sets = [x['policySet']
                            for x in existing_global_policysets][0]
    for idx, x in enumerate(existing_policy_sets['timeOffBalanceEventScripts']):
        if x['scriptTarget']['name'] == 'Yearly Reset':
            yearly_reset_policy_set = x
            if yearly_reset_policy_set['additionalParameters']:
                for i, item in enumerate(yearly_reset_policy_set['additionalParameters']):
                    if item['keyUri'] == 'urn:replicon:script-key:parameter:daily-maximum-balance-amount':
                        yearly_reset_policy_set['additionalParameters'][i] = {
                            "keyUri": 'urn:replicon:script-key:parameter:daily-maximum-balance-amount',
                            "value": {
                                "number": 160
                            }
                        }
                yearly_reset_policy_set['scriptTarget']['uri'] = result(
                    'get_maxbal_script_uri')
                yearly_reset_policy_set['scriptTarget']['name'] = 'Max Balance Limit'
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = yearly_reset_policy_set
        if x['scriptTarget']['name'] == 'Monthly Accrual':
            monthy_accrual_policy_set = x
            if monthy_accrual_policy_set['additionalParameters']:
                for i, item in enumerate(monthy_accrual_policy_set['additionalParameters']):
                    if item['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                        monthy_accrual_policy_set['additionalParameters'][i] = {
                            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                            "value": {
                                "number": 80.0
                            }
                        }
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = monthy_accrual_policy_set

    policy_sets.append({
        'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
        'effectiveDate': {
            'day': effective_date_0_years.day,
            'month': effective_date_0_years.month,
            'year': effective_date_0_years.year
        },
        'policySet': existing_policy_sets
    })
    return policy_sets


def get_pto_policy_assignments_rehire(type_policy):
    dag_run_conf = get_current_context()['dag_run'].conf

    effective_date_0_years = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y')

    effective_date_5_years = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y') + relativedelta(months=+60)

    effective_date_10_years = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y') + relativedelta(months=+120)

    existing_global_policysets = result('get_pto_policyset')

    if type_policy == 'rft_rpt':
        return get_rft_rpt_policysets_rehire(effective_date_0_years, effective_date_5_years, effective_date_10_years, existing_global_policysets)
    if type_policy == 'rftca_rptca':
        return get_rftca_rptca_policysets_rehire(effective_date_0_years, effective_date_5_years, effective_date_10_years, existing_global_policysets)
    if type_policy == 'rftch_rptch':
        return get_rftch_rptch_policysets_rehire(effective_date_0_years, existing_global_policysets, dag_run_conf['policyname'])
    if type_policy == 'rftchca_rptchca':
        return get_rftcha_rptcha_policysets_rehire(effective_date_0_years, existing_global_policysets)
    return ''


def get_rftch_rptch_policysets_update(effective_date_0_years, existing_global_policysets, policyname):
    policy_sets = []

    existing_policy_sets = [x['policySet']
                            for x in existing_global_policysets][0]
    for idx, x in enumerate(existing_policy_sets['timeOffBalanceEventScripts']):
        if x['scriptTarget']['name'] == 'Yearly Reset':
            yearly_reset_policy_set = x
            if yearly_reset_policy_set['additionalParameters']:
                for i, item in enumerate(yearly_reset_policy_set['additionalParameters']):
                    if item['keyUri'] == 'urn:replicon:script-key:parameter:reset-balance-amount':
                        yearly_reset_policy_set['additionalParameters'][i] = {
                            "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                            "value": {
                                "number": 0.0
                            }
                        }
                    elif item['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                        if policyname == 'RPT-CH':
                            yearly_reset_policy_set['additionalParameters'][i] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": 40.0
                                }
                            }
                        else:
                            yearly_reset_policy_set['additionalParameters'][i] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": 80.0
                                }
                            }
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = yearly_reset_policy_set

    policy_sets.append({
        'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
        'effectiveDate': {
            'day': effective_date_0_years.day,
            'month': effective_date_0_years.month,
            'year': effective_date_0_years.year
        },
        'policySet': existing_policy_sets
    })
    return policy_sets


def get_rftchca_rptchca_policysets_update(effective_date_0_years, existing_global_policysets):
    policy_sets = []

    existing_policy_sets = [x['policySet']
                            for x in existing_global_policysets][0]
    for idx, x in enumerate(existing_policy_sets['timeOffBalanceEventScripts']):
        if x['scriptTarget']['name'] == 'Yearly Reset':
            yearly_reset_policy_set = x
            if yearly_reset_policy_set['additionalParameters']:
                for i, item in enumerate(yearly_reset_policy_set['additionalParameters']):
                    if item['keyUri'] == 'urn:replicon:script-key:parameter:daily-maximum-balance-amount':
                        yearly_reset_policy_set['additionalParameters'][i] = {
                            "keyUri": 'urn:replicon:script-key:parameter:daily-maximum-balance-amount',
                            "value": {
                                "number": 160
                            }
                        }
                yearly_reset_policy_set['scriptTarget']['uri'] = result(
                    'get_maxbal_script_uri')
                yearly_reset_policy_set['scriptTarget']['name'] = 'Max Balance Limit'
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = yearly_reset_policy_set
        if x['scriptTarget']['name'] == 'Monthly Accrual':
            monthy_accrual_policy_set = x
            if monthy_accrual_policy_set['additionalParameters']:
                for i, item in enumerate(monthy_accrual_policy_set['additionalParameters']):
                    if item['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                        monthy_accrual_policy_set['additionalParameters'][i] = {
                            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                            "value": {
                                "number": 80.0
                            }
                        }
            existing_policy_sets['timeOffBalanceEventScripts'][idx] = monthy_accrual_policy_set

    policy_sets.append({
        'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
        'effectiveDate': {
            'day': effective_date_0_years.day,
            'month': effective_date_0_years.month,
            'year': effective_date_0_years.year
        },
        'policySet': existing_policy_sets
    })
    return policy_sets


def get_rft_rpt_less_than_5_policysets_update(effective_date_0_years, effective_date_5_years, effective_date_10_years,
                                              existing_global_policysets,
                                              balancetotransfer):
    policy_sets = []

    for item in existing_global_policysets:
        if item['startOffset']['offsetValue'] == 0:
            parsed_policyset = json.loads(json.dumps(item['policySet']).replace(
                '{"keyUri":"urn:replicon:script-key:parameter:amount", "value":{"number": 0.0}}',
                "{'keyUri': 'urn:replicon:script-key:parameter:amount','value': {'number': " + balancetotransfer + "}}"))
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': parsed_policyset
            })
        if item['startOffset']['offsetValue'] == 5:
            policy_sets.append({
                'description': f"Effective on {effective_date_5_years.month}/{effective_date_5_years.day}/{effective_date_5_years.year}",
                'effectiveDate': {
                    'day': effective_date_5_years.day,
                    'month': effective_date_5_years.month,
                    'year': effective_date_5_years.year
                },
                'policySet': item['policySet']
            })
        if item['startOffset']['offsetValue'] == 10:
            policy_sets.append({
                'description': f"Effective on {effective_date_10_years.month}/{effective_date_10_years.day}/{effective_date_10_years.year}",
                'effectiveDate': {
                    'day': effective_date_10_years.day,
                    'month': effective_date_10_years.month,
                    'year': effective_date_10_years.year
                },
                'policySet': item['policySet']
            })
    return policy_sets


def get_rftca_rptca_less_than_5_policysets_update(effective_date_0_years, effective_date_5_years,
                                                  effective_date_10_years, existing_global_policysets):
    policy_sets = []

    for idx, item in enumerate(existing_global_policysets):
        timeoff_balance_event_scripts = item['timeOffBalanceEventScripts']
        monthly_accrual_policyset = json.loads(json.dumps(
            [x for x in timeoff_balance_event_scripts if x[
                'scriptTarget']['name'] == 'Monthly Accrual']).replace(
            '[{"additionalParameters"', '{"additionalParameters"').replace("}}]", "}}").replace(
                    '}},"scriptTarget', '}}],"scriptTarget'))
        max_balance = float(find_first_by_attr_and_get_attr(monthly_accrual_policyset, 'keyUri',
                            'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')) * 1.5
        for idx2, item2 in timeoff_balance_event_scripts:
            if item2['scriptTarget']['name'] == 'Yearly Reset':
                existing_global_policysets[idx2] = {
                    'additionalParameters': [{
                        'keyUri': 'urn:replicon:script-key:parameter:daily-maximum-balance-amount',
                        'value': {
                            'number': max_balance
                        }
                    }],
                    'scriptTarget': {
                        'uri': result('get_maxbal_script_uri')
                    }
                }
        if idx == 0:
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': item['policySet']
            })
        if idx not in (0, len(existing_global_policysets) - 1):
            policy_sets.append({
                'description': f"Effective on {effective_date_5_years.month}/{effective_date_5_years.day}/{effective_date_5_years.year}",
                'effectiveDate': {
                    'day': effective_date_5_years.day,
                    'month': effective_date_5_years.month,
                    'year': effective_date_5_years.year
                },
                'policySet': item['policySet']
            })
        if idx == len(existing_global_policysets) - 1:
            policy_sets.append({
                'description': f"Effective on {effective_date_10_years.month}/{effective_date_10_years.day}/{effective_date_10_years.year}",
                'effectiveDate': {
                    'day': effective_date_10_years.day,
                    'month': effective_date_10_years.month,
                    'year': effective_date_10_years.year
                },
                'policySet': item['policySet']
            })
    return policy_sets


def get_rft_rpt_5_10_policysets_update(effective_date_0_years, effective_date_10_years, existing_global_policysets, balancetotransfer):
    policy_sets = []

    for item in existing_global_policysets:
        if item['startOffset']['offsetValue'] == 5:
            parsed_policyset = json.loads(json.dumps(item['policySet']).replace(
                '{"keyUri":"urn:replicon:script-key:parameter:amount", "value":{"number": 0.0}}',
                "{'keyUri': 'urn:replicon:script-key:parameter:amount','value': {'number': " + balancetotransfer + "}}"))
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': parsed_policyset
            })
        if item['startOffset']['offsetValue'] == 10:
            policy_sets.append({
                'description': f"Effective on {effective_date_10_years.month}/{effective_date_10_years.day}/{effective_date_10_years.year}",
                'effectiveDate': {
                    'day': effective_date_10_years.day,
                    'month': effective_date_10_years.month,
                    'year': effective_date_10_years.year
                },
                'policySet': item['policySet']
            })
    return policy_sets


def get_rftca_rptca_5_10_policysets_update(effective_date_0_years, effective_date_10_years, existing_global_policysets):
    policy_sets = []

    for idx, item in enumerate(existing_global_policysets):
        timeoff_balance_event_scripts = item['timeOffBalanceEventScripts']
        monthly_accrual_policyset = json.loads(json.dumps(
            [x for x in timeoff_balance_event_scripts if x[
                'scriptTarget']['name'] == 'Monthly Accrual']).replace(
            '[{"additionalParameters"', '{"additionalParameters"').replace("}}]", "}}").replace(
                    '}},"scriptTarget', '}}],"scriptTarget'))
        max_balance = float(find_first_by_attr_and_get_attr(monthly_accrual_policyset, 'keyUri',
                            'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')) * 1.5
        for idx2, item2 in timeoff_balance_event_scripts:
            if item2['scriptTarget']['name'] == 'Yearly Reset':
                existing_global_policysets[idx2] = {
                    'additionalParameters': [{
                        'keyUri': 'urn:replicon:script-key:parameter:daily-maximum-balance-amount',
                        'value': {
                            'number': max_balance
                        }
                    }],
                    'scriptTarget': {
                        'uri': result('get_maxbal_script_uri')
                    }
                }
        if idx == 1:
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': item['policySet']
            })
        if idx == 2:
            policy_sets.append({
                'description': f"Effective on {effective_date_10_years.month}/{effective_date_10_years.day}/{effective_date_10_years.year}",
                'effectiveDate': {
                    'day': effective_date_10_years.day,
                    'month': effective_date_10_years.month,
                    'year': effective_date_10_years.year
                },
                'policySet': item['policySet']
            })
    return policy_sets


def get_rft_rpt_greater_than_10_policysets_update(effective_date_0_years, existing_global_policysets, balancetotransfer):
    policy_sets = []

    for item in existing_global_policysets:
        if item['startOffset']['offsetValue'] == 5:
            parsed_policyset = json.loads(json.dumps(item['policySet']).replace(
                '{"keyUri":"urn:replicon:script-key:parameter:amount", "value":{"number": 0.0}}',
                "{'keyUri': 'urn:replicon:script-key:parameter:amount','value': {'number': " + balancetotransfer + "}}"))
        if item['startOffset']['offsetValue'] == 10:
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': parsed_policyset
            })
    return policy_sets


def get_rftca_rptca_greater_than_10_policysets_update(effective_date_0_years, existing_global_policysets):
    policy_sets = []

    for idx, item in enumerate(existing_global_policysets):
        timeoff_balance_event_scripts = item['timeOffBalanceEventScripts']
        monthly_accrual_policyset = json.loads(json.dumps(
            [x for x in timeoff_balance_event_scripts if x[
                'scriptTarget']['name'] == 'Monthly Accrual']).replace(
            '[{"additionalParameters"', '{"additionalParameters"').replace("}}]", "}}").replace(
                    '}},"scriptTarget', '}}],"scriptTarget'))
        max_balance = float(find_first_by_attr_and_get_attr(monthly_accrual_policyset, 'keyUri',
                            'urn:replicon:script-key:parameter:accrual-annual-amount', 'value.number')) * 1.5
        for idx2, item2 in timeoff_balance_event_scripts:
            if item2['scriptTarget']['name'] == 'Yearly Reset':
                existing_global_policysets[idx2] = {
                    'additionalParameters': [{
                        'keyUri': 'urn:replicon:script-key:parameter:daily-maximum-balance-amount',
                        'value': {
                            'number': max_balance
                        }
                    }],
                    'scriptTarget': {
                        'uri': result('get_maxbal_script_uri')
                    }
                }
        if idx == 2:
            policy_sets.append({
                'description': f"Effective on {effective_date_0_years.month}/{effective_date_0_years.day}/{effective_date_0_years.year}",
                'effectiveDate': {
                    'day': effective_date_0_years.day,
                    'month': effective_date_0_years.month,
                    'year': effective_date_0_years.year
                },
                'policySet': item['policySet']
            })
    return policy_sets


def get_pto_policy_assignments_update(type_policy):
    dag_run_conf = get_current_context()['dag_run'].conf

    effective_date_0_years = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y')

    effective_date_5_years = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y') + relativedelta(months=+60)

    effective_date_10_years = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y') + relativedelta(months=+120)

    existing_global_policysets = result('get_pto_policyset')

    if type_policy == 'rftch_rptch':
        return get_rftch_rptch_policysets_update(effective_date_0_years, existing_global_policysets, dag_run_conf['policyname'])
    if type_policy == 'rftchca_rptchca':
        return get_rftchca_rptchca_policysets_update(effective_date_0_years, existing_global_policysets)
    if type_policy == 'rft_rpt_<=5':
        return get_rft_rpt_less_than_5_policysets_update(
            effective_date_0_years, effective_date_5_years, effective_date_10_years,
            existing_global_policysets, dag_run_conf['balancetotransfer'])
    if type_policy == 'rftca_rptca<=5':
        return get_rftca_rptca_less_than_5_policysets_update(
            effective_date_0_years, effective_date_5_years, effective_date_10_years,
            existing_global_policysets)
    if type_policy == 'rft_rpt_5-10':
        return get_rft_rpt_5_10_policysets_update(effective_date_0_years, effective_date_10_years,
                                                  existing_global_policysets, dag_run_conf['balancetotransfer'])
    if type_policy == 'rftca_rptca_5-10':
        return get_rftca_rptca_5_10_policysets_update(effective_date_0_years, effective_date_10_years,
                                                      existing_global_policysets)
    if type_policy == 'rft_rpt_>10':
        return get_rft_rpt_greater_than_10_policysets_update(
            effective_date_0_years, existing_global_policysets, dag_run_conf['balancetotransfer'])
    if type_policy == 'rftca_rptca_>10':
        return get_rftca_rptca_greater_than_10_policysets_update(
            effective_date_0_years, existing_global_policysets)
    return ''


def get_flex_to_be_assigned(pay_grade, pay_group, salary_hourly):
    flex_to_be_assigned = 'no'
    if pay_grade:
        search_entries = list(filter(lambda x: x['paygrade'] == pay_grade and x[
            'salary_hourly'] == salary_hourly, adtalem_us_timeoff_policy_mapper_fto))
        if pay_group != 'ACA':
            if search_entries:
                if salary_hourly == 'S':
                    flex_to_be_assigned = 'yes'
        search_entries2 = list(filter(lambda x: x['paygrade'] == pay_grade and x[
            'salary_hourly'] == 'Any', adtalem_us_timeoff_policy_mapper_fto))
        if search_entries2:
            flex_to_be_assigned = 'yes'
    return flex_to_be_assigned


def add_timeoffs():
    timeofftypeuri_list = []
    if result('is_flextobeassigned_yes') == 'add_timeoffs':
        timeofftypeuri_list.append(
            {
                'uri': find_first_by_attr_and_get_attr(result(
                    'get_alltimeoff_types'), 'displayText', 'FTO', 'uri', ''),
                'name': find_first_by_attr_and_get_attr(result(
                    'get_alltimeoff_types'), 'displayText', 'FTO', 'displayText', '')
            }
        )
        timeofftypeuri_list.append(
            {
                'uri': find_first_by_attr_and_get_attr(result(
                    'get_alltimeoff_types'), 'displayText', 'Holiday', 'uri', ''),
                'name': find_first_by_attr_and_get_attr(result(
                    'get_alltimeoff_types'), 'displayText', 'Holiday', 'displayText', '')
            }
        )
        timeofftypeuri_list.append(
            {
                'uri': find_first_by_attr_and_get_attr(result(
                    'get_alltimeoff_types'), 'displayText', 'Jury Duty', 'uri', ''),
                'name': find_first_by_attr_and_get_attr(result(
                    'get_alltimeoff_types'), 'displayText', 'Jury Duty', 'displayText', '')
            }
        )
        timeofftypeuri_list.append(
            {
                'uri': find_first_by_attr_and_get_attr(result(
                    'get_alltimeoff_types'), 'displayText', 'Bereavement', 'uri', ''),
                'name': find_first_by_attr_and_get_attr(result(
                    'get_alltimeoff_types'), 'displayText', 'Bereavement', 'displayText', '')
            }
        )
    else:
        required_timeofftypes = [x.strip() for x in result(
            'get_timeofftypes_from_mapper').split('|')]
        set_result(required_timeofftypes, 'required_timeofftypes')
        for item in required_timeofftypes:
            timeofftypeuri_list.append(
                {
                    'uri': find_first_by_attr_and_get_attr(result(
                        'get_alltimeoff_types'), 'displayText', item, 'uri', ''),
                    'name': find_first_by_attr_and_get_attr(result(
                        'get_alltimeoff_types'), 'displayText', item, 'displayText', '')
                }
            )

    return timeofftypeuri_list


def get_yearlyaccrual_policyset():
    dag_run_conf = get_current_context()['dag_run'].conf
    rehire_date = datetime.strptime(
        dag_run_conf['rehiredate'], '%m/%d/%Y')
    floating_holiday_policyset = json.loads(json.dumps(result(
        'get_default_time_off_type_policy_schedule_for_user_floatingholiday')[0]['policySet'], ensure_ascii=False).replace(
        'null', '"effective"').replace(
        '"script"', '"scriptTarget"'))
    for i1, script in enumerate(floating_holiday_policyset['timeOffBalanceEventScripts']):
        if script['scriptTarget']['name'] == 'Yearly Accrual':
            yearly_accrual_policyset = script
            for i2, item2 in enumerate(yearly_accrual_policyset['additionalParameters']):
                if item2['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                    floating_holiday_policyset['timeOffBalanceEventScripts'][i1]['additionalParameters'][i2] = {
                        "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                        "value": {
                            "number": 8.0
                        }
                    }
    return [{
        "effectiveDate": {
            "year": rehire_date.year,
            "month": rehire_date.month,
            "day": rehire_date.day
        },
        "description": f"Effective on {rehire_date.month}-{rehire_date.day}-{rehire_date.year}",
        "policySet": floating_holiday_policyset
    }]


def get_date_based_on_userstatus():
    dag_run_conf = get_current_context()['dag_run'].conf
    return get_today_date() if 'Enabled' in dag_run_conf['userstatus'] else get_datetime_obj(
        dag_run_conf['rehiredate'])


def get_sicktimeoff_based_on_paygroup(paygroup):
    if paygroup in ('ACATW', 'ACASG', 'ACAJP', 'ACACH', 'HK', 'ACAIN'):
        return f'Sick Time Off - {paygroup}'
    if paygroup == 'ACAAS':
        return 'Leave of Absence - Australia'
    return ''


def get_annualleavetimeoff_based_on_paygroup(paygroup):
    if paygroup in ('ACATW', 'ACASG', 'ACAJP', 'ACACH', 'HK'):
        return 'Annual Leave(Asia)'
    if paygroup == 'ACAIN':
        return 'Annual Leave(India)'
    if paygroup == 'ACAAS':
        return 'Annual Leave(Australia)'
    return ''


def get_timeoff_policy_assignments_aus_asia(offset):
    dag_run_conf = get_current_context()['dag_run'].conf
    year_multiple = null

    if offset in (3, 6, 9):
        year_multiple = offset

    effective_date = datetime.strptime(
        dag_run_conf['startdate'], '%m/%d/%Y') + relativedelta(
            years=+offset) if year_multiple else datetime.strptime(
        dag_run_conf['startdate'], '%m/%d/%Y')

    return {
        'description': f"Effective on {effective_date.month}/{effective_date.day}/{effective_date.year}",
        'effectiveDate': {
            'day': effective_date.day,
            'month': effective_date.month,
            'year': effective_date.year
        },
        'policySet': result('log_requiredpolicy_21')
    }
