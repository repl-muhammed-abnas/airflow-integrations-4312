from datetime import datetime, timedelta
import itertools
import json
import ast
from operator import itemgetter
import rail

DATE_FORMAT = "%m/%d/%Y"
null = None

def get_date_from_replicon_date(replicon_date):
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, DATE_FORMAT)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

def get_historical_policy_to_assign_list(dag_run, action, for_each_loop, config):
    data = rail.result(for_each_loop)['policy']
    if not data:
        return []
    def get_compare_date():
        if action =="update":
            return dag_run.conf['change_effective_date']
        if action =='rehire':
            if rail.result(for_each_loop)['timeoff_type_name'] in config.SPECIAL_ACCRUAL_TIMEOFF_TYPE_NAMES and\
                (dag_run.conf['previous_employee_status'] =="Unpaid Leave" or dag_run.conf['previous_employee_status'] =="Paid Leave"):
                if dag_run.conf['assigned_event_reason_code']=='10' and \
                    list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS)):
                    return dag_run.conf['change_effective_date']
            return dag_run.conf['start_date']
        return dag_run.conf['end_date'] if dag_run.conf['end_date'] else dag_run.conf['change_effective_date']

    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < datetime.strptime(get_compare_date(), DATE_FORMAT).date(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))

def get_no_accrual_policy_line(dag_run, action):
    if rail.result("for_each_time_off_type_no_accural")['timeoff_type_name'] == "[USA] Sick" and action =='disable':
        return rail.result("get_termination_policy_for_sick_timeoff_Type")

    effective_date = (datetime.strptime(dag_run.conf['end_date'],DATE_FORMAT)+timedelta(days=1)).strftime(DATE_FORMAT)\
        if action =='disable' and dag_run.conf['end_date'] else dag_run.conf['change_effective_date']
    return [{
        "effectiveDate":get_replicon_date(effective_date),
        "description": "Added By Integration on"+
            f"{dag_run.conf['end_date'] if action =='disable'and dag_run.conf['end_date'] else dag_run.conf['change_effective_date']}",
        "policySet": {
            "timeOffBalanceEventScripts":[{
             "script": {
                "description": "Set initial balance for the first day of a policy",
                "name": "Starting Balance Set To",
                "uri": dag_run.conf['starting_balance_script_uri']
            },
            "additionalParameters": [{
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                "number": rail.result('get_balance_summary_for_user')['timeRemaining']
                }
            }]
            }],
            "timeOffValidationScripts": [{
            "script": {
                "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                "name": "Prevent balance overdraw",
                "uri": dag_run.conf['prevent_balance_overdraw_uri']
            },
            "additionalParameters": [{
                "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                "value": {
                "number": "0"
                }
            }],
            }]
        }
        }]

def get_all_policy_to_assign_for_disable_user():
    if rail.result('for_each_time_off_type_no_accural')['policy'] and rail.result('get_no_accrual_policy_line'):
        data =rail.result('get_historical_policy_to_assign_list_disable_user') + rail.result('get_no_accrual_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('for_each_time_off_type_no_accural')['policy'] and rail.result('get_no_accrual_policy_line'):
        data = rail.result('get_no_accrual_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('for_each_time_off_type_no_accural')['policy'] and not rail.result('get_no_accrual_policy_line'):
        data =rail.result('get_historical_policy_to_assign_list_disable_user')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    return null

def get_required_time_off_type_details(required_timeoff_types_names,value,mannual_time_off_types=null):
    log_time_off_type_exception = []
    exception_message = ""
    data = rail.result('get_all_time_off_types')
    all_time_off_types_names = list(map(itemgetter('timeoff_type_name'), data))

    for item in required_timeoff_types_names:
        if item not in all_time_off_types_names:
            log_time_off_type_exception.append(item)

    if log_time_off_type_exception:
        exception_message = f"Time off Type - '{rail.smartjoin_by_delim(log_time_off_type_exception,',')}' not available in Replicon"

    if value =='update':
        for timeoff_type in mannual_time_off_types:
            if rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'),
                    'timeoff_type_name', timeoff_type, 'timeoff_type_name'):
                required_timeoff_types_names.append(timeoff_type)

    return {"time_off_type_exception_log": exception_message if log_time_off_type_exception else [],
            "result": list(filter(lambda time_off: time_off['timeoff_type_name'] in list(set(required_timeoff_types_names))
        ,data))}

def assigned_time_offs_types():
    data = rail.result('get_user_time_off_policy_summary')
    return list(filter(lambda x: x['enabled'], map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "enabled": item['enabled'],
        "policy": item['policy'] if item['policy'] else []
    }, data)))

def time_off_types_to_be_assigned(dag_run, reference_time_off_types, special_time_off_mapper, special_time_off_type_names):
    data = rail.result('get_required_time_off_type_details_to_assign')['result']
    compare_data = rail.result('assigned_time_offs_types')

    def get_policy(item):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'), 'timeoff_type_uri', item['timeoff_type_uri'], 'policy')

    # pylint: disable=too-many-boolean-expressions
    # pylint: disable=too-many-return-statements
    # pylint: disable=too-many-branches
    def get_status(item):
        if item['timeoff_type_name'] == "[CAN] Jour personnel/Personal Days":
            if dag_run.conf['location_level_3'] != 'STCONSTANT':

                if dag_run.conf['pay_type'] in ['Salaried','Exception Hourly'] and dag_run.conf['previous_pay_type'] =="Hourly":
                    return 'No'

                if dag_run.conf['pay_type'] =="Hourly" and dag_run.conf['previous_pay_type'] in ['Salaried','Exception Hourly'] :
                    return 'No'

                if dag_run.conf['pay_type'] in ['Salaried','Exception Hourly'] and dag_run.conf['previous_pay_type'] in ['Salaried','Exception Hourly']:
                    if dag_run.conf['std_hrs'] and float(dag_run.conf['std_hrs'])!= (float(dag_run.conf['previous_std_hrs'])
                        if dag_run.conf['previous_std_hrs'] else null):
                        return "No"

                if dag_run.conf['assigned_location_grp']=="STCONSTANT":
                    return "No"

            if dag_run.conf['location_level_3'] == 'STCONSTANT':
                if dag_run.conf['assigned_location_grp']!="STCONSTANT":
                    return "No"



        if item['timeoff_type_name'] in reference_time_off_types and dag_run.conf['rehire']!='Yes':
            if item['timeoff_type_name'] == "[CAN] Vacances/Vacation":
                if dag_run.conf['job_code']!= dag_run.conf['current_assigned_job_code']:
                    if (str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")) and
                    not str(dag_run.conf['current_assigned_job_code']).endswith(("S1","S2","S3","S4","S5"))) or\
                    (str(dag_run.conf['job_code']).endswith(("A1","A2","T1","T2")) and
                    not str(dag_run.conf['current_assigned_job_code']).endswith(("A1","A2","T1","T2"))) or\
                    (str(dag_run.conf['job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")) \
                    and not str(dag_run.conf['current_assigned_job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6"))):
                        return 'No'

                if dag_run.conf['previous_full_part']=="Part-Time" and dag_run.conf['full_part'] =="Full-Time":
                    return 'No'

                if dag_run.conf['previous_full_part']=="Full-Time" and dag_run.conf['full_part'] =="Part-Time":
                    return 'No'

                if dag_run.conf['previous_full_part']=="Part-Time" and dag_run.conf['full_part'] =="Part-Time" \
                    and (float(dag_run.conf['std_hrs']) != float(dag_run.conf["previous_std_hrs"])):
                    return 'No'

            if item['timeoff_type_name'] == "[CAN] Anniversaire de service/ Service Anniversary":
                if dag_run.conf['job_code']!= dag_run.conf['current_assigned_job_code']:
                    if (str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")) and
                    not str(dag_run.conf['current_assigned_job_code']).endswith(("S1","S2","S3","S4","S5"))) or\
                    (str(dag_run.conf['job_code']).endswith(("A1","A2","A3","A4","A5","A6","T1","T2","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")) and
                    not str(dag_run.conf['current_assigned_job_code']).endswith(("A1","A2","A3","A4","A5","A6","T1","T2","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6"))):
                        return 'No'

                if dag_run.conf["reg_temp"]=="Regular" and dag_run.conf["previous_reg_temp"]=="Temporary":
                    return "No"

                if float(dag_run.conf['std_hrs']) != float(dag_run.conf["previous_std_hrs"]):
                    return "No"

                if dag_run.conf['assigned_location_grp'] == 'STCONSTANT' and dag_run.conf['location_level_3'] != 'STCONSTANT':
                    return "No"

        if dag_run.conf['assigned_event_reason_code']=="10" and \
            list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],special_time_off_mapper)):
            if item['timeoff_type_name'] in special_time_off_type_names and  (dag_run.conf['previous_employee_status'] =="Unpaid Leave"
                or dag_run.conf['previous_employee_status'] =="Paid Leave"):
                return 'No'

        if item['timeoff_type_name'] =="[CAN] Journée Flexible/Flexible Day - St.Constant"\
            and dag_run.conf['rehire']!='Yes':
            if  dag_run.conf['location_level_3']=='STCONSTANT' and dag_run.conf['employee_type_grp']:
                if dag_run.conf['reg_temp'] not in dag_run.conf['employee_type_grp']:
                    return 'No'

        if rail.find_first_by_attr_and_get_attr(compare_data, 'timeoff_type_uri', item['timeoff_type_uri'], 'timeoff_type_name'):
            if item['timeoff_type_name']=="[CAN] Jour personnel (temporaires)/Personal Days Temp" and \
                    dag_run.conf['location_level_3'] != dag_run.conf['assigned_location_grp']:
                if dag_run.conf['location_level_3'] == 'STCONSTANT' or dag_run.conf['assigned_location_grp']=="STCONSTANT":
                    return 'No'
            return 'Yes'
        return 'No'

    return list(filter(lambda x: x['status']=='No',map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "enabled": rail.find_first_by_attr_and_get_attr(compare_data, 'timeoff_type_uri', item['timeoff_type_uri'], 'enabled'),
        "status": get_status(item) if dag_run.conf['rehire']!='Yes' else 'No',
        "policy": get_policy(item)
    }, data)))

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

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

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="total_record_count",val= dag_run.conf['total_records'])

    return final_log_records

def is_vacances_time_off_and_job_code_changed(dag_run):
    if rail.result('for_each_time_off_type_policy')['timeoff_type_name'] == "[CAN] Vacances/Vacation St. Constant":
        return False
    return bool(dag_run.conf['job_code']!= dag_run.conf['current_assigned_job_code'] and (str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")) and
        not str(dag_run.conf['current_assigned_job_code']).endswith(("S1","S2","S3","S4","S5"))) or\
        (str(dag_run.conf['job_code']).endswith(("A1","A2","T1","T2")) and
            not str(dag_run.conf['current_assigned_job_code']).endswith(("A1","A2","T1","T2"))) or\
        (str(dag_run.conf['job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")) \
        and not str(dag_run.conf['current_assigned_job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6"))))

def get_all_policy_to_assign(dag_run,config):
    if rail.result('for_each_time_off_type_policy')['timeoff_type_name'] == "[CAN] Anniversaire de service/ Service Anniversary" \
        and dag_run.conf['rehire']=='Yes':
        data = rail.result('get_default_time_off_policy_schedule')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('for_each_time_off_type_policy')['timeoff_type_name'] in config.SPECIAL_ACCRUAL_TIMEOFF_TYPE_NAMES \
        and not is_vacances_time_off_and_job_code_changed(dag_run):
        if dag_run.conf['previous_employee_status'] =="Unpaid Leave" or dag_run.conf['previous_employee_status'] =="Paid Leave":
            if dag_run.conf['assigned_event_reason_code']=='10' and \
                list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS)):
                if datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT).strftime(DATE_FORMAT) < \
                    get_date_from_replicon_date(dag_run.conf['assigned_change_effective_date']).strftime(DATE_FORMAT):
                    data =rail.result('get_historical_policy_to_assign_list')
                    return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('for_each_time_off_type_policy')['policy'] and rail.result('get_default_time_off_policy_schedule'):
        data =rail.result('get_historical_policy_to_assign_list') + rail.result('get_default_time_off_policy_schedule')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('for_each_time_off_type_policy')['policy'] and rail.result('get_default_time_off_policy_schedule'):
        data = rail.result('get_default_time_off_policy_schedule')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('for_each_time_off_type_policy')['policy'] and not rail.result('get_default_time_off_policy_schedule'):
        data =rail.result('get_historical_policy_to_assign_list')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))
    return null

def time_off_types_to_be_disabled():
    def get_policy(item):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'), 'timeoff_type_uri', item['timeoff_type_uri'], 'policy')

    compare_data = rail.result('get_required_time_off_type_details_to_assign')['result']
    data = rail.result('assigned_time_offs_types')
    return list(filter(lambda x: x['status'] == 'No', map(lambda item: {
        'timeoff_type_name': item['timeoff_type_name'],
        "timeoff_type_uri": item['timeoff_type_uri'],
        "enabled": item['enabled'],
        "status": 'Yes' if rail.find_first_by_attr_and_get_attr(compare_data, 'timeoff_type_uri', item['timeoff_type_uri'], 'timeoff_type_name') else 'No',
        "policy": get_policy(item)
    }, data)))

def get_custom_policy_line(dag_run,config):

    def get_effective_date(date_str):
        mapper_value = list(filter(lambda x:x['event'] == dag_run.conf['event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS))
        if mapper_value:
            date_obj = datetime.strptime(date_str, DATE_FORMAT)
            if mapper_value[0]['count']:
                new_date= date_obj+timedelta(weeks=int(mapper_value[0]['count']))
                return new_date.strftime(DATE_FORMAT)
        return date_str

    return [{
        "effectiveDate":get_replicon_date(get_effective_date(dag_run.conf['change_effective_date'])),
        "description": f"Added By Integration on { dag_run.conf['change_effective_date']}",
        "policySet": []
        }]

def get_all_policy_to_assign_for_special_accrual():
    if rail.result('get_historical_policy_to_assign_special_accrual') and rail.result('get_custom_policy_line'):
        data =rail.result('get_historical_policy_to_assign_special_accrual') + rail.result('get_custom_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('get_historical_policy_to_assign_special_accrual') and rail.result('get_custom_policy_line'):
        data = rail.result('get_custom_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('get_historical_policy_to_assign_special_accrual') and not rail.result('get_custom_policy_line'):
        data =rail.result('get_historical_policy_to_assign_special_accrual')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))
    return null

def get_historical_policy_to_assign_special_accrual_list(dag_run,time_off_type_uri,config):
    data = rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'), 'timeoff_type_uri', time_off_type_uri, 'policy')
    if not data:
        return []
    def get_compare_date(date_str):
        mapper_value = list(filter(lambda x:x['event'] == dag_run.conf['event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS))
        if mapper_value:
            date_obj = datetime.strptime(date_str, DATE_FORMAT)
            if mapper_value[0]['count']:
                new_date= date_obj+timedelta(weeks=int(mapper_value[0]['count']))
                return new_date.strftime(DATE_FORMAT)
        return date_str

    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < datetime.strptime(get_compare_date(dag_run.conf['change_effective_date']), DATE_FORMAT).date(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))


def time_off_type_to_be_updated(dag_run,config):
    data = dag_run.conf['time_off_types_to_assign']
    for time_off_type in config.SPECIAL_ACCRUAL_TIMEOFF_TYPE_NAMES:
        if time_off_type in data :
            return {
                "time_off_type_name": time_off_type,
                "time_off_type_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_time_off_policy_summary'), 'timeoff_type_name', time_off_type, 'timeoff_type_uri')
            } if rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_time_off_policy_summary'), 'timeoff_type_name', time_off_type, 'timeoff_type_uri') else null
    return null

def get_process_users_dag_ids(parallel_count):
    active_users =  list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_active_users_{x+1}') if rail.result(
            f'process_active_users_{x+1}') else []), range(parallel_count)))))

    disable_users = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_disable_users_{x+1}') if rail.result(
            f'process_disable_users_{x+1}') else []), range(parallel_count)))))

    return active_users + disable_users


# ---------------------------------------------------------------------------
# V2.7 - Personal Days proration helpers.
# All business rules are sourced from the mapper module; this code only
# dispatches by mapper data and never hard-codes leave-reason codes.
# ---------------------------------------------------------------------------

# Marker substring written into the policy-line description by the V2.7
# outbound/return writer. The cleanup detector matches on this string to tell
# integration-written lines apart from manual admin overrides. ANY change to
# this string MUST update both _personal_days_line_description() (the writer)
# AND should_write_personal_days_policy() (the reader) - they're glued by
# this constant so the contract is explicit.
PERSONAL_DAYS_INTEGRATION_MARKER = "Added By Integration on"


def _personal_days_line_description(change_eff_str):
    return f"{PERSONAL_DAYS_INTEGRATION_MARKER} {change_eff_str}"


def _personal_days_family_letter(job_code):
    # Per V2.7 docx image32: "Job Code ending letter (Last but 2nd one)" -
    # i.e. the penultimate character of the job code (the letter before the
    # trailing digit, e.g. "...A3" -> "A"). Assumes the CRL job-code suffix
    # convention <letter><digit>; non-conforming codes return None and
    # downstream callers surface that as a bucket-unresolved error.
    if not job_code or len(job_code.strip()) < 2:
        return None
    candidate = job_code.strip()[-2].upper()
    if not candidate.isalpha():
        return None
    return candidate


def resolve_personal_days_bucket(reg_temp, std_hrs, job_code, config):
    family = _personal_days_family_letter(job_code)
    if family is None:
        return None
    # Normalize std_hrs to one decimal place so upstream representations
    # ("22.5", 22.50, 22.5000001) all resolve to the same key.
    try:
        std_hrs_norm = round(float(std_hrs), 1)
    except (TypeError, ValueError):
        return None
    key = (reg_temp, std_hrs_norm, family)
    return config.PERSONAL_DAYS_BUCKET_TABLE.get(key)


def _personal_days_return_lookup_month(return_date):
    # Per V2.7 docx para 876: return on day <=15 maps to that month, day >=16
    # maps to next month. Dec 16-31 wraps to Jan; the 'returns' lookup tables
    # use January's yearly-cap row (e.g. 75h for 10d_FT) which is the desired
    # behavior per PDF page 1 bullet 3 (calendar-year anchored).
    if return_date.day <= 15:
        return return_date.month
    return (return_date.month % 12) + 1


def get_personal_days_return_hours(table_key, return_date_str, config):
    return_date = datetime.strptime(return_date_str, DATE_FORMAT)
    month = _personal_days_return_lookup_month(return_date)
    return config.PERSONAL_DAYS_PRORATION_TABLES[table_key]["returns"][month]


def get_personal_days_outbound_hours(table_key, leave_start_date_str, config):
    leave_start = datetime.strptime(leave_start_date_str, DATE_FORMAT)
    # No 15-day rounding on outbound - raw change-effective month.
    return config.PERSONAL_DAYS_PRORATION_TABLES[table_key]["start_of_leaves"][leave_start.month]


def determine_personal_days_mode(dag_run, config):
    # TEMP employees are excluded outright.
    if dag_run.conf.get("reg_temp") != "Regular":
        return "noop"

    curr_status = dag_run.conf.get("emp_status")
    prev_status = dag_run.conf.get("previous_employee_status")
    # Field-name convention (matches the existing vacation logic in
    # process_special_accrual_time_off_type.py and what production payloads
    # actually carry):
    #   conf["event"]              = text reason code (UNPLTD / UNPADP / ...)
    #   conf["event_reason_code"]  = numeric event code ("10" for LoA)
    # The xlsx column header labels suggest the opposite, but the CSV header
    # names + the established RAIL integration convention dictate the above.
    curr_reason = dag_run.conf.get("event")
    prev_reason = dag_run.conf.get("assigned_event")
    curr_event = dag_run.conf.get("event_reason_code")
    prev_event = dag_run.conf.get("assigned_event_reason_code")

    # Outbound: status is Unpaid Leave with an actionable LoA reason. The
    # text reason in LEAVE_OUT_IMPACT_RULES is the authoritative signal; we
    # deliberately do NOT gate on the numeric event_reason_code matching the
    # LoA code (e.g. "10") because production payloads for UNPSTD->UNPLTD
    # continuation arrive with event_reason_code="23" (the Return-to-Work
    # numeric, used in CRL's source system as a sub-event for type changes).
    # Gating on it caused continuation payloads to silently noop. See
    # emp_id 8966707 (manual__2026-05-20T12:59:38) as the canonical case.
    #
    # The (prev_reason != curr_reason or prev_event != curr_event) guard
    # mirrors the existing vacation pattern (is_reason_or_reasoncode_updated)
    # and prevents re-applying the same outbound policy line on every
    # subsequent payload while the user is still on the same leave. If a
    # corrected effective date arrives without a code change, this will be a
    # no-op - manual cleanup is expected in that case.
    if curr_status == "Unpaid Leave" \
            and curr_reason in config.LEAVE_OUT_IMPACT_RULES \
            and (prev_reason != curr_reason or prev_event != curr_event):
        return "outbound"

    # Source of truth for leave_start is the dedicated 'Leave Start Date' UDF
    # (written ONLY on Active -> Unpaid Leave transition - immune to mid-leave
    # overwrites). assigned_change_effective_date is a TRANSITIONAL fallback
    # for users who were already on leave when V2.7 deployed and therefore
    # never had the Leave Start Date UDF populated on their outbound payload.
    #
    # TODO(V2.7-FALLBACK-REMOVAL): drop the `or dag_run.conf.get(
    # "assigned_change_effective_date")` clause once every user currently on
    # leave at V2.7 deploy time has returned. Maximum CRL leave duration is
    # ~52 weeks (UNPADP / UNPMPA per Sheet1) + one full sync cycle to capture
    # the return -> safe removal bound is deploy_date + 14 months. Procedure:
    #   1. Run a Replicon report at removal time to confirm zero users still
    #      on Unpaid Leave with empty 'Leave Start Date' UDF.
    #   2. Drop the fallback clause and remove this comment.
    #   3. Drop the `assigned_change_effective_date` conf field from the
    #      V2.7 trigger conf in process_update_users.py.
    # Grep this codebase for "V2.7-FALLBACK-REMOVAL" to locate all
    # transitional artifacts at removal time. File a JIRA ticket to track
    # the removal calendar date when the deploy ships.
    leave_start = dag_run.conf.get("assigned_leave_start_date") \
        or dag_run.conf.get("assigned_change_effective_date")
    return_date = dag_run.conf.get("change_effective_date")

    # Return detection: status flip Unpaid Leave -> Active with the prior
    # text reason being an actionable LoA code. We do NOT gate on the prior
    # numeric event_reason_code matching the LoA code "10", for the same
    # reason described in the outbound branch above (CRL continuation
    # payloads can carry event_reason_code="23" instead of "10").
    is_return_payload = (prev_status == "Unpaid Leave" and curr_status == "Active"
                         and prev_reason in config.LEAVE_OUT_IMPACT_RULES
                         and leave_start and return_date)

    if is_return_payload:
        rule = config.LEAVE_OUT_IMPACT_RULES.get(prev_reason, {})
        prev_action = rule.get("action")

        # Per CRL direction (2026-05-20): for zero_immediately codes
        # (UNPPAR, UNPPER, UNPEDU, UNPSBC, UNPSBP) the >26-week return gate
        # does NOT apply. ANY return triggers return mode and writes a
        # prorated returns[month] line (with template accrual/reset/limit
        # rules cloned in). These codes have no future-dated zero line on
        # the schedule, so there is nothing to clean up regardless of when
        # the user returns.
        if prev_action == "zero_immediately":
            return "return"

        # Any parse failure here indicates malformed upstream data and must
        # surface as an error, not a silent no-op.
        leave_start_dt = (get_date_from_replicon_date(leave_start)
                          if isinstance(leave_start, dict)
                          else datetime.strptime(leave_start, DATE_FORMAT))
        return_dt = datetime.strptime(return_date, DATE_FORMAT)
        duration_days = (return_dt - leave_start_dt).days

        if duration_days > config.LONG_LEAVE_THRESHOLD_DAYS:
            return "return"

        # Cleanup: user returned BEFORE 26 weeks. For buffer_then_zero codes
        # (UNPLTD, UNPMED, UNPMIL, UNPWCL) the outbound write placed a
        # future-dated zero-balance policy line; that line must be removed
        # before it activates and wrongly zeros the (still-valid) balance.
        #
        # Anchoring NOTE: we deliberately do NOT compute buffer_end here
        # from leave_start_dt. In the UNPSTD->UNPLTD continuation case the
        # Leave Start Date UDF holds the original UNPSTD start (week 0),
        # but the integration's outbound write placed the zero line at
        # UNPLTD effective + 9w (= week 26 from UNPSTD start). A
        # leave_start_dt + 9w check would wrongly say "buffer already
        # expired" for a return at week 20, leaving the zero line orphaned.
        #
        # Instead, we return cleanup unconditionally for any
        # buffer_then_zero prior reason inside the 26-week window, then
        # let should_write_personal_days_policy() inspect the actual
        # policy schedule and decide whether a future-dated integration
        # line really exists. That helper runs *after* the policy summary
        # is fetched, so it has the ground truth.
        #
        # zero_immediately codes are handled above (always return mode).
        # prorate_then_buffer_then_zero (UNPADP / UNPMPA) writes TWO lines,
        # the second being a future-dated zero - same cleanup requirement as
        # buffer_then_zero.
        if prev_action in ("buffer_then_zero", "prorate_then_buffer_then_zero"):
            return "cleanup"

    return "noop"


_STARTING_BALANCE_AMOUNT_KEY = "urn:replicon:script-key:parameter:amount"


def _build_personal_days_policy_set(amount, dag_run):
    return {
        "timeOffBalanceEventScripts": [{
            "script": {
                "description": "Set initial balance for the first day of a policy",
                "name": "Starting Balance Set To",
                "uri": dag_run.conf["starting_balance_script_uri"],
            },
            "additionalParameters": [{
                "keyUri": _STARTING_BALANCE_AMOUNT_KEY,
                "value": {"number": amount},
            }],
        }],
        "timeOffValidationScripts": [{
            "script": {
                "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                "name": "Prevent balance overdraw",
                "uri": dag_run.conf["prevent_balance_overdraw_uri"],
            },
            "additionalParameters": [{
                "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                "value": {"number": "0"},
            }],
        }],
    }


def resolve_personal_days_update_template_name(dag_run, config):
    # Returns the "[CAN] Personal Days Update {std_hrs}" template name the user
    # is eligible for, based on std_hrs only (mirrors the existing add/update
    # vacation-style logic in request_payload.get_default_timeoff_policy_set_
    # schedule_for_timeofftype). Returns None if std_hrs is not one of the
    # three supported values.
    try:
        std_hrs_norm = round(float(dag_run.conf.get("std_hrs")), 1)
    except (TypeError, ValueError):
        return None
    refs = getattr(config, "PERSONAL_DAYS_REFERENCE_TIME_OFF_TYPES", [])
    match = next((r for r in refs
                  if r.get("action") == "Update"
                  and r.get("hire_date") == "NA"
                  and round(float(r.get("std_hrs", 0)), 1) == std_hrs_norm),
                 None)
    return match["timeoff_type_name"] if match else None


def resolve_personal_days_update_template_uri(dag_run, config):
    # Used by the V2.7 DAG to fetch the template policy schedule for return
    # mode. Only resolves on return mode - other modes do not merge template
    # rules and we skip the lookup entirely.
    mode = rail.result("determine_mode")
    if mode != "return":
        return None
    name = resolve_personal_days_update_template_name(dag_run, config)
    if not name:
        return None
    return rail.find_first_by_attr_and_get_attr(
        rail.result("get_all_time_off_types"),
        "timeoff_type_name", name, "timeoff_type_uri")


def _starting_balance_script(amount, dag_run):
    return {
        "script": {
            "description": "Set initial balance for the first day of a policy",
            "name": "Starting Balance Set To",
            "uri": dag_run.conf["starting_balance_script_uri"],
        },
        "additionalParameters": [{
            "keyUri": _STARTING_BALANCE_AMOUNT_KEY,
            "value": {"number": amount},
        }],
    }


def _ensure_starting_balance(policy_set, amount, dag_run):
    # If the (cloned) template already has a Starting Balance script,
    # override its amount. Otherwise prepend a fresh one so the return-mode
    # line carries the prorated balance alongside the template's accrual /
    # reset / limitation / validation rules.
    balance_scripts = policy_set.setdefault("timeOffBalanceEventScripts", [])
    for script in balance_scripts:
        if script.get("script", {}).get("name") == "Starting Balance Set To":
            for param in script.get("additionalParameters", []) or []:
                if param.get("keyUri") == _STARTING_BALANCE_AMOUNT_KEY:
                    param["value"] = {"number": amount}
                    return policy_set
    balance_scripts.insert(0, _starting_balance_script(amount, dag_run))
    return policy_set


def _build_personal_days_return_policy_set(amount, dag_run):
    # Return-mode policy line clones the user's eligible Update template
    # (accrual + reset + limitation + validation rules) and overrides ONLY the
    # Starting Balance amount with the prorated value from the Returns table.
    # Falls back to the rule-less policy set if the template fetch was skipped
    # or unavailable (e.g. std_hrs outside {37.5, 30, 22.5}).
    template_schedule = rail.result("get_personal_days_update_template_schedule")
    if not template_schedule:
        return _build_personal_days_policy_set(amount, dag_run)
    # Pick the first (and typically only) entry from the default template.
    template_entry = template_schedule[0] if isinstance(template_schedule, list) else template_schedule
    template_policy_set = template_entry.get("policySet") if isinstance(template_entry, dict) else None
    if not template_policy_set:
        return _build_personal_days_policy_set(amount, dag_run)
    # Deep-copy via JSON to avoid mutating the cached task result.
    cloned = json.loads(json.dumps(template_policy_set))
    return _ensure_starting_balance(cloned, amount, dag_run)


def _personal_days_time_taken():
    # Read 'timeTakenForPeriod' from the balance-summary task.
    # This is the authoritative "used hours" value from Replicon - directly
    # answers "how much has the user consumed in the current period" without
    # needing to derive it from (yearly_max - current_balance), which breaks
    # for mid-year hires and users with prior-year integration adjustments.
    # Returns None if the task did not run / result missing.
    summary = rail.result("get_personal_days_balance_summary")
    if not summary:
        return None
    return summary.get("timeTakenForPeriod") if isinstance(summary, dict) else None


def build_personal_days_policy_line(dag_run, config):
    mode = determine_personal_days_mode(dag_run, config)
    if mode in ("noop", "cleanup"):
        # Cleanup mode produces no new line - the cleanup is achieved by
        # re-PUTing the historical-only schedule (future-dated lines dropped
        # via the return_date cutoff in get_personal_days_historical_policy_lines).
        return []

    bucket = resolve_personal_days_bucket(
        dag_run.conf["reg_temp"], dag_run.conf["std_hrs"],
        dag_run.conf["job_code"], config,
    )
    change_eff_str = dag_run.conf["change_effective_date"]
    change_eff_dt = datetime.strptime(change_eff_str, DATE_FORMAT)

    # outbound_lines collects (effective_dt, amount) tuples. Most actions
    # produce a single line; prorate_then_buffer_then_zero (UNPADP/UNPMPA)
    # produces two - prorate at change_eff AND zero at change_eff + N weeks.
    outbound_lines = []

    if mode == "outbound":
        rule = config.LEAVE_OUT_IMPACT_RULES[dag_run.conf["event"]]
        action = rule["action"]
        if action == "zero_immediately":
            effective_dt = change_eff_dt
            amount = 0
        elif action == "buffer_then_zero":
            effective_dt = change_eff_dt + timedelta(weeks=int(rule["buffer_weeks"]))
            amount = 0
        elif action == "prorate_then_buffer_then_zero":
            # UNPADP / UNPMPA per xlsx Sheet1 rows 3-4 column H. Two lines:
            #   1. Prorated balance at change_eff (Start of Leaves table)
            #   2. Zero at change_eff + buffer_weeks*7 days
            #
            # Formula per CRL (2026-05-15): "starting balance 75, user used 15;
            # user goes on leave on Oct; 60-15 = 45 hours" where 60 is
            # start_of_leaves[Oct] for 10d_FT and 15 is the user's hours used
            # in the calendar year so far. We read used directly from
            # GetBalanceSummaryForAccount.timeTakenForPeriod (the authoritative
            # value from Replicon) rather than deriving it from
            # (yearly_max - current_balance), which breaks for mid-year hires
            # and users with prior-year integration adjustments.
            if not bucket:
                raise ValueError(
                    f"Personal Days bucket unresolved for outbound prorate+buffer "
                    f"(reg_temp={dag_run.conf['reg_temp']}, "
                    f"std_hrs={dag_run.conf['std_hrs']}, "
                    f"job_code={dag_run.conf['job_code']})"
                )
            table_key, _yearly_max = bucket
            prorated_entitlement = get_personal_days_outbound_hours(table_key, change_eff_str, config)
            consumed = _personal_days_time_taken()
            if consumed is None:
                raise ValueError(
                    "GetBalanceSummaryForAccount.timeTakenForPeriod unavailable - "
                    "cannot compute consumed amount for UNPADP/UNPMPA outbound prorate."
                )
            prorate_amount = max(0.0, float(prorated_entitlement) - float(consumed))
            outbound_lines.append((change_eff_dt, prorate_amount))
            outbound_lines.append(
                (change_eff_dt + timedelta(weeks=int(rule["buffer_weeks"])), 0)
            )
            effective_dt = None  # signal: outbound_lines is the source of truth
            amount = None
        elif action == "prorate_at_leave_start":
            # Legacy single-line prorate (no codes currently map to this
            # action; kept for back-compat with mapper extensions). Reads
            # consumed directly from timeTakenForPeriod - same formula as
            # prorate_then_buffer_then_zero, minus the second zero line.
            if not bucket:
                raise ValueError(
                    f"Personal Days bucket unresolved for outbound prorate "
                    f"(reg_temp={dag_run.conf['reg_temp']}, "
                    f"std_hrs={dag_run.conf['std_hrs']}, "
                    f"job_code={dag_run.conf['job_code']})"
                )
            table_key, _yearly_max = bucket
            prorated_entitlement = get_personal_days_outbound_hours(table_key, change_eff_str, config)
            consumed = _personal_days_time_taken()
            if consumed is None:
                raise ValueError(
                    "GetBalanceSummaryForAccount.timeTakenForPeriod unavailable - "
                    "cannot compute consumed amount for prorate_at_leave_start."
                )
            amount = max(0.0, float(prorated_entitlement) - float(consumed))
            effective_dt = change_eff_dt
        else:
            # New action type added to mapper without a dispatch arm here -
            # this is the ONE place a code edit is needed for action shapes
            # outside the original three. Reason codes alone do not require it.
            raise ValueError(
                f"Unknown action '{action}' for event_reason_code "
                f"{dag_run.conf['event']} - add a dispatch arm "
                f"in build_personal_days_policy_line."
            )
    else:  # mode == "return"
        if not bucket:
            raise ValueError(
                f"Personal Days bucket unresolved for return proration "
                f"(reg_temp={dag_run.conf['reg_temp']}, "
                f"std_hrs={dag_run.conf['std_hrs']}, "
                f"job_code={dag_run.conf['job_code']})"
            )
        table_key, _ = bucket
        effective_dt = change_eff_dt
        amount = get_personal_days_return_hours(table_key, change_eff_str, config)

    # Return-mode lines preserve the user's accrual/reset/limitation/validation
    # rules from the Update template they are eligible for so the user keeps
    # accruing per their normal policy after coming back from leave. Outbound
    # modes intentionally write a rule-less line (balance zeroed/prorated and
    # no further accrual while the leave is in progress).
    def _build_entry(eff_dt, amt):
        if mode == "return":
            ps = _build_personal_days_return_policy_set(amt, dag_run)
        else:
            ps = _build_personal_days_policy_set(amt, dag_run)
        return {
            "effectiveDate": get_replicon_date(eff_dt.strftime(DATE_FORMAT)),
            "description": _personal_days_line_description(change_eff_str),
            "policySet": ps,
        }

    if outbound_lines:
        return [_build_entry(eff, amt) for eff, amt in outbound_lines]
    return [_build_entry(effective_dt, amount)]


def get_personal_days_historical_policy_lines(dag_run, config):
    summary = rail.result("get_user_time_off_policy_summary")
    policy = rail.find_first_by_attr_and_get_attr(
        summary, "timeoff_type_name", config.PERSONAL_DAYS_TIMEOFF_TYPE_NAME, "policy")
    if not policy:
        return []

    new_line = rail.result("build_personal_days_policy_line")
    mode = rail.result("determine_mode")

    # Cutoff date for keeping historical lines:
    #   - outbound/return: use the new line's effectiveDate.
    #   - cleanup: no new line is produced; the cleanup itself is the act of
    #     dropping future-dated integration lines, so cutoff = return_date
    #     (the current change_effective_date).
    if new_line:
        cutoff_dt = get_date_from_replicon_date(new_line[0]["effectiveDate"]).date()
    elif mode == "cleanup":
        cutoff_dt = datetime.strptime(dag_run.conf["change_effective_date"], DATE_FORMAT).date()
    else:
        # No new line + no cleanup mode (defensive - normal flow gates this out).
        return list(map(lambda item: {
            "description": item["description"],
            "effectiveDate": item["effectiveDate"],
            "policySet": item["policySet"],
        }, policy))

    return list(filter(
        lambda x: get_date_from_replicon_date(x["effectiveDate"]).date() < cutoff_dt,
        map(lambda item: {
            "description": item["description"],
            "effectiveDate": item["effectiveDate"],
            "policySet": item["policySet"],
        }, policy),
    ))


def get_all_personal_days_policy_to_assign():
    historical = rail.result("get_personal_days_historical_policy_lines")
    new_line = rail.result("build_personal_days_policy_line")
    if historical and new_line:
        data = historical + new_line
    elif new_line:
        data = new_line
    elif historical:
        data = historical
    else:
        # Always return a valid JSON array so downstream json.loads() never
        # raises. The DAG's is_policy_line_built guard normally prevents
        # reaching this state when there is no new line.
        return "[]"
    # Replicon's PUT endpoint expects 'scriptTarget' in the schedule entries
    # (the GET endpoint returns 'script' - same key, different name). Mirrors
    # the existing vacation pattern in get_all_policy_to_assign_for_special_accrual.
    return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))


def resolve_personal_days_timeoff_type_uri(config):
    return rail.find_first_by_attr_and_get_attr(
        rail.result("get_user_time_off_policy_summary"),
        "timeoff_type_name",
        config.PERSONAL_DAYS_TIMEOFF_TYPE_NAME,
        "timeoff_type_uri",
    )


def should_write_personal_days_policy(dag_run, config):
    # Optimization #1: in cleanup mode, only proceed to the balance-summary +
    # PUT pipeline if there is actually a future-dated integration-written
    # line to drop. Otherwise the PUT would be a no-op rewrite of the same
    # schedule and we burn three Replicon calls for nothing.
    #
    # Outbound/return modes always proceed - they have new lines to write.
    mode = rail.result("determine_mode")
    if mode in ("outbound", "return"):
        return True
    if mode != "cleanup":
        return False

    summary = rail.result("get_user_time_off_policy_summary")
    policy = rail.find_first_by_attr_and_get_attr(
        summary, "timeoff_type_name", config.PERSONAL_DAYS_TIMEOFF_TYPE_NAME, "policy")
    if not policy:
        return False
    return_dt = datetime.strptime(dag_run.conf["change_effective_date"], DATE_FORMAT).date()
    for line in policy:
        eff_dt = get_date_from_replicon_date(line["effectiveDate"]).date()
        desc = line.get("description") or ""
        if eff_dt >= return_dt and PERSONAL_DAYS_INTEGRATION_MARKER in desc:
            return True
    return False


def is_personal_days_balance_summary_needed(dag_run, config):
    # Optimization #2: only fetch GetBalanceSummaryForAccount when the
    # outbound action actually needs the user's hours-used value. Both
    # prorate_at_leave_start and prorate_then_buffer_then_zero (UNPADP /
    # UNPMPA) read consumed directly from the response's
    # timeTakenForPeriod field. All other paths discard the value, so the
    # call is wasted.
    if rail.result("determine_mode") != "outbound":
        return False
    rule = config.LEAVE_OUT_IMPACT_RULES.get(dag_run.conf.get("event"))
    return bool(rule and rule.get("action") in (
        "prorate_at_leave_start", "prorate_then_buffer_then_zero",
    ))
