import json
from datetime import datetime, timedelta, date
import pendulum
from airflow.models import Variable
from ast import literal_eval
from rail import set_result, result, load_all_records, smartjoin_by_delim, get_replicon_date


DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

LIST_OF_REQUIRED_KEYS = [
    'FIRST_NAME','LAST_NAME','LAST_FIRST_NAME','EMPL_ID','EMAIL_ID','ORIG_HIRE_DT','ADJ_HIRE_DT',
    'TERM_DT','S_EMPL_STATUS_CD','PERS_ACT_RSN_CD','BILL_LAB_CAT_CD','EFFECT_DT','DETL_JOB_CD',
    'MGR_EMPL_ID','EFFECT_DT','TAXBLE_ENTITY_ID','TAXBLE_ENTITY_NAME','ORG','ORG_ID','ORG_NAME','S_EMPL_TYPE_CD','S_HRLY_SAL_CD',
    'COUNTRY_CD','HR_ORG_ID','TS_PD_CD','TC_WORK_SCHED_CD','GENL_LAB_CAT_CD','TRN_CRNCY_CD','POLARIS_ROLE','TITLE_DESC',
    'PAY_PERIOD','ROSTER'
]

MANDATORY_KEY = [
    'first_name','last_name','empl_id','email_id','pay_period_code'
]

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_LAST_RUN = "YYYY-MM-DDTHH:mm:ss"

def get_current_date(time_zone):
    return pendulum.now(time_zone)

def parse_date_json(date_value: dict):
    return date(day=date_value['day'], month=date_value['month'], year=date_value['year'])

def get_updated_start_date():
    prev_timesheet_end_date = result("get_timesheet_details")["timesheet"]["dateRange"]["endDate"]
    return (parse_date_json(prev_timesheet_end_date) + timedelta(days=1)).strftime(DATE_FORMAT)

def do_get_last_run_date(config):
    last_24_hour = get_current_date(config.time_zone) - timedelta(hours=24)
    current_time = get_current_date(config.time_zone)
    last_run = Variable.get(config.last_run_date_var_name, default_var=None)
    last_run = pendulum.from_format(last_run, DATE_FORMAT_LAST_RUN) if last_run else None
    last_run_date = last_run or last_24_hour
    Variable.set(config.last_run_date_var_name, current_time.strftime(DATE_FORMAT))
    return last_run_date.strftime(DATE_FORMAT)

def user_data():
    return json.loads(result('get_conf_payload')) if result('get_conf_payload') else result('get_users_from_costpoint')

def is_costpoint_user_present():
    cost_point_user_obj = user_data()
    if cost_point_user_obj:
        cost_point_user_obj = [cost_point_user_obj] if isinstance(cost_point_user_obj, dict) else cost_point_user_obj
        for companyData in cost_point_user_obj:
            if companyData['document']['rows'] and\
                    len(companyData['document']['rows']) > 0:
                return True
    return False

def get_polaris_users():
    cost_point_user_obj = user_data()
    polaris_users = []
    resp = []
    if cost_point_user_obj:
        cost_point_user_obj = [cost_point_user_obj] if isinstance(cost_point_user_obj, dict) else cost_point_user_obj
        for companyData in cost_point_user_obj:
            polaris_users = [row for row in companyData['document']['rows']]
            for user in polaris_users:
                values = {}
                for key in LIST_OF_REQUIRED_KEYS:
                    raw_value = user['row']['data'].get(key, "")
                    values[key] = raw_value.strip() if isinstance(raw_value, str) else raw_value
                resp.append(values.copy())
    return resp

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key in MANDATORY_KEY:
        if not item[payload_key]:
            payload_key = ' '.join(word.capitalize() for word in payload_key.split('_'))
            missing_fields.append(payload_key)
    log_msg = smartjoin_by_delim(missing_fields, ";")
    log_msg = f"mandatory field(s) {log_msg} is not present in payload"
    return log_msg

def get_update_value(dag_run, current_empl_daterange, time_zone):
    dt = get_current_date(time_zone)
    if dag_run.conf['termination_date']:
        dt = datetime.strptime(dag_run.conf['termination_date'], DATE_FORMAT)
    elif current_empl_daterange.get("endDate"):
        dt = parse_date_json(current_empl_daterange["endDate"])
    return f"{dt.year}-{dt.month}-{dt.day}"

def check_add_or_update_user(dag_run, time_zone):
    user_data_resp = result("get_user_data")[0] if result("get_user_data") and result("get_user_data")[0] else None
    past_hire_date = "Yes" if dag_run.conf['past_hire_date'] else "No"
    pers_act_cd = dag_run.conf['personal_action_code'][:8]
    resp = {
        "setup_new_profile":"",
        "update_existing_profile":{}
    }
    if not user_data_resp:
        entity_trnsfr = pers_act_cd[:6]
        if pers_act_cd in ["HI-NWHIR","HI-INTRN", "HI-ROPTR"] and past_hire_date == "No":
            resp = {
                "setup_new_profile":"Yes",
                "update_existing_profile":{}
            }
        elif pers_act_cd in ["HI-ACQUS"] and past_hire_date == "Yes":
            resp = {
                "setup_new_profile":"Yes",
                "update_existing_profile":{}
            }
        elif entity_trnsfr == "TV-XFR" or (pers_act_cd in ["HI-REHIR", "HI-REINT","HI-ENTRN"] and past_hire_date == "Yes"):
            resp = {
                "setup_new_profile":"Yes",
                "update_existing_profile":{}
            }
        elif pers_act_cd in ["HI-REHIR", "HI-REINT"] and past_hire_date == "No":
            resp = {
                "setup_new_profile":"Yes",
                "update_existing_profile":{}
            }
    else:
        loginName = user_data_resp['securityConfiguration']['loginName']
        current_empl_id = user_data_resp['userDetails']['employeeId']
        current_empl_daterange = user_data_resp['userDetails']['employmentDateRange']
        entity_trnsfr = pers_act_cd[:6]
        is_enabled = user_data_resp['userDetails']['isEnabled']
        if pers_act_cd == "":
            resp = {
                "setup_new_profile":"No",
                "update_existing_profile":{}
            }
        elif entity_trnsfr == "TV-XFR" or (pers_act_cd in ["HI-REHIR", "HI-REINT","HI-ENTRN"] and past_hire_date == "Yes" and \
            current_empl_id != dag_run.conf['empl_id']):
            resp = {
                "setup_new_profile":"Yes",
                "update_existing_profile":{
                    "loginName":{
                        "value": f"{loginName}_{get_update_value(dag_run, current_empl_daterange, time_zone)}"
                    },
                }
            }
        elif pers_act_cd in ["HI-REHIR", "HI-REINT"] and past_hire_date == "Yes":
            resp = {
                "setup_new_profile":"No",
                "update_existing_profile":{}
            }
        elif pers_act_cd in ["HI-REHIR", "HI-REINT"] and past_hire_date == "No":
            resp = {
                "setup_new_profile":"Yes",
                "update_existing_profile":{
                    "loginName": {
                        "value": f"{loginName}_{get_update_value(dag_run, current_empl_daterange, time_zone)}"
                    },
                    "employeeId": {
                        "value": f"{current_empl_id}_{get_update_value(dag_run, current_empl_daterange, time_zone)}"
                    }
                }
            }
        elif pers_act_cd in ["CH-ORGNU","CH-TRANS"] and past_hire_date == "No":
            resp = {
                "setup_new_profile":"No",
                "update_existing_profile":{}
            }
        elif is_enabled:
            resp = {
                "setup_new_profile":"No",
                "update_existing_profile":{}
            }
    return resp

def load_records(log_artifact):
    return load_all_records(log_artifact)

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        elif isinstance(userlogs, str) and userlogs[0] == '[':
            userlogs = literal_eval(userlogs)
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
            **dict(log['properties'].items()),
        }, log_records))

    set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    set_result(key="skipped_record_count",val= len(list(filter(lambda x: x['status'] == 'Skipped', final_log_records ))))

    return  final_log_records

def if_termination_date_in_past(dag_run, time_zone):
    current = get_current_date(time_zone).strftime(DATE_FORMAT)
    return datetime.strptime(dag_run.conf['termination_date'], DATE_FORMAT) < datetime.strptime(current, DATE_FORMAT)

def get_option_list_to_add(dag_run):
    personal_action_code_dropdowns = {item['displayText']: '' for item in result('get_all_personal_action_code_dropdowns')}
    detail_job_title_dropdowns = {item['displayText']: '' for item in result('get_all_detail_job_title_dropdowns')}
    line_of_business_dropdowns = {item['displayText']: '' for item in result('get_all_line_of_business_dropdowns')}

    unique_personal_action_codes = [item['personal_action_code'] for item in load_all_records(result('query_unique_personal_action_code'))]
    unique_detail_job_title = [item['title_desc'] for item in load_all_records(result('query_unique_detail_job_title'))]
    unique_line_of_business = [item['hr_organization'] for item in load_all_records(result('query_unique_line_of_business'))]
    
    return {
        'personal_action_code_to_add': list(filter(lambda item: item not in personal_action_code_dropdowns, unique_personal_action_codes)),
        'detail_job_title_to_add': list(filter(lambda item: item not in detail_job_title_dropdowns, unique_detail_job_title)),
        'line_of_business_to_add': list(filter(lambda item: item not in line_of_business_dropdowns, unique_line_of_business))
    }

def get_updated_effective_date(dag_run):
    resp = result("get_default_time_off_type_policy_schedule_for_user")
    effective_date = datetime.strptime(dag_run.conf["current_hire_date"], DATE_FORMAT)
    effective_date = get_replicon_date(effective_date)
    for i in resp:
        if i:
            i[0].update({"effectiveDate": effective_date})
    return resp

def is_user_start_date_changed(dag_run, instance):
    if instance == 'prod':
        return False
    user_data_list = result("get_user_data")
    if not user_data_list or not user_data_list[0]:
        return False

    user_details = user_data_list[0].get('userDetails', {})
    current_range = user_details.get('employmentDateRange', {})
    start_date = current_range.get('startDate')
    new_hire_date = dag_run.conf.get("current_hire_date")

    if not start_date and new_hire_date:
        return True

    if start_date and new_hire_date:
        current_empl_date = parse_date_json(start_date)
        effective_date = datetime.strptime(new_hire_date, DATE_FORMAT)
        return current_empl_date != effective_date

    return False
