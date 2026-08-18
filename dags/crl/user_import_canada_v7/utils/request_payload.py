from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
from uuid import uuid4
import rail

null = None
DATE_FORMAT = "%m/%d/%Y"

WX3000_SCHEDULE = 'WX3000'
WX4000_SCHEDULE = 'WX4000'
WX3000_HOLIDAY_CALENDAR = 'CAN WX3000 Exception Employees'
RESTRICTED_INSTANCES = ['trial']

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

def get_date_from_replicon_date(replicon_date):
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

MANDATORY_FIELDS = {
        "emp_id":"Empl_ID",
        "first_name":"First_Name",
        "last_name": "Last_Name",
        "email": "Work_Email",
        "login_name": "User_Name",
        "emp_status": "Empl_Status",
        "buisness_unit_full_path": "Bus_Seg_Unit",
        'company_code':'Company',
        'location_full_path': 'Location',
        'reg_temp': 'Reg_Temp',
        'full_part': 'Full_Part',
        'start_date': 'Hire_Date',
        'adjusted_start_date': 'Adj_Hire_Date',
        'job_code': 'Job_Code',
        'pay_type': 'Pay_Type',
        'cost_center_full_path':'Cost_Center_Business_Area',
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")

    if item['emp_status'] not in ['Active','Unpaid Leave',
        'Terminated','Suspended','Retired','Paid Leave','Furlough','Dormant','Discarted','Deceased']:
        missing_fields.append("Employee Status should be from ('Active','Unpaid Leave',\
        'Terminated','Suspended','Retired','Paid Leave','Furlough','Dormant','Discarted','Deceased')")

    if item['pay_type'] not in ['Hourly','Salaried','Exception Hourly']:
        missing_fields.append("Pay Type is not Hourly/Salaried/Exception Hourly")

    if item['full_part'] == "Per Diem":
        missing_fields.append("Full/Part-Time should not be 'Per Diem'")

    return rail.smartjoin_by_delim(missing_fields, ";")

def get_add_buisness_unit_payload(dag_run):
    return {
        "division": {
            "parent": {
                "uri": rail.result("get_parent_buisness_unit_details")[0]['uri']
            },
        } if rail.result("get_parent_buisness_unit_details") else null,
        "modifications": {
            "name": dag_run.conf['buisness_unit_name'],
            "codeToApply": {
                "value": dag_run.conf['buisness_unit_label']
            } if dag_run.conf['buisness_unit_label'] else null,
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid4())
    }

def get_add_location_payload(dag_run):
    return {
        "location": {
            "parent": {
                "uri": rail.result("get_parent_location_details")[0]['uri']
            },
        } if rail.result("get_parent_location_details") else null,
        "modifications": {
            "name": dag_run.conf['location_name'],
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid4())
    }

def get_add_cost_center_payload(dag_run):
    return {
        "costCenter": {
            "parent": {
                "uri": rail.result("get_parent_cost_center_details")[0]['uri']
            },
        }if rail.result("get_parent_cost_center_details") else null ,
        "modifications": {
            "name": dag_run.conf['cost_center_name'],
             "codeToApply": {
                "value": dag_run.conf['cost_center_label']
            } if dag_run.conf['cost_center_label'] else null,
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid4())
    }

def get_all_employee_grp_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def get_process_users_conf(item, user_mapper, active_status, batch_count, instance):
    get_all_permission_sets = rail.result("get_all_permission_set")
    get_user_udfs = rail.result('get_user_udfs')

    def is_job_code_na(value):
        return bool(value['activity_type']=='Populated' and value['reg_temp'] in ['Regular','Temporary'] and
            value['pay_type']=='Salaried' and "STCONSTANT" not in value['location_level_3'])

    def get_activity_type():
        if item['activity_type']:
            return "Populated"
        return "Blank"

    user_details_based_on_mapper = list(filter(lambda x:
            x['location_level_1'] == item['location_full_path'].split('|')[0] and
            x['location_level_2']==item['location_full_path'].split('|')[1] and
            item['location_full_path'].split('|')[2] in x['location_level_3'] and
            (x['activity_type']==get_activity_type()) and
            x['reg_temp']==item['reg_temp'] and
            x['pay_type']==item['pay_type'] and
            (x['job_code'] =="NA" if is_job_code_na(x) else ( x['job_code']=="ALL" or
                (x['job_code']=="TYPE 1" if item['job_code'][-2:] in ['S1','S2','S3','S4','S5','X9'] else x['job_code']=="TYPE 2" ))),
        user_mapper))

    def get_employee_status():
        if item['emp_status'] in active_status:
            return 'Active'
        return 'Terminated'

    def get_employee_type_name():
        if item['is_contingent'] =="Y":
            return "Contingent Worker"

        return ('Salaried OT Eligible' if item['pay_type']=='Exception Hourly' else item['pay_type'])+\
            '_'+item['reg_temp']+'_'+("Part-Time" if item['full_part']=="On Call" else item['full_part'])+('_Project' if item['activity_type'] else '')

    def get_default_time_off_type_uri(item):
        if item['location_full_path'].split('|')[2] in ["MONTREAL", "SHERBROOKE", "LAVAL", "LAVALQ", "TERREBONNE", "BOISBRIAND"]:
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_time_off_types'), 'timeoff_type_name', '[CAN] Vacances/Vacation','timeoff_type_uri')
        if item['location_full_path'].split('|')[2] == "STCONSTANT":
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_time_off_types'), 'timeoff_type_name', '[CAN] Vacances/Vacation St. Constant','timeoff_type_uri')
        return null

    def get_payrule_script_uri_exception():
        if item['work_schedule']==WX3000_SCHEDULE:
            return rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_payrule_scripts"),'displayText','Montreal 30 Hour Emp' ,"uri")
        if item['work_schedule']==WX4000_SCHEDULE:
            return rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_payrule_scripts"),'displayText','Montreal 40 Hour Emp' ,"uri")
        return null

    def get_payrule_name():
        if item['work_schedule']==WX3000_SCHEDULE:
            return 'Montreal 30 Hour Emp'
        if item['work_schedule']==WX4000_SCHEDULE:
            return 'Montreal 40 Hour Emp'
        return null

    return {
        **item,
        **{
            "modulo" : int(item['record_id'])%batch_count,

            "user_mapper_value": rail.write_json_artifact(user_details_based_on_mapper),
            'supervisor_log' : rail.result('create_supervisor_log'),

            "replicon_employee_status": get_employee_status(),

            'title_def_uri': get_user_udfs['title_def_uri'],
            'functional_segment_def_uri': get_user_udfs['functional_segment_def_uri'],
            'std_hrs_def_uri': get_user_udfs['std_hrs_def_uri'],
            'adjusted_hiredate_def_uri': get_user_udfs['adjusted_hiredate_def_uri'],
            'adjusted_hiredate_accrual_def_uri': get_user_udfs['adjusted_hiredate_accrual_def_uri'],
            'job_code_def_uri': get_user_udfs['job_code_def_uri'],
            'pay_grp_def_uri': get_user_udfs['pay_grp_def_uri'],
            'us_flsa_status_def_uri': get_user_udfs['us_flsa_status_def_uri'],
            'profit_center_def_uri': get_user_udfs['profit_center_def_uri'],
            'project_user_def_uri': get_user_udfs['project_user_def_uri'],
            'us_vacation_exception_def_uri': get_user_udfs['us_vacation_exception_def_uri'],
            'us_veterans_day_def_uri': get_user_udfs['us_veterans_day_def_uri'],
            'term_exported_def_uri': get_user_udfs['term_exported_def_uri'],
            'sick_payout_eligible_def_uri': get_user_udfs['sick_payout_eligible_def_uri'],
            'banked_ot_def_uri': get_user_udfs['banked_ot_def_uri'],
            'emp_status_def_uri': get_user_udfs['emp_status_def_uri'],
            'buisness_segment_def_uri': get_user_udfs['buisness_segment_def_uri'],
            'buisness_unit_def_uri': get_user_udfs['buisness_unit_def_uri'],
            'reg_temp_def_uri': get_user_udfs['reg_temp_def_uri'],
            'full_part_def_uri': get_user_udfs['full_part_def_uri'],
            'is_hrbp_def_uri': get_user_udfs['is_hrbp_def_uri'],
            'pay_type_def_uri': get_user_udfs['pay_type_def_uri'],
            'remote_worker_def_uri': get_user_udfs['remote_worker_def_uri'],
            'change_effective_date_def_uri': get_user_udfs['change_effective_date_def_uri'],
            'event_def_uri': get_user_udfs['event_def_uri'],
            'event_reason_def_uri': get_user_udfs['event_reason_def_uri'],
            # V2.7 - propagated per-user; may be None on tenants without the UDFs (writes are gated on truthy URI).
            'leave_start_date_def_uri': get_user_udfs.get('leave_start_date_def_uri'),
            'leave_end_date_def_uri': get_user_udfs.get('leave_end_date_def_uri'),
            'default_activity_def_uri': get_user_udfs['default_activity_def_uri'],
            'cost_center_def_uri': get_user_udfs['cost_center_def_uri'],

            'us_flsa_status_drop_uri': rail.find_first_by_attr_and_get_attr
                (rail.result("get_us_flsa_status_dropdown_values"),'name', item['us_flsa_status'],'uri')
                if item['us_flsa_status'] else null,
            'project_user_drop_uri': rail.find_first_by_attr_and_get_attr
                (rail.result("get_project_user_dropdown_values"),'name',"Yes" if item['activity_type'] else "No",'uri'),
            'us_veterans_drop_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_us_veterans_day_dropdown_values"),'name',"Y",'uri'),
            'term_exported_drop_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_term_exported_dropdown_values"),'name',"No",'uri'),
            'sick_payout_drop_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_sick_payout_dropdown_values"),'name',"No",'uri'),
            'banked_ot_drop_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_banked_ot_dropdown_values"),'name',"No",'uri'),

            'location_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_location_grps'), 'full_path', item['location_full_path'], 'uri'),
            'buisness_unit_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_buisness_unit_grps'),
                'full_path', item['buisness_unit_full_path'], 'uri'),
            'cost_center_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_cost_center_grps'),
                'full_path', item['cost_center_full_path'], 'uri'),
            'company_code_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_company_code'), 'name', item['company_code'], 'uri'),

            "employee_type_name": get_employee_type_name(),
            'employee_type_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_types_grp'), 'name', get_employee_type_name(), 'uri'),

            "timesheet_period": user_details_based_on_mapper[0]['timesheet_period'] if user_details_based_on_mapper else null,

            "timesheet_template_name": user_details_based_on_mapper[0]['timesheet_template']
                    if user_details_based_on_mapper and user_details_based_on_mapper[0]['timesheet_template'] else null,
            "timesheet_template_uri": rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',user_details_based_on_mapper[0]['timesheet_template'],"uri")
                if user_details_based_on_mapper and user_details_based_on_mapper[0]['timesheet_template'] else null,

            "timesheet_approval_path":user_details_based_on_mapper[0]['timesheet_approval_path'] if user_details_based_on_mapper else null,

            "timeoff_template_name": user_details_based_on_mapper[0]['time_off_template']
                    if user_details_based_on_mapper and user_details_based_on_mapper[0]['time_off_template'] else null,
            "timeoff_template_uri": rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',user_details_based_on_mapper[0]['time_off_template'],"uri")
                if user_details_based_on_mapper and user_details_based_on_mapper[0]['time_off_template'] else null,

            "timeoff_approval_path":user_details_based_on_mapper[0]['time_off_approver']
                    if user_details_based_on_mapper and user_details_based_on_mapper[0]['time_off_approver'] else null,

            "punch_policy_name": user_details_based_on_mapper[0]['punch_policy']
                    if user_details_based_on_mapper and user_details_based_on_mapper[0]['punch_policy'] else null,
            "punch_policy_uri":rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',user_details_based_on_mapper[0]['punch_policy'],"uri")
                if user_details_based_on_mapper and user_details_based_on_mapper[0]['punch_policy'] else null,

            "holiday_calendar": WX3000_HOLIDAY_CALENDAR
                    if item['work_schedule'] == WX3000_SCHEDULE and instance in RESTRICTED_INSTANCES
                    else (user_details_based_on_mapper[0]['holiday_calendar'] if user_details_based_on_mapper else null),

            "work_week": f"urn:replicon:day-of-week:{(user_details_based_on_mapper[0]['work_week'].split(' ')[0].lower())}"
                    if user_details_based_on_mapper else null,

            "timezone": user_details_based_on_mapper[0]['timezone'] if user_details_based_on_mapper else null,
            'timezone_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_timezones'),
                'displayText', user_details_based_on_mapper[0]['timezone'], 'uri') if user_details_based_on_mapper else null,

            'payrule_name': user_details_based_on_mapper[0]['payrule_name']
                    if user_details_based_on_mapper and user_details_based_on_mapper[0]['payrule_name'] and item['work_schedule'] not in [WX3000_SCHEDULE, WX4000_SCHEDULE]
                     else get_payrule_name(),

            'payrule_script_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_payrule_scripts"),'displayText', user_details_based_on_mapper[0]['payrule_name'],"uri")
                if  user_details_based_on_mapper and user_details_based_on_mapper[0]['payrule_name'] and item['work_schedule'] not in [WX3000_SCHEDULE, WX4000_SCHEDULE]
                else get_payrule_script_uri_exception(),

            "supervisor_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Supervisor','uri'),
            "report_user_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Report User','uri'),
            "report_user_substitute_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Report User with Substitute','uri'),

            "admin_hrpb_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'View Only Admin HRPB','uri'),
            "ts_hrpb_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'View Only TS HRBP','uri'),

            'starting_balance_script_uri': rail.result('get_timeoff_balance_event_script_uri')['starting_balance_script_uri'],
            'prevent_balance_overdraw_uri': rail.result('get_timeoff_balance_validation_script')['prevent_balance_overdraw_uri'],
            'default_time_off_type_uri': get_default_time_off_type_uri(item),

            "place_name": rail.find_first_by_attr_and_get_attr(rail.result("get_place_details"),
                'place_name', item["location_full_path"].split("|")[2],'place_name'),
            "place_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_place_details"),
                'place_name', item["location_full_path"].split("|")[2],'place_uri'),

            'office_schedule_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),'displayText',item['work_schedule'],"uri")
                if item['work_schedule'] else null,

            'canada_location_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_location_grps'), 'full_path', 'CAN', 'uri')

        }
    }

def test_valid_fields(dag_run):
    # pylint: disable=too-many-return-statements
    startdate = get_replicon_date(dag_run.conf['start_date'])
    adjusted_hire_date = get_replicon_date(dag_run.conf['adjusted_start_date'])
    if not startdate or (dag_run.conf['is_contingent']=='N' and not adjusted_hire_date):
        return False
    if dag_run.conf['end_date']:
        enddate = get_replicon_date(dag_run.conf['end_date'])
        if not enddate:
            return False

    if dag_run.conf['change_effective_date']:
        effective_date = get_replicon_date(dag_run.conf['change_effective_date'])
        if not effective_date:
            return False

    if not rail.load_json_artifact(dag_run.conf['user_mapper_value']) and dag_run.conf['location_full_path'].split('|')[0] != 'USA'\
        and dag_run.conf['is_contingent']!='Y':
        return False
    return True

def get_invalid_fields_message(dag_run):
    log=[]
    startdate = get_replicon_date(dag_run.conf['start_date'])
    if not startdate:
        log.append('Invalid format for Hire Date')

    adjusted_hire_date = get_replicon_date(dag_run.conf['adjusted_start_date'])
    if not adjusted_hire_date:
        log.append('Invalid format for Adjusted Hire Date')

    if dag_run.conf['end_date']:
        enddate = get_replicon_date(dag_run.conf['end_date'])
        if not enddate:
            log.append('Invalid format for Last worked day')

    if dag_run.conf['change_effective_date']:
        enddate = get_replicon_date(dag_run.conf['change_effective_date'])
        if not enddate:
            log.append('Invalid format for Change Effective Date')

    if not rail.load_json_artifact(dag_run.conf['user_mapper_value']):
        log.append('No Mapping data found in Mapper')

    return rail.smartjoin_by_delim(log,";")

def get_process_new_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log' : rail.result('create_user_log')
        }
    }

def get_process_update_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log': rail.result('create_user_log'),
            'useruri': rail.result('get_user_data')[0]['uri'],
            'todays_date': (datetime.now()).strftime(DATE_FORMAT)
        }
    }

def get_remove_timeoff_payload(add_user_taskid):
    return {
        "userUri": rail.result(add_user_taskid)['uri'],
        "timeOffTypeUris": []
    }

def get_udfs(user_status, dag_run):
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    udfs = []
    def add_udf_field_values(definitionuri, dropdownuri = null, textvalue = null , number = null, date = null):
        udfs.append({
        "customField": {
          "uri": definitionuri,
          "name": null,
          "groupUri": null
        },
        "text": textvalue,
        "date": get_replicon_date(date) if date else null,
        "dropDownOption": {
          "uri": dropdownuri,
          "name": null
        } if dropdownuri != null else null,
        "number": number
      })

    def get_adjusted_hiredate_accrual():
        adjusted_start_date = datetime.strptime(dag_run.conf['adjusted_start_date'], DATE_FORMAT)
        return  ((adjusted_start_date + relativedelta(months=13)).replace(day=1)).strftime(DATE_FORMAT)

    if user_status =='adduser':
        add_udf_field_values(definitionuri = dag_run.conf['project_user_def_uri'], dropdownuri= dag_run.conf['project_user_drop_uri'])
        if dag_run.conf['title']:
            add_udf_field_values(definitionuri = dag_run.conf['title_def_uri'], textvalue= dag_run.conf['title'])
        if dag_run.conf['functional_segment']:
            add_udf_field_values(definitionuri = dag_run.conf['functional_segment_def_uri'], textvalue= dag_run.conf['functional_segment'])
        if dag_run.conf['std_hrs']:
            add_udf_field_values(definitionuri = dag_run.conf['std_hrs_def_uri'], number= dag_run.conf['std_hrs'])
        if dag_run.conf['adjusted_start_date']:
            add_udf_field_values(definitionuri = dag_run.conf['adjusted_hiredate_def_uri'], date= dag_run.conf['adjusted_start_date'])
            add_udf_field_values(definitionuri = dag_run.conf['adjusted_hiredate_accrual_def_uri'], date = get_adjusted_hiredate_accrual())
        if dag_run.conf['job_code']:
            add_udf_field_values(definitionuri = dag_run.conf['job_code_def_uri'], textvalue= dag_run.conf['job_code'])
        if dag_run.conf['pay_grp']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_grp_def_uri'], textvalue= dag_run.conf['pay_grp'])
        if dag_run.conf['us_flsa_status']:
            add_udf_field_values(definitionuri = dag_run.conf['us_flsa_status_def_uri'], dropdownuri= dag_run.conf['us_flsa_status_drop_uri'])
        if dag_run.conf['profit_center']:
            add_udf_field_values(definitionuri = dag_run.conf['profit_center_def_uri'], textvalue= dag_run.conf['profit_center'])
        if dag_run.conf['us_vacation_exception']:
            add_udf_field_values(definitionuri = dag_run.conf['us_vacation_exception_def_uri'], textvalue= dag_run.conf['us_vacation_exception'])
        if dag_run.conf['us_veterans_status']:
            add_udf_field_values(definitionuri = dag_run.conf['us_veterans_day_def_uri'], dropdownuri= dag_run.conf['us_veterans_drop_uri'])

        add_udf_field_values(definitionuri = dag_run.conf['emp_status_def_uri'], textvalue= dag_run.conf['emp_status'])
        add_udf_field_values(definitionuri = dag_run.conf['buisness_segment_def_uri'], textvalue= (dag_run.conf['buisness_unit_full_path']).split('|')[0])
        add_udf_field_values(definitionuri = dag_run.conf['buisness_unit_def_uri'], textvalue= (dag_run.conf['buisness_unit_full_path']).split('|')[1])
        if dag_run.conf['full_part']:
            add_udf_field_values(definitionuri = dag_run.conf['full_part_def_uri'], textvalue= dag_run.conf['full_part'])
        if dag_run.conf['reg_temp']:
            add_udf_field_values(definitionuri = dag_run.conf['reg_temp_def_uri'], textvalue= dag_run.conf['reg_temp'])
        if dag_run.conf['pay_type']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_type_def_uri'], textvalue= dag_run.conf['pay_type'])
        if dag_run.conf['is_hrbp']:
            add_udf_field_values(definitionuri = dag_run.conf['is_hrbp_def_uri'], textvalue= dag_run.conf['is_hrbp'])
        if dag_run.conf['remote_worker']:
            add_udf_field_values(definitionuri = dag_run.conf['remote_worker_def_uri'], textvalue= dag_run.conf['remote_worker'])
        if dag_run.conf['event']:
            add_udf_field_values(definitionuri = dag_run.conf['event_def_uri'], textvalue= dag_run.conf['event'])
        if dag_run.conf['event_reason_code']:
            add_udf_field_values(definitionuri = dag_run.conf['event_reason_def_uri'], textvalue= dag_run.conf['event_reason_code'])
        if dag_run.conf['activity_type']:
            add_udf_field_values(definitionuri = dag_run.conf['default_activity_def_uri'], textvalue= dag_run.conf['activity_type'])
        if dag_run.conf['cost_center_full_path']:
            add_udf_field_values(definitionuri = dag_run.conf['cost_center_def_uri'], textvalue= (dag_run.conf['cost_center_full_path']).split('|')[0])

    if user_status !='adduser' and validate_rehire(dag_run) :
        add_udf_field_values(definitionuri = dag_run.conf['term_exported_def_uri'], dropdownuri= dag_run.conf['term_exported_drop_uri'])
        add_udf_field_values(definitionuri = dag_run.conf['sick_payout_eligible_def_uri'], dropdownuri= dag_run.conf['sick_payout_drop_uri'])
        add_udf_field_values(definitionuri = dag_run.conf['banked_ot_def_uri'], dropdownuri= dag_run.conf['banked_ot_drop_uri'])

    if user_status =='updateuser':
        add_udf_field_values(definitionuri = dag_run.conf['project_user_def_uri'], dropdownuri= dag_run.conf['project_user_drop_uri'])

        current_title = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Title', 'text')
        if dag_run.conf['title'] and current_title != dag_run.conf['title']:
            add_udf_field_values(definitionuri = dag_run.conf['title_def_uri'], textvalue= dag_run.conf['title'])

        current_functional_segment = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Functional Segment', 'text')
        if dag_run.conf['functional_segment'] and current_functional_segment != dag_run.conf['functional_segment']:
            add_udf_field_values(definitionuri = dag_run.conf['functional_segment_def_uri'], textvalue= dag_run.conf['functional_segment'])

        current_std_hrs = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Standard Hours', 'text')
        if dag_run.conf['std_hrs'] and (float(current_std_hrs) if current_std_hrs else current_std_hrs) != float(dag_run.conf['std_hrs']):
            add_udf_field_values(definitionuri = dag_run.conf['std_hrs_def_uri'], number= dag_run.conf['std_hrs'])

        current_adjusted_start_date = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Adjusted Hire Date', 'date')
        if dag_run.conf['adjusted_start_date'] and get_date_from_replicon_date(current_adjusted_start_date
                )!= get_date_from_replicon_date(get_replicon_date(dag_run.conf['adjusted_start_date'])):
            add_udf_field_values(definitionuri = dag_run.conf['adjusted_hiredate_def_uri'], date= dag_run.conf['adjusted_start_date'])
            add_udf_field_values(definitionuri = dag_run.conf['adjusted_hiredate_accrual_def_uri'], date= get_adjusted_hiredate_accrual())

        current_job_code= rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Job Code', 'text')
        if dag_run.conf['job_code'] and current_job_code != dag_run.conf['job_code']:
            add_udf_field_values(definitionuri = dag_run.conf['job_code_def_uri'], textvalue= dag_run.conf['job_code'])

        current_pay_grp = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Pay Group', 'text')
        if dag_run.conf['pay_grp'] and current_pay_grp != dag_run.conf['pay_grp']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_grp_def_uri'], textvalue= dag_run.conf['pay_grp'])

        current_profit_center = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Profit Center', 'text')
        if dag_run.conf['profit_center'] and current_profit_center != dag_run.conf['profit_center']:
            add_udf_field_values(definitionuri = dag_run.conf['profit_center_def_uri'], textvalue= dag_run.conf['profit_center'])

        current_us_vacation_exception = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Vacation Exception', 'text')
        if dag_run.conf['us_vacation_exception'] and current_us_vacation_exception != dag_run.conf['us_vacation_exception']:
            add_udf_field_values(definitionuri = dag_run.conf['us_vacation_exception_def_uri'], textvalue= dag_run.conf['us_vacation_exception'])

        current_us_flsa_status = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'US FLSA Status', 'text')
        if dag_run.conf['us_flsa_status'] and current_us_flsa_status != dag_run.conf['us_flsa_status']:
            add_udf_field_values(definitionuri = dag_run.conf['us_flsa_status_def_uri'], dropdownuri= dag_run.conf['us_flsa_status_drop_uri'])

        current_us_veterans_status = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'US Veterans Day', 'text')
        if dag_run.conf['us_veterans_status'] and current_us_veterans_status != dag_run.conf['us_veterans_status']:
            add_udf_field_values(definitionuri = dag_run.conf['us_veterans_day_def_uri'], dropdownuri= dag_run.conf['us_veterans_drop_uri'])

        current_emp_status = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Employee Status', 'text')
        if dag_run.conf['emp_status'] and current_emp_status != dag_run.conf['emp_status']:
            add_udf_field_values(definitionuri = dag_run.conf['emp_status_def_uri'], textvalue= dag_run.conf['emp_status'])

        # V2.7 - Leave End Date: write only on Unpaid Leave -> Active transition.
        # Captures the return date as a stable audit field. Combined with the
        # Leave Start Date UDF (written by the Unpaid Leave branch), this lets
        # the proration child DAG compute leave duration without relying on the
        # general Change Effective Date UDF (which can be overwritten mid-leave).
        leave_end_def_uri = dag_run.conf.get('leave_end_date_def_uri')
        if leave_end_def_uri \
                and dag_run.conf['emp_status'] == 'Active' \
                and current_emp_status == 'Unpaid Leave' \
                and dag_run.conf['change_effective_date']:
            add_udf_field_values(definitionuri=leave_end_def_uri, date=dag_run.conf['change_effective_date'])

        current_buisness_segment = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Business Segment', 'text')
        if dag_run.conf['buisness_unit_full_path'] and current_buisness_segment != (dag_run.conf['buisness_unit_full_path']).split('|')[0]:
            add_udf_field_values(definitionuri = dag_run.conf['buisness_segment_def_uri'], textvalue= (dag_run.conf['buisness_unit_full_path']).split('|')[0])

        current_buisness_segment = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Business Unit', 'text')
        if dag_run.conf['buisness_unit_full_path'] and current_buisness_segment != (dag_run.conf['buisness_unit_full_path']).split('|')[1]:
            add_udf_field_values(definitionuri = dag_run.conf['buisness_unit_def_uri'], textvalue= (dag_run.conf['buisness_unit_full_path']).split('|')[1])

        current_full_part = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Full/Part', 'text')
        if dag_run.conf['full_part'] and current_full_part != dag_run.conf['full_part']:
            add_udf_field_values(definitionuri = dag_run.conf['full_part_def_uri'], textvalue= dag_run.conf['full_part'])

        current_reg_temp = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Reg/Temp', 'text')
        if dag_run.conf['reg_temp'] and current_reg_temp != dag_run.conf['reg_temp']:
            add_udf_field_values(definitionuri = dag_run.conf['reg_temp_def_uri'], textvalue= dag_run.conf['reg_temp'])

        current_pay_type = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Pay Type', 'text')
        if dag_run.conf['pay_type'] and current_pay_type != dag_run.conf['pay_type']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_type_def_uri'], textvalue= dag_run.conf['pay_type'])

        current_is_hrbp = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')
        if dag_run.conf['is_hrbp'] and current_is_hrbp != dag_run.conf['is_hrbp']:
            add_udf_field_values(definitionuri = dag_run.conf['is_hrbp_def_uri'], textvalue= dag_run.conf['is_hrbp'])

        current_remote_worker = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Remote Worker', 'text')
        if dag_run.conf['remote_worker'] and current_remote_worker != dag_run.conf['remote_worker']:
            add_udf_field_values(definitionuri = dag_run.conf['remote_worker_def_uri'], textvalue= dag_run.conf['remote_worker'])

        current_change_effective_date = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Change Effective Date', 'date')
        if dag_run.conf['change_effective_date'] and \
            (get_date_from_replicon_date(current_change_effective_date).strftime(DATE_FORMAT) if current_change_effective_date else null) \
            != datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT).strftime(DATE_FORMAT):
            add_udf_field_values(definitionuri = dag_run.conf['change_effective_date_def_uri'], date= dag_run.conf['change_effective_date'])

        current_event = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Event', 'text')
        if dag_run.conf['event'] and current_event != dag_run.conf['event']:
            add_udf_field_values(definitionuri = dag_run.conf['event_def_uri'], textvalue= dag_run.conf['event'])

        current_event_reason = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Event Reason', 'text')
        if dag_run.conf['event_reason_code'] and current_event_reason != dag_run.conf['event_reason_code']:
            add_udf_field_values(definitionuri = dag_run.conf['event_reason_def_uri'], textvalue= dag_run.conf['event_reason_code'])

        current_activity_type = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Default Activity', 'text')
        if current_activity_type != dag_run.conf['activity_type']:
            add_udf_field_values(definitionuri = dag_run.conf['default_activity_def_uri'], textvalue= dag_run.conf['activity_type'])

        current_cost_center = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Cost Center', 'text')
        if dag_run.conf['cost_center_full_path'] and (current_cost_center != (dag_run.conf['cost_center_full_path']).split('|')[0]):
            add_udf_field_values(definitionuri = dag_run.conf['cost_center_def_uri'], textvalue= (dag_run.conf['cost_center_full_path']).split('|')[0])


    return udfs

def get_put_user_payload(dag_run):
    def get_policy_sets_for_new_user(dag_run):
        policy_set=[]
        if dag_run.conf['timesheet_template_name']:
            policy_set.append({
                    "uri": dag_run.conf['timesheet_template_uri'],
                    "name": null
                })
        if dag_run.conf['timeoff_template_name']:
            policy_set.append({
                    "uri": dag_run.conf['timeoff_template_uri'],
                    "name": null
                })
        if dag_run.conf['punch_policy_name']:
            policy_set.append({
                    "uri": dag_run.conf['punch_policy_uri'],
                    "name": null
                })
        if not policy_set:
            return null
        return policy_set

    def get_schedule_name():
        if dag_run.conf['location_full_path'].split('|')[2] in ["MONTREAL", "SHERBROOKE", "LAVAL", "LAVALQ", "TERREBONNE", "BOISBRIAND"]:
            return "M-F 7.5 hours/day"
        if dag_run.conf['location_full_path'].split('|')[2] == "STCONSTANT":
            return "M-F 8 hours/day"
        return null

    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['login_name'],
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "emailAddress": dag_run.conf['email'],
            "employeeId": dag_run.conf['emp_id'],
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule":
            [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['work_schedule'],
                        "officeSchedule": {
                            "officeScheduleUri":null,
                            "name": dag_run.conf['work_schedule']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['work_schedule'] and dag_run.conf['office_schedule_uri'] else
            [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": get_schedule_name(),
                        "officeSchedule": {
                            "officeScheduleUri":null,
                            "name": get_schedule_name()
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ],
            "workWeekStartDayUri": dag_run.conf['work_week'],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['start_date']),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                   "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['login_name'],
                "SSOName": dag_run.conf['login_name'],
            },
            "holidayCalendar": {
                "uri": null,
                "name": dag_run.conf['holiday_calendar']
            },
            "permissionSets": [
                {
                    "uri": dag_run.conf['report_user_permission_uri'],
                    "name": null
                }
            ]if dag_run.conf['is_hrbp'] != 'Y' else
            [
                {
                    "uri": dag_run.conf['report_user_permission_uri'],
                    "name": null
                },
                {
                    "uri": dag_run.conf['admin_hrpb_permission_uri'],
                    "name": null
                },
                {
                    "uri": dag_run.conf['ts_hrpb_permission_uri'],
                    "name": null
                }
            ],
            "policySets": get_policy_sets_for_new_user(dag_run),
            "timesheetApprovalPath": {
                "uri": null,
                "name": dag_run.conf['timesheet_approval_path']
                },
            "timeOffApprovalPath": {
                "uri": null,
                "name": dag_run.conf['timeoff_approval_path']
            } if dag_run.conf['timeoff_approval_path'] else null,
            "customFieldValues": get_udfs('adduser', dag_run),
            "assignedActivities": [],
            "timeZone":{
                "uri": dag_run.conf['timezone_uri'],
                "IANAName": null
            },
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf['location_uri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "divisionSchedule":  [
                {
                    "division": {
                    "uri": dag_run.conf['buisness_unit_uri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
                ],
            "costCenterSchedule": [
                {
                    "costCenter": {
                    "uri": dag_run.conf['cost_center_uri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }],
            "serviceCenterSchedule": [
                {
                    "serviceCenter": {
                    "uri": dag_run.conf['company_code_uri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
                ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf['employee_type_uri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": dag_run.conf['timesheet_period']
                    },
                    "effectiveDate": null
                }
            ],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_script_uri'],
                        "name": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['payrule_name'] else [],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": []
        }
    }

def stop_user_notification_preferences_payload():
    return {
        "user": {
            "uri": rail.result('add_contingent_or_usa_user')['uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "preferences": {
            "notificationDeliveryPreferences": [
            {
                "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:project",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:time-off",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:user",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:timesheet",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:holiday",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            }
            ],
            "sharedDeliveryPreferenceOptionUris": [
            "urn:replicon:user-shared-delivery-preference-option:always-deliver"
            ]
        }
        }

def get_put_contingent_or_usa_user_payload(dag_run):
    def get_schedule_name():
        if dag_run.conf['location_full_path'].split('|')[2] in ["MONTREAL", "SHERBROOKE", "LAVAL", "LAVALQ", "TERREBONNE", "BOISBRIAND"]:
            return "M-F 7.5 hours/day"
        if dag_run.conf['location_full_path'].split('|')[2] == "STCONSTANT":
            return "M-F 8 hours/day"
        if dag_run.conf['location_full_path'].split('|')[0] == "USA":
            return "M-F 8 hours/day"
        return null

    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['login_name'],
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "emailAddress": dag_run.conf['email'],
            "employeeId": dag_run.conf['emp_id'],
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule":
               [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['work_schedule'],
                        "officeSchedule": {
                            "officeScheduleUri":null,
                            "name": dag_run.conf['work_schedule']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['work_schedule'] and dag_run.conf['office_schedule_uri'] else
            [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": get_schedule_name(),
                        "officeSchedule": {
                            "officeScheduleUri":null,
                            "name": get_schedule_name()
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ],
            "workWeekStartDayUri": dag_run.conf['work_week'],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['start_date']),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                   "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "false",
                "loginName": dag_run.conf['login_name'],
                "SSOName": dag_run.conf['login_name'],
            },
            "permissionSets": [
                {
                    "uri": dag_run.conf['report_user_permission_uri'],
                    "name": null
                }
            ],
            "customFieldValues": get_udfs('adduser', dag_run),
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf['location_uri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "divisionSchedule":  [
                {
                    "division": {
                    "uri": dag_run.conf['buisness_unit_uri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
                ],
            "costCenterSchedule": [
                {
                    "costCenter": {
                    "uri": dag_run.conf['cost_center_uri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }],
            "serviceCenterSchedule": [
                {
                    "serviceCenter": {
                    "uri": dag_run.conf['company_code_uri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
                ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf['employee_type_uri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['is_contingent'] == 'Y' else null
        }
    }

def validate_enddate(dag_run):
    if dag_run.conf['end_date']:
        return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)
    return datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)

def is_enddate_in_future(dag_run):
    if dag_run.conf['end_date']:
        return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['todays_date'], DATE_FORMAT)
    return datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['todays_date'], DATE_FORMAT)

def validate_rehire(dag_run):
    return not rail.result('get_user_info')['userDetails']['isEnabled'] and not dag_run.conf['end_date'] and not validate_previous_suspended_leave_or_non_live_transfer_employee(dag_run)

def update_user_details(dag_run):
    user_details = rail.result("get_user_info")['userDetails']

    return {
      "firstName": dag_run.conf['first_name'] if user_details['firstName'] != dag_run.conf['first_name'] else null,
      "lastName": dag_run.conf['last_name'] if user_details['lastName'] != dag_run.conf['last_name'] else null,
      "emailAddress": {
        "emailAddress": dag_run.conf['email']
      } if user_details['emailAddress'] != dag_run.conf['email'] else null,
      "language": null,
      "employmentDateRange": null,
      "employmentStartDate": {
        "date": get_replicon_date(dag_run.conf['start_date'])
      } if user_details['employmentDateRange']['startDate'] != get_replicon_date(dag_run.conf['start_date']) and validate_rehire(dag_run) else null,
       "employmentEndDate": {
         "date": null
       } if validate_rehire(dag_run) or not dag_run.conf['end_date'] else ({
         "date": get_replicon_date(dag_run.conf['end_date'])
       }if dag_run.conf['end_date'] else null),
    }

def update_holiday_calendar(dag_run):
    if not dag_run.conf['holiday_calendar']:
        return null
    current_holiday_calendar = rail.result("get_user_info")['holidayCalendar']
    if current_holiday_calendar and current_holiday_calendar['displayText'] == dag_run.conf['holiday_calendar']:
        return null
    return{
        "holidayCalendar": {
            "uri": null,
            "name": dag_run.conf['holiday_calendar']
        }
    }

def update_location_grp(location_uri, current_location_uri, dag_run):
    return {
        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementLocationSchedule": [],
        "updateLocationScheduleOverDateRange": {
            "replacementLocationScheduleEntries": [
                {
                    "location": {
                        "uri": location_uri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ],
            "endDate": null
        }
    } if current_location_uri != location_uri else null

def update_buisness_unit_grp(buisness_unit_uri, current_buisness_unit_uri, dag_run):
    return {
        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDivisionSchedule": [],
        "updateDivisionScheduleOverDateRange": {
            "replacementDivisionScheduleEntries": [
                {
                    "division": {
                        "uri": buisness_unit_uri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ],
            "endDate": null
        }
    } if buisness_unit_uri != current_buisness_unit_uri else null

def update_cost_center_grp(cost_center_uri, current_cost_center_uri, dag_run):
    return {
        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementCostCenterSchedule": [],
        "updateCostCenterScheduleOverDateRange": {
            "replacementCostCenterScheduleEntries": [
                {
                    "costCenter": {
                        "uri": cost_center_uri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ],
            "endDate": null
        }
    } if cost_center_uri != current_cost_center_uri else null

def update_employee_type_grp(employee_type_uri, current_employee_type_uri, dag_run):
    return {
        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementEmployeeTypeGroupSchedule": [],
        "updateEmployeeTypeGroupScheduleOverDateRange": {
            "replacementEmployeeTypeGroupScheduleEntries": [
                {
                    "employeeTypeGroup": {
                        "uri": employee_type_uri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ],
            "endDate": null
        }
    } if employee_type_uri != current_employee_type_uri else null

def update_company_code_grp(company_code_uri, current_company_code_uri, dag_run):
    return {
      "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementServiceCenterSchedule": [],
      "updateServiceCenterScheduleOverDateRange": {
        "replacementServiceCenterScheduleEntries": [
          {
            "serviceCenter": {
              "uri": company_code_uri,
              "parentUri": null,
              "name": null
            },
            "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
          }
        ],
        "endDate": null
      }
    } if company_code_uri != current_company_code_uri else null

def update_timesheet_period(dag_run):
    user_udf_values = rail.result('get_user_info')[
                    'userDetails']['customFieldValues']

    previous_emp_status = rail.find_first_by_attr_and_get_attr(user_udf_values,
        'customField.displayText', 'Employee Status', 'text')

    current_timesheet_period = rail.result("get_user_info")['timesheetPeriodSchedule']

    def check_update_timesheet_period_required():
        if previous_emp_status == "Unpaid Leave":
            return True
        return current_timesheet_period and current_timesheet_period[-1]['timesheetPeriod']['displayText']!= dag_run.conf['timesheet_period']

    return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "name": dag_run.conf['timesheet_period'] if previous_emp_status != "Unpaid Leave" else "Canada - Weekly starting on Monday",
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ],
            "endDate": null
        }
    } if check_update_timesheet_period_required() else null

def update_timesheet_approval_path(dag_run):
    current_timesheet_approval_path = rail.result("get_user_info")['timesheetApprovalPath']

    if not current_timesheet_approval_path and dag_run.conf['timesheet_approval_path']:
        return {
            "uri": null,
            "name": dag_run.conf['timesheet_approval_path']
            }

    if dag_run.conf['timesheet_approval_path'] and dag_run.conf['timesheet_approval_path']!= \
        current_timesheet_approval_path['displayText']:
        return {
            "uri": null,
            "name": dag_run.conf['timesheet_approval_path']
            }

    return null

def get_payrule_effective_date(dag_run):
    date = datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT)
    day = (date).strftime("%A")
    if day == "Monday":
        return get_replicon_date(dag_run.conf['change_effective_date'])
    new_date = date + timedelta(days=(7-date.weekday())%7)
    return get_replicon_date(new_date.strftime("%m/%d/%Y"))

def update_payrule_script(dag_run):
    current_payrulescript = rail.result("get_user_info")['payRuleScriptSchedule']
    if not current_payrulescript and dag_run.conf['payrule_name']:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_script_uri'],
                        "name": null
                    },
                    "effectiveDate":  get_payrule_effective_date(dag_run)
                }
            ]
        }

    if dag_run.conf['payrule_name'] and (dag_run.conf['payrule_name'] != current_payrulescript[-1]['payRuleScript']['displayText']):
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_script_uri'],
                        "name": null
                    },
                    "effectiveDate": get_payrule_effective_date(dag_run)
                }
            ]
        }

    return null

def update_policy_set(dag_run):
    """
    Assigns timesheet template with effective date based on Monday week start for Canada.
    The effective date is calculated to align with the start of the timesheet period.
    """
    assigned_timesheet_template = rail.result("get_user_info")['timesheetTemplate']
    assigned_timeoff_template = rail.result("get_user_info")['timeOffTemplate']
    assigned_punch_policy = rail.result("get_assigned_policy_to_user")

    policy_set_to_assign = []
    
    # Check if timesheet template needs to be updated
    if dag_run.conf['timesheet_template_name'] and \
        (not assigned_timesheet_template or ((assigned_timesheet_template and dag_run.conf['timesheet_template_uri']) \
        and (dag_run.conf['timesheet_template_uri'] != assigned_timesheet_template['uri']))):
            # Return scheduled policy set with Monday-based effective date
           policy_set_to_assign.append(
                {
                    "policyUri": "urn:replicon:policy:timesheet",
                    "schedule": [
                        {
                            "policySetUri": dag_run.conf['timesheet_template_uri'],
                            "effectiveDate": get_payrule_effective_date(dag_run)  # Uses Monday calculation
                        }
                    ]
                }
           )
    if dag_run.conf['timeoff_template_name'] and (not assigned_timeoff_template or
        (( dag_run.conf['timeoff_template_name'] and assigned_timeoff_template and dag_run.conf['timeoff_template_uri']) \
        and (dag_run.conf['timeoff_template_uri'] != assigned_timeoff_template['uri']))):
            policy_set_to_assign.append(
                {
                    "policyUri": "urn:replicon:policy:time-off",
                    "schedule": [
                        {
                            "policySetUri": dag_run.conf['timeoff_template_uri'],
                            "effectiveDate": null
                        }
                    ]
                }
            )
    else:
        if not dag_run.conf['timeoff_template_name']:
            policy_set_to_assign.append(
                {
                    "policyUri": "urn:replicon:policy:time-off",
                    "schedule": []
                }
            )

    
    if dag_run.conf['punch_policy_name'] and (not assigned_punch_policy or (( assigned_punch_policy and dag_run.conf['punch_policy_uri']) \
        and (dag_run.conf['punch_policy_uri'] != assigned_punch_policy[0]['policySet']['uri']))):
            policy_set_to_assign.append(
                {
                    "policyUri": "urn:replicon:policy:time-punch",
                    "schedule": [
                        {
                            "policySetUri": dag_run.conf['punch_policy_uri'],
                            "effectiveDate": null
                        }
                    ]
                }
            )
    else:
        if not dag_run.conf['punch_policy_name']:
            policy_set_to_assign.append(
                {
                    "policyUri": "urn:replicon:policy:time-punch",
                    "schedule": []
                }
            )
    
    return policy_set_to_assign

def update_workweek(dag_run):
    assigned_workweek = rail.result('get_user_info')['userDetails']['workWeekStartDay']
    if not assigned_workweek:
        if dag_run.conf['work_week']:
            return {
                "workWeekStartDayUri": dag_run.conf['work_week']
            }
    if dag_run.conf['work_week'] and (dag_run.conf['work_week'] != rail.result('get_user_info')['userDetails']['workWeekStartDay']['uri']):
        return {
            "workWeekStartDayUri": dag_run.conf['work_week']
        }
    return null

def update_timezone(dag_run):
    assigned_timezone = rail.result('get_user_info')['timeZone']
    if not assigned_timezone:
        if dag_run.conf['timezone']:
            return {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": dag_run.conf['timezone_uri'],
                    "IANAName": null
            }
        }
        return null
    if dag_run.conf['timezone'] and (dag_run.conf['timezone'] != rail.result('get_user_info')['timeZone']['displayText']):
        return {
            "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
            "timezone": {
                "uri": dag_run.conf['timezone_uri'],
                "IANAName": null
            }
        }
    return null

def update_place_assignment(dag_run):
    previous_assigned_place = rail.result("get_assigned_place_to_user")

    if dag_run.conf['pay_type'] =="Hourly" and dag_run.conf['remote_worker']=="N":
        if previous_assigned_place and previous_assigned_place[-1]["place_uri"] == dag_run.conf['place_uri']:
            return null
        return {
                "placeAssignmentScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementPlaceAssignmentSchedule": [],
                "updatePlaceAssignmentScheduleOverDateRange": {
                    "replacementPlaceAssignmentScheduleEntries": [
                    {
                        "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date']),
                        "places": [
                        {
                            "uri": dag_run.conf['place_uri'],
                            "name": null
                        }
                        ]
                    }
                    ],
                    "endDate": null
                }
            }

    if not previous_assigned_place or not previous_assigned_place[-1]['place_uri']:
        return null
    return {
        "placeAssignmentScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementPlaceAssignmentSchedule": [],
        "updatePlaceAssignmentScheduleOverDateRange": {
            "replacementPlaceAssignmentScheduleEntries": [
            {
                "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date']),
                "places": []
            }
            ],
            "endDate": null
        }
    }


def update_schedule(dag_run, log):
    current_schedule = rail.result("get_user_info")['schedulePolicies']
    if current_schedule and current_schedule[-1]['scheduleTypeUri']=="urn:replicon:schedule-type:shift":
        return null
    if dag_run.conf['office_schedule_uri']:
        if not current_schedule or (dag_run.conf['work_schedule'] !=
            (current_schedule[-1]['officeSchedule']['displayText'] if current_schedule[-1]['scheduleTypeUri']==
            'urn:replicon:schedule-type:office-schedule' else null)):
            return {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                    {
                        "schedulePolicy": {
                        "officeScheduleUri": dag_run.conf['office_schedule_uri'],
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": dag_run.conf['office_schedule_uri'],
                            "name": null,
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                    }
                    ],
                    "endDate": null
                }
                }
        return null

    log.append("Work Schedule not available in Replicon")
    return null

def update_permission_set(dag_run):    
    permission_set_uri_to_assign=[]
    if validate_rehire(dag_run):
        permission_set_uri_to_assign.append(dag_run.conf['report_user_permission_uri'])
    previous_is_hrbp_value = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')

    if dag_run.conf['is_hrbp']=="Y" and dag_run.conf['is_hrbp']!= previous_is_hrbp_value:
        permission_set_uri_to_assign.append(dag_run.conf['ts_hrpb_permission_uri'])
        permission_set_uri_to_assign.append(dag_run.conf['admin_hrpb_permission_uri'])
    if not permission_set_uri_to_assign:
        return null
    return {
            "permissionSetUrisToAssign": permission_set_uri_to_assign,
            "policyUrisToRemovePermissionSet": []
        }

def update_permission__hrbp_access_scope(dag_run):
    previous_is_hrbp_value = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')
    if dag_run.conf['is_hrbp']!= previous_is_hrbp_value and dag_run.conf['is_hrbp']=="Y":
        return {
            "policyDataAccessScopes": [
                {
                "policyUri": "urn:replicon:policy:administration",
                "locations":[
                    {
                        "location": {
                            "uri": dag_run.conf['canada_location_uri'],
                            "parentUri": null,
                            "name": null
                        }
                    }
                ],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": [],
                "scopeObjectTypeUri": null
                },
                {
                "policyUri": "urn:replicon:policy:payroll-management",
                "locations": [
                    {
                        "location": {
                            "uri": dag_run.conf['canada_location_uri'],
                            "parentUri": null,
                            "name": null
                        },
                    }
                ],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": [],
                "scopeObjectTypeUri": null
                }
            ]
            }
    return null

def apply_user_modifications_payload(dag_run):
    log=[]
    user_update_payload = {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": update_timezone(dag_run),
            "workWeekStartToApply": update_workweek(dag_run),
            "holidayCalendarToApply": update_holiday_calendar(dag_run),
            "schedulePolicyToApply": update_schedule(dag_run, log),
            "locationScheduleToApply": update_location_grp(dag_run.conf['location_uri'],
                rail.result('get_effective_user_groupmembership','location').get('uri', ''), dag_run),
            "divisionScheduleToApply": update_buisness_unit_grp(dag_run.conf['buisness_unit_uri'],
                rail.result('get_effective_user_groupmembership', 'division').get('uri', ''), dag_run),
            "costCenterScheduleToApply": update_cost_center_grp(dag_run.conf['cost_center_uri'],
                rail.result('get_effective_user_groupmembership', 'costcenter').get('uri', ''), dag_run),
            "employeeTypeGroupScheduleToApply": update_employee_type_grp(dag_run.conf['employee_type_uri'],
                rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), dag_run),
            "serviceCenterScheduleToApply": update_company_code_grp(dag_run.conf['company_code_uri'],
                rail.result('get_effective_user_groupmembership', 'servicecenter').get('uri', ''), dag_run),
            "permissionSetsToApply": update_permission_set(dag_run),
            "policyDataAccessScopesToApply2": update_permission__hrbp_access_scope(dag_run),
            "timesheetPeriodScheduleToApply": update_timesheet_period(dag_run),
            "timesheetApprovalPathToApply": update_timesheet_approval_path(dag_run),
            "policySetsScheduleToApply": update_policy_set(dag_run),  # New: scheduled policy sets with effective dates
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": update_payrule_script(dag_run),
            "placeAssignmentsModifications": update_place_assignment(dag_run)
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    rail.set_result(key="exception_logs",val= log)

    return user_update_payload

def get_assigned_policy_to_user(dag_run):
    return {
        "userUri": dag_run.conf['useruri']
    }

def put_user_timeoff_policy_schedule_blank_policy(dag_run):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('for_each_time_off_type_no_accural')['timeoff_type_uri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign_for_disable_user'))
    }

def validate_update_data(dag_run, end_date_status):
    if dag_run.conf['emp_status'] in end_date_status and not dag_run.conf['end_date']:
        return False
    if dag_run.conf['is_contingent'] == 'Y' and dag_run.conf['replicon_employee_status']=='Active':
        return False
    if dag_run.conf['location_full_path'].split('|')[0] == 'USA' and dag_run.conf['replicon_employee_status']=='Active':
        return False
    return True

def get_invalid_update_message(dag_run, end_date_status):
    log=[]

    if dag_run.conf['is_contingent'] == 'Y' and dag_run.conf['replicon_employee_status']=='Active' \
        and not dag_run.conf['end_date']:
        log.append('User not updated because Contingent Worker')

    if dag_run.conf['location_full_path'].split('|')[0] == 'USA' and dag_run.conf['replicon_employee_status']=='Active'\
        and not dag_run.conf['end_date']:
        log.append('User not updated because USA user')

    if dag_run.conf['emp_status'] in end_date_status and not dag_run.conf['end_date']:
        log.append(f'User not disabled because end date not present')

    return rail.smartjoin_by_delim(log,";")

def get_time_off_to_assign_based_on_mapper(timeoff_type_mapper,dag_run):
    def conditions_for_specific_timeoffs(data):
        if data['time_off_name']=="[CAN] Anniversaire de service/ Service Anniversary":
            if str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5","A1","A2","A3","A4","A5","A6","T1","T2","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6"))\
                and dag_run.conf["full_part"] in ["Full-Time","Part-Time"]:
                return True
            return False
        return True

    time_off_details_based_on_mapper = list(map(lambda item:item['time_off_name'],filter(lambda x:
        x['location_level_1'] == dag_run.conf['location_full_path'].split('|')[0] and
        x['location_level_2']==dag_run.conf['location_full_path'].split('|')[1] and
        ((dag_run.conf['location_full_path'].split('|')[2] in x['location_level_3']) or x['location_level_3']=="All") and
        x['reg_temp']==dag_run.conf['reg_temp'] and
        dag_run.conf['pay_type'] in x['pay_type'] and conditions_for_specific_timeoffs(x),
    timeoff_type_mapper)))
    return time_off_details_based_on_mapper

def get_default_timeoff_policy_schedule_payload(dag_run, reference_time_off_types, for_each_loop, personal_days_reference_timeoff_types):
    def get_reference_time_off_type_uri():
        if rail.result(for_each_loop)['timeoff_type_name'] == "[CAN] Jour personnel/Personal Days" and dag_run.conf['pay_type']!="Hourly" \
            and dag_run.conf['location_level_3'] != 'STCONSTANT':
            start_date_month= datetime.strptime(dag_run.conf['start_date'],DATE_FORMAT).month
            def get_hire_date_months():
                if 1 <= start_date_month <=3:
                    return "Jan 1 - Mar 31"
                if 3 < start_date_month <=6:
                    return "April 1 - June 30"
                if 6 < start_date_month <=9:
                    return "July 1 - Sept  30"
                return "Oct 1 to Dec 31"

            reference_timeoff_details = []
            if dag_run.conf['std_hrs'] and (float(dag_run.conf['std_hrs'])==37.5 or
                float(dag_run.conf['std_hrs'])==30.0 or float(dag_run.conf['std_hrs'])==22.5):
                reference_timeoff_details = list(filter(lambda x:x['std_hrs']==float(dag_run.conf['std_hrs']) and x['hire_date']== get_hire_date_months()
                    ,personal_days_reference_timeoff_types))

            if reference_timeoff_details:
                return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                    'timeoff_type_name',reference_timeoff_details[0]["timeoff_type_name"],"timeoff_type_uri")

        return rail.result(for_each_loop)['timeoff_type_uri']

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result(for_each_loop)['timeoff_type_uri']
                if rail.result(for_each_loop)['timeoff_type_name']  not in reference_time_off_types else
                get_reference_time_off_type_uri(),
        }
    }

def put_timeoff_assignment_for_user(dag_run):
    timeofftype_uris = list(map(lambda x: x['timeoff_type_uri'] , rail.result('get_required_time_off_type_details_to_assign')['result']))
    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": timeofftype_uris
    }


def get_user_timeoff_policy_payload(dag_run,for_each_loop):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result(for_each_loop)['timeoff_type_uri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_default_time_off_policy_schedule'))
    }

def disable_time_off_types_assignment(dag_run, mannual_time_off_types):
    time_off_uris =[]
    for timeoff_type in mannual_time_off_types:
        uri = rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_policy_summary'),
                 'timeoff_type_name', timeoff_type, 'timeoff_type_uri')
        if uri:
            time_off_uris.append(uri)

    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": [] if dag_run.conf['action']=='disable' else time_off_uris
    }

def validate_rehire_exception(dag_run):
    user_details = rail.result("get_user_info")['userDetails']
    if user_details['employmentDateRange']['startDate'] == get_replicon_date(dag_run.conf['start_date']):
        return True
    return False

def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
        rail.result('search_supervisor_in_replicon')['uri'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['uri']:
        return False
    return True

def get_update_user_message():
    exception_logs = rail.result('apply_user_modifications', 'exception_logs')

    if exception_logs:
        if rail.result('log_supervisor_not_present'):
            return ""
        if get_task_state('log_supervisor_end_date_in_past') =='success':
            return 'User Partially Updated, Supervisor end date in past'+rail.smartjoin_by_delim(exception_logs, ";")
        return "User Partially Updated"+rail.smartjoin_by_delim(exception_logs, ";")

    if rail.result('log_supervisor_not_present'):
        return ""
    if get_task_state('log_supervisor_end_date_in_past') =='success':
        return 'User Partially Updated, Supervisor end date in past'
    return "User Updated Successfully"

def get_update_user_severity():
    exception_logs = rail.result('apply_user_modifications', 'exception_logs')

    if  rail.result('log_supervisor_not_present') or get_task_state('log_supervisor_end_date_in_past')=='success' or exception_logs:
        return 'Exception'
    return 'Success'

def get_add_user_message():
    if rail.result('log_supervisor_not_present'):
        return ""
    if get_task_state('log_supervisor_end_date_in_past') =='success':
        return 'User Partially Added, Supervisor end date in past'
    return "User Added Successfully"

def get_add_user_severity():
    if rail.result('log_supervisor_not_present') or get_task_state('log_supervisor_end_date_in_past') =='success':
        return 'Exception'
    return 'Success'

def get_default_timeoff_policy_set_schedule_for_timeofftype(dag_run, reference_time_off_types,for_each_loop, personal_days_reference_timeoff_types):
    def get_reference_time_off_type_uri():
        if rail.result(for_each_loop)['timeoff_type_name'] == "[CAN] Vacances/Vacation":
            if str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")):
                return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                    'timeoff_type_name',"[CAN] Vacation Montreal S1-S5","timeoff_type_uri")
            if str(dag_run.conf['job_code']).endswith(("A1","A2","T1","T2")):
                return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                    'timeoff_type_name',"[CAN] Vacation Montreal Prof","timeoff_type_uri")
            if str(dag_run.conf['job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")):
                return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                    'timeoff_type_name',"[CAN] Vacation Montreal Sr. Prof","timeoff_type_uri")

        if rail.result(for_each_loop)['timeoff_type_name'] == "[CAN] Jour personnel/Personal Days" and dag_run.conf['reg_temp']=='Regular' and \
            dag_run.conf['location_level_3'] != 'STCONSTANT':

            if dag_run.conf['pay_type']!="Hourly":
                reference_timeoff_details = []
                if dag_run.conf['std_hrs'] and (float(dag_run.conf['std_hrs'])==37.5 or
                    float(dag_run.conf['std_hrs'])==30.0 or float(dag_run.conf['std_hrs'])==22.5):
                    reference_timeoff_details = list(filter(lambda x:x['std_hrs']==float(dag_run.conf['std_hrs']) and x['hire_date']== "NA"
                        ,personal_days_reference_timeoff_types))

                if reference_timeoff_details:
                    return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                        'timeoff_type_name',reference_timeoff_details[0]["timeoff_type_name"],"timeoff_type_uri")

            if dag_run.conf['pay_type']=="Hourly":
                if dag_run.conf['previous_pay_type'] in ["Salaried", "Exception Hourly"]:
                    if rail.result('get_balance_summary_for_user_update')['timeRemaining']<= 37.5:
                        return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                        'timeoff_type_name',"[CAN] Pers. Leave. Sal to Hrly 2","timeoff_type_uri")
                    return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                        'timeoff_type_name',"[CAN] Pers. Leave. Sal to Hrly 1","timeoff_type_uri")

        if rail.result(for_each_loop)['timeoff_type_name'] == "[CAN] Anniversaire de service/ Service Anniversary":
            if str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")):
                return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                    'timeoff_type_name','[CAN] Anniversaire de service/ Service Anniversary( Job Code: S)',"timeoff_type_uri")

            if str(dag_run.conf['job_code']).endswith(("A1","A2","A3","A4","A5","A6","T1","T2","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")):
                return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                    'timeoff_type_name','[CAN] Anniversaire de service/ Service Anniversary( Job codes: A,T,M)',"timeoff_type_uri")

            return null

        return rail.result(for_each_loop)['timeoff_type_uri']

    return {
        "timeOffTypeUri": rail.result(for_each_loop)['timeoff_type_uri']
                if rail.result(for_each_loop)['timeoff_type_name']  not in reference_time_off_types else
                get_reference_time_off_type_uri()
    }

def get_update_user_timeoff_policy_payload(dag_run,for_each_loop):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result(for_each_loop)['timeoff_type_uri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign'))
    }

def get_disable_message():
    if get_task_state('log_end_date_future') =='success':
        return "User End date in Future, Enddate updated but Profile will be disabled on end date"
    return "User disabled Successfully"

def get_disable_status():
    if get_task_state('log_end_date_future') =='success':
        return "Exception"
    return "Success"

def get_supervisor_message(status, action, details, dag_run):
    # pylint: disable=too-many-return-statements
    exception_logs = dag_run.conf['exception_logs']
    if status == 'Error':
        return details

    if status == 'Exception' and not get_task_state('log_supervisor_not_present') == 'success' \
        and not get_task_state('log_supervisor_end_date_in_past') == 'success'  and details:
        return details if not dag_run.conf['exception_logs'] else details + rail.smartjoin_by_delim(exception_logs, ";")
    if get_task_state('log_supervisor_not_present') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + ',Supervisor not present in replicon'+\
        (','+ (details if not dag_run.conf['exception_logs'] else details + rail.smartjoin_by_delim(exception_logs, ";"))
         if status == 'Exception' and details else '')
    if get_task_state('log_supervisor_end_date_in_past') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + ',Supervisor end date in past'+\
        (','+ (details if not dag_run.conf['exception_logs'] else details + rail.smartjoin_by_delim(exception_logs, ";") )
          if status == 'Exception' and details else '')
    if dag_run.conf['exception_logs']:
        return  f"""User {('Added' if action=='Add' else 'Updated')} Partially"""+ rail.smartjoin_by_delim(exception_logs, ";")
    return f"""User {('Added' if action=='Add' else 'Updated')} Successfully"""

def get_supervisor_status(status, details, dag_run):
    if status == 'Error':
        return 'Error'
    if get_task_state('log_supervisor_not_present') == 'success' or get_task_state('log_supervisor_end_date_in_past') == 'success' or\
        dag_run.conf['exception_logs']:
        return 'Exception'
    if status == 'Exception' and not get_task_state('log_supervisor_not_present') == 'success' \
        and not get_task_state('log_supervisor_end_date_in_past') == 'success' and details:
        return status
    return 'Success'

def update_end_date_payload(dag_run):
    return {
            "user": {
                "uri": dag_run.conf['useruri']
            },
            "modifications": {
                "userDetailsToApply":   {
                    "employmentEndDate": {
                        "date": get_replicon_date(dag_run.conf['end_date']) if dag_run.conf['end_date'] else \
                            get_replicon_date((datetime.strptime(
                        dag_run.conf['change_effective_date'],DATE_FORMAT)-timedelta(days=1)).strftime(DATE_FORMAT))
                    },
                },
                "customFieldValuesToApply": get_udfs_disable_user(dag_run, "disable"),
                },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def validate_special_time_off_accrual(dag_run,config):
    if dag_run.conf['event_reason_code']=="10" and list(filter(lambda x: x['event']==dag_run.conf['event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS)):
        if rail.result('for_each_time_off_type_no_accural')['timeoff_type_name'] in config.SPECIAL_ACCRUAL_TIMEOFF_TYPE_NAMES:
            return True
    return False

def get_udfs_disable_user(dag_run, action):
    current_udf_values  = dag_run.conf['current_udf_values'] if action == "disable" else rail.result('get_user_info')['userDetails']['customFieldValues']
    udfs = []
    def add_udf_field_values(definitionuri, dropdownuri = null, textvalue = null , number = null, date = null):
        udfs.append({
        "customField": {
          "uri": definitionuri,
          "name": null,
          "groupUri": null
        },
        "text": textvalue,
        "date": get_replicon_date(date) if date else null,
        "dropDownOption": {
          "uri": dropdownuri,
          "name": null
        } if dropdownuri != null else null,
        "number": number
      })
    current_emp_status = rail.find_first_by_attr_and_get_attr(current_udf_values,
        'customField.displayText', 'Employee Status', 'text')
    if dag_run.conf['emp_status'] and current_emp_status != dag_run.conf['emp_status']:
        add_udf_field_values(definitionuri = dag_run.conf['emp_status_def_uri'], textvalue= dag_run.conf['emp_status'])

    current_change_effective_date = rail.find_first_by_attr_and_get_attr(current_udf_values,
        'customField.displayText', 'Change Effective Date', 'date')
    if dag_run.conf['change_effective_date'] and \
        (get_date_from_replicon_date(current_change_effective_date).strftime(DATE_FORMAT) if current_change_effective_date else null)\
            != datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT).strftime(DATE_FORMAT):
        add_udf_field_values(definitionuri = dag_run.conf['change_effective_date_def_uri'], date= dag_run.conf['change_effective_date'])

    current_event = rail.find_first_by_attr_and_get_attr(current_udf_values,
        'customField.displayText', 'Event', 'text')
    if dag_run.conf['event'] and current_event != dag_run.conf['event']:
        add_udf_field_values(definitionuri = dag_run.conf['event_def_uri'], textvalue= dag_run.conf['event'])

    current_event_reason = rail.find_first_by_attr_and_get_attr(current_udf_values,
        'customField.displayText', 'Event Reason', 'text')
    if dag_run.conf['event_reason_code'] and current_event_reason != dag_run.conf['event_reason_code']:
        add_udf_field_values(definitionuri = dag_run.conf['event_reason_def_uri'], textvalue= dag_run.conf['event_reason_code'])

    # V2.7 - Leave Start Date: write only on Active -> Unpaid Leave transition.
    # The UDF holds the original leave-start change-effective-date and is NOT
    # overwritten on subsequent payloads while the user remains on Unpaid Leave
    # (the transition guard prevents this). Required to compute leave duration
    # reliably for the > 26-week proration gate.
    leave_start_def_uri = dag_run.conf.get('leave_start_date_def_uri')
    if leave_start_def_uri \
            and dag_run.conf['emp_status'] == 'Unpaid Leave' \
            and current_emp_status != 'Unpaid Leave' \
            and dag_run.conf['change_effective_date']:
        add_udf_field_values(definitionuri=leave_start_def_uri, date=dag_run.conf['change_effective_date'])

    # V2.7 - Leave End Date: clear on Active -> Unpaid Leave transition so the
    # previous leave's return date does not linger on the user record. Without
    # this, a returning-then-re-leaving employee would show a stale Leave End
    # Date alongside the freshly-written Leave Start Date.
    leave_end_def_uri = dag_run.conf.get('leave_end_date_def_uri')
    if leave_end_def_uri \
            and dag_run.conf['emp_status'] == 'Unpaid Leave' \
            and current_emp_status != 'Unpaid Leave':
        add_udf_field_values(definitionuri=leave_end_def_uri, date=None)

    return udfs

def validate_previous_suspended_leave_or_non_live_transfer_employee(dag_run):
    user_udf_values = rail.result('get_user_info')[
                    'userDetails']['customFieldValues']
    if not user_udf_values:
        return False
    previous_emp_status = rail.find_first_by_attr_and_get_attr(user_udf_values,
        'customField.displayText', 'Employee Status', 'text')
    if bool(previous_emp_status in ["Suspended"]) or (not rail.result('get_user_info')['userDetails']['isEnabled'] and
        not dag_run.conf['end_date'] and previous_emp_status != 'Terminated'):
        return True
    return False

def get_montreal_vacation_reference_timeoff_uri(dag_run, default_timeoff_type_uri):
    if str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
            'timeoff_type_name',"[CAN] Vacation Montreal S1-S5","timeoff_type_uri")
    if str(dag_run.conf['job_code']).endswith(("A1","A2","T1","T2")):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
            'timeoff_type_name',"[CAN] Vacation Montreal Prof","timeoff_type_uri")
    if str(dag_run.conf['job_code']).endswith(("A3","A4","A5","A6","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
            'timeoff_type_name',"[CAN] Vacation Montreal Sr. Prof","timeoff_type_uri")
    return default_timeoff_type_uri

def validate_enddate_for_old_profile():
    return bool(rail.result('get_user_data_based_on_login_name')[0]['userDetails']["employmentDateRange"]['endDate'])

def update_old_profile_login_name(dag_run):
    def get_end_date_for_oldprofile():
        end_date = rail.result('get_user_data_based_on_login_name')[0]['userDetails']["employmentDateRange"]['endDate']
        return get_date_from_replicon_date(end_date).strftime("%d%m%Y")
    return {
        'userUri': rail.result('get_user_data_based_on_login_name')[0]['userDetails']['uri'],
        'loginName': str(dag_run.conf['login_name'])+"."+ get_end_date_for_oldprofile()
    }

def assign_policyDataAccessScopes_to_projectmanager(dag_run):
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "policyDataAccessScopes": [
            {
            "policyUri": "urn:replicon:policy:administration",
            "locations": [
                 {
                    "location": {
                        "uri": dag_run.conf['canada_location_uri'],
                        "parentUri": null,
                        "name": null
                    }
                }
            ],
            "divisions": [],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": [],
            "scopeObjectTypeUri": null
            },
            {
            "policyUri": "urn:replicon:policy:payroll-management",
            "locations": [
                {
                    "location": {
                        "uri": dag_run.conf['canada_location_uri'],
                        "parentUri": null,
                        "name": null
                    }
                }
            ],
            "divisions": [],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": [],
            "scopeObjectTypeUri": null
            }
        ]
        }

def validate_is_remove_hrbp_permossion_set(dag_run):
    previous_is_hrbp_value = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')
    return bool(dag_run.conf['is_hrbp']=="N" and dag_run.conf['is_hrbp']!= previous_is_hrbp_value)

def update_timesheet_period_for_unpaid_leave(dag_run):

    current_timesheet_period = rail.result("get_user_info")['timesheetPeriodSchedule']

    return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": null,
                    "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ],
            "endDate": null
        }
    } if (current_timesheet_period and current_timesheet_period[-1]['timesheetPeriod']['displayText']!= "No timesheet period") or not current_timesheet_period else null

def update_required_udfs_and_timesheet_period_payload(dag_run):
    return {
            "user": {
                "uri": dag_run.conf['useruri']
            },
            "modifications": {
                "timesheetPeriodScheduleToApply": update_timesheet_period_for_unpaid_leave(dag_run),
                "customFieldValuesToApply": get_udfs_disable_user(dag_run,"unpaid_leave"),
                },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

def get_service_anniversary_reference_timeoff_uri(dag_run):
    if str(dag_run.conf['job_code']).endswith(("S1","S2","S3","S4","S5")):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
            'timeoff_type_name','[CAN] Anniversaire de service/ Service Anniversary( Job Code: S)',"timeoff_type_uri")

    if str(dag_run.conf['job_code']).endswith(("A1","A2","A3","A4","A5","A6","T1","T2","T3","T4","T5","T6","M1","M2","M3","M4","M5","M6")):
        return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
            'timeoff_type_name','[CAN] Anniversaire de service/ Service Anniversary( Job codes: A,T,M)',"timeoff_type_uri")

    return null


# V2.7 - Personal Days proration: payload builder for the PUT call. The
# time-off-type URI is resolved by name from the user's policy summary, and
# the schedule entries come from get_all_personal_days_policy_to_assign.
def put_personal_days_timeoff_policy(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('resolve_personal_days_timeoff_type_uri')
        },
        "policySetScheduleEntries": json.loads(rail.result('get_all_personal_days_policy_to_assign'))
    }

