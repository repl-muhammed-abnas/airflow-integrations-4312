from datetime import datetime, timedelta
from dateutil.parser import parse as date_parser
import json
from uuid import uuid4
import rail

null = None
DATE_FORMAT = "%m/%d/%Y"


MANDATORY_FIELDS = {
        "emp_id":"Empl_ID",
        "first_name":"First_Name",
        "last_name": "Last_Name",
        "email": "Work_Email",
        "login_name": "User_Name",
        "emp_status": "Empl_Status",
        "buisness_unit_full_path": "Bus_Seg_Unit",
        "functional_segment": "Functional_Segment",
        'location_full_path': 'Location',
        'job_code': 'Job_Code',
        'pay_type': 'Pay_Type',
        'cost_center_full_path':'Cost_Center_Business_Area',
        "staff_category": "Staff_Category",
        "functional_sub_segment":"Functional_Sub_Segment"
}

KEY_MAPPING_FOR_FEED_FIELDS = {'location_level_2': 'location_full_path', 'location_level_3': 'location_full_path',
    'staff_category': 'staff_category', 'functional_sub_segment': 'functional_sub_segment', 'employee_type': 'employee_type',
    'reg_temp': 'reg_temp', 'pay_type': 'pay_type'}

MAPPER_KEYS_FOR_DATA_RETRIEVE = ['location_level_2', 'location_level_3',
    'staff_category', 'functional_sub_segment', 'employee_type', 'reg_temp',
    'pay_type']

SHOULD_CHECK_FOR_ALL_EXCEPT = ['location_level_2', 'location_level_3']

def get_replicon_date(date_str):
    if not date_str:
        return None

    date = datetime.strptime(date_str, DATE_FORMAT)
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }


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

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")

    if item['emp_status'] not in ['Active','Unpaid Leave',
        'Terminated','Suspended','Retired','Paid Leave','Furlough','Dormant','Discarted','Deceased']:
        missing_fields.append("Employee Status should be from ('Active','Unpaid Leave',\
        'Terminated','Suspended','Retired','Paid Leave','Furlough','Dormant','Discarted','Deceased')")

    if item['pay_type'] not in ['Hourly','Salaried','Exception Hourly','Daily','N/A','Weekly']:
        missing_fields.append("Pay Type is not Hourly/Salaried/Exception Hourly/Daily/Weekly/N/A")

    if item['activity_type']:
        missing_fields.append("Activity Type should be blank")

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

def check_for_exception(value, mapper_value, key):
    if key == 'location_level_2':
        value = value.split('|')[1]
    if key == 'location_level_3':
        value = value.split('|')[2]
    if key == 'buisness_unit_level_2':
        value = value.split('|')[1]

    return value in mapper_value

def is_mapper_value_found(mapper_data, input_data, value_found=False):

    def compare(value, compare_value:str):
        if isinstance(value, str):
            return value.lower() == compare_value.lower()
        return False

    def bool_value_check(key,input_value,mapper_value, value_type):
        if key == 'location_level_2':
            input_value = input_value.split('|')[1]
        if key == 'location_level_3':
            input_value = input_value.split('|')[2]

        if value_type=="list":
            return bool(input_value in mapper_value)
        return bool(input_value == mapper_value)

    key_mapping_to_consider = KEY_MAPPING_FOR_FEED_FIELDS

    for key in MAPPER_KEYS_FOR_DATA_RETRIEVE:
        if compare(mapper_data[key], "NA"):
            value_found = True
            continue

        if isinstance(mapper_data[key], list):
            value_found = bool_value_check(key, input_data[key_mapping_to_consider.get(key)], mapper_data[key], "list")
        else:
            value_found = bool_value_check(key, input_data[key_mapping_to_consider.get(key)], mapper_data[key], "str")

        if not value_found:
            break

    return value_found

# pylint: disable=too-many-arguments
def get_process_users_conf(item, config):
    get_all_permission_sets = rail.result("get_all_permission_set")
    get_user_udfs = rail.result('get_user_udfs')

    def get_employee_type_name():
        if item['staff_category'] == 'Staff':
            return 'Staff'
        employee_type_list = list(filter(lambda x: x["functional_sub_segment"] == item['functional_sub_segment'] and item["location_full_path"].split('|')[2] in x["location_3"], config.EMPLOYEE_TYPE_MAPPER ))
        return employee_type_list[0]['employee_type'] if employee_type_list else ''

    item['employee_type'] = get_employee_type_name()

    def get_mapper_values_for_keys(user_mapper, item):
        for mapper_row in user_mapper:
            if is_mapper_value_found(mapper_row, item):
                return mapper_row
        return {}

    user_mapper_values = get_mapper_values_for_keys(config.USER_MAPPER, item)

    def get_employee_status():
        if item['emp_status'] in config.ACTIVE_STATUS:
            return 'Active'
        return 'Terminated'

    return {
        **item,
        **{
            "modulo" : int(item['record_id'])%config.BATCH_COUNT,
            "location_level_2": item["location_full_path"].split('|')[1],
            "location_level_3": item["location_full_path"].split('|')[2],
            "buisness_unit_level_2": item["buisness_unit_full_path"].split('|')[1] if item["buisness_unit_full_path"] else null,

            "user_mapper_value": rail.write_json_artifact(user_mapper_values),
            'supervisor_log' : rail.result('create_supervisor_log'),

            "replicon_employee_status": get_employee_status(),

            'title_def_uri': get_user_udfs['title_def_uri'],
            'functional_segment_def_uri': get_user_udfs['functional_segment_def_uri'],
            'std_hrs_def_uri': get_user_udfs['std_hrs_def_uri'],
            'job_code_def_uri': get_user_udfs['job_code_def_uri'],
            'pay_grp_def_uri': get_user_udfs['pay_grp_def_uri'],
            'profit_center_def_uri': get_user_udfs['profit_center_def_uri'],
            'emp_status_def_uri': get_user_udfs['emp_status_def_uri'],
            'buisness_segment_def_uri': get_user_udfs['buisness_segment_def_uri'],
            'buisness_unit_def_uri': get_user_udfs['buisness_unit_def_uri'],
            'reg_temp_def_uri': get_user_udfs['reg_temp_def_uri'],
            'full_part_def_uri': get_user_udfs['full_part_def_uri'],
            'is_hrbp_def_uri': get_user_udfs['is_hrbp_def_uri'],
            'pay_type_def_uri': get_user_udfs['pay_type_def_uri'],
            'change_effective_date_def_uri': get_user_udfs['change_effective_date_def_uri'],
            'event_def_uri': get_user_udfs['event_def_uri'],
            'event_reason_def_uri': get_user_udfs['event_reason_def_uri'],
            "holiday_calendar_def_uri": get_user_udfs['holiday_calendar_def_uri'],
            'staff_category_def_uri': get_user_udfs['staff_category_def_uri'],
            'functional_sub_segment_def_uri': get_user_udfs['functional_sub_segment_def_uri'],

            'staff_category_drop_uri': rail.find_first_by_attr_and_get_attr
                (rail.result("get_staff_category_dropdown_values"),'name',item["staff_category"],'uri'),

            'location_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_location_grps'), 'full_path', item['location_full_path'], 'uri'),
            'department_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_department_grps'), 'displayText', item['department_name'], 'uri'),
            'buisness_unit_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_buisness_unit_grps'),
                'full_path', item['buisness_unit_full_path'], 'uri'),
            'cost_center_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_cost_center_grps'),
                'full_path', item['cost_center_full_path'], 'uri'),
            'company_code_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_company_code'), 'name', item['company_code'], 'uri'),

            "employee_type_name": get_employee_type_name(),
            'employee_type_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_types_grp'), 'name', get_employee_type_name(), 'uri'),

            "timesheet_period": user_mapper_values['timesheet_period'] if user_mapper_values else null,

            "timesheet_template_name": user_mapper_values['timesheet_template']
                    if user_mapper_values and user_mapper_values['timesheet_template'] and user_mapper_values['timesheet_template'] !="NA" else null,
            "timesheet_template_uri": rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',user_mapper_values['timesheet_template'],"uri")
                if user_mapper_values and user_mapper_values['timesheet_template'] and user_mapper_values['timesheet_template'] !="NA" else null,

            "timesheet_approval_path":user_mapper_values['timesheet_approval_path'] if user_mapper_values else null,

            "timeoff_template_name": user_mapper_values['time_off_template']
                    if user_mapper_values and user_mapper_values['time_off_template'] else null,
            "timeoff_template_uri": rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',user_mapper_values['time_off_template'],"uri")
                if user_mapper_values and user_mapper_values['time_off_template'] else null,

            "timeoff_approval_path":user_mapper_values['time_off_approver']
                    if user_mapper_values and user_mapper_values['time_off_approver'] else null,

            "punch_policy_name": user_mapper_values['punch_policy']
                    if user_mapper_values and user_mapper_values['punch_policy'] and user_mapper_values['punch_policy']!= "NA" else null,
            "punch_policy_uri":rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',user_mapper_values['punch_policy'],"uri")
                if user_mapper_values and user_mapper_values['punch_policy'] and user_mapper_values['punch_policy']!= "NA" else null,

            "holiday_calendar": user_mapper_values['holiday_calendar']
                    if user_mapper_values and user_mapper_values['holiday_calendar'] and user_mapper_values['holiday_calendar']!= "NA" else null,
            "holiday_calendar_uri":rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_holiday_calenders"),'displayText',user_mapper_values['holiday_calendar'],"uri")
                if user_mapper_values and user_mapper_values['holiday_calendar'] and user_mapper_values['holiday_calendar']!= "NA" else null,

            "work_week": f"urn:replicon:day-of-week:{(user_mapper_values['work_week'].split(' ')[0].lower())}"
                    if user_mapper_values else null,

            "timezone": user_mapper_values['timezone']
                    if user_mapper_values and user_mapper_values['timezone'] and user_mapper_values['timezone']!= "NA" else null,
            "timezone_uri":rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_timezones"),'displayText',user_mapper_values['timezone'],"uri")
                if user_mapper_values and user_mapper_values['timezone'] and user_mapper_values['timezone']!= "NA" else null,

            "payrule_name": user_mapper_values['payrule_name']
                    if user_mapper_values and user_mapper_values['payrule_name'] and user_mapper_values['payrule_name']!= "NA" else null,
            "payrule_script_uri":rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_payrule_scripts"),'displayText',user_mapper_values['payrule_name'],"uri")
                if user_mapper_values and user_mapper_values['payrule_name'] and user_mapper_values['payrule_name']!= "NA" else null,

            "supervisor_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Supervisor','uri'),
            "report_user_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Report User','uri'),
            "report_user_substitute_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'Report User with Substitute','uri'),

            "admin_hrpb_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'View Only Admin HRPB','uri'),
            "ts_hrpb_permission_uri": rail.find_first_by_attr_and_get_attr(get_all_permission_sets, 'name', 'View Only TS HRBP','uri'),

            'starting_balance_script_uri': rail.result('get_timeoff_balance_event_script_uri')['starting_balance_script_uri'],
            'prevent_balance_overdraw_uri': rail.result('get_timeoff_balance_validation_script')['prevent_balance_overdraw_uri'],
            'default_time_off_type_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_time_off_types'), 'timeoff_type_name',config.DEFAULT_TIME_OFF_TYPE_STAFF,'timeoff_type_uri') 
                if item['staff_category'] == 'Staff' else rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_time_off_types'), 'timeoff_type_name',config.DEFAULT_TIME_OFF_TYPE_NON_STAFF,'timeoff_type_uri'),

            "place_name": rail.find_first_by_attr_and_get_attr(rail.result("get_place_details"),
                'place_name', item["location_full_path"].split("|")[2],'place_name'),
            "place_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_place_details"),
                'place_name', item["location_full_path"].split("|")[2],'place_uri'),

            'office_schedule_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),'displayText',item['work_schedule'],"uri")
                if item['work_schedule'] else null,

            "all_actvity_uris": rail.result("get_all_updated_activity_uris"),
        }
    }

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

def test_valid_fields(dag_run):
    # pylint: disable=too-many-return-statements
    startdate = get_replicon_date(dag_run.conf['start_date'])
    if not startdate:
        return False
    if dag_run.conf['end_date']:
        enddate = get_replicon_date(dag_run.conf['end_date'])
        if not enddate:
            return False

    if dag_run.conf['change_effective_date']:
        effective_date = get_replicon_date(dag_run.conf['change_effective_date'])
        if not effective_date:
            return False
        
    if not rail.load_json_artifact(dag_run.conf['user_mapper_value']):
        return False

    return True

def get_invalid_fields_message(dag_run):
    log=[]
    startdate = get_replicon_date(dag_run.conf['start_date'])
    if not startdate:
        log.append('Invalid format for Hire Date')

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

    if user_status =='adduser':
        add_udf_field_values(definitionuri = dag_run.conf['staff_category_def_uri'], dropdownuri= dag_run.conf['staff_category_drop_uri'])
        if dag_run.conf['title']:
            add_udf_field_values(definitionuri = dag_run.conf['title_def_uri'], textvalue= dag_run.conf['title'])
        if dag_run.conf['functional_segment']:
            add_udf_field_values(definitionuri = dag_run.conf['functional_segment_def_uri'], textvalue= dag_run.conf['functional_segment'])
        if dag_run.conf['std_hrs']:
            add_udf_field_values(definitionuri = dag_run.conf['std_hrs_def_uri'], number= dag_run.conf['std_hrs'])
        if dag_run.conf['job_code']:
            add_udf_field_values(definitionuri = dag_run.conf['job_code_def_uri'], textvalue= dag_run.conf['job_code'])
        if dag_run.conf['pay_grp']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_grp_def_uri'], textvalue= dag_run.conf['pay_grp'])
        if dag_run.conf['profit_center']:
            add_udf_field_values(definitionuri = dag_run.conf['profit_center_def_uri'], textvalue= dag_run.conf['profit_center'])
        if dag_run.conf['holiday_calendar_code']:
            add_udf_field_values(definitionuri = dag_run.conf['holiday_calendar_def_uri'], textvalue= dag_run.conf['holiday_calendar_code'])
        
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
        if dag_run.conf['event']:
            add_udf_field_values(definitionuri = dag_run.conf['event_def_uri'], textvalue= dag_run.conf['event'])
        if dag_run.conf['event_reason_code']:
            add_udf_field_values(definitionuri = dag_run.conf['event_reason_def_uri'], textvalue= dag_run.conf['event_reason_code'])
        if dag_run.conf['functional_sub_segment']:
            add_udf_field_values(definitionuri = dag_run.conf['functional_sub_segment_def_uri'], textvalue= dag_run.conf['functional_sub_segment'])

    if user_status =='updateuser':

        current_title = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Title', 'text')
        if dag_run.conf['title'] and current_title != dag_run.conf['title']:
            add_udf_field_values(definitionuri = dag_run.conf['title_def_uri'], textvalue= dag_run.conf['title'])

        current_functional_segment = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Functional Segment', 'text')
        if not dag_run.conf['functional_segment']:
            add_udf_field_values(definitionuri = dag_run.conf['functional_segment_def_uri'], textvalue= null)
        if dag_run.conf['functional_segment'] and current_functional_segment != dag_run.conf['functional_segment']:
            add_udf_field_values(definitionuri = dag_run.conf['functional_segment_def_uri'], textvalue= dag_run.conf['functional_segment'])

        current_std_hrs = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Standard Hours', 'text')
        if dag_run.conf['std_hrs'] and (float(current_std_hrs) if current_std_hrs else current_std_hrs) != float(dag_run.conf['std_hrs']):
            add_udf_field_values(definitionuri = dag_run.conf['std_hrs_def_uri'], number= dag_run.conf['std_hrs'])

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

        current_emp_status = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Employee Status', 'text')
        if dag_run.conf['emp_status'] and current_emp_status != dag_run.conf['emp_status']:
            add_udf_field_values(definitionuri = dag_run.conf['emp_status_def_uri'], textvalue= dag_run.conf['emp_status'])

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

        current_holiday_calendar = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Holiday Calendar Code', 'text')
        if dag_run.conf['holiday_calendar_code'] and current_holiday_calendar != dag_run.conf['holiday_calendar_code']:
            add_udf_field_values(definitionuri = dag_run.conf['holiday_calendar_def_uri'], textvalue= dag_run.conf['holiday_calendar_code'])

        current_staff_category = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Staff Category', 'text')
        if current_staff_category != dag_run.conf['staff_category']:
            add_udf_field_values(definitionuri = dag_run.conf['staff_category_def_uri'], dropdownuri= dag_run.conf['staff_category_drop_uri'])

        current_functional_sub_segment = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Functional Sub Segment', 'text')
        if dag_run.conf['functional_sub_segment'] and (current_functional_sub_segment != (dag_run.conf['functional_sub_segment'])):
            add_udf_field_values(definitionuri = dag_run.conf['functional_sub_segment_def_uri'], textvalue= dag_run.conf['functional_sub_segment'])

    return udfs

def get_put_user_payload(dag_run):
    log=[]
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

    payload_add_user = {
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
            ] if dag_run.conf['work_schedule'] and dag_run.conf['office_schedule_uri'] and (dag_run.conf['staff_category'] == 'Staff') else
            [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": "MUR-Staff Schedule",
                        "officeSchedule": {
                            "officeScheduleUri":null,
                            "name": "MUR-Staff Schedule"
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['staff_category'] == 'Staff' else
            [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": null,
                        "officeSchedule": null,
                        "scheduleTypeUri": "urn:replicon:schedule-type:shift"
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
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['department_uri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
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
    rail.set_result(key="exception_logs",val= log)

    return payload_add_user

def validate_supervisor_end_date():
    return datetime.now().date() > (date_parser(rail.result('search_supervisor_in_replicon')['end_date'])).date()\
        if rail.result('search_supervisor_in_replicon')['end_date'] else False

def get_supervisor_message(status, action, details, dag_run):
    # pylint: disable=too-many-return-statements
    log_supervisor_not_present = rail.result('search_supervisor_in_replicon') == []
    log_supervisor_end_date_in_past = validate_supervisor_end_date() if rail.result('search_supervisor_in_replicon') != [] else False
    exception_logs = dag_run.conf['exception_logs']

    if status == 'Error':
        return details

    if status == 'Exception' and not log_supervisor_not_present \
        and not log_supervisor_end_date_in_past  and details:
        return details if not dag_run.conf['exception_logs'] else details + rail.smartjoin_by_delim(exception_logs, ";")
    if log_supervisor_not_present:
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + ',Supervisor not present in replicon'+\
        (','+ (details if not dag_run.conf['exception_logs'] else details + rail.smartjoin_by_delim(exception_logs, ";"))
         if status == 'Exception' else '')
    if log_supervisor_end_date_in_past:
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + ',Supervisor end date in past'+\
        (','+ (details if not dag_run.conf['exception_logs'] else details + rail.smartjoin_by_delim(exception_logs, ";") )
          if status == 'Exception' else '')
    if dag_run.conf['exception_logs']:
        return  f"""User {('Added' if action=='Add' else 'Updated')} Partially, """+ rail.smartjoin_by_delim(exception_logs, ";")
    return f"""User {('Added' if action=='Add' else 'Updated')} Successfully"""

def get_supervisor_status(status, details, dag_run):
    log_supervisor_not_present = rail.result('search_supervisor_in_replicon') != []
    log_supervisor_end_date_in_past = validate_supervisor_end_date() if rail.result('search_supervisor_in_replicon') != [] else False
    if status == 'Error':
        return 'Error'
    if log_supervisor_not_present or log_supervisor_end_date_in_past or\
        dag_run.conf['exception_logs']:
        return 'Exception'
    if status == 'Exception' and not log_supervisor_not_present\
        and not log_supervisor_end_date_in_past and details:
        return status
    return 'Success'

def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
        rail.result('search_supervisor_in_replicon')['uri'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['uri']:
        return False
    return True

def validate_update_data(dag_run, end_date_status):
    if dag_run.conf['emp_status'] in end_date_status and not dag_run.conf['end_date']:
        return False
    if dag_run.conf['is_contingent'] == 'Y' and dag_run.conf['replicon_employee_status']=='Active':
        return False
    return True

def get_invalid_update_message(dag_run, end_date_status):
    log=[]

    if dag_run.conf['emp_status'] in end_date_status and not dag_run.conf['end_date']:
        log.append(f'User not disabled because end date not present')

    return rail.smartjoin_by_delim(log,";")

def validate_enddate(dag_run):
    if dag_run.conf['end_date']:
        return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)
    return datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)

def is_enddate_in_future(dag_run):
    if dag_run.conf['end_date']:
        return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) >= datetime.strptime(dag_run.conf['todays_date'], DATE_FORMAT)
    return datetime.strptime(dag_run.conf['change_effective_date'], DATE_FORMAT) >= datetime.strptime(dag_run.conf['todays_date'], DATE_FORMAT)

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

def update_holiday_calendar(dag_run,log):
    if not dag_run.conf['holiday_calendar']:
        log.append("Holiday Calendar not available in payload")
        return null
    if dag_run.conf['holiday_calendar'] and not dag_run.conf['holiday_calendar_uri']:
        log.append("Holiday Calendar not available in replicon")
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

    if not current_timesheet_period:
        return {
            "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementTimesheetPeriodSchedule": [],
            "updateTimesheetPeriodScheduleOverDateRange": {
                "replacementTimesheetPeriodScheduleEntries": [
                    {
                        "timesheetPeriod": {
                            "name": dag_run.conf['timesheet_period'] if previous_emp_status != "Unpaid Leave" else "MUR - Weekly starting on Monday",
                        },
                        "effectiveDate": null if previous_emp_status != "Unpaid Leave" else get_replicon_date(dag_run.conf['change_effective_date'])
                    }
                ],
                "endDate": null
            }
        }

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
                        "name": dag_run.conf['timesheet_period'] if previous_emp_status != "Unpaid Leave" else "MUR - Weekly starting on Monday",
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
                }
            ],
            "endDate": null
        }
    } if check_update_timesheet_period_required() else null

def update_policy_set(dag_run):
    # pylint: disable=too-many-boolean-expressions
    policy_set_to_assign = []
    policy_set_to_remove = []

    assigned_timesheet_template = rail.result("get_user_info")['timesheetTemplate']
    assigned_timeoff_template = rail.result("get_user_info")['timeOffTemplate']
    assigned_punch_policy = rail.result("get_assigned_policy_to_user")

    if dag_run.conf['timesheet_template_name'] and \
        (not assigned_timesheet_template or ((assigned_timesheet_template and dag_run.conf['timesheet_template_uri']) \
        and (dag_run.conf['timesheet_template_uri'] != assigned_timesheet_template['uri']))):
        policy_set_to_assign.append(dag_run.conf['timesheet_template_uri'])

    if dag_run.conf['timeoff_template_name'] and (not assigned_timeoff_template or
        (( dag_run.conf['timeoff_template_name'] and assigned_timeoff_template and dag_run.conf['timeoff_template_uri']) \
        and (dag_run.conf['timeoff_template_uri'] != assigned_timeoff_template['uri']))):
        policy_set_to_assign.append(dag_run.conf['timeoff_template_uri'])

    if dag_run.conf['punch_policy_name'] and (not assigned_punch_policy or (( assigned_punch_policy and dag_run.conf['punch_policy_uri']) \
        and (dag_run.conf['punch_policy_uri'] != assigned_punch_policy[0]['policySet']['uri']))):
        policy_set_to_assign.append(dag_run.conf['punch_policy_uri'])

    if not dag_run.conf['timeoff_template_name']:
        if assigned_timeoff_template:
            policy_set_to_remove.append("urn:replicon:policy:time-off")

    if not dag_run.conf['timesheet_template_name']:
        if assigned_timesheet_template:
            policy_set_to_remove.append("urn:replicon:policy:timesheet")

    if not dag_run.conf['punch_policy_name']:
        if assigned_punch_policy:
            policy_set_to_remove.append("urn:replicon:policy:time-punch")

    if policy_set_to_assign or policy_set_to_remove:
        return {
            "policySetUrisToAssign": policy_set_to_assign,
            "policyUrisToRemovePolicySet": policy_set_to_remove
        }
    return null

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
    if day == "Sunday":
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
    previous_is_hrbp_value = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')

    if dag_run.conf['is_hrbp']=="Y" and dag_run.conf['is_hrbp']!= previous_is_hrbp_value:
        return {
            "permissionSetUrisToAssign": [dag_run.conf['ts_hrpb_permission_uri'],dag_run.conf['admin_hrpb_permission_uri']],
            "policyUrisToRemovePermissionSet": []
        }
    return null

def update_permission__hrbp_access_scope(dag_run):
    previous_is_hrbp_value = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'is HRBP', 'text')
    if dag_run.conf['is_hrbp']!= previous_is_hrbp_value and dag_run.conf['is_hrbp']=="Y":
        return {
        "policyDataAccessScopes": [
            {
            "policyUri": "urn:replicon:policy:administration",
            "locations": [],
            "divisions": [
                {
                "division": null,
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                }
            ],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": [],
            "scopeObjectTypeUri": null
            },
            {
            "policyUri": "urn:replicon:policy:payroll-management",
            "locations": [],
            "divisions": [
                {
                "division": null,
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                }
            ],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": [],
            "scopeObjectTypeUri": null
            }
        ]
        }
    return null

def update_department_grp(department_uri, current_department_uri, dag_run):
    return {
      "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementDepartmentGroupSchedule": [],
      "updateDepartmentGroupScheduleOverDateRange": {
        "replacementDepartmentGroupScheduleEntries": [
          {
            "departmentGroup": {
              "uri": department_uri,
            },
            "effectiveDate": get_replicon_date(dag_run.conf['change_effective_date'])
          }
        ],
        "endDate": null
      }
    } if department_uri != current_department_uri else null

def apply_user_modifications_payload(dag_run):
    log=[]
    user_update_payload = {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": update_timezone(dag_run),
            "workWeekStartToApply": update_workweek(dag_run),
            "holidayCalendarToApply": update_holiday_calendar(dag_run, log),
            "schedulePolicyToApply": update_schedule(dag_run, log),
            "locationScheduleToApply": update_location_grp(dag_run.conf['location_uri'],
                rail.result('get_effective_user_groupmembership','location').get('uri', ''), dag_run),
            "divisionScheduleToApply": update_buisness_unit_grp(dag_run.conf['buisness_unit_uri'],
                rail.result('get_effective_user_groupmembership', 'division').get('uri', ''), dag_run),
            "costCenterScheduleToApply": update_cost_center_grp(dag_run.conf['cost_center_uri'],
                rail.result('get_effective_user_groupmembership', 'costcenter').get('uri', ''), dag_run),
            "departmentGroupScheduleToApply": update_department_grp(dag_run.conf['department_uri'],
                rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), dag_run),
            "employeeTypeGroupScheduleToApply": update_employee_type_grp(dag_run.conf['employee_type_uri'],
                rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), dag_run),
            "serviceCenterScheduleToApply": update_company_code_grp(dag_run.conf['company_code_uri'],
                rail.result('get_effective_user_groupmembership', 'servicecenter').get('uri', ''), dag_run),
            "permissionSetsToApply": update_permission_set(dag_run),
            "policyDataAccessScopesToApply2": update_permission__hrbp_access_scope(dag_run),
            "timesheetPeriodScheduleToApply": update_timesheet_period(dag_run),
            "timesheetApprovalPathToApply": update_timesheet_approval_path(dag_run),
            'activitiesToApply': [],
            "policySetsToApply": update_policy_set(dag_run),
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": update_payrule_script(dag_run),
            "placeAssignmentsModifications": update_place_assignment(dag_run)
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    rail.set_result(key="exception_logs",val= log)

    return user_update_payload


def put_timeoff_assignment_for_user(dag_run):
    timeofftype_uris = list(map(lambda x: x['timeoff_type_uri'] , rail.result('get_required_time_off_type_details_to_assign')['result']))
    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": timeofftype_uris
    }


def get_default_timeoff_policy_schedule_payload(dag_run, config, for_each_loop):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result(for_each_loop)['timeoff_type_uri']
        }
    }

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


def validate_rehire_exception(dag_run):
    user_details = rail.result("get_user_info")['userDetails']
    if user_details['employmentDateRange']['startDate'] == get_replicon_date(dag_run.conf['start_date']):
        return True
    return False


def get_update_user_message(dag_run):
    log_supervisor_end_date_in_past = validate_supervisor_end_date() if bool(dag_run.conf['sup_emp_id']) and \
        rail.result('search_supervisor_in_replicon') != [] else False

    exception_logs = rail.result('apply_user_modifications', 'exception_logs')

    if exception_logs:
        if rail.result('log_supervisor_not_present'):
            return ""
        if log_supervisor_end_date_in_past:
            return 'User Partially Updated, Supervisor end date in past, '+rail.smartjoin_by_delim(exception_logs, ";")
        return "User Partially Updated, "+rail.smartjoin_by_delim(exception_logs, ";")

    if rail.result('log_supervisor_not_present'):
        return ""
    if log_supervisor_end_date_in_past:
        return 'User Partially Updated, Supervisor end date in past'
    return "User Updated Successfully"

def get_update_user_severity(dag_run):
    log_supervisor_end_date_in_past = validate_supervisor_end_date() if bool(dag_run.conf['sup_emp_id']) and \
        rail.result('search_supervisor_in_replicon') != [] else False
    exception_logs = rail.result('apply_user_modifications', 'exception_logs')

    if  rail.result('log_supervisor_not_present') or log_supervisor_end_date_in_past or exception_logs:
        return 'Exception'
    return 'Success'

def get_default_timeoff_policy_set_schedule_for_timeofftype(for_each_loop):
    return {
        "timeOffTypeUri": rail.result(for_each_loop)['timeoff_type_uri']
    }

def get_update_user_timeoff_policy_payload(dag_run,for_each_loop):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result(for_each_loop)['timeoff_type_uri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign'))
    }

def get_add_user_message(dag_run):
    log_supervisor_end_date_in_past = validate_supervisor_end_date() if bool(dag_run.conf['sup_emp_id']) and \
        rail.result('search_supervisor_in_replicon') != [] else False
    exception_logs = rail.result('add_new_user', 'exception_logs')

    if exception_logs:
        if rail.result('log_supervisor_not_present'):
            return ""
        if log_supervisor_end_date_in_past:
            return 'User Partially Added, Supervisor end date in past, '+ rail.smartjoin_by_delim(exception_logs, ";")
        return "User Partially Added,  "+ rail.smartjoin_by_delim(exception_logs, ";")

    if rail.result('log_supervisor_not_present'):
        return ""
    if log_supervisor_end_date_in_past:
        return 'User Partially Added, Supervisor end date in past'
    return "User Added Successfully"

def get_add_user_severity(dag_run):
    log_supervisor_end_date_in_past = validate_supervisor_end_date() if bool(dag_run.conf['sup_emp_id']) and \
        rail.result('search_supervisor_in_replicon') != [] else False
    exception_logs = rail.result('add_new_user', 'exception_logs')
    if rail.result('log_supervisor_not_present') or log_supervisor_end_date_in_past or exception_logs:
        return 'Exception'
    return 'Success'

def get_user_timeoff_policy_payload(dag_run,for_each_loop):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result(for_each_loop)['timeoff_type_uri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_default_time_off_policy_schedule'))
    }

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

    return udfs

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

def get_disable_message(dag_run):
    if is_enddate_in_future(dag_run):
        return "User End date in Future, Enddate updated but Profile will be disabled on end date"
    return "User disabled Successfully"

def get_disable_status(dag_run):
    if is_enddate_in_future(dag_run):
        return "Exception"
    return "Success"

def assign_policyDataAccessScopes_to_projectmanager():
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "policyDataAccessScopes": [
            {
            "policyUri": "urn:replicon:policy:administration",
            "locations": [],
            "divisions": [
                {
                "division": null,
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                }
            ],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": [],
            "scopeObjectTypeUri": null
            },
            {
            "policyUri": "urn:replicon:policy:payroll-management",
            "locations": [],
            "divisions": [
                {
                "division": null,
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                }
            ],
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

def validate_reg_to_temp_transfer(dag_run):
    previous_reg_temp_value = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
        'customField.displayText', 'Reg/Temp', 'text')
    if previous_reg_temp_value:
        if previous_reg_temp_value=="Regular" and dag_run.conf['reg_temp']=="Temporary":
            return "Yes"
    return "No"
