import itertools
import logging
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from functools import lru_cache
import pendulum
import rail
from os import path
from json import loads, dumps

OPEN_BRACKETS = "{{"
CLOSE_BRACKETS = "}}"

from dxctechnology.workday_user_import_v1.user_import_uki_csc.utils.constants import (
    LOCATION_DELIMITER, EMPLOYEE_TYPE_DELIMITER,
    DEFAULT_TIMEZONE, DEFAULT_TIMEZONE_URI, DEFAULT_WORK_WEEK, DEFAULT_SCHEDULE_TYPE,
    POLICY_ADMINISTRATION, POLICY_PAYROLL, TIMEOFF_CONFIG
)

from dxctechnology.workday_user_import_v1.user_import_uki_csc.utils.date_utils import (
    parse_date,
    build_json_formatted_dates
)
from dxctechnology.workday_user_import_v1.user_import_uki_csc.utils.request_payload import get_json_date_from_date_str, get_todays_date_in_json, convert_json_date_to_date, get_replicon_date, INPUT_DATE_FORMAT


def get_annual_brought_sold_holiday_leave_list(return_for, country, part_time_full_time, return_type="list"):
    from airflow.exceptions import AirflowException
    if return_for == "country":
        ft = TIMEOFF_CONFIG['fulltime'][country]
        pt = TIMEOFF_CONFIG['parttime'][country]
        if part_time_full_time == 'both':
            if return_type == "json":
                return {
                    "part_time": pt,
                    "full_time": ft
                }
            return [
                ft['annual'], pt['annual'],
                ft['bought'], pt['bought'],
                ft['sold'], pt['sold'],
                pt['holiday']
            ]
        if part_time_full_time == 'parttime':
            if return_type == "json":
                return pt
            return [pt['annual'], pt['bought'], pt['sold'], pt['holiday']]
        if part_time_full_time == 'fulltime':
            if return_type == "json":
                return ft
            return [ft['annual'], ft['bought'], ft['sold']]
        raise ValueError("Invalid part_time_full_time value. Use 'parttime', 'fulltime', or 'both'.")

    if part_time_full_time == "parttime":
        res_data = TIMEOFF_CONFIG[part_time_full_time]
        return (
            [res_data['UK']['annual'], res_data['IRL']['annual']],
            [res_data['UK']['bought'], res_data['IRL']['bought']],
            [res_data['UK']['sold'], res_data['IRL']['sold']],
            [res_data['UK']['holiday'], res_data['IRL']['holiday']]
        )
    if part_time_full_time == "fulltime":
        res_data = TIMEOFF_CONFIG[part_time_full_time]
        return (
            [res_data['UK']['annual'], res_data['IRL']['annual']],
            [res_data['UK']['bought'], res_data['IRL']['bought']],
            [res_data['UK']['sold'], res_data['IRL']['sold']],
        )
    raise AirflowException("Invalid part_time_full_time value. Use 'parttime' or 'fulltime'.")

def get_user_uri(dag_run, task_id='create_user'):
    if dag_run.conf.get('user_uri'):
        return dag_run.conf.get('user_uri')
    return rail.result(task_id)['uri']

def get_all_run_ids_callable(trigger_id, parallel_count):
    results = []
    for x in range(parallel_count):
        result = rail.result(f'{trigger_id}_{x+1}')
        if result is not None:
            results.append(result)
    return list(itertools.chain(*results))

logger = logging.getLogger(__name__)

def get_tenure_value(date_1, date_2):
    return (min(float(((date_1-date_2).days)/365), 0))*(-1)


def is_profile_enabled(dag_run):
    return dag_run.conf['user_security_config']['profile_status'].lower() == 'enabled'

def get_process_uki_csc_user_data_config(dag_run, item, config):
    # Get mapper data for the user
    # from dxctechnology.workday_user_import_v1.user_import_uki_csc.utils.mapper import MASTER_MAPPER

    # Determine company code full path
    full_path = list(filter(lambda mapper_item: mapper_item.get('Company_Code') == item['companycode'], 
                            config.COMPANY_CODE_MAPPER))
    if not full_path:
        full_path = [{}]
    
    item['company_code_full_path'] = full_path[0].get('Full_Path')
    item['_parent_company_code'] = full_path[0].get('Parent')
    
    # Get all the required data
    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data()
    policy_data = get_policy_data()
    
    # Get mapper derived data
    _mapper_derived_data = get_mapper_derived_data(item, config)
    
    # Get company code URI
    company_code_uri = rail.find_first_by_attr_and_get_attr(
        division_data,
        "full_path",
        item["company_code_full_path"],
        default={}
    )
    schedule_uri = "shift" if item.get("workshift", "").lower() == "shift schedule" else "office-schedule"
    return {
        "user_record_index": int(item.get('user_record_index', 0)),
        "supervisor_user_log": rail.result("create_supervisor_log"),
        "file_name": path.split(rail.result("new_file_sensor"))[1],
        "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
        "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
        "employee_type_data": cached_write_json_artifact('get_all_employeegroup_data', 'employee_data_for_assignment'),
        "division_data": cached_write_json_artifact('get_all_companycode_data'),
        "item": item,
        "file_data": get_file_data_mapping(item),
        "company_code_list": "",
        "employee_type_list": "",
        "mapper_data": _mapper_derived_data,
        "payrule": {
            "payrule": _mapper_derived_data['payrule'],
        },
        "user_security_config": {
            "allowed_country": "enable",
            "profile_status": "enabled",
            "replicon_field": 'true' if item['status'] in [1,'1'] else 'false',
            "auth_uri": config.AUTHS.get(_mapper_derived_data.get('Authentication Type', 'SSO')),
            "products": [product['Value'] for product in config.PRODUCT],
            "product_uri": [product['URI'] for product in config.PRODUCT]
        },
        "udfs": _get_user_udfs_details(),
        "oefs": _get_user_oefs_details(),
        "work_week":{
            "workweek_uri": f"urn:replicon:day-of-week:{_mapper_derived_data['work_week'].split(' ')[0].lower()}",
            "workweek_name": _mapper_derived_data['work_week'],
        },
        "holiday_calendar": {
            "holiday_calendar": _mapper_derived_data['holiday_calendar'],
            "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                get_holiday_calender_details(),
                'name',
                _mapper_derived_data['holiday_calendar'],
                'uri')
        },
        "approval_path": {
            "timesheet_approval_path": {
                "timesheet_approval_path": _mapper_derived_data['timesheet_approval']
            },
            "timeoff_approval": {
                "time_off_approval_path": _mapper_derived_data['time_off_approval'],
            },
            "time_entry_approval_path": {
                "time_entry_approval_path": _mapper_derived_data['time_entry_approval_path'],
            }
        },
        "activities": {
            "activity_list" : _mapper_derived_data['mapper_activities']
        },
        "user_policies": {
            "punch_entry_policy": {},
            "timeoff_template": {
                **{"timeoff_template": _mapper_derived_data['time_off_template']},
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data['time_off_template'],
                    default={}
                )
            },
            "timesheet_period": {
                **{"timesheet_period": _mapper_derived_data.get('timesheet_period', '')},
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data.get('timesheet_period', ''),
                    default={}
                )
            },
            "timesheet_template": {
                **{"timesheet_template": _mapper_derived_data.get('timesheet_template', '')},
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data.get('timesheet_template', ''),
                    default={}
                )
            },
            "schedule_policy": {
                "schedule_policy": _mapper_derived_data.get('schedule_policy', ''),
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data.get('schedule_policy', ''),
                    default={}
                )
            },
            "overtime_requests": {
                "overtime_requests": _mapper_derived_data.get('overtime_requests', ''),
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data.get('overtime_requests', ''),
                    default={}
                )
            },
            "overtime_request_approval_paths": {
                "overtime_request_approval_paths": _mapper_derived_data.get('overtime_request_approval_paths', ''),
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data.get('overtime_request_approval_paths', ''),
                    default={}
                )
            }
        },
        "timezone": {
            "timezone": _mapper_derived_data.get("timezone"),
            "timezone_uri" : _mapper_derived_data.get("timezone_uri")
        },
        "user_permissions" : _get_user_permission_set_details(
                config.end_user_permission,
                config.supervisor_end_user_permission,
                config.supervisor_end_user_supervision_permission
            ),
        "groups": {
            # Location
            "location_path": _get_location_full_path(item),
            "location": rail.find_first_by_attr_and_get_attr(
                location_data,
                'fullpath',
                _get_location_full_path(item),
                default={}
            ),
            "location_exception": f"Location '{_get_location_full_path(item)}' not found in Replicon" if not rail.find_first_by_attr_and_get_attr(
                location_data,
                'fullpath',
                _get_location_full_path(item),
                default={}
            ).get('uri') else None,
            # Employee Type - For CSC, use emp_group_code/emp_group_name not exempt
            "employee_type" : {
                "is_exempt" : item["exempt"]=="Yes",
                "exempt_salaried" : rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                "Exempt – Salaried",
                                default={}),
                "non_exempt_hourly" : rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                "Non Exempt - Hourly",
                                default={}),
                "other" : rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                _get_employee_type_full_path(item),
                                default={}),
                "employee_type_full_path": rail.find_first_by_attr_and_get_attr(employee_data, 'full_path', _get_employee_type_full_path(item), default={}),
                "uri": ((rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                "Exempt – Salaried",
                                default={})) if item["exempt"]=="Yes" else (rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                "Non Exempt - Hourly",
                                default={})))
            },
            # Organizational Unit
            "department": rail.find_first_by_attr_and_get_attr(
                department_data,
                "displayText",
                item.get("orgcode", ""),
                default={}
            ),
            # Cost Center
            "cost_center": rail.find_first_by_attr_and_get_attr(
                cost_center_data,
                "displayText",
                item.get("costcenter", ""),
                default={}
            ),
            # Company Code
            "division": company_code_uri,
            # PayGroup
            "service_center":{} # We are making use of the name not the URI
        },
        "schedule": {
            "default_office_schedule": {
                "name" : _mapper_derived_data.get('default_office_schedule', '')
            },
            "schedule_name": item.get("workshift", ""),
            "schedule_type_uri": f"urn:replicon:schedule-type:{schedule_uri}",
            "schedule_type": "shift" if schedule_uri == "shift" else "office-schedule",
            "office_schedule_details": {
                **rail.find_first_by_attr_and_get_attr(
                    get_replicon_schedule_details(),
                    "displayText",
                    item.get("workshift", ""),
                    default={}
                )
            } if _mapper_derived_data.get('schedule_type', 'office') != "shift" else {}
        },
        "json_formatted_dates": build_json_formatted_dates(item, config.instance, _mapper_derived_data['work_week'])
    }


def get_mapper_derived_data(item, config):
    from dxctechnology.workday_user_import_v1.user_import_uki_csc.mapper.mapper import get_user_assignments
    
    # Prepare user data for mapper matching
    user_data = {
        'additional_job_classification': item.get('additionaljobclassifications', ''),
        'company_code': item.get('companycode', ''),
        'location': item.get('country', ''),  # This should map to "United Kingdom" or "Ireland"
        'work_shift': item.get('workshift', '') if item.get('workshift', '').lower() == "shift schedule" else "No Shift Schedule",
        'fte_percent': item.get('ftepct', '')
    }

    # Get assignment from restrictions-based mapper
    assignment = get_user_assignments(user_data)
    if assignment:
        # Use assignment data from mapper
        mapper_data = {
            'mapper_data_found': 'yes',
            'user_data_used': user_data,
            'payrule': assignment.get('payrule', ''),
            'Authentication Type': 'SSO',  # Default to SSO
            'work_week': assignment.get('work_week', DEFAULT_WORK_WEEK),
            'holiday_calendar': '',  # To be populated from holiday calendar field
            'timesheet_approval': assignment.get('timesheet_approval_path', 'Supervisor'),
            'time_off_approval': assignment.get('timeoff_approval_path', 'Supervisor'),
            'time_entry_approval_path': '',  # Not in CSC mapper
            'time_off_template': 'Time Off',  # To be populated from timeoff mapper
            'mapper_activities': [],  # not available
            'timesheet_period': assignment.get('timesheet_period', 'Weekly'),
            'timesheet_template': assignment.get('timesheet_template', ''),
            'schedule_policy': '',  # Not in CSC mapper
            'overtime_requests': '',  # Not in CSC mapper
            'overtime_request_approval_paths': '',  # Not in CSC mapper
            'schedule': '',  # One to One mapping
            'schedule_type': DEFAULT_SCHEDULE_TYPE,
            'timezone': DEFAULT_TIMEZONE,
            'timezone_uri': DEFAULT_TIMEZONE_URI,
            'timeoffs': assignment['timeoff_to_assign_and_disable'] + assignment['timeoff_to_assign_and_enable'],  # Include timeoffs (Enabled and assign and disable both are included) from mapper
            'timeoff_to_assign_and_disable' : assignment['timeoff_to_assign_and_disable'],
            'timeoff_to_assign_and_enable': assignment['timeoff_to_assign_and_enable']
        }
    else:
        # Fallback to default values if no mapper match
        mapper_data = {
            'mapper_data_found': 'no',
            'user_data_used': user_data,
            'payrule': '',
            'Authentication Type': 'SSO',
            'work_week': DEFAULT_WORK_WEEK,
            'holiday_calendar': '',
            'timesheet_approval': 'Supervisor',
            'time_off_approval': 'Supervisor',
            'time_entry_approval_path': '',
            'time_off_template': '',
            'mapper_activities': [],
            'timesheet_period': 'Weekly',
            'timesheet_template': '',
            'schedule_policy': '',
            'overtime_requests': '',
            'overtime_request_approval_paths': '',
            'schedule': '',
            'schedule_type': DEFAULT_SCHEDULE_TYPE,
            'timezone': DEFAULT_TIMEZONE,
            'timezone_uri': DEFAULT_TIMEZONE_URI,
            'timeoffs': [],  # Include timeoffs (Enabled and assign and disable both are included) from mapper
            'timeoff_to_assign_and_disable' : [],
            'timeoff_to_assign_and_enable': []
        }
    
    # Add default values from master mapper
    default_office_schedule = "8 hours/day; Mon-Fri"
    if default_office_schedule:
        mapper_data['default_office_schedule'] = "8 hours/day; Mon-Fri"
    
    return mapper_data

def get_file_data_mapping(item):
    return {
        "emp_id": item.get("empid", ""),
        "perner_id": item.get("pernerid", ""),
        "email_id": item.get("email", ""),
        "first_name": item.get("firstname", ""),
        "last_name": item.get("lastname", ""),
        "country": item.get("country", ""),
        "state": item.get("state", ""),
        "workcity": item.get("workcity", ""),
        "exempt": item.get("exempt", ""),
        "exempt_effective_date": item.get("exempteffectivedate", ""),
        "employee_type": item.get("employeetype", ""),
        "hire_date": item.get("hiredate", ""),
        "gender": item.get("gender", ""),
        "service_date": item.get("servicedate", ""),
        "term_date": item.get("termdate", ""),
        "status": item.get("status", ""),
        "on_leave": item.get("onleave", ""),
        "parent_company": item.get('_parent_company_code', ''),
        "company_code": item.get("companycode", ""),
        "company_name": item.get("companyname", ""),
        "area_code": item.get("areacode", ""),
        "area_name": item.get("areaname", ""),
        "sub_area_code": item.get("subareacode", ""),
        "emp_group_code": item.get("empgroupcode", ""),
        "emp_group_name": item.get("empgroupname", ""),
        "emp_subgroup_code": item.get("empsubgroupcode", ""),
        "emp_subgroup_name": item.get("empsubgroupname", ""),
        "supervisor_id": item.get("supervisorid", ""),
        "supervisor_date": item.get("supervisordate", ""),
        "supervisor_f_name": item.get("supervisorfname", ""),
        "supervisor_l_name": item.get("supervisorlname", ""),
        "supervisor_email_id": item.get("supervisoremail", ""),
        "pay_group": item.get("paygroup", ""),
        "location_effective_date": item.get("locationeffectivedate", ""),
        "home_country": item.get("homecountry", ""),
        "cost_center": item.get("costcenter", ""),
        "cost_center_name": item.get("costcentername", ""),
        "cost_center_effective_date": item.get("costcentereffectivedate", ""),
        "org_code": item.get("orgcode", ""),
        "org_name": item.get("orgname", ""),
        "work_shift": item.get("workshift", ""),
        "work_shift_effective_date": item.get("workshifteffectivedate", ""),
        "job_level": item.get("joblevel", ""),
        "job_change_effective_date": item.get("jobchangeeffectivedate", ""),
        "fte": item.get("fte", ""),
        "fte_pct": item.get("ftepct", ""),
        "is_ia": item.get("isia", ""),
        "ia_start_date": item.get("iastartdate", ""),
        "ia_end_date": item.get("iaenddate", ""),
        "rut": item.get("rut", ""),
        "middle_name": item.get("middlename", ""),
        "time_type": item.get("timetype", ""),
        "dob": item.get("dob", ""),
        "management_lvl": item.get("managementlvl", ""),
        "ausjc": item.get("ausjc", ""),
        "terms_conditions": item.get("termsconditions", ""),
        "industrial_instrument_classification": item.get("industrialinstrumentclassification", ""),
        "additional_data_effective_date": item.get("additionaldataeffectivedate", ""),
        "termination_reason": item.get("terminationreason", ""),
        "scheduled_weekly_hours": item.get("scheduledweeklyhours", ""),
        "assignment_type": item.get("assignment_type", ""),
        "marital_status_ind": item.get("marital_status_ind", ""),
        "marital_status_efft_dt": item.get("marital_status_efft_dt", ""),
        # UK&I specific fields
        "additional_job_classifications": item.get("additionaljobclassifications", ""),
        "holiday_schedule_calendar": item.get("holidayschedulecalendar", ""),
        "employee_representative_status": item.get("employeerepresentativestatus", ""),
        "employee_representative_effective_date": item.get("employeerepresentativeeffectivedate", "")
    }

def _get_location_full_path(item):
    country = item.get('country', '')
    state = item.get('state', '')
    
    # Build location path using constant delimiter
    location_parts = [country]
    if state:
        location_parts.append(state)
    
    return LOCATION_DELIMITER.join(location_parts)

def _get_employee_type_full_path(item):
    return rail.smartjoin_by_delim(
        arr=[item.get('subareacode', ''), item.get('empgroupcode', ''), item.get('empsubgroupcode', '')], 
        separator=EMPLOYEE_TYPE_DELIMITER
    )

@lru_cache(maxsize=32)
def _get_user_permission_set_details(end_user_permission, supervisor_end_user_permission, supervisor_user_permission):
    permissions_details = rail.result("get_all_permission_sets")
    return {
        "end_user_permission": rail.find_first_by_attr_and_get_attr(
            permissions_details, 'displayText', end_user_permission, default={}),
        "supervisor_end_user_permission": rail.find_first_by_attr_and_get_attr(
            permissions_details, 'displayText', supervisor_end_user_permission, default={}),
        "supervisor_end_user_supervision_permission": rail.find_first_by_attr_and_get_attr(
            permissions_details, 'displayText', supervisor_user_permission, default={})
    }

@lru_cache(maxsize=64)
def get_groups_data():
    return (rail.result("get_all_employeegroup_data")['employee_data_for_assignment'], rail.result("get_all_locations"), rail.result("get_all_enabled_departments"),
            rail.result('get_all_companycode_data'), rail.result('get_all_cost_centers'))

@lru_cache(maxsize=8)
def get_policy_data():
    return rail.result("get_all_policy_sets")

@lru_cache(maxsize=8)
def get_holiday_calender_details():
    return rail.result("get_all_holiday_calendar")

@lru_cache(maxsize=8)
def get_replicon_schedule_details():
    return rail.result("get_all_office_schedules")

@lru_cache(maxsize=8)
def _get_user_udfs_details():
    replicon_udf_list = rail.result("get_all_user_custom_fields")
    
    return {
        "gender": replicon_udf_list.get("gender", {}),
        "service_date": replicon_udf_list.get("continuous_service_date", {}),
        "on_leave": replicon_udf_list.get("on_leave", {}),
        "personnel_area_name": replicon_udf_list.get("personnel_area_description", {}),
        "personnel_area_code": replicon_udf_list.get("personnel_area_code", {}),
        "job_level": replicon_udf_list.get("job_activity_type", {}),
        "fte": replicon_udf_list.get("fte", {}),
        "ftepct": replicon_udf_list.get("fte_%", {}),
        "international_assignee": replicon_udf_list.get("international_assignee", {}),
        "international_assignee_start_date": replicon_udf_list.get("international_assignee_start_date", {}),
        "international_assignee_end_date": replicon_udf_list.get("international_assignee_end_date", {}),
        "perner": replicon_udf_list.get("perner", {}),
        "rut": replicon_udf_list.get("rut", {}),
        "middle_name": replicon_udf_list.get("middle_name", {}),
        "time_type": replicon_udf_list.get("time_type", {}),
        "date_of_birth": replicon_udf_list.get("date_of_birth", {}),
        "employee_type_udf": replicon_udf_list.get("employee_group", {}),
        "work_shift": replicon_udf_list.get("work_shift", {}),
        "management_level": replicon_udf_list.get("management_level", {}),
        "ia_perner_id": replicon_udf_list.get("ia_perner_id", {}),
        "terms_and_conditions": replicon_udf_list.get("terms_and_conditions", {}),
        "termination_reason": replicon_udf_list.get("termination_reason", {}),
        "termination_reason_code": replicon_udf_list.get("termination_reason_code", {}),
        "employee_sub_group": replicon_udf_list.get("employee_sub_group", {}),
        "assignment_type": replicon_udf_list.get("assignment_type", {}),
        "annual_leave_anni_date": replicon_udf_list.get("annual_leave_anni_date", {}),
        "lsl_anniversary_date": replicon_udf_list.get("lsl_anniversary_date", {}),
        "personal_leave_anni_date": replicon_udf_list.get("personal_leave_anni_date", {}),
        "weekly_scheduled_hours": replicon_udf_list.get("weekly_scheduled_hours", {}),
        "ee_group": replicon_udf_list.get("ee_group", {}),
        "psa_user": replicon_udf_list.get("psa_user", {}),
        # UK&I specific UDFs
        "holiday_schedule_calendar": replicon_udf_list.get("holiday_schedule_calendar", {}),
    }

def _get_user_oefs_details():
    replicon_oef_list = rail.result("get_user_oefs")
    emp_representative_status = replicon_oef_list.get("employee_representative_status", {})
    emp_representative_status['drop_down_vals'] = rail.result("get_dropdown_options_for_employee_representative_status")
    return {
        "additional_job_classifications": replicon_oef_list.get("additional_job_classifications", {}),
        "employee_representative_status": emp_representative_status,
        "employee_representative_effective_date": replicon_oef_list.get("employee_representative_effective_date", {})
    }

def cached_write_json_artifact(task_id, key=None):
    try:
        # Get data from the task result
        data = rail.result(task_id)
        if key is None:
            artifact_data = rail.write_json_artifact(data)
        else:    
            artifact_data = rail.write_json_artifact(data[key])
        # In production, this would write to artifact storage
        # For now, return the filename as a placeholder
        logger.info(f"Would write artifact: {key} from task {task_id}")
        return artifact_data
    except Exception as e:
        logger.error(f"Error writing artifact {artifact_data}: {str(e)}")
        raise

def get_trigger_dag_id(dag_id_template, batch_count, item_index):
    if item_index == 1:
        return dag_id_template
    return f"{dag_id_template}_{item_index}"

def get_item_index(dag_run, batch_count, item=None, use_item= False):
    if use_item and item and item.get('user_record_index') is not None:
        return (int(item['user_record_index']) % batch_count) + 1
    try:
        return (int(dag_run.conf.get('user_record_index', 0)) % batch_count) + 1
    except:
        return 1


def calculate_prorated_timeoff(entitlement, termination_date, hire_date=None, leaves_used=0, is_hours=False):
    term_date = parse_date(termination_date)
    current_year_start = pendulum.now().start_of('year')
    
    if hire_date and parse_date(hire_date).year == term_date.year:
        # New hire termination
        start_date = parse_date(hire_date)
        total_months = 12 - start_date.month
        months_worked = term_date.month - start_date.month
    else:
        # Regular termination
        total_months = 12
        months_worked = term_date.month
    
    # Calculate prorated amount
    prorated = (entitlement / total_months) * months_worked
    
    # Round up for positive values
    if prorated > 0:
        prorated = round(prorated + 0.5) if not is_hours else round(prorated)
    
    # Deduct used leaves
    final_balance = prorated - leaves_used
    
    return final_balance


def is_user_disabled_for_non_go_live_country(dag_run, user_details_task_id):
    user_details = rail.result(user_details_task_id)
    return user_details['userDetails']['isEnabled'] is True \
        and dag_run.conf['user_security_config']['profile_status'] != "enabled"

def is_division_uki_csc_test():
    if rail.result("get_effective_group_membership").get('parent_division'):
        division_name = rail.result("get_effective_group_membership")['parent_division']['division']['displayText']
        return division_name in ["GSAP"]
    return False

def can_update_user_end_date_test_uki_csc(dag_run):
    return (bool(dag_run.conf['file_data'].get('term_date'))
            and not bool(rail.result("get_user_details")['userDetails']['employmentDateRange'].get('endDate', False)))

def user_does_not_have_admin_and_payroll_permission_test_uki_csc():
    permissions = rail.result("get_assigned_permission_for_user")
    if not permissions:
        return True  # No permissions means no admin or payroll

    # Single pass through permissions checking both URIs
    admin_found = False
    payroll_found = False
    
    for permission in permissions:
        if permission.get('policyUri') == POLICY_ADMINISTRATION:
            admin_found = True
        elif permission.get('policyUri') == POLICY_PAYROLL:
            payroll_found = True
        
        if admin_found and payroll_found:
            break  # Early exit if both found
    
    return not (admin_found or payroll_found)

def is_user_already_disabled_test_uki_csc(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is not True and dag_run.conf['user_security_config']['replicon_field'] in [False , 'false']

def is_user_rehire_test_uki_csc(dag_run):
    user_details = rail.result('get_user_details')
    rehire_value = user_details['userDetails']['isEnabled'] is False \
        and dag_run.conf['user_security_config']['replicon_field'] in ['true', True] \
            and dag_run.conf['user_security_config']['profile_status'] == "enabled"
    rail.set_result(key="rehire", val=("yes" if rehire_value else "no"))
    return rehire_value

def can_update_user_start_date_test_uki_csc(dag_run):
    user_start_date = rail.result("get_user_details")['userDetails']['employmentDateRange'].get('startDate', False)
    if not user_start_date:
        return True
    return dag_run.conf['file_data']['hire_date'] != f"{user_start_date['year']}-{user_start_date['month']:02d}-{user_start_date['day']:02d}"

def should_disabled_user_test_uki_csc(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is True and\
        dag_run.conf['user_security_config']['replicon_field'] in [False , 'false']

def is_end_date_less_than_today_test_uki_csc(dag_run):
    return get_replicon_date(dag_run.conf['file_data']['term_date'], "date").date() < convert_json_date_to_date(get_todays_date_in_json())

def get_current_assigned_udf_values(custom_field_values):
    return {
        "perner_id": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "IA PERNER ID", "text"),
        "gender": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Gender", "text"),
        "service_date": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Continuous Service Date", "text"),
        "on_leave": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "On Leave", "text"),
        "business_title": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Business Title", "text"),
        "cost_center_id": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Cost Center ID", "text"),
        "cost_center_name": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Cost Center Name", "text"),
        "worker_category": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Worker Category", "text"),
    }

def get_trigger_process_timeoff_policies_items():
    current_timeoff_policies = rail.result("get_user_timeoff_policy_summary").get('policiesByTimeOffType', [])
    
    return list(filter(lambda to_policy: to_policy.get('isTimeOffAllowedAgainstThisTimeOffType') is True
        and bool(to_policy.get('policySetSchedule') and to_policy['policySetSchedule'][0].get('effectiveDate')), current_timeoff_policies))

def get_mapper_timeoff_data(dag_run, timeoff_mapper):
    if dag_run.conf['file_data']['employee_representative_status'].lower() == "yes":
        if dag_run.conf['file_data']['country'].lower() == "ireland":
            return dag_run.conf['mapper_data']['timeoffs'] + ['[IRL] Employee Representative Duties']
        if dag_run.conf['file_data']['country'].lower() == "united kingdom":
            return dag_run.conf['mapper_data']['timeoffs'] + ['[UK] Employee representative duties']
    return dag_run.conf['mapper_data']['timeoffs']

def get_policy_set_to_assign_uki_csc():
    if rail.result("get_default_timeoff_policy"):
        return loads(dumps(rail.result("get_default_timeoff_policy")
                                ).replace("null", "\"effective\""
                                ).replace("\"script\"", "\"scriptTarget\""
                            )
                    )
    return []

def get_specified_json_date_minus_specified_days_months_years_date_in_json(_date_to_use: dict, days_in_number:int=0, months_in_number:int=0, years_in_number:int=0, return_type="json"):
    _date_to_use:date = convert_json_date_to_date(_date_to_use)
    _date_to_return = _date_to_use - relativedelta(
        days=days_in_number,
        months=months_in_number,
        years=years_in_number
    )
    if return_type == "date":
        return _date_to_return
    return {
        "day": _date_to_return.day,
        "month": _date_to_return.month,
        "year": _date_to_return.year
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

@lru_cache(maxsize=32)
def get_cached_replicon_timeoff_data():
    return rail.result("get_all_timeoffs")

def get_end_date_to_use(dag_run):
    _date = datetime.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT)

    return _date

def add_new_policy_line(dag_run):
    _date = get_end_date_to_use(dag_run)
    return {
        "effectiveDate": {
            "day": _date.day,
            "month": _date.month,
            "year": _date.year
        },
        "description": f"Added by Integration on {_date.strftime('%d-%m-%Y')}",
        "policySet": {
            "timeOffBalanceEventScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:amount",
                    "value": {
                        "number": 0
                    }
                }],
                "script": {
                    "description": "Set initial balance for the first day of a policy",
                    "name": "Starting Balance Set To",
                    "uri": dag_run.conf['starting_balance_set_to_uri']
                }
            }],
            "timeOffValidationScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                    "value": {
                        "number": "0"
                    }
                }],
                "script": {
                    "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                    "name": "Prevent balance overdraw",
                    "uri": dag_run.conf['prevent_balance_overdraw_uri']
                }
            }]
        }
    }

def get_timeoff_polices_to_assign_callable(dag_run):
    policies = loads(dag_run.conf['policy_set'])
    user_end_date: datetime = convert_json_date_to_date(
        dag_run.conf['user_end_date_json'])
    existing_policy = list(filter(
        lambda policy: convert_json_date_to_date(
            policy['effectiveDate']) < user_end_date,
        policies
    ))

    existing_policy.append(add_new_policy_line(dag_run))

    return existing_policy

def format_timeoff_polices_to_assign_callable(dag_run):
    return dumps(rail.result("get_timeoff_polices_to_assign")
                ).replace("/null/", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                ).replace('":{"additionalParameters', '":[{"additionalParameters'
                ).replace(':{"keyUri"', ':[{"keyUri"'
                ).replace('}},"scriptTarget"', '}}],"scriptTarget"'
                ).replace('}},"timeOffValidationScripts', '}}],"timeOffValidationScripts'
                ).replace('}}},"description', '}}]},"description')

def get_marital_status_effective_date_if_applicable(timeoff_name, dag_run):
    # Check if this timeoff requires marital status
    timeoff_details = dag_run.conf.get('timeoff_type_details', {})
    mapper_data = timeoff_details.get('mapper_data', {})
    
    if mapper_data.get('Marital Status Required', 'No').lower() == 'yes':
        file_data = dag_run.conf.get('file_data', {})
        marital_status_ind = file_data.get('marital_status_ind', '')
        marital_status_efft_dt = file_data.get('marital_status_efft_dt', '')
        
        if marital_status_ind.lower() == 'yes' and marital_status_efft_dt:
            # Convert to JSON date format
            return get_json_date_from_date_str(marital_status_efft_dt)
    
    return None

def process_marital_status_policy_for_type(dag_run):
    default_policy = rail.result("get_default_timeoff_policy_for_type", [])
    
    if not default_policy:
        return []
    
    # Get marital status effective date
    marital_status_date = get_marital_status_effective_date_if_applicable(
        dag_run.conf.get('timeoff_type_details', {}).get('name', ''),
        dag_run
    )
    
    if not marital_status_date:
        # Use hire date as fallback
        marital_status_date = dag_run.conf['json_formatted_dates'].get('hire_date')
    
    # Process the policy with the effective date
    processed_policies = []
    for policy in default_policy:
        policy_entry = {
            "effectiveDate": marital_status_date,
            "policySet": policy.get('policySet'),
            "description": f"Policy effective from marital status date"
        }
        processed_policies.append(policy_entry)
    
    return processed_policies

def timeoff_to_assign_uki_csc(dag_run):
    mapper_timeoff_data = rail.result('get_mapper_timeoff_data')
    replicon_timeoff_data = rail.result("get_all_timeoffs")# get_cached_replicon_timeoff_data()

    # Create name-to-record mapping for faster lookups (O(1) vs O(n))
    timeoff_names_dict = {mapper_timeoff.strip(): mapper_timeoff for mapper_timeoff in mapper_timeoff_data}

    # More efficient data processing
    timeoff_list = []
    not_found_names = set(timeoff_names_dict.keys())  # Use set for O(1) removal

    for replicon_timeoff in replicon_timeoff_data:
        if replicon_timeoff['name'] in timeoff_names_dict:
            timeoff_list.append({
                **replicon_timeoff,
                "mapper_data": timeoff_names_dict[replicon_timeoff['name']]
            })
            not_found_names.remove(replicon_timeoff['name'])
    timeoff_to_disable_after_assignment = dag_run.conf['mapper_data']['timeoff_to_assign_and_disable']
    return {
        "mapper_timeoff_data": mapper_timeoff_data,
        "timeoff_not_found_in_replicon": list(not_found_names),
        "timeoff_data_to_assign": [ {**{"to_index": timeoff_index}, **_timeoff} for timeoff_index, _timeoff in enumerate(timeoff_list)],
        "timeoff_data_to_assign_uri_list": [timeoff['uri'] for timeoff in timeoff_list],
        "timeoff_data_to_assign_uri_list_disabled_removed": [timeoff['uri'] for timeoff in timeoff_list if timeoff['name'] not in timeoff_to_disable_after_assignment]
    }

def get_effective_grp_with_disabled_assigned_grp_handler(_data, grp_key, sub_grp_key, list_item_index=0):
    if not _data:
        return {}
    
    if not _data[list_item_index]:
        return {}
    
    if not _data[list_item_index][grp_key]:
        return {}

    if not _data[list_item_index][grp_key][sub_grp_key]:
        return {}

    return _data[list_item_index][grp_key][sub_grp_key]


def get_effective_grp_membership_data_handler(response):
    return_data = {}
    rail.set_result(key="response", val=response)
    return_data['costCenter'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['costCenters'],
        grp_key = 'costCenter',
        sub_grp_key = 'costCenter',
        list_item_index = 0
    ) if response['costCenters'] else {})

    return_data['department'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['departments'],
        grp_key = 'department',
        sub_grp_key = 'department',
        list_item_index = 0
    ) if response['departments'] else {})

    return_data['division'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['divisions'],
        grp_key = 'division',
        sub_grp_key = 'division',
        list_item_index = 0
    ) if response['divisions'] else {})

    return_data['employeeType'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['employeeTypes'],
        grp_key = 'employeeType',
        sub_grp_key = 'employeeType',
        list_item_index = 0
    ) if response['employeeTypes'] else {})

    return_data['location'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['locations'],
        grp_key = 'location',
        sub_grp_key = 'location',
        list_item_index = 0
    ) if response['locations'] else {})

    return_data['serviceCenter'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['serviceCenters'],
        grp_key = 'serviceCenter',
        sub_grp_key = 'serviceCenter',
        list_item_index = 0
    ) if response['serviceCenters'] else {})

    return_data['parent_location'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['locations'],
        grp_key = 'location',
        sub_grp_key = 'parent',
        list_item_index = 0
    ) if response['locations'] else {})

    return_data['parent_division'] = (get_effective_grp_with_disabled_assigned_grp_handler(
        _data = response['divisions'],
        grp_key = 'division',
        sub_grp_key = 'parent',
        list_item_index = 0
    ) if response['divisions'] else {})

    return return_data

def _get_trigger_dag_id(trigger_dag_id, max_batch_count, item_index):
    modulo = (item_index % max_batch_count) + 1
    if modulo == 1:
        return f"{trigger_dag_id}"
    return f"{trigger_dag_id}_{modulo}"

def should_trigger_delete_time_and_timeoff_for_disabled_user(dag_run):
    if dag_run.conf['file_data']['on_leave'] == "1":
        return True

    # Check if user is disabled (profile_status != "enabled")
    profile_status = dag_run.conf.get('file_data', {}).get('status', {})

    if not profile_status or profile_status.lower() != "0":
        return False

    # Check if term_date exists and is in the past
    term_date_str = dag_run.conf.get('file_data', {}).get('term_date')

    if not term_date_str:
        return False

    try:
        # Parse term_date and compare with today
        term_date = datetime.strptime(term_date_str, INPUT_DATE_FORMAT).date()
        today = datetime.now().date()

        # Return True if term_date is in the past
        return term_date < today
    except (ValueError, TypeError):
        return False
