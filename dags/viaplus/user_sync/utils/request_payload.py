"""
Request Payload Construction for ViaPlus User Sync

Contains all payload construction and validation functions for Replicon API calls.
Matches CRL user_import_ireland_v1 patterns.
"""
from datetime import datetime
from dateutil.parser import parse as date_parser
from airflow.models import Variable
from urllib.parse import urlencode
from uuid import uuid4
import json
import rail

from viaplus.user_sync import config

null = None
DATE_FORMAT = "%d/%m/%Y"

# Keka API Group Type Constants (from Keka API documentation)
GROUP_TYPE_NONE = 0
GROUP_TYPE_BUSINESS_UNIT = 1
GROUP_TYPE_DEPARTMENT = 2
GROUP_TYPE_LOCATION = 3
GROUP_TYPE_COST_CENTER = 4
GROUP_TYPE_PAYGROUP = 5
GROUP_TYPE_PROJECT_TEAM = 6
GROUP_TYPE_TEAM = 7
GROUP_TYPE_CLIENT_TEAM = 8
GROUP_TYPE_LEGAL_ENTITY = 9

# Keka Employment Status Constants (from Keka API)
# 0 = Working (Active)
# 1 = Relieved (Terminated)
KEKA_STATUS_WORKING = 0
KEKA_STATUS_RELIEVED = 1

# Mandatory fields for validation (matching CRL pattern)
MANDATORY_FIELDS = {
    "emp_id": "Employee Number",
    "first_name": "First Name",
    "last_name": "Last Name",
    "email": "Work Email",
    "login_name": "Login Name",
    "emp_status": "Employee Status",
    "department_name": "Department",
    "location_name": "Location",
    "start_date": "Start Date",
}


# ============================================================================
# Keka API Helper Functions (for SimpleHttpOperator-based approach)
# ============================================================================

def get_keka_token_request_body(instance_config):
    """
    Get the request body for Keka OAuth2 token endpoint.
    Returns URL-encoded form data string.

    DAG Task: get_keka_access_token (used in SimpleHttpOperator data parameter)

    Expects Airflow Variable to be stored as JSON with keys:
    - KEKA_CLIENT_ID
    - KEKA_CLIENT_SECRET
    - KEKA_API_KEY
    """
    # Get credentials from Airflow Variable (stored as JSON)
    credentials = Variable.get(instance_config.keka_conn_variables,default_var={}, deserialize_json=True)

    return urlencode({
        'grant_type': config.KEKA_GRANT_TYPE,  # Static value from config
        'scope': config.KEKA_SCOPE,  # Static value from config
        'client_id': credentials.get('KEKA_CLIENT_ID'),
        'client_secret': credentials.get('KEKA_CLIENT_SECRET'),
        'api_key': credentials.get('KEKA_API_KEY')
    })


def get_keka_token_headers():
    """
    Get headers for Keka OAuth2 token request.
    Some APIs require additional headers to pass through Azure Gateway.
    """
    return {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "User-Agent": "Mozilla"
    }


def extract_keka_access_token():
    """
    Extract access token from Keka OAuth2 response.

    DAG Task: extract_keka_token (PythonOperator)
    Uses result from get_keka_access_token via XCom.
    Returns: access_token string
    """
    response = rail.result('get_keka_access_token')
    if isinstance(response, str):
        response = json.loads(response)
    return response.get('access_token', '')


def get_employees_from_conf(dag_run):
    """
    Get test employees data from dag_run.conf for testing without Keka API.

    DAG Task: use_conf_employees_data (PythonOperator)

    Expected conf format:
    {
        "test_employees_data": [
            {
                "employeeNumber": "T01085",
                "firstName": "Santosh",
                ...
            }
        ]
    }

    Returns: dict in same format as Keka API response
    """
    test_data = dag_run.conf.get('test_employees_data', [])

    # Wrap in same format as Keka API response
    return {
        "succeeded": True,
        "data": test_data if isinstance(test_data, list) else [],
        "pageNumber": 1,
        "pageSize": len(test_data) if isinstance(test_data, list) else 0,
        "totalRecords": len(test_data) if isinstance(test_data, list) else 0
    }


def merge_keka_employees_data(dag_run):
    """
    Merge/select employees data from either conf (test mode) or Keka API.

    DAG Task: merge_keka_employees (PythonOperator with trigger_rule='one_success')

    Checks which upstream task produced data and returns it.
    Returns: dict in Keka API response format
    """
    # Check if test data was used (conf path) - check conf directly
    if dag_run.conf.get('test_employees_data'):
        try:
            conf_result = rail.result('use_conf_employees_data')
            if conf_result:
                return conf_result
        except Exception:
            pass

    # Otherwise use Keka API result
    try:
        api_result = rail.result('fetch_employees_from_keka')
        if api_result:
            if isinstance(api_result, str):
                return json.loads(api_result)
            return api_result
    except Exception:
        pass

    # Fallback: return empty result
    return {"succeeded": True, "data": [], "pageNumber": 1, "pageSize": 0, "totalRecords": 0}


def parse_keka_employees_response_merged():
    """
    Parse the merged Keka employees response (from either conf or API).

    DAG Task: parse_employees_response (PythonOperator)
    Reads from merge_keka_employees task.
    Returns: list of employee records
    """
    response = rail.result('merge_keka_employees')
    if isinstance(response, str):
        response = json.loads(response)

    # Keka API returns { "succeeded": true, "data": [...], "pageNumber": 1, "pageSize": 100 }
    if isinstance(response, dict):
        return response.get('data', [])
    return response if isinstance(response, list) else []


def filter_employees_by_groups():
    """
    Filter employees by legal entity and department names.

    Uses groupType integers from employee groups array:
    - groupType 9 = Legal Entity (keep only "VPTI Solutions Private Limited")
    - groupType 2 = Department (exclude "General and Administration" variants)

    DAG Task: filter_employees (PythonOperator)
    Returns: list of filtered employees
    """
    employees = rail.result('parse_employees_response')

    filtered = []
    for emp in employees:
        groups = emp.get('groups', [])

        # Find legal entity from groups (groupType=9)
        legal_entity_name = None
        department_name = None

        for group in groups:
            group_type = group.get('groupType')
            title = group.get('title', '')

            if group_type == GROUP_TYPE_LEGAL_ENTITY:
                legal_entity_name = title
            elif group_type == GROUP_TYPE_DEPARTMENT:
                department_name = title

        # Filter: Must be VPTI legal entity
        if legal_entity_name != config.LEGAL_ENTITY_NAME:
            continue

        #Filter: Exclude G&A department
        if department_name in config.EXCLUDED_DEPARTMENTS:
            continue

        # Store extracted group names for later use
        emp['_legal_entity_name'] = legal_entity_name
        emp['_department_name'] = department_name

        # Also extract location from groups (groupType=3)
        for group in groups:
            if group.get('groupType') == GROUP_TYPE_LOCATION:
                emp['_location_name'] = group.get('title', '')
                break

        filtered.append(emp)

    return filtered


def _get_employee_status_text(status_value):
    """Convert Keka employmentStatus integer to text.

    Keka API returns:
    - 0 = Working (maps to 'Active')
    - 1 = Relieved (maps to 'Terminated')
    """
    status_map = {
        KEKA_STATUS_WORKING: 'Active',
        KEKA_STATUS_RELIEVED: 'Terminated'
    }
    return status_map.get(status_value, 'Unknown')


def _format_date(date_str):
    """Convert Keka ISO date to DD/MM/YYYY format."""
    if not date_str:
        return ''
    try:
        # Parse ISO 8601 format (e.g., "2023-01-15T00:00:00Z")
        date_str = date_str.split('T')[0]  # Take only date part
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return dt.strftime(config.DATE_FORMAT)
    except (ValueError, AttributeError):
        return date_str


def transform_employees_for_collection():
    """
    Transform filtered employees to collection format.

    DAG Task: transform_employees (PythonOperator)
    Returns: list of transformed employee records matching collection columns

    Keka API field mappings (from developers.keka.com/reference/employees):
    - workEmail (work email address)
    - personalEmail (personal email address)
    - jobTitle (object with id, name)
    - dateOfJoining (hire/joining date)
    - exitDate (termination/exit date)
    - reportsTo (object with id, displayName)
    - l2Manager (second level manager)
    - gender is integer (0=NotSpecified, 1=Male, 2=Female)
    - mobilePhone, workPhone for phone numbers
    - dateOfBirth for DOB
    """
    filtered_employees = rail.result('filter_employees')

    def get_job_title(emp):
        """Get job title from jobTitle object."""
        job_title = emp.get('jobTitle')
        if not job_title:
            return ''
        if isinstance(job_title, dict):
            # jobTitle object has 'name' field (not 'title')
            return job_title.get('name', '') or job_title.get('title', '')
        return str(job_title)

    return [
        {
            # CRL naming convention: emp_id, sup_emp_id, start_date, end_date
            "emp_id": emp.get('employeeNumber', ''),
            "first_name": emp.get('firstName', ''),
            "last_name": emp.get('lastName', ''),
            "middle_name": emp.get('middleName', ''),
            "display_name": emp.get('displayName', ''),
            "email": emp.get('email', ''),
            "login_name": emp.get('email', ''),
            "emp_status": _get_employee_status_text(emp.get('employmentStatus')),
            "job_title": get_job_title(emp),
            "department_name": emp.get('_department_name', ''),
            "location_name": emp.get('_location_name', ''),
            "legal_entity_name": emp.get('_legal_entity_name', ''),
            "start_date": _format_date(emp.get('joiningDate', '')),
            "end_date": _format_date(emp.get('exitDate', '')),
            "sup_emp_id": emp.get('reportsTo', {}).get('id', '') if emp.get('reportsTo') else '',
        }
        for emp in filtered_employees
    ]


def get_mandatory_fields_exception_message(item):
    """Generate exception message for missing mandatory fields (CRL pattern)."""
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item.get(payload_key):
            missing_fields.append(f"{log_value} is not present in payload")

    return rail.smartjoin_by_delim(missing_fields, ";") if missing_fields else "Unknown validation error"


def get_replicon_date(date_str):
    """Convert date string to Replicon date format."""
    if not date_str:
        return None

    try:
        # Handle DD/MMM/YYYY format
        date = datetime.strptime(date_str, config.DATE_FORMAT)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except ValueError:
        # Try other formats
        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y']:
            try:
                date = datetime.strptime(date_str, fmt)
                return {
                    'year': date.year,
                    'month': date.month,
                    'day': date.day
                }
            except ValueError:
                continue
    return None


def get_date_from_replicon_date(replicon_date):
    """Convert Replicon date format to datetime."""
    if not replicon_date:
        return None
    return datetime(
        day=replicon_date['day'],
        month=replicon_date['month'],
        year=replicon_date['year']
    )


def get_today_date():
    """Get today's date in Replicon format."""
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_initial_date():
    """Get initial date for effective dates (a date far in the past)."""
    return {
        'year': 1900,
        'month': 1,
        'day': 1
    }


def is_end_date_in_future(dag_run):
    """Check if end date is in the future."""
    end_date = dag_run.conf.get('end_date')
    if not end_date:
        return False

    end_replicon = get_replicon_date(end_date)
    if not end_replicon:
        return False

    end_dt = get_date_from_replicon_date(end_replicon)
    today = datetime.utcnow().date()

    return end_dt.date() >= today

# ============================================================================
# Payload Construction - New User
# ============================================================================
def get_udfs(user_status, dag_run):
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    udfs = []
    def add_udf_field_values(definitionuri, dropdownuri = null, textvalue = null , number = null, date = null):
        udfs.append({"value":{
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
      }})
    if user_status == 'adduser':
        if dag_run.conf.get('middle_name_def_uri') and dag_run.conf.get('middle_name'):
            add_udf_field_values(
                definitionuri=dag_run.conf['middle_name_def_uri'],
                textvalue=dag_run.conf['middle_name']
            )
    if user_status == 'updateuser':
        current_middle_name = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Middle Name', 'text')
        if dag_run.conf.get('middle_name') and (dag_run.conf['middle_name'] != current_middle_name):
            add_udf_field_values(
                definitionuri=dag_run.conf['middle_name_def_uri'],
                textvalue=dag_run.conf.get('middle_name','')
            )
    return udfs

def get_all_eligible_timeoff_types_add(dag_run, config):
    eligible_timeoff_types = config.APPLICABLE_TIME_OFF_TYPES
    timeoff_list = []
    for item in eligible_timeoff_types:
        timeoff_list.append(
        {
            "timeOffType": {
            "uri": null,
            "name": item
            },
            "isTimeOffAllowedAgainstThisTimeOffType": 1,
            "applyDefaultTimeOffTypePolicy": 0,
            "defaultTimeOffTypePolicyEffectiveDate": null,
            "policySchedule": []
        }
        )
    return timeoff_list

def get_licenses_add(dag_run):
    license_uris = dag_run.conf['license_uris']
    licenses = []
    for item in license_uris:
        licenses.append(
        {
            "uri": item
        }
        )
    return licenses
def get_put_user_payload(dag_run,config):
    """
    Construct payload for creating a new user
    """
    log =[]
    payload_add_user = {
            "target": null,
            "template": null,
            "modifications": {
                "firstName": {
                "value": dag_run.conf['first_name']
                },
                "lastName": {
                "value": dag_run.conf['last_name']
                },
                "loginName": {
                "value": dag_run.conf['login_name']
                },
                "displayName": {
                "value": dag_run.conf['display_name']
                },
                "emailAddress": {
                "value": dag_run.conf['email']
                },
                "employeeId": {
                "value": dag_run.conf['emp_id']
                },
                "employmentDateRange": {
                "value": {
                    "startDate": get_replicon_date(dag_run.conf['start_date']),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
                },
                "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "true"
                    },
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": dag_run.conf["email"]
                    },
                    "ssoNameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name",
                    "password": null,
                    "authenticationProviders": [],
                    "emailMFAResendVerificationEmail": null,
                    "emailMFATryAddMethodFromUsersEmail": null,
                    "isMFAMethodRequired": null,
                    "clearIsLockedOut": null
                }
            },
                "timesheetApprovalPath": {
                "value": {
                    "uri": null,
                    "name": dag_run.conf['timesheet_approval_path']
                }
                },
                "timeoffApprovalPath": {
                "value": {
                    "uri": null,
                    "name": dag_run.conf['timeoff_approval_path']
                }
                },
                "timeZone": {
                "value": {
                    "uri": dag_run.conf['timezone_uri'],
                    "IANAName": null
                }
                },
                "workWeekStartDay": {
                "value": {
                    "uri": dag_run.conf['work_week']
                }
                },
                "formattings": null,
                "notificationPreferences": null,
                "timesheetTemplate": {
                "value": {
                    "uri": dag_run.conf['timesheet_template_uri'],
                    "name": null
                }
                },
                "timeoffTemplate": {
                "value": {
                    "uri": dag_run.conf['timeoff_template_uri'],
                    "name": null
                }
                },
                "holidayCalendar": {
                "value": {
                    "uri": null,
                    "name": dag_run.conf['holiday_calendar']
                }
                },
                "extensionFields": [],
                "customFields": get_udfs('adduser', dag_run),
                "products": [
                    {
                        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                        "items": dag_run.conf['license_uris']
                    }
                ],
                "skills": [],
                "activities": [],
                "policySets":[],
                "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                    {
                        "permissionSetPolicy": {
                        "uri": dag_run.conf['report_user_permission_uri'],
                        "name": null
                        },
                        "groupAccessFilter": null
                    }
                    ]
                }
                ],
                "timeOffTypes": [
                    {
                        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                        "items": get_all_eligible_timeoff_types_add(dag_run,config)
                    }
                    ],
                "locationSchedule": [
                {
                    "dateRange": null,
                    "item": {
                    "uri": dag_run.conf['location_uri'],
                    "parentUri": null,
                    "name": null
                    }
                }
                ],
                "costCenterSchedule": [
                {
                    "dateRange": null,
                    "item": {
                    "uri": dag_run.conf['legal_entity_uri'],
                    "parentUri": null,
                    "name": null
                    }
                }
                ],
                "departmentGroupSchedule": [
                {
                    "dateRange": null,
                    "item": {
                    "uri": dag_run.conf['department_uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                    }
                }
                ],
                "employeeTypeGroupSchedule": [
                {
                    "dateRange": null,
                    "item": {
                    "uri": dag_run.conf['employee_type_uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                    }
                }
                ],
                "timesheetPeriodSchedule": [
                {
                    "dateRange": null,
                    "item": {
                    "uri": null,
                    "name": dag_run.conf['timesheet_period']
                    }
                }
                ],
                "scheduleTypeSchedule": [
                {
                    "dateRange": null,
                    "item": {
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                    "officeSchedule": {
                        "officeScheduleUri": dag_run.conf['office_schedule_uri'],
                        "name": null
                    }
                    }
                }
                ],
                "projectRoleSchedule": [
                {
                    "dateRange": null,
                    "item": {
                    "projectRole": {
                        "uri": dag_run.conf['project_role_uri'],
                        "name": null
                    },
                    "isPrimary": 1
                    }
                }
                ] if dag_run.conf['project_role_uri'] else null,           
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save",
            "unitOfWorkId": str(uuid4())
            }
    if not dag_run.conf['project_role_uri']:
        log.append('Project Role is not available in Replicon')

    rail.set_result(key="exception_logs",val= log)
    return payload_add_user

def get_update_end_date_payload(dag_run):
    """
    Construct payload to update end date and disable user.
    """
    conf = dag_run.conf

    return {
        "target": {
            "uri": conf['user_uri'],
        },
        "modifications": {
            "employmentDateRange": {
                "value": {
                    "startDate": rail.result('get_current_user_info')['userDetails']['employmentDateRange']['startDate'],
                    "endDate": get_replicon_date(conf['end_date'])
            }
        }},
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


# ============================================================================
# Conf Construction for Child DAGs
# ============================================================================

def get_holiday_calendar_name(holiday_calendar_mapper, item):
    if item['location_name'] == "Hyderabad":
        return holiday_calendar_mapper['Hyderabad']
    if item['location_name'] == "Bangalore":
        return holiday_calendar_mapper['Bangalore']
    return null

def get_process_users_conf(item, config_module):
    """
    Construct conf object for process_users child DAG (CRL pattern).
    """
    get_all_permission_sets = rail.result("get_all_permission_sets")
    get_user_udfs = rail.result('get_user_udfs')

    return {
        **item,
        "modulo": int(item.get('record_id', 0)) % config_module.BATCH_COUNT,
        "supervisor_log": rail.result('create_supervisor_log'),

        # User UDF URIs
        "middle_name_def_uri": get_user_udfs.get('middle_name_def_uri') if get_user_udfs else None,

        # Group URIs
        "location_uri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_locations'), 'name', item.get('location_name'), 'uri'),
        "department_uri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_departments'), 'displayText', item.get('department_name'), 'uri'),
        "legal_entity_uri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_legal_entities'), 'name', item.get('legal_entity_name'), 'uri'),

        # Employee Type
        "employee_type_name": config_module.DEFAULT_EMPLOYEE_TYPE,
        "employee_type_uri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_employee_types'), 'name', config_module.DEFAULT_EMPLOYEE_TYPE, 'uri'),

        # Policy Set URIs
        "timesheet_template_name": config_module.TIMESHEET_TEMPLATE,
        "timesheet_template_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), 'displayText', config_module.TIMESHEET_TEMPLATE, "uri"),

        "timesheet_approval_path": config_module.TIMESHEET_APPROVAL_PATH,
        "timeoff_approval_path": rail.result('get_default_timeoff_approval_path')['displayText'],
        "timesheet_period": config_module.TIMESHEET_PERIOD,

        "timeoff_template_name": config_module.TIME_OFF_TEMPLATE,
        "timeoff_template_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), 'displayText', config_module.TIME_OFF_TEMPLATE, "uri"),

        # Holiday Calendar
        "holiday_calendar": get_holiday_calendar_name(config_module.HOLIDAY_CALENDAR, item),
        "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_holiday_calendars'), 
            'displayText', get_holiday_calendar_name(config_module.HOLIDAY_CALENDAR, item), 'uri') 
            if get_holiday_calendar_name(config_module.HOLIDAY_CALENDAR, item) else null,

        # Timezone
        "timezone": config_module.TIMEZONE,
        "timezone_uri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_timezones'), 'displayText', config_module.TIMEZONE, 'uri'),

        # Work week
        "work_week": config_module.WORK_WEEK_START,

        # Office Schedule
        "office_schedule": config_module.OFFICE_SCHEDULE,
        "office_schedule_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_office_schedules"), 'displayText', config_module.OFFICE_SCHEDULE, "uri"),

        # Project Role (Job Title)
        "project_role_name": item.get('job_title'),
        "project_role_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_project_roles"), 'displayText', item.get('job_title'), 'uri'),

        # License URIs
        "license_uris": rail.result('get_all_licenses'),

        # Permission Set URIs
        "supervisor_permission_uri": rail.find_first_by_attr_and_get_attr(
            get_all_permission_sets, 'name', config_module.SUPERVISOR_PERMISSION, 'uri'),
        "default_permission_uri": rail.find_first_by_attr_and_get_attr(
            get_all_permission_sets, 'name', config_module.DEFAULT_PERMISSION, 'uri'),
        "report_user_permission_uri": rail.find_first_by_attr_and_get_attr(
            get_all_permission_sets, 'name', config_module.REPORT_USER_PERMISSION, 'uri'),

        "token": rail.result('extract_keka_token')
    }


def _get_supervisor_details():
    """
    Parse supervisor details from Keka API response.
    SimpleHttpOperator returns JSON string, needs parsing.
    """
    response = rail.result('get_supervisor_details_from_keka')
    if not response:
        return {}
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except (json.JSONDecodeError, TypeError):
            return {}
    # Keka API returns { "data": { ... employee details ... } }
    return response.get('data', {}) if isinstance(response, dict) else {}


def get_process_new_users_conf(dag_run):
    """
    Construct conf object for process_new_users child DAG.
    """
    conf = dag_run.conf
    sup_details = _get_supervisor_details() if conf.get('sup_emp_id') else {}
    return {
        **conf,
        "action": "add",
        "sup_employee_number": sup_details.get('employeeNumber', '') if sup_details else "",
        "sup_email": sup_details.get('email', '') if sup_details else "",
        "sup_display_name": sup_details.get('displayName', '') if sup_details else "",
        "user_log": rail.result('create_user_log')
    }


def get_process_update_users_conf(dag_run):
    """
    Construct conf object for process_update_users child DAG.
    """
    conf = dag_run.conf
    sup_details = _get_supervisor_details() if conf.get('sup_emp_id') else {}
    return {
        **conf,
        "action": "update",
        "sup_employee_number": sup_details.get('employeeNumber', '') if sup_details else "",
        "sup_email": sup_details.get('email', '') if sup_details else "",
        "sup_display_name": sup_details.get('displayName', '') if sup_details else "",
        "user_log": rail.result('create_user_log'),
        'user_uri': rail.result('get_user_data')[0]['uri'],
        'todays_date': (datetime.now()).strftime(DATE_FORMAT)
    }


# ============================================================================
# Log Message Functions
# ============================================================================

def get_add_user_message(dag_run):
    """Generate log message for user creation."""
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
    return "User created successfully"

def get_add_user_severity(dag_run):
    log_supervisor_end_date_in_past = validate_supervisor_end_date() if bool(dag_run.conf['sup_emp_id']) and \
        rail.result('search_supervisor_in_replicon') != [] else False
    exception_logs = rail.result('add_new_user', 'exception_logs')
    if rail.result('log_supervisor_not_present') or log_supervisor_end_date_in_past or exception_logs:
        return 'Exception'
    return 'Success'

def get_update_user_message(dag_run):
    """Generate log message for user update."""
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
    return "User updated successfully"

def get_update_user_severity(dag_run):
    log_supervisor_end_date_in_past = validate_supervisor_end_date() if bool(dag_run.conf['sup_emp_id']) and \
        rail.result('search_supervisor_in_replicon') != [] else False
    exception_logs = rail.result('apply_user_modifications', 'exception_logs')

    if  rail.result('log_supervisor_not_present') or log_supervisor_end_date_in_past or exception_logs:
        return 'Exception'
    return 'Success'

def get_disable_user_message():
    """Generate log message for user disable."""
    return "User disabled successfully"


def get_error_properties(dag_run, action):
    """Generate error properties for logging."""
    conf = dag_run.conf
    return {
        "employee_id": conf.get('emp_id', ''),
        "first_name": conf.get('first_name', ''),
        "last_name": conf.get('last_name', ''),
        "action": action,
        "status": "Error"
    }


# ============================================================================
# Process Users Validation Functions (CRL pattern)
# ============================================================================

def test_valid_fields(dag_run):
    """
    Test if all required fields are valid (CRL pattern).
    Returns True if valid, False otherwise.
    """
    conf = dag_run.conf

    # Check start date is valid
    startdate = get_replicon_date(conf.get('start_date'))
    if not startdate:
        return False

    # Check end date is valid if present
    if conf.get('end_date'):
        enddate = get_replicon_date(conf.get('end_date'))
        if not enddate:
            return False
        # End date should be after start date
        if get_date_from_replicon_date(enddate) < get_date_from_replicon_date(startdate):
            return False

    return True


def get_invalid_fields_message(dag_run):
    """
    Get invalid fields message for logging (CRL pattern).
    """
    conf = dag_run.conf
    messages = []

    startdate = get_replicon_date(conf.get('start_date'))
    if not startdate:
        messages.append("Invalid start date format")

    if conf.get('end_date'):
        enddate = get_replicon_date(conf.get('end_date'))
        if not enddate:
            messages.append("Invalid end date format")
        elif startdate and get_date_from_replicon_date(enddate) < get_date_from_replicon_date(startdate):
            messages.append("End date is before start date")

    return rail.smartjoin_by_delim(messages, ";") if messages else "Unknown validation error"


def validate_enddate_for_old_profile():
    """
    Validate if old profile has end date set (for re-hire scenario).
    """
    return bool(rail.result('get_user_data_based_on_login_name')[0]['userDetails']["employmentDateRange"]['endDate'])


def update_old_profile_login_name(dag_run):
    """
    Generate payload to update old profile's SSO identifier (for re-hire).
    """
    def get_end_date_for_oldprofile():
        end_date = rail.result('get_user_data_based_on_login_name')[0]['userDetails']["employmentDateRange"]['endDate']
        return get_date_from_replicon_date(end_date).strftime("%d%m%Y")
    return {
        'userUri': rail.result('get_user_data_based_on_login_name')[0]['userDetails']['uri'],
        'loginName': str(dag_run.conf['login_name'])+"."+ get_end_date_for_oldprofile()
    }

def set_user_notification_preferences_payload():
    return {
        "user": {
            "uri": rail.result('add_new_user')['uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "preferences": {
            "notificationDeliveryPreferences": [
            {
                "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:project",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:time-off",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:user",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:timesheet",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:holiday",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            }
            ],
            "sharedDeliveryPreferenceOptionUris": [
            "urn:replicon:user-shared-delivery-preference-option:always-deliver"
            ]
        }
        }

def can_update_timesheet_approval_path(dag_run):
    current_timesheet_approval_path = rail.result("get_current_user_info")['timesheetApprovalPath']
    if not current_timesheet_approval_path and dag_run.conf['timesheet_approval_path']:
        return True
    if dag_run.conf['timesheet_approval_path'] and dag_run.conf['timesheet_approval_path']!= \
        current_timesheet_approval_path['displayText']:
        return True
    return False
def can_update_timeoff_approval_path(dag_run):
    current_timeoff_approval_path = rail.result("get_current_user_info")['timeOffApprovalPath']
    if not current_timeoff_approval_path and dag_run.conf['timeoff_approval_path']:
        return True
    if dag_run.conf['timeoff_approval_path'] and dag_run.conf['timeoff_approval_path']!= \
        current_timeoff_approval_path['displayText']:
        return True
    return False

def can_update_location_grp(location_uri, current_location_uri):
    return bool(current_location_uri != location_uri)

def can_update_cost_center_grp(cost_center_uri, current_cost_center_uri):
    return bool(cost_center_uri != current_cost_center_uri)

def can_update_department_grp(department_uri, current_department_uri):
    return bool(department_uri != current_department_uri)


def apply_user_modifications_payload(dag_run, config):
    """
    Construct payload for creating a new user
    """
    user_details = rail.result("get_current_user_info")['userDetails']
    assigned_timezone = rail.result('get_current_user_info')['timeZone']
    assigned_workweek = rail.result('get_current_user_info')['userDetails']['workWeekStartDay']
    assigned_timesheet_template = rail.result("get_current_user_info")['timesheetTemplate']
    assigned_timeoff_template = rail.result("get_current_user_info")['timeOffTemplate']
    assigned_holiday_calendar = rail.result("get_current_user_info")['holidayCalendar']
    assigned_timesheet_period = rail.result("get_current_user_info")['timesheetPeriodSchedule']
    assigned_schedule = rail.result("get_current_user_info")['schedulePolicies']
    assigned_project_role = rail.result('get_user_project_role')
    log=[]

    payload_add_user = {
            "target":  {
                "uri": null,
                "loginName": null,
                "employeeId": dag_run.conf['emp_id'],
                "parameterCorrelationId": null
            },
            "template": null,
            "modifications": {
                "firstName": {
                "value": dag_run.conf['first_name']
                } if dag_run.conf['first_name'] != user_details['firstName']else null,
                "lastName": {
                "value": dag_run.conf['last_name']
                }if user_details['lastName'] != dag_run.conf['last_name'] else null,
                "loginName": {
                "value": dag_run.conf['login_name']
                }if user_details['emailAddress'] != dag_run.conf['email'] else null,
                "displayName": {
                "value": dag_run.conf['display_name']
                }if user_details['customDisplayName'] != dag_run.conf['display_name'] else null,
                "emailAddress": {
                "value": dag_run.conf['email']
                }if user_details['emailAddress'] != dag_run.conf['email'] else null,
                "employeeId": null,
                "employmentDateRange": null,
                "securitySettings": {
                "value": {
                    "loginEnabled": null,
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": dag_run.conf["email"]
                    },
                    "ssoNameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name",
                    "password": null,
                    "authenticationProviders": [],
                    "emailMFAResendVerificationEmail": null,
                    "emailMFATryAddMethodFromUsersEmail": null,
                    "isMFAMethodRequired": null,
                    "clearIsLockedOut": null
                }
            } if user_details['emailAddress'] != dag_run.conf['email'] else null,
                "timesheetApprovalPath": {
                "value": {
                    "uri": null,
                    "name": dag_run.conf['timesheet_approval_path']
                }
                } if can_update_timesheet_approval_path(dag_run) else null,
                "timeoffApprovalPath": {
                "value": {
                    "uri": null,
                    "name": dag_run.conf['timeoff_approval_path']
                }
                }if can_update_timeoff_approval_path(dag_run) else null,
                "timeZone": {
                "value": {
                    "uri": dag_run.conf['timezone_uri'],
                    "IANAName": null
                }
                }if not assigned_timezone or 
                (dag_run.conf['timezone'] != rail.result('get_current_user_info')['timeZone']['displayText']) else null,
                "workWeekStartDay": {
                "value": {
                    "uri": dag_run.conf['work_week']
                }
                } if not assigned_workweek or
                (dag_run.conf['work_week'] != rail.result('get_current_user_info')['userDetails']['workWeekStartDay']['uri']) else null,
                "timesheetTemplate": {
                "value": {
                    "uri": dag_run.conf['timesheet_template_uri'],
                    "name": null
                }
                } if not assigned_timesheet_template or
                (dag_run.conf['timesheet_template_uri'] != assigned_timesheet_template['uri']) else null,
                "timeoffTemplate": {
                "value": {
                    "uri": dag_run.conf['timeoff_template_uri'],
                    "name": null
                }
                } if not assigned_timeoff_template or
                (dag_run.conf['timeoff_template_uri'] != assigned_timeoff_template['uri']) else null,
                "holidayCalendar": {
                "value": {
                    "uri": null,
                    "name": dag_run.conf['holiday_calendar']
                }
                }if not assigned_holiday_calendar or
                (assigned_holiday_calendar['displayText'] != dag_run.conf['holiday_calendar']) else null,
                "customFields": get_udfs('updateuser', dag_run),
                "skills": [],
                "activities": [],
                "policySets":[],
                "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                    {
                        "permissionSetPolicy": {
                        "uri": dag_run.conf['report_user_permission_uri'],
                        "name": null
                        },
                        "groupAccessFilter": null
                    }
                    ]
                }
                ],
                "timeOffTypes": [
                    {
                        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                        "items": get_all_eligible_timeoff_types_update(config)
                    }
                    ] if get_all_eligible_timeoff_types_update(config) else [],
                "locationSchedule": [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(dag_run.conf['todays_date'])
                    },
                    "item": {
                    "uri": dag_run.conf['location_uri'],
                    "parentUri": null,
                    "name": null
                    }
                }
                ] if can_update_location_grp(dag_run.conf['location_uri'],
                rail.result('get_effective_user_groupmembership','location').get('uri', '')) else [],
                "costCenterSchedule": [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(dag_run.conf['todays_date'])
                    },
                    "item": {
                    "uri": dag_run.conf['legal_entity_uri'],
                    "parentUri": null,
                    "name": null
                    }
                }
                ]if can_update_cost_center_grp(dag_run.conf['legal_entity_uri'],
                rail.result('get_effective_user_groupmembership', 'costcenter').get('uri', '')) else [],
                "departmentGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(dag_run.conf['todays_date'])
                    },
                    "item": {
                    "uri": dag_run.conf['department_uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                    }
                }
                ]if can_update_department_grp(dag_run.conf['department_uri'],
                rail.result('get_effective_user_groupmembership', 'department').get('uri', '')) else [],
                "timesheetPeriodSchedule": [
                {
                    "dateRange": {
                        "startDate":get_replicon_date(dag_run.conf['todays_date'])
                    }
                    if assigned_timesheet_period else null,
                    "item": {
                    "uri": null,
                    "name": dag_run.conf['timesheet_period']
                    }
                }
                ] if not assigned_timesheet_period or 
                assigned_timesheet_period[-1]['timesheetPeriod']['displayText']!= dag_run.conf['timesheet_period'] else [],
                "scheduleTypeSchedule": [
                {
                    "dateRange": null if not assigned_schedule else
                    {
                        "startDate":get_replicon_date(dag_run.conf['todays_date'])
                    },
                    "item": {
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                    "officeSchedule": {
                        "officeScheduleUri": dag_run.conf['office_schedule_uri'],
                        "name": null
                    }
                    }
                }
                ] if not assigned_schedule or
                (dag_run.conf['office_schedule'] !=
                    assigned_schedule[-1]['officeSchedule']['displayText']) else [],
                "projectRoleSchedule": [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(dag_run.conf['todays_date'])
                    },
                    "item": {
                    "projectRole": {
                        "uri": dag_run.conf['project_role_uri'],
                        "name": null
                    },
                    "isPrimary": 1
                    }
                }
                ] if dag_run.conf['project_role_uri'] and (not assigned_project_role or
                dag_run.conf['project_role_name']!= assigned_project_role) else [],           
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save",
            "unitOfWorkId": str(uuid4())
            }
    if not dag_run.conf['project_role_uri']:
        log.append('Project Role is not available in Replicon')

    rail.set_result(key="exception_logs",val= log)
    return payload_add_user

def get_all_eligible_timeoff_types_update(config):
    eligible_timeoff_types = config.APPLICABLE_TIME_OFF_TYPES
    assigned_timeoff_types = rail.result('get_user_time_off_policy_summary')
    timeoff_list = []
    for item in eligible_timeoff_types:
        if item not in assigned_timeoff_types:
            timeoff_list.append(
            {
                "timeOffType": {
                "uri": null,
                "name": item
                },
                "isTimeOffAllowedAgainstThisTimeOffType": 1,
                "applyDefaultTimeOffTypePolicy": 0,
                "defaultTimeOffTypePolicyEffectiveDate": null,
                "policySchedule": []
            }
            )
    return timeoff_list

def get_all_licenses_to_remove(dag_run):
    license_uris = dag_run.conf['license_uris']
    if not license_uris:
        return []
    payload=[]
    for uri in license_uris:
        payload.append({
            "uri":uri
        })
    return payload

def validate_supervisor_end_date():
    return datetime.now().date() > (date_parser(rail.result('search_supervisor_in_replicon')['end_date'])).date()\
        if rail.result('search_supervisor_in_replicon')['end_date'] else False

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

def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
        rail.result('search_supervisor_in_replicon')['uri'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['uri']:
        return False
    return True
