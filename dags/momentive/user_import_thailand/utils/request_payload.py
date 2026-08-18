# pylint: disable=line-too-long
import json
from datetime import datetime
from pendulum import now
from airflow.models import Variable
import rail
from momentive.user_import_thailand import config
from momentive.user_import_thailand.config import time_zone
from momentive.user_import_thailand.utils import python_callable

null = None

# Raw user-row field names (the workdayuserdata collection columns). The master
# passes these verbatim to process_each_user, which forwards them to the children.
USER_FIELDS = (
    'User_ID', 'Worker_Reference_Employee_ID', 'Email_Address', 'First_Name', 'Last_Name',
    'Worker_Type', 'Effective_Date_of_Worker_Type', 'Exemption_Status', 'Exemption_Eff_Date',
    'Gender', 'Hire_Date', 'Termination_Date', 'Active', 'Function',
    'Function_Change_Effective_Date', 'Business_Title', 'CF_LRV_Business_Title_Change_Eff_Date',
    'Field_HR', 'Manager_ID', 'Effective_Date_of_Manager_Change', 'Work_Shift',
    'Work_Shift_Change_Effective_Date', 'Location', 'CF_LRV_Location_Change_Effective_Date',
    'Country', 'CF_Date_of_Birth_MM_DD_YYYY', 'CF_LRV_Manager_Email', 'CF_LRV_Manager_First_Name',
    'CF_LRV_Manager_Last_Name', 'Legal_entity', 'Worker_subType', 'Cost_center',
    'Worker_cc_change_date', 'Year_of_service', 'Paygroup',
)


def _user(field, dag_run):
    """Read a field of the user row being processed.

    process_each_user receives the row in its own dag_run.conf (same field names as
    the workdayuserdata collection), so every child conf is built from there.
    """
    return dag_run.conf.get(field)


def process_each_user_payload(item):
    """Conf passed from the master to one process_each_user DAG run.

    Carries the raw user row plus the values the per-user DAG cannot resolve itself:
    the department-group URI (looked up once in the master from the enabled-departments
    prefetch), the master's ecid as `parentjobid` (every log entry written downstream
    must carry it so the master's `search_log_entries` jobid filter picks it up), and
    the two log handles the children write into.
    """
    departments = rail.result('getall_enabled_departments_28')
    conf = {field: item.get(field) for field in USER_FIELDS}
    conf.update({
        "parentjobid": rail.render_template("{{ dag_run_ecid() }}"),
        "departmentgroupuri": rail.find_first_by_attr_and_get_attr(
            departments, 'departmentgroupname', item.get('Location'), 'departmentgroupuri') if departments else null,
        "user_import_logs": rail.result('create_log_momentive_user_import_log'),
        "supervisor_assignment_logs": rail.result('create_log_momentive_supervisor_assignment'),
    })
    return conf



def get_user_by_search_payload(text_search_term):
    """UserListService GetData payload to search Replicon for an existing user by login name."""
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:end-date",
            "urn:replicon:user-list-column:start-date",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:employee-type"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": text_search_term
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def conf_payload(action, dag_run):
    """Build the conf passed from process_each_user to a child DAG.

    Thailand passes raw text values (the child recipes resolve URIs themselves),
    unlike Japan which resolved permission-set / custom-field / division URIs in
    the master. Key names mirror the Workato child recipe input parameters so the
    child DAGs can read them directly:
        - 'add'                -> Thailand_Momentive_User Sync Add        (flow 1145870)
        - 'update' / 'rehire'  -> Thailand_Momentive_User Sync Update     (flow 1145874)
        - 'disable' / 'disablewithenddate' -> Child Workflow to disable user (flow 1145871)

    `parentjobid`, `departmentgroupuri` and both log handles are forwarded from the
    conf the master handed to process_each_user, so children keep logging against
    the master's ecid and log handles exactly as before.
    """
    conf = {
        "parentjobid": dag_run.conf['parentjobid'],
        "User_ID": _user('User_ID', dag_run),
        "Worker_Reference_Employee_ID": _user('Worker_Reference_Employee_ID', dag_run),
        "Email_Address": _user('Email_Address', dag_run),
        "First_Name": _user('First_Name', dag_run),
        "Last_Name": _user('Last_Name', dag_run),
        "Worker_Type": _user('Worker_Type', dag_run),
        "Effective_Date_of_Worker_Type": _user('Effective_Date_of_Worker_Type', dag_run) or null,
        "Exemption_Status": _user('Exemption_Status', dag_run),
        "Exemption_Eff_Date": _user('Exemption_Eff_Date', dag_run) or null,
        "Gender": _user('Gender', dag_run),
        "Hire_Date": _user('Hire_Date', dag_run) or null,
        "Termination_Date": _user('Termination_Date', dag_run) or null,
        "Active": _user('Active', dag_run),
        "Function": _user('Function', dag_run),
        "Function_Change_Effective_Date": _user('Function_Change_Effective_Date', dag_run) or null,
        "Business_Title": _user('Business_Title', dag_run) or null,
        "CF_LRV_Business_Title_Change_Eff_Date": _user('CF_LRV_Business_Title_Change_Eff_Date', dag_run) or null,
        "Field_HR": _user('Field_HR', dag_run),
        "Manager_ID": _user('Manager_ID', dag_run),
        "Effective_Date_of_Manager_Change": _user('Effective_Date_of_Manager_Change', dag_run) or null,
        "Work_Shift": _user('Work_Shift', dag_run),
        "Work_Shift_Change_Effective_Date": _user('Work_Shift_Change_Effective_Date', dag_run) or null,
        "Location": _user('Location', dag_run),
        "CF_LRV_Location_Change_Effective_Date": _user('CF_LRV_Location_Change_Effective_Date', dag_run) or null,
        "Country": _user('Country', dag_run),
        "CF_Date_of_Birth_MM_DD_YYYY": _user('CF_Date_of_Birth_MM_DD_YYYY', dag_run) or null,
        "CF_LRV_Manager_Email": _user('CF_LRV_Manager_Email', dag_run),
        "CF_LRV_Manager_First_Name": _user('CF_LRV_Manager_First_Name', dag_run),
        "CF_LRV_Manager_Last_Name": _user('CF_LRV_Manager_Last_Name', dag_run),
        "departmentgroupuri": dag_run.conf['departmentgroupuri'],
        "user_import_logs": dag_run.conf['user_import_logs'],
        "supervisor_assignment_logs": dag_run.conf['supervisor_assignment_logs'],
    }

    if action == 'add':
        # Thailand_Momentive_User Sync Add input parameter names.
        conf.update({
            "legalentity": _user('Legal_entity', dag_run),
            "worker_sub_type": _user('Worker_subType', dag_run) or null,
            "costcenter": _user('Cost_center', dag_run),
            "eff_date_costcenter": _user('Worker_cc_change_date', dag_run),
            "paygroup": _user('Paygroup', dag_run),
            "Years_of_service": _user('Year_of_service', dag_run),
        })
    elif action in ('update', 'rehire'):
        # Thailand_Momentive_User Sync Update input parameter names.
        conf.update({
            "useruri": rail.result('log_ifuserexistsuseruri_36')['useruri'],
            "rehire_update": "rehire" if action == 'rehire' else "update",
            "legal_entity": _user('Legal_entity', dag_run),
            "worker_sub_type": _user('Worker_subType', dag_run) or null,
            "cost_center": _user('Cost_center', dag_run),
            "eff_cost_center": _user('Worker_cc_change_date', dag_run),
            "Year_of_service": _user('Year_of_service', dag_run),
            "paygroup": _user('Paygroup', dag_run),
        })
    elif action in ('disable', 'disablewithenddate'):
        # Child Workflow to disable user. Termination_Date is already in the base conf.
        conf.update({
            "useruri": rail.result('log_ifuserexistsuseruri_36')['useruri'],
        })

    return conf


def search_user_by_empid_payload(employee_id):
    """UserListService GetData payload to find a user (supervisor) by employee id,
    returning the employee-id, login-name and enabled columns.
    """
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {"text": employee_id},
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def enabled_list_getdata_payload(list_name, column_name):
    """GetData payload that returns effectively-enabled rows for a *ListService
    (division / department-group / service-center / cost-center), with the
    group, enabled flag and full path columns. Used by the add/update children
    to resolve a group name to its URI.
    """
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            f"urn:replicon:{list_name}-list-column:{column_name}",
            f"urn:replicon:{list_name}-list-column:effectively-enabled",
            f"urn:replicon:{list_name}-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": f"urn:replicon:{list_name}-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {"uri": null, "uris": [], "bool": "true", "date": null, "money": null, "number": null, "text": null, "time": null, "calendarDayDurationValue": null, "workdayDurationValue": null, "dateRange": null, "dateTimeUtc": null},
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def enabled_list_rows_handler(response):
    """Turn a *ListService GetData response into [{uri, name, fullpath}, ...]."""
    return [{
        "uri": row["cells"][0]["uri"],
        "name": row["cells"][0]["textValue"] or null,
        "fullpath": " / ".join([cell["textValue"] for cell in row["cells"][2]["cellCollection"]]) if row["cells"][2].get("cellCollection") else row["cells"][2].get("textValue", "")
    } for row in response['rows']]


def final_policyset_schedule_entry(dag_run):
    """Build the policySetScheduleEntries for PutUserTimeOffAccountPolicySetSchedule.

    Appends a new schedule entry effective on the termination date that sets the
    remaining balance (balance_amount dag-run variable) via the "Starting Balance
    Set To" script, after the user's past schedule entries.
    Mirrors the put-remaining-balance recipe step [37].
    """
    final_entries = rail.result("get_past_policysetschedule_entries")
    # terminationdate arrives as "d/m/yyyy"; the recipe sends effectiveDate as integers
    # (matching the past schedule entries returned by Replicon), so convert here.
    end_date_string_split = {
        'day': int(dag_run.conf['terminationdate'].split("/")[0]),
        'month': int(dag_run.conf['terminationdate'].split("/")[1]),
        'year': int(dag_run.conf['terminationdate'].split("/")[2])
    }
    final_entries.append({
        "effectiveDate": end_date_string_split,
        "description": "Effective on " + str(end_date_string_split['month']) + "/" + str(end_date_string_split['day']) + "/" + str(end_date_string_split['year']),
        "policySet": {
            "timeOffBalanceEventScripts": [
                {
                    "scriptTarget": {
                        "uri": dag_run.conf['startingbalancesettouri'],
                        "slug": null,
                        "name": null
                    },
                    "additionalParameters": [
                        {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": str(rail.get_dag_run_var('balance_amount')),
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        },
                        {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": "20",
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        }
                    ]
                }
            ],
            "timeOffValidationScripts": []
        }
    })

    return final_entries


# --------------------------------------------------------------------------- #
# update_user_child_dag — request bodies (data=) and child conf (conf=)
# --------------------------------------------------------------------------- #

def getdata_sup_emp_grp_dept_grp(dag_run):
    """UserListService GetData for the user: login-name, employee-type, supervisor
    columns (cells[2] is the current supervisor, used for the null check)."""
    return {
        "page": "1", "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-type",
            "urn:replicon:user-list-column:supervisor"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {"filterDefinitionUri": "urn:replicon:user-list-filter:text"},
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {"value": {"text": dag_run.conf['User_ID']}, "filterDefinitionUri": null},
            "value": null, "filterDefinitionUri": null
        }
    }


def search_supervisor_payload(dag_run):
    """UserListService GetData to find the supervisor by manager employee id."""
    return search_user_by_empid_payload(dag_run.conf['Manager_ID'])


def rehire_employment_daterange_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "dateRange": {"startDate": python_callable.split_date_string(dag_run.conf['Hire_Date'], 'datetime')}
    }


def termination_employment_daterange_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "dateRange": {
            "startDate": python_callable.split_date_string(dag_run.conf['Hire_Date'], 'datetime'),
            "endDate": python_callable.split_date_string(dag_run.conf['Termination_Date'], 'datetime')
        }
    }


def update_dob_payload(dag_run):
    return {
        "objectUri": dag_run.conf['useruri'],
        "customFieldUri": rail.result('get_user_udf_values')['dob_uri'],
        "value": python_callable.split_date_string(dag_run.conf['CF_Date_of_Birth_MM_DD_YYYY'], 'int')
    }


def supervisor_permissionsets_payload():
    return {"userUri": rail.result('log_supervisor_details')['useruri']}


def assign_supervisor_permission_payload():
    return {
        "userUri": rail.result('log_supervisor_details')['useruri'],
        "permissionSetUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'), 'displayText', 'Supervisor - Edit', 'uri', '')
    }


def put_initial_supervisor_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "initialSupervisorUri": rail.result('log_supervisor_details')['useruri'],
        "scheduleEntries": []
    }


def update_supervisor_daterange_payload(dag_run):
    eff = dag_run.conf['Effective_Date_of_Manager_Change'] if dag_run.conf.get('Effective_Date_of_Manager_Change') else str(now(tz=time_zone).date())
    return {
        "userUri": dag_run.conf['useruri'],
        "supervisorUri": rail.result('log_supervisor_details')['useruri'],
        "dateRange": {"startDate": python_callable.split_date_string(eff, 'datetime')}
    }


def put_employee_type_group_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": [{"employeeTypeGroup": {"uri": rail.result('get_all_employee_type_groups'), "parent": null, "name": null, "parameterCorrelationId": null}, "effectiveDate": null}]
    }


def put_shift_schedule_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": [{"schedulePolicy": {"officeScheduleUri": null, "name": null, "officeSchedule": null, "scheduleTypeUri": "urn:replicon:schedule-type:shift"}, "effectiveDate": null}]
    }


def put_office_schedule_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": [{"schedulePolicy": {"officeScheduleUri": rail.result('get_all_office_schedules'), "name": null, "officeSchedule": null, "scheduleTypeUri": null}, "effectiveDate": null}]
    }


def put_payrule_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": [{"payRuleScript": {"uri": rail.result('get_all_payrule_scripts'), "name": null}, "effectiveDate": null}]
    }


def assign_activities_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "activityUris": [rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_activities'), 'name', rail.result('log_activity_name'), 'uri', '')]
    }


def trigger_update_user_timeoff(dag_run):
    """conf for the Update User - Time Off child (grandchild of the master)."""
    return {
        "parentjobid": dag_run.conf['parentjobid'],
        "user_import_logs": dag_run.conf['user_import_logs'],
        "useruri": dag_run.conf['useruri'],
        "User_ID": dag_run.conf['User_ID'],
        "Worker_Reference_Employee_ID": dag_run.conf['Worker_Reference_Employee_ID'],
        "First_Name": dag_run.conf['First_Name'],
        "Last_Name": dag_run.conf['Last_Name'],
        "Worker_Type": dag_run.conf['Worker_Type'],
        "Exemption_Status": dag_run.conf['Exemption_Status'],
        "Gender": dag_run.conf['Gender'],
        "Hire_Date": dag_run.conf['Hire_Date'],
        "Termination_Date": dag_run.conf['Termination_Date'],
        "Active": dag_run.conf['Active'],
        "Location": dag_run.conf['Location'],
        "Work_Shift": dag_run.conf['Work_Shift'],
        "Work_Shift_Change_Effective_Date": dag_run.conf['Work_Shift_Change_Effective_Date'],
        "rehire": dag_run.conf['rehire_update'],
        "exmp_status_derivedvalue": rail.result('compute_mapper_keys')['exemptstatus'],
        "timeofftypes": rail.result('log_timeoff_types'),
    }


def put_service_center_schedule_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": [{"serviceCenter": {"uri": rail.result('search_service_center'), "parent": null, "name": null, "parameterCorrelationId": null}, "effectiveDate": null}]
    }


def put_cost_center_schedule_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": [{"costCenter": {"uri": rail.result('search_cost_center'), "parent": null, "name": null, "parameterCorrelationId": null}, "effectiveDate": null}]
    }


def put_division_schedule_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": [{"division": {"uri": rail.result('search_division'), "parent": null, "name": null, "parameterCorrelationId": null}, "effectiveDate": null}]
    }


def put_department_group_schedule_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": [{"departmentGroup": {"uri": rail.result('search_department_group'), "parent": null, "name": null, "parameterCorrelationId": null}, "effectiveDate": null}]
    }


def department_access_scope_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "policyDataAccessScopes": [{
            "policyUri": "urn:replicon:policy:time-off",
            "locations": [], "divisions": [], "costCenters": [], "serviceCenters": [],
            "departmentGroups": [{"departmentGroup": {"uri": rail.result('search_department_group'), "parentUri": null, "name": null}, "groupSpecificationModeUri": null, "groupDescendantModeUri": null}],
            "employeeTypeGroups": []
        }]
    }


def current_supervisor_details_payload(dag_run):
    """BulkGetUsers3 on the current supervisor URI (to read their employeeId for the #108 gate)."""
    return {
        "users": [{"uri": rail.result('log_current_supervisor_uri')}],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def clear_activities_payload(dag_run):
    """Recipe #475: clear activity assignments when no activity is mapped."""
    return {
        "userUri": dag_run.conf['useruri'],
        "activityUris": []
    }


def schedule_policy_payload(dag_run):
    """Recipe #457/#466: PutSchedulePolicyScheduleForUser, office-schedule branch."""
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": rail.result('build_schedule_entries')
    }


def shift_schedule_policy_payload(dag_run):
    """Recipe #460/#471: PutSchedulePolicyScheduleForUser, Shift branch. Same rebuilt
    entries, read from the shift branch's own build task."""
    return {
        "userUri": dag_run.conf['useruri'],
        "scheduleEntries": rail.result('build_shift_schedule_entries')
    }


def _costcenter_eff_date(dag_run):
    eff = dag_run.conf.get('eff_cost_center')
    base = eff if eff else str(now(tz=time_zone).date())
    return python_callable.split_date_string(base, 'datetime')


def update_costcenter_modifications_payload(dag_run):
    """Recipe #271: ApplyUserModifications2 update-over-date-range, cost center by name.

    Cost center is the only group the recipe updates through ApplyUserModifications2
    (its PutCostCenterScheduleForUser path, blocks 274-310, is skip:true). Member names
    are resource-specific, exactly as authored in block 271. The entries value is sent
    as a one-element array: block 271's `input.schema` declares
    `replacementCostCenterScheduleEntries` as `type: array, of: object` (the authored
    literal omits the brackets and Workato coerces it), and that is also the shape
    Japan/India send in production.
    """
    return {
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "user": {"uri": dag_run.conf['useruri']},
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {"name": dag_run.conf['cost_center']},
                            "effectiveDate": _costcenter_eff_date(dag_run)
                        }
                    ]
                }
            }
        }
    }


# --------------------------------------------------------------------------- #
# update_user_child_dag (Japan-structured port) — request bodies (data=) and
# child conf (conf=). Conf keys are capitalized; group/UDF URIs resolved in-DAG.
# --------------------------------------------------------------------------- #

def get_data_sup_emp_grp_dept_grp(dag_run):
    """UserListService GetData for the user's department-group, employee-type-group,
    supervisor columns (cells[2] = supervisor), filtered by the user URI."""
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:department-group",
            "urn:replicon:user-list-column:employee-type-group",
            "urn:replicon:user-list-column:supervisor"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null, "operatorUri": null, "rightExpression": null,
                "value": null, "filterDefinitionUri": "urn:replicon:user-list-filter:user"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null, "operatorUri": null, "rightExpression": null,
                "value": {
                    "uri": dag_run.conf['useruri'], "uris": [], "bool": null, "date": null,
                    "money": null, "number": null, "text": null, "time": null,
                    "calendarDayDurationValue": null, "workdayDurationValue": null,
                    "dateRange": null, "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null, "filterDefinitionUri": null
        }
    }


def get_manager_details_payload():
    """BulkGetUsers3 on the supervisor matched by employee id. Reads search_for_user_with_empid[0]['uri']."""
    return {
        "users": [{"uri": rail.result('search_for_user_with_empid')[0]['uri']}],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def add_missing_supervisor_permission_payload_2(dag_run):
    """Assign the Supervisor permission set to the matched user. The permission-set
    URI is resolved in-DAG from GetAllPermissionSets (Thailand does not pass it in conf).
    """
    return {
        'userUri': rail.result('search_for_user_with_empid')[0]['uri'],
        'permissionSetUri': rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_permission_sets'), 'displayText', 'Supervisor - Edit', 'uri', '')
    }


def search_supervisor_for_assignment_payload(dag_run):
    """UserListService GetData to find the supervisor by login id (supervisorloginname).
    Used by the supervisor-assignment leaf DAG. Recipe step 8.
    """
    return search_user_by_empid_payload(dag_run.conf['supervisorloginname'])


def search_supervisor_by_email_payload(dag_run):
    return search_user_by_empid_payload(dag_run.conf['sup_email'])


def add_supervisor_permission_conf_payload(dag_run):
    """AssignPermissionSetToUser for the matched supervisor. The 'Supervisor - Edit'
    permission-set URI is resolved once by the master and passed in conf. Recipe step 21.
    """
    return {
        'userUri': rail.result('search_for_user_with_empid')[0]['uri'],
        'permissionSetUri': dag_run.conf['supervisor']
    }


def create_supervisor_payload(dag_run):
    """PutUser3 to create a foreign supervisor who does not yet exist in Replicon.
    Recipe step 32: SSO login from sup_email, employee id from supervisorloginname,
    employment start from the supervisor-change effective date, supervision permission
    set ('Supervisor - Edit' URI passed in conf by the master), placed in the
    'Momentive' department group and 'Foreign Supervisors' employee-type group.
    """
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['sup_email'],
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['sup_firstname'],
            "lastname": dag_run.conf['sup_lastname'],
            "emailAddress": dag_run.conf['sup_email'],
            "employeeId": dag_run.conf['supervisorloginname'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
            "employmentDateRange": {
                "startDate": rail.result('get_split_dates')['sup_eff_date'],
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": True,
                "loginName": dag_run.conf['sup_email'],
                "SSOName": dag_run.conf['sup_email'],
                # doc 04/06: credentials are never stored in source -- read at runtime
                # from the Airflow Variable named by config.default_user_password_variable.
                "password": Variable.get(config.default_user_password_variable)
            },
            "holidayCalendar": null,
            "holidayCalendarAssignmentSchedule": null,
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": dag_run.conf['supervisor'],
                    "name": null
                }
            ],
            "policySets": [],
            "policySetsSchedule": [],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": null,
            "expenseApprovalPath": null,
            "expenseDefaultReimbursementCurrency": null,
            "timeOffApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [],
            "divisionSchedule": [],
            "costCenterSchedule": [],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": null,
                        "parent": null,
                        "name": "Momentive",
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": null,
                        "parent": null,
                        "name": "Foreign Supervisors",
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "timesheetPeriodSchedule": [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": [],
            "workCompliancePolicyAssignmentSchedule": []
        }
    }


def supervisor_assignment_log_payload(dag_run):
    """Deferred supervisor-assignment log row the master fans out by parentjobid."""
    return {
        "parentjobid": dag_run.conf['parentjobid'],
        "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
        "loginid": dag_run.conf['User_ID'],
        "supervisorempid": dag_run.conf['Manager_ID'],
        "useruri": dag_run.conf['useruri'],
        "type": "update",
        "sup_email": dag_run.conf['CF_LRV_Manager_Email'] or '',
        "sup_firstname": dag_run.conf['CF_LRV_Manager_First_Name'] or '',
        "sup_lastname": dag_run.conf['CF_LRV_Manager_Last_Name'] or '',
        "sup_change_effective_date": dag_run.conf['Effective_Date_of_Manager_Change']
        if dag_run.conf.get('Effective_Date_of_Manager_Change')
        else str(now(tz=time_zone).date()),
    }


def update_employeetypegrp_payload(dag_run):
    """ApplyUserModifications2 — update employee-type-group over date range.
    URI from get_all_employee_type_details; effective date from get_startdate_of_next_timesheet.
    """
    return {
        "user": {"uri": dag_run.conf['useruri'], "loginName": null, "parameterCorrelationId": null},
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": rail.result('get_all_employee_type_details'),
                                "parent": null, "name": null, "parameterCorrelationId": null
                            },
                            "effectiveDate": python_callable.split_date_string(rail.result('get_startdate_of_next_timesheet'))
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def _grouplist_getdata_payload(list_name, column_name, text_value):
    """GetData payload for a *ListService filtered by a text search on the group name."""
    return {
        "page": "1", "pagesize": "100000",
        "columnUris": [
            f"urn:replicon:{list_name}-list-column:{column_name}",
            f"urn:replicon:{list_name}-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null, "operatorUri": null, "rightExpression": null,
                "value": null, "filterDefinitionUri": f"urn:replicon:{list_name}-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": null, "operatorUri": null, "rightExpression": null,
                "value": {
                    "uri": null, "uris": [], "bool": null, "date": null, "money": null,
                    "number": null, "text": text_value, "time": null,
                    "calendarDayDurationValue": null, "workdayDurationValue": null,
                    "dateRange": null, "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null, "filterDefinitionUri": null
        }
    }


def get_servicecenter_group_data_payload(dag_run):
    """GetData service-center list filtered by the incoming paygroup name."""
    return _grouplist_getdata_payload('service-center', 'service-center', dag_run.conf['paygroup'])


def get_costcenter_group_data_payload(dag_run):
    """GetData cost-center list filtered by the incoming cost-center name."""
    return _grouplist_getdata_payload('cost-center', 'cost-center', dag_run.conf['cost_center'])


def get_division_group_data_payload(dag_run):
    """GetData division list filtered by the incoming legal-entity name."""
    return _grouplist_getdata_payload('division', 'division', dag_run.conf['legal_entity'])


def _apply_schedule_over_daterange(dag_run, resource, group_uri, effective_date):
    """ApplyUserModifications2 envelope for an update-schedule-over-date-range group change.

    `resource` is the PascalCase resource name ('ServiceCenter', 'Division',
    'DepartmentGroup'); every member name is derived from it.

    ApplyUserModifications2 uses RESOURCE-SPECIFIC member names inside each
    `<resource>ScheduleToApply` wrapper -- e.g. `updateDivisionScheduleOverDateRange` /
    `replacementDivisionScheduleEntries`. The generic `updateScheduleOverDateRange` /
    `replacementScheduleEntries` pair is only correct for `schedulePolicyToApply`, where
    the resource itself is the schedule; sending it for a group resource leaves the
    modification unrecognised. Shape matches Japan/India production payloads.
    """
    member = resource[0].lower() + resource[1:]
    return {
        "user": {"uri": dag_run.conf['useruri'], "loginName": null, "parameterCorrelationId": null},
        "modifications": {
            f"{member}ScheduleToApply": {
                f"user{resource}ScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                f"replacement{resource}Schedule": [],
                f"update{resource}ScheduleOverDateRange": {
                    f"replacement{resource}ScheduleEntries": [
                        {
                            member: {"uri": group_uri, "parent": null, "name": null, "parameterCorrelationId": null},
                            "effectiveDate": effective_date
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_service_center_payload(dag_run):
    """ApplyUserModifications2 — update service-center over date range, effective today.
    Service-center URI from log_req_servicecenter_uri (resolved by name in-DAG).
    """
    return _apply_schedule_over_daterange(
        dag_run,
        "ServiceCenter",
        rail.result('log_req_servicecenter_uri'),
        python_callable.split_date_string(str(now(tz=time_zone).date())))


def update_division_group_payload(dag_run):
    """ApplyUserModifications2 — update division over date range, effective today.
    Division URI from log_req_legalentity_division_uri (resolved by name in-DAG).
    """
    return _apply_schedule_over_daterange(
        dag_run,
        "Division",
        rail.result('log_req_legalentity_division_uri'),
        python_callable.split_date_string(str(now(tz=time_zone).date())))


def update_department_group_payload(dag_run):
    """ApplyUserModifications2 — update department-group over date range.
    Department-group URI from conf['departmentgroupuri']; effective date from log_location_change_eff_date.
    """
    return _apply_schedule_over_daterange(
        dag_run,
        "DepartmentGroup",
        dag_run.conf['departmentgroupuri'],
        rail.result('log_location_change_eff_date'))


def dict_to_datetime(dict_date):
    """Convert {day, month, year} to a datetime."""
    return datetime(day=dict_date['day'], month=dict_date['month'], year=dict_date['year'])


def get_current_value_from_schedule_list_for_user(user_schedule, scrpit_name, required_key):
    """Current active value from a schedule list by smallest non-negative day-diff
    (today minus effectiveDate), falling back to the null-dated initial entry.
    """
    current_value = null
    initial_value = null
    current_min_day_diff = "*"

    if 'urn' in json.dumps(user_schedule):
        for item in user_schedule:
            if not item['effectiveDate']:
                initial_value = item
                continue
            daydiff = (now().date()) - dict_to_datetime(item['effectiveDate']).date()
            if daydiff.days < 0:
                continue
            if current_min_day_diff == "*":
                current_value = item
                current_min_day_diff = daydiff
                continue
            if current_min_day_diff > daydiff:
                current_min_day_diff = daydiff
                current_value = item

    return current_value[scrpit_name][required_key] if current_value else (
        initial_value[scrpit_name][required_key] if initial_value else '')


def get_current_schedule_policy_from_list(schedule_policies):
    """Current active schedule policy by smallest non-negative day-diff, with a
    null-dated base-policy fallback. Returns the entry dict + 'daydiff', or {}.
    """
    if not schedule_policies or 'urn' not in json.dumps(schedule_policies):
        return {}
    current_policy = None
    min_daydiff = None
    for policy in schedule_policies:
        if 'uri' not in policy or not policy.get('uri'):
            continue
        if policy.get('effectiveDate'):
            policy_date = dict_to_datetime(policy['effectiveDate']).date()
        else:
            continue
        daydiff = (now().date() - policy_date).days
        if daydiff < 0:
            continue
        if min_daydiff is None or daydiff < min_daydiff:
            min_daydiff = daydiff
            current_policy = policy
    if not current_policy:
        for policy in schedule_policies:
            if 'uri' in policy and not policy.get('effectiveDate'):
                current_policy = policy
                break
    if current_policy:
        result = current_policy.copy()
        result['daydiff'] = min_daydiff if min_daydiff is not None else 0
        return result
    return {}


def _full_modifications_skeleton():
    """All ApplyUserModifications2 'modifications' keys defaulted to null/[]."""
    return {
        "timezoneToApply": null, "workWeekStartToApply": null, "holidayCalendarToApply": null,
        "holidayCalendarAssignmentsToApply": null, "schedulePolicyToApply": null,
        "locationScheduleToApply": null, "divisionScheduleToApply": null,
        "costCenterScheduleToApply": null, "departmentGroupScheduleToApply": null,
        "employeeTypeGroupScheduleToApply": null, "timesheetPeriodScheduleToApply": null,
        "serviceCenterScheduleToApply": null, "totalBusinessCostScheduleToApply": null,
        "permissionSetsToApply": null, "policySetsToApply": null, "policySetsScheduleToApply": [],
        "policyDataAccessScopesToApply": null, "policyDataAccessScopesToApply2": null,
        "notificationPreferencesToApply": null, "timesheetPeriodTypeToApply": null,
        "timesheetApprovalPathToApply": null, "timeEntryRevisionGroupApprovalPathToApply": null,
        "validationRuleToApply": null, "activitiesToApply": [], "activitiesToApply2": null,
        "defaultActivityToApply": null, "defaultActivityToApply2": null,
        "defaultTimeOffTypeForBookingsToApply": null, "expenseApprovalPathToApply": null,
        "expenseDefaultReimbursementCurrencyToApply": null, "timeOffApprovalPathToApply": null,
        "productAssignmentsToApply": null, "timeBankPolicyToApply": null,
        "securitySettingsToApply": null, "supervisorsToApply": null, "supervisorsModifications": null,
        "payrollRatesToApply": null, "payrollRatesModifications": null,
        "overtimeRulesToApply": null, "overtimeRulesModifications": null,
        "customFieldValuesToApply": [], "departmentToApply": null, "employeeTypeToApply": null,
        "userDetailsToApply": null, "payRulesToApply": null, "payRulesScheduleModifications": null,
        "payRatesModifications": null, "placeAssignmentsModifications": null,
        "resourceAllocationAfterUserEndDateOptionUri": null, "projectRolesToApply": null,
        "projectRoleAssignmentSchedulesToApply": null, "decimalSeparatorToApply": null,
        "numberGroupSeparatorToApply": null, "dateFormatToApply": null, "clockFormatToApply": null,
        "hoursFormatToApply": null, "timeZoneFormatToApply": null, "objectExtensionFieldsToApply": [],
        "costRateScheduleModifications": null, "workAuthorizationApprovalPathToApply": null,
        "displayNameFormatSettingsToApply": null, "timePunchTimeZoneDisplayOptionToApply": null,
        "defaultTimesheetToDisplayOptionToApply": null, "reportSettingsToApply": null,
        "timeOffBalancePayoutApprovalPathToApply": null,
        "workCompliancePolicyAssignmentScheduleToApply": null, "userConsentModificationsToApply": null
    }


def update_payrule_for_user_payload(dag_run):
    """ApplyUserModifications2 — set the pay-rule script schedule.
    Pay-rule URI from get_req_payrule_script; effective date from get_startdate_of_next_timesheet.
    """
    modifications = _full_modifications_skeleton()
    modifications["payRulesScheduleModifications"] = {
        "scheduleEntries": [
            {
                "payRuleScript": {"uri": rail.result('get_req_payrule_script'), "name": null},
                "effectiveDate": python_callable.split_date_string(rail.result('get_startdate_of_next_timesheet'))
            }
        ]
    }
    return {
        "user": {"uri": dag_run.conf['useruri'], "loginName": null, "employeeId": null, "parameterCorrelationId": null},
        "modifications": modifications,
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }




def trigger_updateuser_timeoff(dag_run):
    """conf for the Thailand update-timeoff child (grandchild of the master).
    Japan-only continuous-service / timeoff-service-date fields are dropped.
    """
    strt = rail.result('get_user_data_14')[0]['userDetails']['employmentDateRange']['startDate']
    return {
        "parentjobid": dag_run.conf['parentjobid'],
        "user_import_logs": dag_run.conf['user_import_logs'],
        "User_ID": dag_run.conf['User_ID'],
        "Hire_Date": dag_run.conf['Hire_Date'],
        "Termination_Date": dag_run.conf['Termination_Date'],
        "Active": dag_run.conf['Active'],
        "rehire": dag_run.conf['rehire_update'],
        "timeofftypes": rail.result('log_timeofftypes_tobeassigned'),
        "old_startdate": f"{strt['year']}-{strt['month']}-{strt['day']}",
        "useruri": dag_run.conf['useruri'],
        "Work_Shift_Change_Effective_Date": dag_run.conf['Work_Shift_Change_Effective_Date'],
    }


# --------------------------------------------------------------------------- #
# update_user_timeoff_assign_dag — request bodies (data=) and entry builders
# --------------------------------------------------------------------------- #

def assign_timeofftypes_payload(dag_run):
    """Recipe [31]: replace the user's time-off type assignments with the incoming set.
    URI list resolved by final_list_of_timeoff_uris (displayText -> uri).
    """
    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": rail.result('final_list_of_timeoff_uris')
    }


def get_assigned_timeofftypes_payload(dag_run):
    """Recipe [14]: current time-off type assignments for the user."""
    return {"userUris": [dag_run.conf['useruri']]}


def _past_policyset_entries(assigned_policy_schedule):
    """Recipe [48]-[55]: keep schedule entries whose effectiveDate (M/D/Y) is before
    today, applying the null -> "effective" / "script" -> "scriptTarget" transform.
    """
    entries = assigned_policy_schedule or []
    today = now(tz=time_zone).date()
    kept = []
    for item in entries:
        eff = item.get('effectiveDate') or {}
        if not eff.get('day'):
            continue
        eff_date = datetime(year=eff['year'], month=eff['month'], day=eff['day']).date()
        if eff_date < today:
            kept.append(item)
    return json.loads(json.dumps(kept).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))


def build_annual_leave_policy_entries(dag_run):
    """Recipe [48]-[71]: rebuild the policy-set schedule entries for an annual-leave type.

    Keeps the user's past schedule entries (effectiveDate before today, with the
    null -> "effective" / "script" -> "scriptTarget" transform) from the assigned
    policy summary, then appends one new entry effective on the hire date (rehire)
    or today (update), carrying the default policy set for the type. The type URI for
    the current loop iteration is resolved in-DAG via log_annual_leave_enabled_uri.
    """
    type_uri = rail.result('log_annual_leave_enabled_uri')
    summary = rail.result('get_user_timeoff_policy_summary') or {}
    assigned_schedule = rail.find_first_by_attr_and_get_attr(
        summary.get('policiesByTimeOffType') or [], 'timeOffType.uri', type_uri, 'policySetSchedule', []) or []
    past_entries = _past_policyset_entries(assigned_schedule)

    # GetDefaultTimeOffPolicySetScheduleForTimeOffType returns the list directly
    # (RAIL unwraps the REST 'd' envelope); recipe takes response.d.pluck('policySet').first.
    default_list = rail.result('get_default_policyset_for_type') or []
    default_policyset = default_list[0].get('policySet') if default_list else None
    if default_policyset is not None:
        default_policyset = json.loads(json.dumps(default_policyset).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))

    eff_source = dag_run.conf['Hire_Date'] if dag_run.conf.get('rehire') == 'rehire' else str(now(tz=time_zone).date())
    eff_split = python_callable.split_date_string(eff_source, 'datetime')

    new_entry = {
        "effectiveDate": eff_split,
        "description": "Effective from " + str(eff_split['month']) + "/" + str(eff_split['day']) + "/" + str(eff_split['year']),
        "policySet": default_policyset
    }

    return past_entries + [new_entry]


def put_annual_leave_policy_schedule_payload(dag_run):
    """Recipe [76]/[78]: write the rebuilt policy-set schedule for the annual-leave type."""
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('log_annual_leave_enabled_uri')
        },
        "policySetScheduleEntries": rail.result('build_annual_leave_policy_entries')
    }


def get_user_timeoff_policy_summary_payload(dag_run):
    """Recipe [45]: the user's time-off policy summary (per type policy schedules)."""
    return {"userUri": dag_run.conf['useruri']}


def get_default_policyset_for_type_payload(dag_run):
    """Recipe [61]: default policy-set schedule for the current annual-leave type URI."""
    return {"timeOffTypeUri": rail.result('log_annual_leave_enabled_uri')}
