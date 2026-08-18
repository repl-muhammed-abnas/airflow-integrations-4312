from airflow.models import Variable
import rail

from capefoxcorporation.timesheet_sync.utils.custom_methods import (
    is_data_reversed,
    is_project_mo,
    is_project_allocation_type,
    get_total_hours,
    get_formatted_date,
    get_reg_hours,
    get_other_hours
)
from capefoxcorporation.timesheet_sync.utils.response_filters import (
    get_pay_type,
    get_project_id,
    get_plc_code,
    get_task_codes,
    get_line_date,
    get_comments,
    get_mo_details,
    get_mo_project_id,
    get_task_uri,
    get_financial_year,
    get_period_number,
    get_timesheet_date,
    get_timesheet_header_seq,
    get_time_off_project_id,
    get_time_off_pay_type,
    is_timeoff_allocated_entry
)

# Alias for JSON compatibility in API payloads
null = None


def get_reversing_record(costpoint_timesheet, config):
    """Build a reversing record for an existing timesheet."""
    if costpoint_timesheet:
        for row in costpoint_timesheet['document']['rows']:
            if row['row']['data']['S_TS_TYPE_CD'] == 'R' \
                    and not is_data_reversed(row, costpoint_timesheet['document']['rows']):
                return change_to_reversed(row, config)
    return None


def change_to_reversed(row, config):
    """Transform a row into a reversed (correcting) record."""
    return {
        "document": {
            "id": "replicon_imp_ldmtime",
            "rows": [
                {
                    "row": {
                        "rsId": "LDMTIME_TSHDR",
                        "tranType": "INSERT",
                        "data": {
                            "EMPL_ID": row['row']['data']['EMPL_ID'],
                            "FY_CD": row['row']['data']['FY_CD'],
                            "OTH_HRS": -1.0 * row['row']['data']["OTH_HRS"],
                            "PD_NO": row['row']['data']['PD_NO'],
                            "REG_HRS": -1.0 * row['row']['data']["REG_HRS"],
                            "SUB_PD_NO": row['row']['data']['SUB_PD_NO'],
                            "S_TS_TYPE_CD": 'C',
                            "REFERENCE_SEQ_NO": row['row']['data']['TS_HDR_SEQ_NO'],
                            "REFERENCE_TS_TYPE_CD": "R",
                            "TH___CORRECTING_REF_DT": row['row']['data']['TS_DT'],
                            "TH___AUTO_ADJ_PCT_RT": row['row']['data']['TH___AUTO_ADJ_PCT_RT'],
                            "TH___S_JNL_CD": "LD",
                            "TS_DT": row['row']['data']['TS_DT'],
                            "TS_HDR_SEQ_NO": row['row']['data']['TS_HDR_SEQ_NO']
                        },
                        "children": get_revert_children(row['row']['children'], config)
                    }
                }
            ]
        }
    }


def get_revert_children(children, config):
    """Build children records for a revert operation."""
    revert_children = []
    for child in children:
        if child['row']['rsId'] == 'LDMTIME_TSLN':
            if child['row']['data']['TS_LN___S_TS_LN_TYPE_CD'] == config.mo_line_type:
                revert_children.append(
                    {
                        "row": {
                            "rsId": "LDMTIME_TSLN",
                            "tranType": "INSERT",
                            "data": get_project_data(child),
                            "children": [
                                {
                                    "row": {
                                        "rsId": "LDMTIME_TSLNMO",
                                        "tranType": "INSERT",
                                        "data": {
                                            "MO_ID": child['row']['children'][0]['row']['data']['MO_ID'],
                                            "MO_OPER_SEQ_NO": child['row']['children'][0]['row']['data']['MO_OPER_SEQ_NO'],
                                            "MO_OPER_STEP_NO": child['row']['children'][0]['row']['data']['MO_OPER_STEP_NO'],
                                            "S_ACTIVITY_TYPE": child['row']['children'][0]['row']['data']['S_ACTIVITY_TYPE'],
                                            "WC_ID": child['row']['children'][0]['row']['data']['WC_ID']
                                        }
                                    }
                                }
                            ]
                        }
                    })
            else:
                revert_children.append(
                    {
                        "row": {
                            "rsId": "LDMTIME_TSLN",
                            "tranType": "INSERT",
                            "data": get_project_data(child)
                        }
                    })
    return revert_children


def get_project_data(child):
    """Build project data for a child record with negated hours."""
    data = child['row']['data']
    return {
        "ACCT_ID": data.get('ACCT_ID', ''),
        "BILL_LAB_CAT_CD": data.get('BILL_LAB_CAT_CD'),
        "GENL_LAB_CAT_CD": data.get('GENL_LAB_CAT_CD'),
        "ORG_ID": data.get('ORG_ID', ''),
        "PAY_TYPE": data['PAY_TYPE'],
        "PROJ_ID": data['PROJ_ID'],
        "TS_LN___CHG_HRS": -1.0 * data['TS_LN___CHG_HRS'],
        "TS_LN___LAB_LOC_CD": data.get('TS_LN___LAB_LOC_CD'),
        "TS_LN___S_TS_LN_TYPE_CD": data['TS_LN___S_TS_LN_TYPE_CD'],
        "TS_LN___WORK_COMP_CD": data.get('TS_LN___WORK_COMP_CD')
    }


def get_children(entries, task_details, pay_codes, account_details, projects, oef_tags, config,
                 time_offs=None, time_off_type_details=None, employee_id=None):
    """Build children records for timesheet line items.

    Args:
        time_offs: Optional list of timeoff bookings from Replicon
        time_off_type_details: Optional list of timeoff type details for mapping PAY_TYPE and PROJ_ID

    Design Decision (grouping key with missing task details):
    When grouping entries by project, entries with missing task details may result in
    empty project_id or plc values. Rather than skip these entries, we let them flow
    through to Costpoint. Costpoint will return validation errors (e.g., "PROJ_ID does
    not exist") which are logged via get_log_properties. This approach ensures:
    - All time entries are attempted (no silent data loss)
    - Errors are visible in logs for troubleshooting
    - Behavior is consistent with deltek_costpoint/timesheet_sync
    """
    child_rows = []
    project_entries = {}
    if Variable.get(config.group_by_project_var_name, default_var='false') == '1':
        for entry in entries:
            # Skip timeoff-allocated entries (handled via timeoff bookings API)
            if is_timeoff_allocated_entry(entry, pay_codes, oef_tags, config):
                continue
            if is_project_allocation_type(entry):
                project_id, plc = get_task_codes(entry, task_details)
                pay_type = get_pay_type(entry, pay_codes, oef_tags, config)
                # Key may have empty values if task details are missing - see docstring
                key = project_id + "_" + plc + "_" + pay_type

                if key in project_entries:
                    project_entries[key].append(entry)
                else:
                    project_entries[key] = [entry]
        for key, project_entry in project_entries.items():
            mo_project = is_project_mo(
                project_entry[0], task_details, projects, config)
            if mo_project is True:
                ch_rows = get_grouped_project_timeentries(
                    task_details, pay_codes, account_details, projects, oef_tags, project_entry, config)
                child_rows.append(ch_rows)
            else:
                child_rows.append({
                    "row": {
                        "rsId": "LDMTIME_TSLN",
                        "tranType": "INSERT",
                        "data": {
                            "ACCT_ID": "",
                            "BILL_LAB_CAT_CD": get_plc_code(project_entry[0], task_details),
                            "ORG_ID": "",
                            "PAY_TYPE": get_pay_type(project_entry[0], pay_codes, oef_tags, config),
                            "PROJ_ID": get_project_id(project_entry[0], task_details),
                            "TS_LN___CHG_HRS": get_total_hours(project_entry, pay_codes),
                            "TS_LN___S_TS_LN_TYPE_CD": get_line_type_code_value(project_entry[0], config)
                        }
                    }
                })
    else:
        for entry in entries:
            # Skip timeoff-allocated entries (handled via timeoff bookings API)
            if is_timeoff_allocated_entry(entry, pay_codes, oef_tags, config):
                continue
            if is_project_allocation_type(entry):
                if get_total_hours([entry], pay_codes) != 0:
                    mo_project = is_project_mo(
                        entry, task_details, projects, config)
                    if mo_project is True:
                        get_timesheet_mo_line_item(
                            child_rows, entry, account_details, pay_codes, oef_tags, task_details, projects, config)
                    else:
                        child_rows.append(
                            {
                                "row": {
                                    "rsId": "LDMTIME_TSLN",
                                    "tranType": "INSERT",
                                    "data": {
                                        "ACCT_ID": "",
                                        "BILL_LAB_CAT_CD": get_plc_code(entry, task_details),
                                        "ORG_ID": "",
                                        "PAY_TYPE": get_pay_type(entry, pay_codes, oef_tags, config),
                                        "PROJ_ID": get_project_id(entry, task_details),
                                        "TS_LN_DT": get_line_date(entry),
                                        "TS_LN___CHG_HRS": get_total_hours([entry], pay_codes),
                                        "TS_LN___S_TS_LN_TYPE_CD": get_line_type_code_value(entry, config),
                                        "TS_LN___NOTES": get_comments(entry)
                                    }
                                }
                            }
                        )

    # Add timeoff entries if timeoff sync is enabled
    # Cape Fox uses employee ID prefix to route sick leave to the correct Costpoint leave project
    if time_offs:
        for time_off in time_offs:
            for entry in time_off.get('entries', []):
                child_rows.append(
                    {
                        "row": {
                            "rsId": "LDMTIME_TSLN",
                            "tranType": "INSERT",
                            "data": {
                                "ACCT_ID": "",
                                "BILL_LAB_CAT_CD": "",
                                "ORG_ID": "",
                                "PAY_TYPE": get_time_off_pay_type(employee_id, config),
                                "PROJ_ID": get_time_off_project_id(employee_id, config),
                                "TS_LN_DT": get_line_date(entry),
                                "TS_LN___CHG_HRS": get_timeoff_duration_hours(entry),
                                "TS_LN___S_TS_LN_TYPE_CD": config.line_type,
                                "TS_LN___NOTES": time_off.get('comments', '')[0:254] if time_off.get('comments') else ''
                            }
                        }
                    }
                )

    return child_rows


def get_line_type_code_value(entry, config):
    """Get line type code value from config."""
    return config.line_type if entry else None


def get_timeoff_duration_hours(entry):
    """Get duration hours from a timeoff entry."""
    duration = entry.get('duration', {})
    hours = duration.get('hours', 0) or 0
    minutes = duration.get('minutes', 0) or 0
    seconds = duration.get('seconds', 0) or 0
    return hours + (minutes / 60.0) + (seconds / 3600.0)


def get_grouped_project_timeentries(task_details, pay_codes, account_details, projects, oef_tags, project_entry, config):
    """Build grouped project time entries for MO projects."""
    mo_info = get_mo_details(task_details, project_entry[0], config)
    return {
        "row": {
            "rsId": "LDMTIME_TSLN",
            "tranType": "INSERT",
            "data": {
                "ACCT_ID": "",
                "BILL_LAB_CAT_CD": get_plc_code(project_entry[0], task_details),
                "ORG_ID": "",
                "PAY_TYPE": get_pay_type(project_entry[0], pay_codes, oef_tags, config),
                "PROJ_ID": get_mo_project_id(projects, task_details, project_entry[0], config),
                "TS_LN___CHG_HRS": get_total_hours(project_entry, pay_codes),
                "TS_LN___S_TS_LN_TYPE_CD": get_line_type_code_value(project_entry[0], config)
            }
        },
        "children": [
            {
                "row": {
                    "rsId": "LDMTIME_TSLNMO",
                    "tranType": "INSERT",
                    "data": {
                        "MO_ID": mo_info['mo_id'] if mo_info else "",
                        "MO_OPER_SEQ_NO": mo_info['seq'] if mo_info else "",
                        "MO_OPER_STEP_NO": mo_info['step'] if mo_info else "",
                        "S_ACTIVITY_TYPE": mo_info['activity_type'] if mo_info else ""
                    }
                }
            }
        ]
    }


def get_timesheet_mo_line_item(child_rows, entry, account_details, pay_codes, oef_tags, task_details, projects, config):
    """Build MO line item for timesheet and append to child_rows."""
    mo_info = get_mo_details(task_details, entry, config)
    mo_line_item = {
        "row": {
            "rsId": "LDMTIME_TSLN",
            "tranType": "INSERT",
            "data": {
                "ACCT_ID": "",
                "BILL_LAB_CAT_CD": "",
                "ORG_ID": "",
                "PAY_TYPE": get_pay_type(entry, pay_codes, oef_tags, config),
                "PROJ_ID": get_mo_project_id(projects, task_details, entry, config),
                "TS_LN_DT": get_line_date(entry),
                "TS_LN___CHG_HRS": get_total_hours([entry], pay_codes),
                "TS_LN___S_TS_LN_TYPE_CD": config.mo_line_type,
                "TS_LN___NOTES": get_comments(entry)
            },
            "children": [
                {
                    "row": {
                        "rsId": "LDMTIME_TSLNMO",
                        "tranType": "INSERT",
                        "data": {
                            "MO_ID": mo_info['mo_id'] if mo_info else "",
                            "MO_OPER_SEQ_NO": mo_info['seq'] if mo_info else "",
                            "MO_OPER_STEP_NO": mo_info['step'] if mo_info else "",
                            "S_ACTIVITY_TYPE": mo_info['activity_type'] if mo_info else ""
                        }
                    }
                }
            ]
        }
    }

    child_rows.append(mo_line_item)


def get_existing_timesheet_filter_payload():
    """Build filter payload to query existing timesheets from Costpoint."""
    return {
        "filter": {
            "id": "replicon_exp_ldmtime",
            "where": [
                {
                    "rsWhere": {
                        "rsId": "LDMTIME_TSHDR",
                        "conditions": [
                            {
                                "joinWithParent": "N",
                                "relations": [
                                    {
                                        "name": "EMPL_ID",
                                        "relation": "=",
                                        "value": rail.result('get_replicon_user_details')[0]['userDetails']['employeeId']
                                    },
                                    {
                                        "name": "TS_DT",
                                        "relation": "=",
                                        "value": get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate'])
                                    }
                                ]
                            }
                        ],
                        "children": [
                            {
                                "rsWhere": {
                                    "rsId": "LDMTIME_TSLN",
                                    "conditions": [],
                                    "children": []
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }


def get_timesheet_import_payload(config):
    """Build payload to import timesheet to Costpoint."""
    # Get timeoff data (may be None if timeoff sync is disabled)
    time_offs = rail.result('get_replicon_timeoffs')
    time_off_type_details = rail.result('get_replicon_time_off_type_details')
    employee_id = rail.result('get_replicon_user_details')[0]['userDetails']['employeeId']

    return {
        "document": {
            "id": "replicon_imp_ldmtime",
            "rows": [
                {
                    "row": {
                        "rsId": "LDMTIME_TSHDR",
                        "tranType": "INSERT",
                        "data": {
                            "EMPL_ID": employee_id,
                            "FY_CD": get_financial_year(rail.result('get_replicon_timesheet')),
                            "OTH_HRS": get_other_hours(rail.result('get_replicon_time_entries'),
                                                       rail.result('get_replicon_pay_codes'),
                                                       rail.result('get_oef_tag_details'),
                                                       time_offs),
                            "PD_NO": get_period_number(rail.result('get_replicon_timesheet')),
                            "REG_HRS": get_reg_hours(rail.result('get_replicon_time_entries'),
                                                     rail.result('get_replicon_pay_codes'),
                                                     rail.result('get_oef_tag_details'),
                                                     time_offs),
                            "SUB_PD_NO": 1,
                            "S_TS_TYPE_CD": "R",
                            "TH___AUTO_ADJ_PCT_RT": 1,
                            "TH___S_JNL_CD": "LD",
                            "TS_DT": get_timesheet_date(rail.result('get_replicon_timesheet')),
                            "TS_HDR_SEQ_NO": get_timesheet_header_seq(rail.result('get_existing_deltek_timesheet')[0])
                        },
                        "children": get_children(
                            rail.result('get_replicon_time_entries'),
                            rail.result('get_replicon_task_details'),
                            rail.result('get_replicon_pay_codes'),
                            rail.result('get_replicon_account_details'),
                            rail.result('get_replicon_project_details'),
                            rail.result('get_oef_tag_details'),
                            config,
                            time_offs,
                            time_off_type_details,
                            employee_id
                        )
                    }
                }
            ]
        }
    }


def get_action_type():
    """Determine if this is an Add or Update action based on existing timesheet."""
    existing_timesheet = rail.result('get_existing_deltek_timesheet')
    if existing_timesheet and existing_timesheet[0] and existing_timesheet[0].get('document'):
        rows = existing_timesheet[0].get('document', {}).get('rows', [])
        if len(rows) > 0:
            return "Update"
    return "Add"


def is_export_successful():
    """Check if the export to Costpoint was successful (severity < 3)."""
    costpoint_response = rail.result('push_time_to_costpoint')
    if costpoint_response and len(costpoint_response) > 0:
        severity = costpoint_response[0].get('MethodResponse', {}).get('Severity', 3)
        return severity < 3
    return False


def get_log_severity():
    """Get log severity based on export result."""
    return "Success" if is_export_successful() else "Error"


def get_log_properties():
    """Build properties for log entry based on export result."""
    timesheet = rail.result('get_replicon_timesheet')
    user_details = rail.result('get_replicon_user_details')
    costpoint_response = rail.result('push_time_to_costpoint')

    employee_id = None
    login_name = None
    timesheet_date = None

    if user_details and len(user_details) > 0:
        employee_id = user_details[0].get('userDetails', {}).get('employeeId')

    if timesheet:
        login_name = timesheet.get('owner', {}).get('loginName')
        start = timesheet.get('dateRange', {}).get('startDate', {})
        end = timesheet.get('dateRange', {}).get('endDate', {})
        if start and end:
            timesheet_date = f"{start.get('month')}/{start.get('day')}/{start.get('year')} - {end.get('month')}/{end.get('day')}/{end.get('year')}"

    if is_export_successful():
        status = "Success"
        details = "Timesheet synced successfully"
    else:
        status = "Error"
        details = "Export failed"
        if costpoint_response and len(costpoint_response) > 0:
            method_response = costpoint_response[0].get('MethodResponse', {})
            # Try Messages (plural/array) first, then fallback to Message (singular/string)
            messages = method_response.get('Messages', [])
            if messages:
                error_messages = [msg.get('MsgText', '') for msg in messages if msg.get('MsgText')]
                if error_messages:
                    details = '; '.join(error_messages)
            elif method_response.get('Message'):
                details = method_response.get('Message')

    return {
        "employee_id": employee_id,
        "login_name": login_name,
        "timesheet_date": timesheet_date,
        "action": get_action_type(),
        "status": status,
        "details": details,
    }


def get_error_log_properties():
    """Build properties for error log entry (task failure)."""
    timesheet = rail.result('get_replicon_timesheet')
    user_details = rail.result('get_replicon_user_details')

    employee_id = None
    login_name = None
    timesheet_date = None

    if user_details and len(user_details) > 0:
        employee_id = user_details[0].get('userDetails', {}).get('employeeId')

    if timesheet:
        login_name = timesheet.get('owner', {}).get('loginName')
        start = timesheet.get('dateRange', {}).get('startDate', {})
        end = timesheet.get('dateRange', {}).get('endDate', {})
        if start and end:
            timesheet_date = f"{start.get('month')}/{start.get('day')}/{start.get('year')} - {end.get('month')}/{end.get('day')}/{end.get('year')}"

    return {
        "employee_id": employee_id,
        "login_name": login_name,
        "timesheet_date": timesheet_date,
        "action": get_action_type(),
        "status": "Error",
        "details": rail.render_template("{{ get_error_message() }}"),
    }
