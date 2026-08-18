from datetime import datetime as dt
import functools
import json
import rail
from rail.lib.artifact import existing_artifact, new_artifact
from eisner_amper.time_export_s4hc.utils.custom_methods import (
    safe_parse_date,
    build_lookup_dict,
    extract_service_line,
    extract_location_code,
    build_mapper_lookup
)
from eisner_amper.time_export_s4hc.mappers.location_mapper import location_code_map
from eisner_amper.time_export_s4hc.mappers.service_line_mapper import service_line_map
null = None

def get_slug():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_timesheet_report_details")["uri"],
                "filterValues": [
                    {
                        "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                            rail.result('get_timesheet_report_details')['filterConfiguration']['enabledFilters'], 
                            'displayText', 'TimeEntryApprovalDateFilter', 'uri'
                        ),
                        "value": null
                    },
                    {
                        "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                            rail.result('get_timesheet_report_details')['filterConfiguration']['enabledFilters'], 
                            'displayText', 'TimeEntryApprovalDateFilter', 'uri'
                        ),
                        "value": rail.result('get_logging_details')['filter_date']
                    },
                    {
                        "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                            rail.result('get_timesheet_report_details')['filterConfiguration']['enabledFilters'], 
                            'displayText', 'TimeEntryApprovalDateFilter', 'uri'
                        ),
                        "value": rail.result('get_logging_details')['filter_date']
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_user_details():
    return {
        "users": [
            {
                "loginName": "radmin"
            }
        ]
    }

def update_user_timezone():
    return {
        "userUri": rail.result('get_user_details')['uri'],
        "timeZoneUri": "urn:replicon:time-zone:america-new-york"
    }


@functools.lru_cache(maxsize=1)
def load_artifact_data(artifact_name):
    return rail.load_all_records(rail.result(artifact_name))

# Global cache for cost center lookup - built once, used many times
_cost_center_lookup_cache = None

def _build_cost_center_lookup():
    global _cost_center_lookup_cache

    if _cost_center_lookup_cache is not None:
        return _cost_center_lookup_cache

    cost_data = load_artifact_data('create_cost_data_collection')
    lookup = {}

    if not cost_data:
        _cost_center_lookup_cache = lookup
        return lookup

    # Build lookup: {useruri: {'cost_centers': [(date, code)], 'roles': [(date, role)]}}
    for cost_record in cost_data:
        user_uri = cost_record.get('useruri', '')
        if not user_uri:
            continue

        if user_uri not in lookup:
            lookup[user_uri] = {'cost_centers': [], 'roles': []}

        # Parse and store cost center records
        cc_eff_date_str = cost_record.get('Cost_Center_Effective_Date') or cost_record.get('Cost Center Effective Date', '')
        cc_code = cost_record.get('Cost_Center_Code') or cost_record.get('Cost Center Code', '')
        if cc_eff_date_str and cc_code:
            try:
                cc_eff_date = dt.strptime(cc_eff_date_str, '%b %d, %Y')
                lookup[user_uri]['cost_centers'].append((cc_eff_date, cc_code))
            except:
                pass

        # Parse and store roles records
        roles_eff_date_str = cost_record.get('Roles_Effective_Date') or cost_record.get('Roles Effective Date', '')
        role = cost_record.get('Roles', '')
        if roles_eff_date_str and role:
            try:
                roles_eff_date = dt.strptime(roles_eff_date_str, '%b %d, %Y')
                lookup[user_uri]['roles'].append((roles_eff_date, role))
            except:
                pass

    # Sort all records by effective date (ascending) - do this once
    for user_uri in lookup:
        lookup[user_uri]['cost_centers'].sort(key=lambda x: x[0])
        lookup[user_uri]['roles'].sort(key=lambda x: x[0])

    _cost_center_lookup_cache = lookup
    return lookup


def _lookup_cost_center_and_role(user_uri, entry_date, lookup):
    cost_center_code = ''
    roles = ''

    if not user_uri or not entry_date or user_uri not in lookup:
        return cost_center_code, roles

    user_data = lookup[user_uri]

    # Find most recent cost center where effective_date <= entry_date
    # Records are already sorted, so iterate backwards for efficiency
    for eff_date, cc_code in reversed(user_data['cost_centers']):
        if eff_date <= entry_date:
            cost_center_code = cc_code
            break

    # Find most recent role where effective_date <= entry_date
    for eff_date, role in reversed(user_data['roles']):
        if eff_date <= entry_date:
            roles = role
            break

    return cost_center_code, roles

def get_formated_row(item):
    if not item:
        return []

    employee_id = item.get("Employee_ID", '')
    company_code = item.get("Company_Code_Code__Current_", '')
    submitted_on = safe_parse_date(item.get("Entry_Date", ''))
    entry_date_str = item.get("Entry_Date", '')
    sap_employee_id = item.get('SAP_Employee_ID', '')
    project_profile = item.get("Project_Profile", '')
    task_level1_code = item.get("Work_Package_Code", '')
    work_package_work_item_code = item.get("Work_Item_Code", '')
    comments = item.get("Comments", '')
    hours_worked = item.get("Hours", '0.00')
    location = item.get("Receiver_Cost_Center_Code", '')
    time_entry_code = item.get("Time___Expense_Entry_Type", '')
    timesheet_period_uri = item.get("Entry_ID", '')
    work_location = item.get("Work_Location", '')
    user_uri = item.get("User_uri", '')

    # Build cost center lookup cache once (lazy initialization)
    cost_center_lookup = _build_cost_center_lookup()

    # Parse entry date for comparison
    try:
        entry_date = dt.strptime(entry_date_str, '%b %d, %Y')
    except:
        entry_date = None

    # Fast O(k) lookup for cost center and role
    cost_center_code, roles = _lookup_cost_center_and_role(user_uri, entry_date, cost_center_lookup)

    service_line = extract_service_line(cost_center_code, 4, 7)
    
    try:
        hours_float = float(hours_worked) if hours_worked else 0.0
        process = "Yes" if hours_float > 0 else "No"
    except (ValueError, TypeError):
        process = "No"

    return [
        employee_id,
        company_code,
        submitted_on,
        sap_employee_id,
        project_profile,
        task_level1_code,
        work_package_work_item_code,
        comments,
        hours_worked,
        location,
        time_entry_code,
        timesheet_period_uri,
        cost_center_code,
        roles,
        service_line,
        work_location,
        user_uri,
        process
    ]

def get_enriched_intermediate_csv_row(item, config):
    if not item:
        return []

    # Get lookup data from Replicon API responses
    division_details = rail.result('get_bulk_division_details')
    work_location_tags = rail.result('get_object_extension_tag_definition_details')

    # Build lookup dictionaries from Replicon data
    division_lookup = build_lookup_dict(division_details, 'name', 'description')
    worklocation_lookup = build_lookup_dict(work_location_tags, 'name', 'code')

    # Build mapper lookups from static data (matching Workato EA_Location_mapper and EA_Service_Mapper)
    location_lookup = build_mapper_lookup(location_code_map, 'locationcode', 'lc_code')
    serviceline_lookup = build_mapper_lookup(service_line_map, 'servicelinecode', 'sl_code')

    # Extract base fields
    employee_id = item.get('EmployeeID', '')
    company_code = item.get('CompanyCode', '')
    time_entry_date = item.get('SubmittedOn', '')
    project_profile = item.get('ProjectProfile', '')
    roles = item.get('Roles', '')
    cost_center_code = item.get('CostCenterCode', '')
    work_location = item.get('WorkLocation', '')

    # Workato formula: receivercostcenter = ProjectProfile.include?("YP04") ? CostCenterCode : ""
    receiver_cost_center = cost_center_code if config.PROJECT_PROFILE_YP04 in project_profile else ''

    # Perform enrichment lookups - return None for failed lookups (matching Workato null behavior)
    roles_description = division_lookup.get(roles) or None

    # Extract TWO different codes from cost center (matching Workato slice logic):
    # 1. Location code (positions 7-10) - Cost center code.slice(7,3) in Workato
    location_code = extract_location_code(
        cost_center_code,
        config.LOCATION_CODE_START_INDEX,
        config.LOCATION_CODE_END_INDEX
    )

    # 2. Service line (positions 4-7) - Cost center code.slice(4,3) in Workato
    service_line = extract_service_line(
        cost_center_code,
        config.SERVICE_LINE_START_INDEX,
        config.SERVICE_LINE_END_INDEX
    )

    # Lookup in mappers (matching Workato EA_Location_mapper and EA_Service_Mapper)
    lc_code = location_lookup.get(location_code) if location_code else None
    sl_code = serviceline_lookup.get(service_line) if service_line else None
    work_location_code = worklocation_lookup.get(work_location) or None

    return [
        employee_id,
        company_code,
        time_entry_date,
        receiver_cost_center,
        roles,
        roles_description,
        cost_center_code,
        lc_code,
        sl_code,
        work_location,
        work_location_code
    ]


def build_error_reason(record):
    if not record:
        return ''

    errors = []

    # Check employeeid
    if not record.get('employeeid'):
        errors.append("Employee Id is not present in user profile")

    # Check roles
    if not record.get('roles'):
        errors.append("Roles is not present on user profile")

    # Check rolesdescription
    if not record.get('rolesdescription'):
        errors.append("Role Description is not present at global level")

    # If costcenter exists, check lccode and slcode
    if record.get('costcenter'):
        # Check lccode
        if not record.get('lccode'):
            costcenter = record.get('costcenter', '')
            # Extract from position 7-10 (indices 7:10)
            lscode_extracted = costcenter[7:10]
            errors.append(f"LCCode is not available in mapper for value {lscode_extracted}")

        # Check slcode
        if not record.get('slcode'):
            costcenter = record.get('costcenter', '')
            # Extract from position 4-7 (indices 4:7)
            slcode_extracted = costcenter[4:7]
            errors.append(f"SLCode is not available in mapper for value {slcode_extracted}")
    else:
        errors.append("Cost center is not present in user profile")

    # Check worklocationcode
    if not record.get('worklocationcode'):
        errors.append("Worklocationcode not present at global level")

    return ','.join(errors) if errors else ''

def get_item_value(value):
   return value if value else '""'

def get_final_output_csv_row(item, config):
    if not item:
        return []

    # Get lookup data from Replicon API responses
    division_details = rail.result('get_bulk_division_details')
    work_location_tags = rail.result('get_object_extension_tag_definition_details')
    service_center_details = rail.result('get_bulk_servicecenter_details')

    # Build lookup dictionaries from Replicon data
    division_lookup = build_lookup_dict(division_details, 'name', 'description')
    worklocation_lookup = build_lookup_dict(work_location_tags, 'name', 'code')
    cost_center_lookup = build_lookup_dict(service_center_details, 'name', 'code')

    # Build mapper lookups from static data (matching Workato EA_Location_mapper and EA_Service_Mapper)
    location_lookup = build_mapper_lookup(location_code_map, 'locationcode', 'lc_code')
    serviceline_lookup = build_mapper_lookup(service_line_map, 'servicelinecode', 'sl_code')

    # Extract base fields from conf data (Mixed Case)
    employee_id = item.get('EmployeeID', '')
    company_code = item.get('CompanyCode', '')
    time_entry_date = item.get('SubmittedOn', '')
    sap_employee_id = item.get('SAPEmployeeID', '')
    project_profile = item.get('ProjectProfile', '')
    task_level1_code = item.get('TaskLevel1Code', '')
    work_item_code = item.get('WorkPackageWorkItemCode', '')
    comments = item.get('Comments', '')
    hours = item.get('HoursWorked', '0.00')
    time_entry_code = item.get('TimeEntryCode', '')
    entry_id = item.get('TimesheetPeriodUri', '')
    cost_center_code = item.get('CostCenterCode', '')
    roles = item.get('Roles', '')
    work_location = item.get('WorkLocation', '')

    # Perform enrichment lookups
    roles_description = division_lookup.get(roles, '')
    cost_center = cost_center_lookup.get(cost_center_code[4:7], '')

    # Lookup in mappers (matching Workato EA_Location_mapper and EA_Service_Mapper)
    work_location_code = worklocation_lookup.get(work_location, '')

    # Calculate Activity Type
    if project_profile == config.PROJECT_PROFILE_YP02:
        activity_type = work_item_code
    else:
        activity_type = f"{roles_description}{cost_center}"

    if project_profile == config.PROJECT_PROFILE_YP04:
        receiver_cost_center = item.get('CostCenterCode', '')
    else:
        receiver_cost_center = ''

    # Calculate WBS Element
    if project_profile == config.PROJECT_PROFILE_P001:
        wbs_element = task_level1_code
    elif project_profile == config.PROJECT_PROFILE_YP04:
        wbs_element = f"{task_level1_code}.{company_code}" if task_level1_code and company_code else task_level1_code
    else:
        wbs_element = task_level1_code

    # Calculate Billing Control Category
    billing_control = config.NON_BILLABLE_CODE if (
        project_profile == config.PROJECT_PROFILE_P001 and
        time_entry_code == config.NON_BILLABLE_ENTRY_TYPE
    ) else ""

    # Calculate Work Item
    work_item = work_item_code if project_profile == config.PROJECT_PROFILE_P001 else ''

    return [
        get_item_value(employee_id),
        get_item_value(company_code),
        '""',
        get_item_value(time_entry_date),
        get_item_value(sap_employee_id),
        get_item_value(config.TIMESHEET_OPERATION),
        get_item_value(config.CONTROLLING_AREA),
        get_item_value(receiver_cost_center),
        get_item_value(activity_type),
        get_item_value(wbs_element),
        get_item_value(billing_control),
        get_item_value(work_item),
        get_item_value(comments),
        get_item_value(hours),
        get_item_value(config.HOURS_UNIT),
        get_item_value(work_location_code),
        get_item_value(config.APPROVAL_STATUS),
        get_item_value(entry_id)
    ]


def fix_csv_empty_value_quotes(task_id):
    # Read the original CSV artifact
    csv_artifact_name = rail.result(task_id)

    with existing_artifact(csv_artifact_name, mode='r', encoding='utf-8') as input_artifact:
        csv_content = input_artifact.file.read()

    # Fix empty value quotes
    fixed_content = csv_content.replace('""""""', '""')

    # Write to new artifact with proper CSV type
    with new_artifact(mode='w', encoding='utf-8') as output_artifact:
        output_artifact.file.write(fixed_content)
        output_artifact.set_attribute('type', 'csv')
        return output_artifact.name


def create_json_payload_for_s4hc_from_csv():
    csv_content = rail.load_all_records(rail.result('csv_data_update'))

    if not csv_content:
        return json.dumps({"timedata": [], "log": ""}, ensure_ascii=True)

    # Convert CSV records to JSON with lowercase keys
    json_records = []
    for row in csv_content:
        json_record = {
            "employeeid": row.get('EmployeeID', ''),
            "companycode": row.get('Companycode', ''),
            "sapgeneratedinternalnumber": row.get('SAPGeneratedInternalNumber', ''),
            "timeentrydate": row.get('TimeEntryDate', ''),
            "sapemployeeid": row.get('SAPEmployeeID', ''),
            "timesheetoperation": row.get('TimesheetOperation', ''),
            "controllingarea": row.get('ControllingArea', ''),
            "receivercostcenter": row.get('ReceiverCostCenter', ''),
            "activitytype": row.get('ActivityType', ''),
            "wbselement": row.get('WBSElement', ''),
            "billingcontrolcategory": row.get('BillingControlCategory', ''),
            "workitem": row.get('WorkItem', ''),
            "comments": row.get('Comments', ''),
            "hours": row.get('Hours', ''),
            "hoursunitofmeasure": row.get('HoursUnitofMeasure', ''),
            "worklocationcode": row.get('WorkLocationCode', ''),
            "timeentryapprovalstatus": row.get('TimeEntryApprovalStatus', ''),
            "entryid": row.get('EntryID', '')
        }
        json_records.append(json_record)

    json_output = json.dumps({"timedata": json_records, "log": ""}, separators=(',', ':'), ensure_ascii=True)
    return json_output
