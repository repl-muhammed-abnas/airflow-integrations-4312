from datetime import datetime as dt, timedelta
from functools import lru_cache
import pendulum
import rail
from dxctechnology.philippines_payroll_export.mapper.payroll_calendar_mapper import PAYROLL_CALENDAR

DATE_FORMAT = "%Y-%m-%d"

def can_process_run_test(config):
    current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
    return bool(list(filter(lambda calendar_mapper: calendar_mapper["payroll_run_date"] == current_date, config.PAYROLL_CALENDAR))) if rail.get_company_key().lower() not in ('dxctrial01', 'dxcsandbox') else True


def convert_location_hierarchy(resp):
    if len(resp.json()['d']['rows']) > 0:
        rows = [row["cells"] for row in resp.json()['d']['rows']]

        def map_row(cells):
            full_path_names = [elem['textValue']
                               for elem in cells[1]['cellCollection']]
            return {
                "name": cells[0]['textValue'],
                "fullpath": " | ".join(full_path_names),
                "uri": cells[0]['uri']
            }
        return [map_row(row) for row in rows]
    return None

def get_employee_type_uris():
    contractor_uri = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_enabled_employeetype_groups"), 'displayText', 'Contractor', 'uri')

    return list(map(lambda item: item['uri'], rail.result("get_child_hierarchy_data"))) + [contractor_uri]

def get_export_dates(time_zone):
    today = dt.today()

    def get_start_date(end_date=False):
        date_obj = (today - timedelta(days=90)) if not end_date else (dt.strptime(end_date, '%d-%m-%Y') - timedelta(days=90))
        return date_obj.strftime(DATE_FORMAT)

    current_date = pendulum.now(time_zone).strftime("%d-%m-%Y")

    mapper_end_date = rail.find_first_by_attr_and_get_attr(
        PAYROLL_CALENDAR, "payroll_run_date", current_date, "payrun_end_date", '')

    if mapper_end_date:
        return {
            'start_date': get_start_date(mapper_end_date),
            'end_date': dt.strptime(mapper_end_date, "%d-%m-%Y").strftime(DATE_FORMAT)
        }

    return {
        'start_date': get_start_date(),
        'end_date': today.strftime(DATE_FORMAT)
    }

def get_current_export_details(config):
    today = dt.today()

    return {
        'replicon_export_name': "Philippines_Payroll_Export_" + today.strftime("%Y%m%d%H%M%S"),
        'regular_filename': "WDIT_SP_OT_Cycle_" + today.strftime("%Y%m%d%H%M%S"),
        'timeoff_filename': "PAYRG33_Global_Time_Off_Transaction_Report_" + today.strftime("%Y%m%d%H%M%S"),
        "fileformat_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            "get_all_scripts"), 'displayText', config.fileformat_name, 'uri'),
        "startdate": get_export_dates(config.time_zone)['start_date'],
        "enddate": get_export_dates(config.time_zone)['end_date'],
        "contractor_uris": get_employee_type_uris(),
        "divisionuri": rail.result("get_enabled_companycodes"),
        "process_started": dt.now().strftime("%Y-%m-%dT%H:%M:%S")
    }


def get_load_users_data_from_report():
    filter_values = []

    dates = rail.load_all_records(rail.result(
        "query_list_in_final_timeoff_payroll_collection"))[0]

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                                                                            )['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri'),
        "value": None
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                                                                            )['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri'),
        "value": dt.strptime(dates['min_entry_date'], DATE_FORMAT).strftime("%m/%d/%Y")
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                                                                            )['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri'),
        "value": dt.strptime(dates['max_entry_date'], DATE_FORMAT).strftime("%m/%d/%Y")
    })

    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_first_name_last_name(emp_id,full_name):
    before_id = full_name.split(emp_id)[0].strip()
    last_name, first_name = [p.strip() for p in before_id.split(",", 1)]
    return str(first_name)+ ' ' + str(last_name)

def get_compose_item_regular_payroll(items):
    return [
        items['Employee_ID'],
        get_first_name_last_name(items['Employee_ID'],items['User']),
        dt.strptime(items['Entry_Date'], DATE_FORMAT).strftime("%m-%d-%Y"),
        items['Pay_Code_Code'],
        items['Pay_Code_Hours']
    ]

def load_artifact_data(task_id):
    return rail.load_all_records(rail.result(task_id))

def check_dates(_date, format):
    return dt.strptime(_date, format)

def get_final_compose_data(item):
    return [
        item['Employee_ID'],
        item['username'],
        item['Worker_Type'],
        item['User_Status'],
        item['Cost_Center_Name'],
        item['Shift_Hours'] if item['Shift_Hours'] else '0',
        dt.strptime(item['Entry_Date'], DATE_FORMAT).strftime("%m-%d-%Y"),
        item['Pay_Code_Hours'],
        item['Pay_Code_Name']
    ]

def get_shift_hours_callable():    
    """Returns payroll data with Shift_Hours and username added to each item"""
    try:
        payroll_data = load_artifact_data('query_list_in_final_timeoff_payroll_collection')
        user_report_data = load_artifact_data('user_report_collection')
        
        if not user_report_data:
            for item in payroll_data:
                item['Shift_Hours'] = '0'
                item['username'] = get_first_name_last_name(item['Employee_ID'], item['User'])
            return payroll_data
        
        # Build efficient lookups
        shift_lookup = {}
        username_lookup = {}
        
        for data in user_report_data:
            try:
                emp_id = data.get('employeeid')
                if emp_id:
                    # Store username if available
                    username_lookup[emp_id] = data.get('username', '')
                    
                    # Store shift hours by (employee, date)
                    parsed_date = check_dates(data['date'], '%d %B %Y')
                    if parsed_date:
                        date_key = parsed_date.strftime(DATE_FORMAT)
                        lookup_key = (emp_id, date_key)
                        shift_lookup[lookup_key] = data.get('hours', '0')
            except Exception:
                continue
        
        # Apply lookups to payroll data
        for item in payroll_data:
            emp_id = item.get('Employee_ID')
            lookup_key = (emp_id, item.get('Entry_Date'))
            
            item['Shift_Hours'] = shift_lookup.get(lookup_key, '0')
            item['username'] = username_lookup.get(emp_id) or get_first_name_last_name(emp_id, item.get('User', ''))
        
        return payroll_data
        
    except Exception as e:
        for item in payroll_data:
            item['Shift_Hours'] = '0'
            item['username'] = get_first_name_last_name(item.get('Employee_ID', ''), item.get('User', ''))
        return payroll_data
