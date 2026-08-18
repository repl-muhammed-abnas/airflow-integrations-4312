from datetime import datetime
from dateutil.relativedelta import relativedelta
import pendulum
null = None

REPORT_DATE_FORMAT = "%d/%m/%Y"
EXPORT_DATE_FORMAT = "%Y-%m-%d"


def get_date_json(date_obj):
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }


def get_logging_details(time_zone):
    today = pendulum.now(time_zone)
    prev_4_months_date = today - relativedelta(months=4, day=1)
    prev_month_end_date = today - relativedelta(months=1, day=31)
    current_time = today.strftime('%Y%m%d_%H%M%S')
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_start_date": prev_4_months_date.strftime("%Y/%m/%d"),
        "export_end_date": prev_month_end_date.strftime("%Y/%m/%d"),
        "export_start_date_json": get_date_json(prev_4_months_date),
        "export_end_date_json": get_date_json(prev_month_end_date),
        "payroll_name_suffix": f"UK_Payroll_{current_time}",
        "oncall_export_filename_suffix": f"OnCall_{current_time}",
        "overtime_export_filename_suffix": f"OT_{current_time}"
    }


def get_overtime_payroll_data_rows(item):
    entry_date = datetime.strptime(item["Entry_Date"], "%Y/%m/%d")
    return [
        item["Local_Employee_Number"],
        item["Cost_Center_Code"],
        item["User"],
        item["User"].split(",")[-1].strip()[0],
        item["Pay_Code_Code"],
        item["Pay_Code_Hours"],
        entry_date.strftime("%y-%b"),
        entry_date.strftime("%m/%d/%Y"),
        "",
        item["Project_Code"]
    ]


def get_oncall_payroll_data_rows(item):
    return [
        item["Local_Employee_Number"],
        datetime.strptime(item["Entry_Date"], "%Y/%m/%d").strftime("%Y-%m-%d"),
        item["Cost_Center_Code"],
        item["User"],
        item["User"].split(",")[-1].strip()[0],
        item["Pay_Code_Hours"]
    ]

def get_uk_location_uri(response, uk_location):
    return list(map(lambda locations_data: locations_data["location"]["uri"],
        filter(lambda locations_data: locations_data["location"]["displayText"] == uk_location
            and str(locations_data["hierarchyLevel"]) == "0", response)))[0]

def collect_cost_center_uris_dynamic(ti, dag_run, **kwargs):
    """
    Dynamically collect URIs based on actual cost center count in the group
    This replaces the fixed-range approach with a dynamic one
    """
    cost_centers_list = dag_run.conf.get("cost_centers_list", [])
    cost_centers_count = len(cost_centers_list)
    cost_center_uris = []
    
    print(f"Processing group '{dag_run.conf.get('cost_center_group_name', 'Unknown')}' with {cost_centers_count} cost centers")
    
    # Only check the tasks we actually need based on cost center count
    for i in range(cost_centers_count):
        task_id = f'get_cost_center_uri_{i}'
        try:
            uri = ti.xcom_pull(task_ids=task_id)
            if uri:  # Only add non-null URIs
                cost_center_uris.append(uri)
                print(f"  ✓ Task {task_id}: Found URI {uri}")
            else:
                print(f"  ✗ Task {task_id}: No URI found")
        except Exception as e:
            print(f"  ✗ Task {task_id}: Error - {str(e)}")
            continue
    
    print(f"Successfully collected {len(cost_center_uris)} URIs out of {cost_centers_count} expected cost centers")
    
    # Log the cost center names and their URIs for debugging
    for i, uri in enumerate(cost_center_uris):
        if i < len(cost_centers_list):
            print(f"  {cost_centers_list[i]} -> {uri}")
    
    return cost_center_uris

def get_costcenter_uri(response, dag_run, item):
    costcenter = list(map(lambda costcenters_data: costcenters_data["costCenter"]["uri"],
        filter(lambda costcenters_data: costcenters_data["costCenter"]["displayText"] == item
            and str(costcenters_data["hierarchyLevel"]) == dag_run.conf["cost_center_hierarchy_level"], response)))
    return costcenter[0] if costcenter else null

def get_cost_center_search_data(dag_run, index):
    """Generate search payload for cost center at given index"""
    cost_centers_list = dag_run.conf["cost_centers_list"]
    
    # Return None if index is beyond the list (task will be skipped)
    if index >= len(cost_centers_list):
        return None
    
    cost_center_name = cost_centers_list[index]
    
    return {
        "page": "1",
        "pageSize": "100",
        "textSearch": {
            "queryText": cost_center_name,
            "searchInDisplayText": "false",
            "searchInName": "true",
            "searchInDescription": "false",
            "searchInCode": "false"
        }
    }

def extract_uri_from_response(response, dag_run, index):
    """Extract URI from service response for cost center at index"""
    cost_centers_list = dag_run.conf["cost_centers_list"]
    
    if index >= len(cost_centers_list):
        return None
    
    cost_center_name = cost_centers_list[index]
    hierarchy_level = dag_run.conf["cost_center_hierarchy_level"]
    
    # Use existing logic pattern
    costcenter = list(map(lambda costcenters_data: costcenters_data["costCenter"]["uri"],
        filter(lambda costcenters_data: costcenters_data["costCenter"]["displayText"] == cost_center_name
            and str(costcenters_data["hierarchyLevel"]) == hierarchy_level, response)))
    
    result_uri = costcenter[0] if costcenter else null
    print(f"Cost center '{cost_center_name}' -> URI: {result_uri}")
    return result_uri

def collect_cost_center_uris(ti, **kwargs):
    """Collect URIs from all get_cost_center_uri_* tasks"""
    cost_center_uris = []
    
    # Check all 15 possible URI tasks
    for i in range(15):
        task_id = f'get_cost_center_uri_{i}'
        try:
            uri = ti.xcom_pull(task_ids=task_id)
            if uri:  # Only add non-null URIs
                cost_center_uris.append(uri)
        except Exception as e:
            # Task was skipped or failed - that's expected for indices beyond the list
            continue
    
    print(f"Collected {len(cost_center_uris)} cost center URIs: {cost_center_uris}")
    return cost_center_uris

def get_costcenter_uri_from_response(response, cost_center_name, hierarchy_level):
    """
    Extract cost center URI from API response
    Reusable version of the existing get_costcenter_uri logic
    """
    costcenter = list(map(lambda costcenters_data: costcenters_data["costCenter"]["uri"],
        filter(lambda costcenters_data: costcenters_data["costCenter"]["displayText"] == cost_center_name
            and str(costcenters_data["hierarchyLevel"]) == hierarchy_level, response)))
    return costcenter[0] if costcenter else null


def get_cost_center_group_summary(dag_run):
    """
    Generate a summary of cost centers in the group for logging/emails
    """
    cost_centers_list = dag_run.conf.get("cost_centers_list", [])
    group_name = dag_run.conf.get("cost_center_group_name", "Unknown")
    
    return {
        "group_name": group_name,
        "cost_center_count": len(cost_centers_list),
        "cost_centers": cost_centers_list
    }