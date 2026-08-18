import pendulum
import requests
import rail
from airflow.models import Variable

null = None


def get_logging_details(config):
    """Generate logging details including date range for Keka API."""
    today = pendulum.now(config.time_zone)
    
    # Calculate date range: 29 days back and 60 days forward (90 days total)
    from_date = today.subtract(days=29)  # 29 days back
    to_date = today.add(days=60)         # 60 days forward
    
    # Get configurable lookback hours
    lookback_hours = int(Variable.get(config.lookback_hours_var_name, default_var=str(config.default_lookback_hours)))
    cutoff_time = today.subtract(hours=lookback_hours)
    
    return {
        "time_zone": config.time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "log_filename": 'Timeoff_Sync_Keka_to_Replicon_Logs_' + today.strftime("%Y%m%d_%H%M%S") + '.csv',
        "from_date": from_date.format('YYYY-MM-DD'),
        "to_date": to_date.format('YYYY-MM-DD'),
        "lookback_hours": lookback_hours,
        "cutoff_time": cutoff_time.isoformat(),
        "cutoff_time_obj": cutoff_time
    }


def fetch_all_keka_leave_requests(config):
    """
    Fetch all leave requests from Keka API for the date range.
    Handles pagination automatically.
    """
    logging_details = rail.result("logging_details")
    access_token = rail.result("extract_keka_token")
    
    base_url = Variable.get(config.keka_base_url_var)
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla'
    }
    
    all_leaves = []
    page = 1
    total_pages = 1
    
    while page <= total_pages:
        url = f"{base_url}/api/v1/time/leaverequests"
        params = {
            'from': logging_details['from_date'],
            'to': logging_details['to_date'],
            'pageNumber': page,
            'pageSize': config.keka_page_size
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=60)
        
        if response.status_code != 200:
            raise Exception(f"Keka API error: {response.status_code} - {response.text}")
        
        data = response.json()
        
        if data.get('succeeded', True) and data.get('data'):
            all_leaves.extend(data['data'])
            total_pages = data.get('totalPages', 1)
        
        page += 1
    
    return {
        "all_leaves": all_leaves,
        "total_records": len(all_leaves),
        "from_date": logging_details['from_date'],
        "to_date": logging_details['to_date']
    }


def check_timeoff_type_assigned_to_user(dag_run):
    """Check if the time-off type is assigned to the user in Replicon."""
    user_info = rail.result("get_user_info")
    
    if not user_info:
        return False
    
    policy_summary = user_info.get("timeOffTypePolicySummary", {})
    policies_by_type = policy_summary.get("policiesByTimeOffType", [])
    
    if not policies_by_type:
        return False
    
    leave_type_name = dag_run.conf["booking_data"]["leaveTypeName"]
    
    result = rail.find_first_by_attr_and_get_attr(
        policies_by_type, 
        "timeOffType.name", 
        leave_type_name, 
        "timeOffType.uri"
    )
    
    return result is not None


def check_user_legal_entity(dag_run):
    """
    Check if the user belongs to the required legal entity (Cost Center group).
    Returns True if user's cost center matches the legal entity filter.
    """
    user_info = rail.result("get_user_info")
    legal_entity_filter = dag_run.conf.get("legal_entity_filter", "")
    
    if not legal_entity_filter:
        return True
    
    if not user_info:
        return False
    
    # Check costCenterSchedule at root level (primary location)
    cost_center_schedule = user_info.get("costCenterSchedule", []) or []
    for entry in cost_center_schedule:
        cost_center = entry.get("costCenter", {})
        if cost_center.get("displayText") == legal_entity_filter:
            return True
        if cost_center.get("name") == legal_entity_filter:
            return True
    
    # Fallback: Check userDetails for other possible locations
    user_details = user_info.get("userDetails", {})
    
    # Check in groups/cost centers
    groups = user_details.get("groups", []) or []
    for group in groups:
        group_type = group.get("groupType", {})
        if group_type.get("name") == "Cost Centers":
            if group.get("name") == legal_entity_filter:
                return True
    
    # Check costCenter in userDetails
    cost_center = user_details.get("costCenter", {}) or {}
    if cost_center.get("name") == legal_entity_filter or cost_center.get("displayText") == legal_entity_filter:
        return True
    
    # Check departmentGroup as fallback
    dept_group = user_details.get("departmentGroup", {}) or {}
    if dept_group.get("name") == legal_entity_filter or dept_group.get("displayText") == legal_entity_filter:
        return True
    
    return False


def parse_keka_datetime(datetime_str):
    """Parse Keka datetime string to pendulum datetime object."""
    if not datetime_str:
        return None
    
    try:
        if datetime_str.endswith('Z'):
            return pendulum.parse(datetime_str)
        return pendulum.parse(datetime_str)
    except Exception:
        return None