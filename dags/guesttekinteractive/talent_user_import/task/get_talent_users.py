"""
Get Talent Users Task Group - GuestTek Talent User Import Integration

Fetches users from the Talent API using event-log polling.

Flow:
    1. Read last_processed_time from Airflow Variable (default: 24h ago on first run)
    2. Fetch event logs (paginated) for users modified since last_processed_time
    3. Extract unique user_ids from event logs
    4. Fetch each user individually via GET /api/v1/users/{user_id}
    5. Normalize and return the full user list (same format as before)
"""
import pendulum
import requests
import rail
from airflow.models import Variable
from guesttekinteractive.talent_user_import.utils import response_filters
from guesttekinteractive.talent_user_import import config as base_config

null = None


def get_talent_users_task_group(config):
    """Create task group for fetching users from Talent API via event-log polling."""
    with rail.TaskGroup(group_id='get_talent_users', prefix_group_id=False) as get_talent_users:
        
        dummy_start = rail.EmptyOperator(task_id='dummy_get_talent_users_start')
        
        # Step 1: Read last processed time from Airflow Variable
        get_last_processed_time = rail.PythonOperator(
            task_id='get_last_processed_time',
            python_callable=lambda: _get_last_processed_time(config)
        )
        
        # Step 2+3: Fetch event logs (paginated) and extract unique user_ids
        fetch_event_log_user_ids = rail.PythonOperator(
            task_id='fetch_event_log_user_ids',
            python_callable=lambda: fetch_event_logs_paginated(
                config, rail.result('get_last_processed_time')
            )
        )
        
        # Step 4+5: Fetch each user by ID and normalize
        # Task ID kept as 'filter_delta_users' so all downstream references remain unchanged
        filter_delta_users = rail.PythonOperator(
            task_id='filter_delta_users',
            python_callable=lambda: fetch_users_by_ids(
                config, rail.result('fetch_event_log_user_ids')
            )
        )
        
        get_delta_count = rail.PythonOperator(
            task_id='get_delta_count',
            python_callable=lambda: len(rail.result('filter_delta_users'))
        )
        
        dummy_end = rail.EmptyOperator(task_id='dummy_get_talent_users_end')
        
        dummy_start >> get_last_processed_time >> fetch_event_log_user_ids >> filter_delta_users >> get_delta_count >> dummy_end
    
    return dummy_start, get_talent_users


# ---------------------------------------------------------------------------
# Talent API helpers
# ---------------------------------------------------------------------------

def _get_talent_api_auth(config):
    """
    Get Talent API base URL and auth headers from Airflow connection.
    
    Returns:
        tuple: (base_url, headers)
    """
    from airflow.hooks.base import BaseHook
    
    conn = BaseHook.get_connection(config.talent_conn_id)
    base_url = config.talent_api_base_url
    
    auth_token = conn.password if conn.password else ''
    if not auth_token and conn.extra:
        import json
        extra = json.loads(conn.extra) if isinstance(conn.extra, str) else conn.extra
        auth_token = extra.get('Auth-Token', extra.get('auth_token', ''))
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Auth-Token': auth_token
    }
    
    return base_url, headers


def _get_last_processed_time(config):
    """
    Read the last processed timestamp from an Airflow Variable.
    
    On the very first run the variable won't exist, so we default to
    DEFAULT_LOOKBACK_HOURS (24) hours ago.
    
    Returns:
        str: Timestamp string in 'YYYY-MM-DD HH:mm:ss' format
    """
    default = pendulum.now('UTC').subtract(
        hours=base_config.DEFAULT_LOOKBACK_HOURS
    ).format('YYYY-MM-DD HH:mm:ss')
    
    return Variable.get(config.last_processed_time_var, default_var=default)


# ---------------------------------------------------------------------------
# Event-log polling
# ---------------------------------------------------------------------------

def fetch_event_logs_paginated(config, last_processed_time):
    """
    Fetch event logs from Talent API (paginated) and return unique user_ids.
    
    Endpoint:
        GET /api/v1/event-logs?event_log_fired[eq]=0&event_log_date_modified[gte]={last_processed_time}
    
    Args:
        config: Instance configuration
        last_processed_time (str): Timestamp to query from
        
    Returns:
        list: Unique user_ids that have been modified
    """
    base_url, headers = _get_talent_api_auth(config)
    
    all_user_ids = set()
    page_number = 1
    page_size = base_config.EVENT_LOG_PAGE_SIZE
    
    while True:
        url = f"{base_url}/api/v1/event-logs"
        params = {
            'event_log_fired[eq]': 0,
            'event_log_date_modified[gte]': last_processed_time,
            'page[size]': page_size,
            'page[number]': page_number,
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=120)
        
        if response.status_code != 200:
            raise Exception(f"Talent API event-logs error: {response.status_code} - {response.text}")
        
        data = response.json()
        user_ids = response_filters.filter_event_logs_response(data)
        all_user_ids.update(user_ids)
        
        # Check if there's a next page
        pagination = data.get('pagination', {})
        if not pagination.get('next'):
            break
        
        page_number += 1
    
    return list(all_user_ids)


# ---------------------------------------------------------------------------
# Individual user fetch
# ---------------------------------------------------------------------------

def fetch_users_by_ids(config, user_ids):
    """
    Fetch full user records for each user_id via GET /api/v1/users/{user_id}.
    
    Each response is normalized through filter_talent_users_response so the
    returned list has exactly the same shape as the old fetch-all + delta-filter
    pipeline produced.
    
    Args:
        config: Instance configuration
        user_ids (list): List of user_id integers
        
    Returns:
        list: Normalized user dictionaries (same format downstream expects)
    """
    if not user_ids:
        return []
    
    base_url, headers = _get_talent_api_auth(config)
    all_users = []
    
    for user_id in user_ids:
        url = f"{base_url}/api/v1/users/{user_id}"
        
        response = requests.get(url, headers=headers, timeout=60)
        
        if response.status_code != 200:
            # Log warning but continue with other users
            print(f"Warning: Failed to fetch user {user_id}: {response.status_code} - {response.text}")
            continue
        
        users = response_filters.filter_talent_users_response(response.json())
        all_users.extend(users)
    
    return all_users


# ---------------------------------------------------------------------------
# Additional info (unchanged - called from child DAGs)
# ---------------------------------------------------------------------------

def fetch_additional_user_info(config, user_id):
    """
    Fetch additional user info from Talent API.
    Called from child DAGs for each user.
    """
    base_url, headers = _get_talent_api_auth(config)
    
    url = f"{base_url}/api/v1/employee-additional-information/{user_id}"
    response = requests.get(url, headers=headers, timeout=60)
    
    if response.status_code != 200:
        raise Exception(f"Talent API error fetching additional info: {response.status_code} - {response.text}")
    
    return response_filters.filter_talent_additional_info(response.json())
