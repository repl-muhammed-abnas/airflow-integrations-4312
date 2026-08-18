import rail
from datetime import datetime, timedelta, timezone
import json
import itertools
import re


def last_sync_time(last_sync_var):
    sync_time = (datetime.now(
                timezone.utc) - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    return rail.get_lastsync_time_variable(
        variable_name= last_sync_var,
        date_format='%Y-%m-%dT%H:%M:%SZ',
        initial_sync_time= sync_time,
        reset_after_threshold=False
        )

def update_last_sync(update_sync_time):
    return rail.set_lastsync_time_variable(
            variable_name= update_sync_time,
            value_to_set= rail.result('get_last_sync_time')['current_time']
        )

def json_formatter():
    item = rail.load_all_records(rail.result("get_clients"))
    return json.dumps(item, indent=4, ensure_ascii=False)

def page_handler(request, response):
    if len(response['rows']) > 0:
        request['page'] += 1
        return request
    return None

def get_client_rows(row):
    try:
        return {
            "client_name": row["cells"][1]["textValue"],
            "client_uri": row["cells"][1]["uri"],
            "is_active": row["cells"][0]["textValue"]
        }
    except KeyError:
        return None

def filter_data(response):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    active_status = ('True')
    filtered_rows = filter(
        lambda x: x["cells"][0]["textValue"] in active_status, 
        flatten_rows
    ) if flatten_rows else []
    filtered_clients = list(
        map(lambda row: get_client_rows(row), filtered_rows))
    return [client for client in filtered_clients if client is not None]

def escape_sql_string(string):
    """Properly escape strings for QuickBooks SQL queries."""
    if not string:
        return ''
    # Escape backslashes first, then single quotes
    return string.replace('\\', '\\\\').replace("'", "\\'")

def build_customer_search_query():
    """Build SQL query string for customer search with proper escaping."""
    client_names = [escape_sql_string(i['client_name']) for i in rail.result("get_clients")]
    if not client_names:
        return "''"
    names = "', '".join(client_names)
    return f"'{names}'"

def parse_qb_customer():
    """
    Parse QuickBooks customer search response.

    Returns:
        list: List of customers found, or empty list if none found
    """
    search_result = rail.result('search_customer')
    query_response = search_result.get('QueryResponse')
    if not query_response:
        return []
    return query_response.get('Customer', [])

