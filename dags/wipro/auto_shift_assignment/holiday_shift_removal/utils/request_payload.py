from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import pendulum
import rail
null = None
DATE_FORMAT = "%Y/%m/%d"

def get_user_report_payload(dag_run):
    get_specific_report_details = rail.result('get_user_report_details')
    filter_values = []
    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_user_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentServiceCenterFilter', 'uri'),
        "value": null,
    })
    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_user_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentServiceCenterFilter', 'uri'),
        "value": (rail.find_first_by_attr_and_get_attr(rail.result("get_all_locations" ), "displayText", dag_run.conf['country'], "uri")).split(':')[-1],
    })
    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

# KEEP THIS - Used by monthly assignment workflow
def get_week_start_end_date(config):
    start_date = pendulum.now(config.time_zone)
    enddate = (start_date + relativedelta(months=int(3)))
    return {
            "startDate": rail.parse_date(start_date.strftime(DATE_FORMAT), DATE_FORMAT),
            "endDate": rail.parse_date(enddate.strftime(DATE_FORMAT), DATE_FORMAT),
          }

# NEW FUNCTION: Dynamic date range based on user start date for holiday removal
def get_holiday_date_range_for_user(config, user_data):
    """
    Calculate holiday removal date range with special handling for recent hires.
    
    Logic:
    1. For users who joined within last 90 days:
       - ASSIGNEE: Use Onsite Start Date (if available), else User Start Date
       - LOCAL_HIRE/Others: Use User Start Date
       - Look forward 3 months from their start date
    
    2. For users who joined more than 90 days ago:
       - Use rolling 3-month window starting from 90 days ago
       - This ensures we cover the full period without gaps
    
    This ensures new users get proper holiday removal from their join date,
    while maintaining efficiency for existing users.
    """
    current_date = pendulum.now(config.time_zone)
    three_month_ago = current_date - relativedelta(days=90)
    
    # Parse user start date
    try:
        user_start_date_str = user_data.get('user_start_date', '').strip()
        if user_start_date_str:
            # Expected format: YYYY/MM/DD (e.g., "2012/12/01")
            user_start_date = pendulum.parse(user_start_date_str, tz=config.time_zone)
        else:
            # No start date, use rolling window
            user_start_date = None
    except (ValueError, KeyError):
        user_start_date = None
    
    # Determine effective start date based on hire type
    effective_start_date = None
    
    if user_start_date:
        onsite_direct_recruit = user_data.get('onsite_direct_recruit', '').strip().upper()
        
        if onsite_direct_recruit == 'ASSIGNEE':
            # For assignees, try to use onsite start date
            try:
                onsite_start_date_str = user_data.get('onsite_start_date', '').strip()
                if onsite_start_date_str:
                    effective_start_date = pendulum.parse(
                        onsite_start_date_str, 
                        tz=config.time_zone
                    )
                else:
                    # No onsite start date, fall back to user start date
                    effective_start_date = user_start_date
            except (ValueError, KeyError):
                effective_start_date = user_start_date
        else:
            # For LOCAL_HIRE or any other value, use user start date
            effective_start_date = user_start_date
    
    if effective_start_date and effective_start_date >= three_month_ago:
        # RECENT USER: Joined within last 90 days
        # Use their actual start date and look forward 3 months
        
        # Don't process holidays before current date (even for recent users)
        start_date = max(effective_start_date, current_date)
        
        # Look forward 3 months from their start date
        end_date = effective_start_date + relativedelta(months=3)
        
        # But don't go beyond current date + 3 months
        max_end_date = current_date + relativedelta(months=3)
        end_date = min(end_date, max_end_date)
        
    else:
        start_date = current_date
        end_date = current_date + relativedelta(months=3)
    
    return {
        "startDate": rail.parse_date(start_date.strftime(DATE_FORMAT), DATE_FORMAT),
        "endDate": rail.parse_date(end_date.strftime(DATE_FORMAT), DATE_FORMAT),
    }

def do_format_logs():
    log_artifacts = []
    log_records = []

    user_shift_logs = rail.result('gather_shift_logs')

    if user_shift_logs:
        if isinstance(user_shift_logs, list):
            log_artifacts.extend(user_shift_logs)
        else:
            log_artifacts.append(user_shift_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)
    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))

    return final_log_records


def get_default_shift():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:shift-list-column:name",
            "urn:replicon:shift-list-column:is-enabled",
            "urn:replicon:shift-list-column:shift"
        ]
    }

# MODIFIED: get_holiday_list_for_user now uses dynamic date range
def get_holiday_list_for_user(config, dag_run):
    # Get user data for this holiday calendar
    users_for_calendar = rail.load_all_records(
        rail.result("query_distinct_users_for_hoilday_calendar")
    )
    
    # Use first user's data to determine date range
    # All users with same holiday calendar will have holidays removed in same range
    sample_user = users_for_calendar[0] if users_for_calendar else {}
    
    # Get dynamic date range based on user start date
    date_range = get_holiday_date_range_for_user(config, sample_user)
    
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:holiday-list-column:holiday-name",
            "urn:replicon:holiday-list-column:date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:holiday-list-filter:holiday-calendar"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": dag_run.conf["holiday_calendar_uri"],
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null,
                        "numberRange": null
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:holiday-list-filter:date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": date_range,  # CHANGED: Now using dynamic date range
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null,
                        "numberRange": null
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def get_shifts_assigned_on_holiday(item, dag_run):
    user_uris =  rail.load_all_records(rail.result("query_distinct_users_for_hoilday_calendar"))
    specific_user_uris = list(set(map(lambda i: i["user_uri"], user_uris)))
    return  {
        "userSearch": {"specificUserUris":specific_user_uris},
        "shiftSearch": {
            "includeShiftAssignmentsWithNoShift": "false",
            "specificShiftUris": dag_run.conf["default_shift_uri"] if isinstance(
                dag_run.conf["default_shift_uri"], list) else [dag_run.conf["default_shift_uri"]],
            "shiftTypeUri": null
        },
        "objectExtensionFieldSearches": [],
        "dateRange": {
            "startDate": {**item},
            "endDate": {**item},
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

def check_and_get_items(dag_run):
    return dag_run.conf['item']
