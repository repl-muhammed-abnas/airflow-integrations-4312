"""
Request payload builders for Replicon API calls
"""
import rail
from crl.office_schedule_import_v1.utils import custom_methods


def get_simple_schedule_pattern_payload():
    """
    Build the payload for PutSimpleSchedulePattern API call (7-day patterns)
    Pattern starts on MONDAY

    Returns:
        Dictionary with pattern data
    """
    # Get the parsed pattern array (7 float values)
    pattern_array = rail.result('parse_pattern')

    # Get the draft URI
    draft_uri = rail.result('create_new_draft')

    # Convert each hour value to duration format
    day_durations = [
        custom_methods.convert_hours_to_duration(hours)
        for hours in pattern_array
    ]

    return {
        "officeScheduleUri": draft_uri,
        "pattern": {
            "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
            "day1WorkDuration": day_durations[6],  # Sunday
            "day2WorkDuration": day_durations[0],  # Monday - First day in the payload, thats why day2 is set as day_durations[0]
            "day3WorkDuration": day_durations[1],  # Tuesday
            "day4WorkDuration": day_durations[2],  # Wednesday
            "day5WorkDuration": day_durations[3],  # Thursday
            "day6WorkDuration": day_durations[4],  # Friday
            "day7WorkDuration": day_durations[5],  # Saturday
        }
    }


def get_recurring_schedule_pattern_payload(start_date_str, start_date_format):
    """
    Build the payload for PutRecurringSchedulePattern API call (non-7-day patterns)

    Args:
        start_date_str: Start date in MM/DD/YYYY format

    Returns:
        Dictionary with recurring pattern data
    """
    # Get the parsed pattern array (variable length)
    pattern_array = rail.result('parse_pattern')

    # Get the draft URI
    draft_uri = rail.result('create_new_draft')

    # Parse start date
    reference_start = custom_methods.parse_start_date(
        start_date_str, start_date_format)

    # Build pattern entries for each day (patternDay is 1-based)
    pattern_entries = []
    for index, hours in enumerate(pattern_array):
        pattern_entries.append({
            "patternDay": index + 1,
            "workDuration": custom_methods.convert_hours_to_duration(hours)
        })

    return {
        "officeScheduleUri": draft_uri,
        "pattern": {
            "referenceStart": {
                "date": reference_start
            },
            "patternEntries": pattern_entries
        }
    }
