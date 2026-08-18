from collections import defaultdict

null = None

def get_required_timeoff_type_uris_details(response, config):
    required_timeoff_types = defaultdict(dict)
    required_timeoff_types = {item['displayText']: item for item in response if item['displayText'] in config.REQUIRED_TIMEOFF_TYPES}
    uri_to_name = {item['uri']: item['displayText'] for item in response if item['displayText'] in config.REQUIRED_TIMEOFF_TYPES}
    missing_types = [name for name in config.REQUIRED_TIMEOFF_TYPES if name not in required_timeoff_types]
    return {
        'from': {
            'timeoff_annual_leave_uri': required_timeoff_types[config.ANNUAL_LEAVE].get('uri'),
            'timeoff_annual_leave_accrued_uri': required_timeoff_types[config.ANNUAL_LEAVE_ACCRUED].get('uri'),
            'timeoff_annual_leave_seniority_days_uri': required_timeoff_types[config.ANNUAL_LEAVE_SENIORITY_DAYS].get('uri'),
            'timeoff_annual_leave_rtt_uri': required_timeoff_types[config.ANNUAL_LEAVE_RTT].get('uri'),
            'timeoff_annual_leave_rtt_for_forfait_jours_uri': required_timeoff_types[config.ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS].get('uri'),
        },
        'into': {
            'timeoff_annual_leave_uri': required_timeoff_types[config.ANNUAL_LEAVE].get('uri'),
            'timeoff_annual_leave_carried_over_uri': required_timeoff_types[config.ANNUAL_LEAVE_CARRIED_OVER].get('uri'),
            'timeoff_annual_leave_seniority_days_carried_over_uri': required_timeoff_types[config.ANNUAL_LEAVE_SENIORITY_DAYS_CARRIED_OVER].get('uri'),
            'timeoff_annual_leave_rtt_carried_over_uri': required_timeoff_types[config.ANNUAL_LEAVE_RTT_CARRIED_OVER].get('uri'),
            'timeoff_annual_leave_rtt_for_forfait_jours_carried_over_uri': required_timeoff_types[config.ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS_CARRIED_OVER].get('uri'),
        },
        'uri_to_name': uri_to_name,
        'missing_types': missing_types
    }