region = 'us-east-1'
environment = 'pre-production'
execution_timeout_days = 14
child_dag_max_active_runs = 10
provider = 'jira'
workflow = 'create_user'

# S3 folder name inside the bucket used for the user-sync reference CSV
aws_conn_id = 'aws_jira_conn'
s3_bucket = 'airflow-systemtest'
s3_folder = 'jira-user-integration'

# Segregated reference files per sync type
s3_all_users_reference_file = 'all_users_references.csv'
s3_role_based_reference_file = 'role_based_references.csv'
s3_group_based_reference_file = 'group_based_references.csv'

# Maps Jira timezone SID keys to the IANA names recognised by Replicon.
# Most are identity; the entries below differ (Jira uses a modern IANA name,
# Replicon expects the legacy alias).
JIRA_TIMEZONE_MAP = {
    'Etc/GMT+12': 'Etc/GMT+12',
    'Pacific/Honolulu': 'Pacific/Honolulu',
    'America/Anchorage': 'America/Anchorage',
    'America/Los_Angeles': 'America/Los_Angeles',
    'America/Chihuahua': 'America/Chihuahua',
    'America/Phoenix': 'America/Phoenix',
    'America/Denver': 'America/Denver',
    'America/Regina': 'America/Regina',
    'America/Chicago': 'America/Chicago',
    'America/Guatemala': 'America/Guatemala',
    'America/Mexico_City': 'America/Mexico_City',
    'America/Bogota': 'America/Bogota',
    'America/New_York': 'America/New_York',
    'America/Caracas': 'America/Caracas',
    'America/La_Paz': 'America/La_Paz',
    'America/Santiago': 'America/Santiago',
    'America/Halifax': 'America/Halifax',
    'America/Asuncion': 'America/Asuncion',
    'America/Cuiaba': 'America/Cuiaba',
    'America/St_Johns': 'America/St_Johns',
    'America/Montevideo': 'America/Montevideo',
    'America/Cayenne': 'America/Cayenne',
    'America/Sao_Paulo': 'America/Sao_Paulo',
    'Etc/GMT+2': 'Etc/GMT+2',
    'Atlantic/Azores': 'Atlantic/Azores',
    'Atlantic/Cape_Verde': 'Atlantic/Cape_Verde',
    'Europe/London': 'Europe/London',
    'Atlantic/Reykjavik': 'Atlantic/Reykjavik',
    'Etc/GMT': 'Etc/GMT',
    'Europe/Warsaw': 'Europe/Warsaw',
    'Europe/Budapest': 'Europe/Budapest',
    'Europe/Berlin': 'Europe/Berlin',
    'Africa/Casablanca': 'Africa/Casablanca',
    'Africa/Lagos': 'Africa/Lagos',
    'Europe/Paris': 'Europe/Paris',
    'Africa/Cairo': 'Africa/Cairo',
    'Asia/Amman': 'Asia/Amman',
    'Asia/Beirut': 'Asia/Beirut',
    'Africa/Johannesburg': 'Africa/Johannesburg',
    'Europe/Kiev': 'Europe/Kiev',
    'Asia/Jerusalem': 'Asia/Jerusalem',
    'Africa/Windhoek': 'Africa/Windhoek',
    'Europe/Minsk': 'Europe/Minsk',
    'Asia/Baghdad': 'Asia/Baghdad',
    'Asia/Riyadh': 'Asia/Riyadh',
    'Europe/Moscow': 'Europe/Moscow',
    'Africa/Nairobi': 'Africa/Nairobi',
    'Europe/Istanbul': 'Europe/Istanbul',
    'Asia/Tehran': 'Asia/Tehran',
    'Asia/Dubai': 'Asia/Dubai',
    'Asia/Baku': 'Asia/Baku',
    'Asia/Yerevan': 'Asia/Yerevan',
    'Indian/Mauritius': 'Indian/Mauritius',
    'Asia/Tbilisi': 'Asia/Tbilisi',
    'Asia/Kabul': 'Asia/Kabul',
    'Asia/Yekaterinburg': 'Asia/Yekaterinburg',
    'Asia/Tashkent': 'Asia/Tashkent',
    'Asia/Karachi': 'Asia/Karachi',
    'Asia/Colombo': 'Asia/Colombo',
    'Asia/Almaty': 'Asia/Almaty',
    'Asia/Bangkok': 'Asia/Bangkok',
    'Asia/Novosibirsk': 'Asia/Novosibirsk',
    'Asia/Krasnoyarsk': 'Asia/Krasnoyarsk',
    'Asia/Shanghai': 'Asia/Shanghai',
    'Asia/Irkutsk': 'Asia/Irkutsk',
    'Asia/Singapore': 'Asia/Singapore',
    'Australia/Perth': 'Australia/Perth',
    'Asia/Taipei': 'Asia/Taipei',
    'Asia/Tokyo': 'Asia/Tokyo',
    'Asia/Seoul': 'Asia/Seoul',
    'Asia/Yakutsk': 'Asia/Yakutsk',
    'Australia/Adelaide': 'Australia/Adelaide',
    'Australia/Darwin': 'Australia/Darwin',
    'Pacific/Port_Moresby': 'Pacific/Port_Moresby',
    'Australia/Brisbane': 'Australia/Brisbane',
    'Australia/Sydney': 'Australia/Sydney',
    'Asia/Vladivostok': 'Asia/Vladivostok',
    'Australia/Hobart': 'Australia/Hobart',
    'Pacific/Guadalcanal': 'Pacific/Guadalcanal',
    'Pacific/Fiji': 'Pacific/Fiji',
    'Asia/Kamchatka': 'Asia/Kamchatka',
    'Pacific/Auckland': 'Pacific/Auckland',
    'Pacific/Tongatapu': 'Pacific/Tongatapu',
    'Pacific/Apia': 'Pacific/Apia',
    # Jira uses the modern IANA name; Replicon expects the legacy alias
    'Asia/Kolkata': 'Asia/Calcutta',
    'Asia/Kathmandu': 'Asia/Katmandu',
    'America/Nuuk': 'America/Godthab',
    'America/Argentina/Buenos_Aires': 'America/Buenos_Aires',
    'America/Indiana/Indianapolis': 'America/Indianapolis',
    'Asia/Yangon': 'Asia/Rangoon',
    # Some Jira tenants store the legacy IANA name directly as their timezone SID.
    # These entries ensure the lookup still resolves for those accounts.
    'Asia/Calcutta': 'Asia/Calcutta',
    'Asia/Katmandu': 'Asia/Katmandu',
    'America/Godthab': 'America/Godthab',
    'America/Buenos_Aires': 'America/Buenos_Aires',
    'America/Indianapolis': 'America/Indianapolis',
    'Asia/Rangoon': 'Asia/Rangoon',
}
