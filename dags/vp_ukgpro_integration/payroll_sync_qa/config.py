# pylint: disable=invalid-name
# pylint: disable=missing-module-docstring

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 5
execution_timeout_days = 7

# Retry configuration
max_retries = 3
retry_delay_minutes = 5/60  # 5 seconds

# Batch configuration per UKG Pro requirements
batch_size = 50  # Process 50 records per batch for optimal performance

# Employee validation
validate_employees_in_ukgpro = False  # Temporarily disabled - UKG Pro test env returning 502/503

# UKG Pro configuration
ukgpro_source = 'Deltek'  # Source identifier for UKG Pro

# Hours code mapping (VantagePoint Hours3_code -> UKG Pro code)
hours_code_mapping = {
    '1': 'REG',    # Regular hours
    '2': 'OT',     # Overtime
    '3': 'DT',     # Double time
    'Sick': 'SICK',
    'Vacation': 'VAC'
}

# Field mappings from VantagePoint export to UKG Pro
field_mapping = {
    'company_code': 'companyCode',
    'emp_no': 'empNo',
    'charge_date': 'Entry_Date',
    'hours_code': 'Hours3_code',
    'hours_amount': 'Hours3_Amt',
}
