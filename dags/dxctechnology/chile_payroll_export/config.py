region = 'us-east-2'
environment = 'pre-production'

max_active_runs_master = 1

chile_config = {
    "name": "Chile",
    "payroll_format": "Chile Payroll Format",
    "division_name": "CLES",
}
time_off_report_name = "Time off Booking details for CHILE payroll export"
pgp_public_key_var_name = "dxctechnology_chile_payroll_export_pgp_public_key"
paycode_mapper = [
    {
        "paycode": "BONEDS",
        "positive": "DIFHOR",
        "negative": "ANTHRS",
    },
    {
        "paycode": "HORA80",
        "positive": "AJUS80",
        "negative": "DESH80",
    },
    {
        "paycode": "HORA50",
        "positive": "AJUS50",
        "negative": "DESH50",
    },
    {
        "paycode": "HORA30",
        "positive": "AJUS30",
        "negative": "DESH50",
    }
]
can_debug_test_data = False  # warning only for local testing
