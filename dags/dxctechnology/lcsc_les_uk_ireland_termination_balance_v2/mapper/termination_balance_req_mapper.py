UK_ES_TIMEOFF_TYPES = [
    {
        "leave_type": "[UK] Annual Leave",
        "wage_code": "2011",
        "info_type": "0015",
        "measurement_unit": "010"
    },
    {
        "leave_type": "[UK] P/T Annual Leave Hrs",
        "wage_code": "2013",
        "info_type": "0015",
        "measurement_unit": "001"
    }
]

IE_ES_TIMEOFF_TYPES = [
    {
        "leave_type": "[IRL] Annual Leave",
        "wage_code": "2010",
        "info_type": "2010",
        "measurement_unit": "010"
    },
    {
        "leave_type": "[IRL] P/T Annual Leave Hrs",
        "wage_code": "2020",
        "info_type": "2010",
        "measurement_unit": "001"
    }
]

UK_CSC_TIMEOFF_TYPES = [
    {
        "leave_type": "[UK] Annual Leave",
        "wage_code": "2504",
        "info_type": "2010",
        "measurement_unit": "010"
    },
    {
        "leave_type": "[UK] P/T Annual Leave Hrs",
        "wage_code": "2503",
        "info_type": "2010",
        "measurement_unit": "001"
    }
]

IE_CSC_TIMEOFF_TYPES = [
    {
        "leave_type": "[IRL] Annual Leave",
        "wage_code": "2401",
        "info_type": "2010",
        "measurement_unit": "010"
    },
    {
        "leave_type": "[IRL] P/T Annual Leave Hrs",
        "wage_code": "2400",
        "info_type": "2010",
        "measurement_unit": "001"
    }
]

# Encryption is set to False for testing, have to be updated while deploying to UAT
TERMINATION_BALANCE_REQ_DATA = [
    {
        "region": "LES",
        "location": "United Kingdom",
        "location_code": "GB",
        "users_report_name": "UK User Details - Termination Balances",
        "termination_balance_report_name": "UK Ireland Timeoff Termination Balances",
        "timeoff_types": UK_ES_TIMEOFF_TYPES,
        "sequence_no": "01",
        "encrypt": True
    },
    {
        "region": "LCSC",
        "location": "United Kingdom",
        "location_code": "GB",
        "users_report_name": "UK User Details - Termination Balances",
        "termination_balance_report_name": "UK Ireland Timeoff Termination Balances",
        "timeoff_types": UK_CSC_TIMEOFF_TYPES,
        "sequence_no": "02",
        "encrypt": True
    },
    {
        "region": "LES",
        "location": "Ireland",
        "location_code": "IE",
        "users_report_name": "Ireland User Details - Termination Balances",
        "termination_balance_report_name": "UK Ireland Timeoff Termination Balances",
        "timeoff_types": IE_ES_TIMEOFF_TYPES,
        "sequence_no": "01",
        "encrypt": True
    },
    {
        "region": "LCSC",
        "location": "Ireland",
        "location_code": "IE",
        "users_report_name": "Ireland User Details - Termination Balances",
        "termination_balance_report_name": "UK Ireland Timeoff Termination Balances",
        "timeoff_types": IE_CSC_TIMEOFF_TYPES,
        "sequence_no": "02",
        "encrypt": True
    }
]
