region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14

employee_required_fields = 'uuid, company_uuid , code, first_name, last_name, class, union, local, active, pay_interval'

paytype_employeetype_map = {
    'H': 'Hourly',
    'S': 'Salaried'
}
initial_password = 'Replicon@123'

ce_time_format = '%Y-%m-%dT%H:%M:%SZ'

user_configuration = [
    {
        'timesheettemplate': 'Project Time Entry With Billing',
        'timesheetperiod': 'Weekly starting on Monday',
        'timesheetapprovalpath': 'Supervisor',
        'workweek': 'urn:replicon:day-of-week:monday',
        'timezone': 'urn:replicon:time-zone:america-new-york',
        'permissions': [
            'Project Resource with Reports',
        ]
    }
]
