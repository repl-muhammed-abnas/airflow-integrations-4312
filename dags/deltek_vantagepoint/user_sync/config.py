region = 'us-east-1'
environment = 'pre-production'

tenant_email = "MPTeamReplicon@deltek.com"
internal_email = "MPTeamReplicon@deltek.com"
execution_timeout_days = 14

initial_run_flag = 'initial_run_flag'
employee_required_fields = 'HomeCompany,OrganizationName,Supervisor,Status,HireDate,TerminationDate,FirstName,LastName,PreferredName,EMail,BillingCategory,DefaultLC1,DefaultLC2,DefaultLC3,DefaultLC4,DefaultLC5,ChangeDefaultLC,YearsOtherFirms,PriorYearsFirm,PayType,State,Country,Locale,ModDate,ReadyForProcessing,TKGroup'
paytype_employeetype_map = {
    'H': 'Hourly',
    'S': 'Salaried'
}
initial_password = 'VantagepointUser123'

sync_users_by_status = ['A']
sync_users_not_allowed_for_use_in_processing = False

user_configuration = [
    {
        'timesheettemplate': 'Project Time Entry With Billing',
        'timesheetperiod': 'Weekly starting on Monday',
        'timesheetapprovalpath': 'Supervisor',
        'scheduletype': '8 hours/day, Su, Sa off',
        'workweek': 'urn:replicon:day-of-week:monday',
        'supervisorpermission': 'Supervisor - Operations',
        'timezone': 'urn:replicon:time-zone:america-new-york',
        'permissions': [
            'Basic User with Reports',
        ]
    }
]

YES = 'Y'
