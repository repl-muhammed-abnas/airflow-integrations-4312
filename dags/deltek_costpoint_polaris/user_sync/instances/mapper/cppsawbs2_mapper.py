# pylint: disable = line-too-long

user_mapper = [
    {
        "mapping_criteria": {
            "employee_status": ["ACT","IN"],
            "country": ["USA"],
            "account": [],
            "employee_type": [],
            "general_labor_category": [],
            "labor_location": [],
            "organizations": []
        },
        "holiday_calendar": "United States",
        "office_schedule": "8 hours/day; Mon-Fri",
        "timesheet_approval_path": "Supervisor",
        "timeoff_approval_path": "Supervisor",
        "expenses_approval_path": "",  
        "timeZone": "urn:replicon:time-zone:etc-gmt",
        "workweek": "urn:replicon:day-of-week:sunday",
        "timesheet_period_type": "urn:replicon:timesheet-period-type:system",
        "user_permission": "Project Resource with Reports",
        "supervisor_permission": "Supervisor",
        "timesheet_template": "Simpletime user Timesheet Template",
        "time_off_template": "Time Off",
        "activities": ["General Admin", "Training"],
        "default_password": "Replicon@12",
        "authenticationtype": "replicon",
        "payrule": "Alaska",
        "punchentrypolicy": "All Devices Access"
    }
]
