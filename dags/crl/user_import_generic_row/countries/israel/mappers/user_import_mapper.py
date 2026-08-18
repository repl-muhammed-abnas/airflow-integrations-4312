# Israel User Import Mapper
# Single row — all Israel employees map to same configuration
# Matching: ISR location, All other dimensions

user_import_mapper = [
    {
        # --- Matching Keys ---
        "location_level_1": "ISR",
        "location_level_2": "All",
        "location_level_3": "All",
        "company_code": "All",
        "reg_temp": "Regular",
        "pay_type": "Salaried",
        "full_part": "Full-Time",
        "activity_type": "All",

        # --- Output Keys ---
        "employee_type": "ISR_Salaried",
        "punch_policy": "NA",
        "timesheet_template": "ISR_Salaried",
        "timesheet_approval_path": "System Approval",
        "timesheet_period": "ISR - Weekly starting on Sunday",
        "time_off_template": "Time Off",
        "time_off_approver": "Supervisor",
        "schedule_type": "Office Schedule",
        "schedule_policy": "Default Shift Schedule Policy",
        "overtime_requests_template": "NA",
        "overtime_request_approval_path": "NA",
        "holiday_calendar": "ISR_Holiday Calendar",
        "timezone": "(UTC+2:00) Israel Standard Time",
        "work_week": "Sunday - Saturday",
        "payrule_name": "ISR_Payrule",
        "default_schedule": "S140501",
        "overtime_eligible": False,
    }
]
