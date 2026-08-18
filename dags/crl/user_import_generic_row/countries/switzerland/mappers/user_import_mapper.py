# Switzerland User Import Mapper
# Two rows split by Functional Segment:
#   Row 1: IS functional segment  → CHE_OT Eligible  (has punch policy, 8 time-off types)
#   Row 2: All Except IS          → CHE_OT Not Eligible (no punch policy, 7 time-off types)
#
# Holiday Calendar is NOT in this mapper — it is driven by the holidayCalendarCode
# field from the SuccessFactors feed (canton-specific calendars: CHE_ZH, CHE_BS, CHE_TI, etc.)

user_import_mapper = [
    {
        # --- Matching Keys ---
        "location_level_1": "CHE",
        "location_level_2": "All",
        "location_level_3": "All",
        "company_code": "All",
        "reg_temp": "All",
        "pay_type": "All",
        "full_part": "All",
        "activity_type": "All",
        "functional_segment": "IS",

        # --- Output Keys ---
        "employee_type": "CHE_OT Eligible",
        "punch_policy": "CHE_OT_Eligible Punch Policy",
        "timesheet_template": "CHE_OT_Eligible",
        "timesheet_approval_path": "Supervisor",
        "timesheet_period": "CHE - Weekly starting on Monday",
        "time_off_template": "Time Off",
        "time_off_approver": "Supervisor",
        "schedule_type": "Office Schedule",
        "schedule_policy": "Default Shift Schedule Policy",
        "overtime_requests_template": "NA",
        "overtime_request_approval_path": "NA",
        "holiday_calendar": "",  # driven by holidayCalendarCode from feed
        "timezone": "(UTC+1:00) Central Europe Standard Time",
        "work_week": "Monday to Sunday",
        "payrule_name": "CHE_OT_Eligible",
        "default_schedule": "CHES140",
        "overtime_eligible": True,
    },
    {
        # --- Matching Keys ---
        "location_level_1": "CHE",
        "location_level_2": "All",
        "location_level_3": "All",
        "company_code": "All",
        "reg_temp": "All",
        "pay_type": "All",
        "full_part": "All",
        "activity_type": "All",
        "functional_segment": "All Except IS",

        # --- Output Keys ---
        "employee_type": "CHE_OT Not Eligible",
        "punch_policy": "NA",
        "timesheet_template": "CHE_OT_Not_Eligible",
        "timesheet_approval_path": "System Approval",
        "timesheet_period": "CHE - Weekly starting on Monday",
        "time_off_template": "Time Off",
        "time_off_approver": "Supervisor",
        "schedule_type": "Office Schedule",
        "schedule_policy": "Default Shift Schedule Policy",
        "overtime_requests_template": "NA",
        "overtime_request_approval_path": "NA",
        "holiday_calendar": "",  # driven by holidayCalendarCode from feed
        "timezone": "(UTC+1:00) Central Europe Standard Time",
        "work_week": "Monday to Sunday",
        "payrule_name": "CHE_OT_Not_Eligible",
        "default_schedule": "CHES140",
        "overtime_eligible": False,
    },
]
