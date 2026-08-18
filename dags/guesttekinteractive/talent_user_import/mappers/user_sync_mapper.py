"""
User Sync Mapper - GuestTek Talent User Import Integration

Maps (Location, Department Code) combinations to Replicon configuration settings.
This mapper contains 832 rows (26 locations × ~32 departments) with settings for:
    - License assignment (TimeOff Enterprise, Workforce Management)
    - Timesheet Template
    - Time Off Types (pipe-delimited)
    - Holiday Calendar
    - Schedule Type
    - Overtime Request Template (optional)
    - Payrule

Mapper Key Format:
    (location_name, department_code) -> dict of Replicon settings

Usage:
    from guesttekinteractive.talent_user_import.mappers.user_sync_mapper import LOCATION_DEPARTMENT_MAPPER, get_mapper_settings
    settings = get_mapper_settings(location_name, department_code)
"""

# Key: (location_name, department_code) tuple
# Value: dict with Replicon assignment settings

LOCATION_DEPARTMENT_MAPPER = {
    ("Australia", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Australia"
    },
    ("Australia", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Australia"
    },
    ("Australia", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Australia"
    },
    ("Australia", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|AUS Vacation Leave|AUS Sick Leave|AUS Unpaid Carer's Leave|AUS Compassionate Leave/Bereavement Leave",
        "holiday_calendar": "Australia",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Australia"
    },
    ("Brazil", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Brazil"
    },
    ("Brazil", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Brazil"
    },
    ("Brazil", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Brazil"
    },
    ("Brazil", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Vacation Leave",
        "holiday_calendar": "Brazil",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Brazil"
    },
    ("Canada", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Canada Federal Overtime"
    },
    ("Canada", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Alberta - Tiered OT"
    },
    ("Canada", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Alberta - Tiered OT"
    },
    ("Canada", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|CDN Bereavement Leave|CDN Sick Leave|CDN Vacation Leave|CDN Voting| CDN Statutory Holiday",
        "holiday_calendar": "Canada",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Alberta - Tiered OT"
    },
    ("Dubai", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "UAE - Dubai"
    },
    ("Dubai", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|UAE Vacation Leave|UAE Unpaid Leave|UAE Other (Manager Approved)|UAE Sick Leave|UAE Sick Leave",
        "holiday_calendar": "Dubai",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "UAE - Dubai"
    },
    ("Egypt", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Egypt - Standard"
    },
    ("Egypt", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Egypt - CC"
    },
    ("Egypt", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Egypt - CC"
    },
    ("Egypt", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|ET Vacation Leave|ET Day On Demand|ET Sick Leave|ET Unpaid Leave|ET Other (Manager Approved)|ET Statutory Holiday",
        "holiday_calendar": "Egypt",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Egypt - CC"
    },
    ("France", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "France"
    },
    ("France", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "France"
    },
    ("France", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "France"
    },
    ("France", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|FR Vacation Leave/Vacances|FR Sick Leave/Conge Maladie|FR RTT/RTT|FR Other (Manager Approved)/TOIL",
        "holiday_calendar": "France",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "France"
    },
    ("Germany", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Germany"
    },
    ("Germany", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Germany"
    },
    ("Germany", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Germany"
    },
    ("Germany", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|DE Vacation Leave/Urlaubsanspruch|DE Sick Leave/Krankenstand|DE Special Unpaid Leave/Gesonderter Unbezahlter Urlaub",
        "holiday_calendar": "Germany",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Germany"
    },
    ("Guatemala", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave|GUAT Vacation Leave |GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave CC|GUAT Vacation Leave CC|GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave CC|GUAT Vacation Leave CC|GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Guatemala - Tiered OT"
    },
    ("Guatemala", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GUAT Bereavement Leave|GUAT Sick Leave CC|GUAT Vacation Leave CC|GUAT Statutory Holiday",
        "holiday_calendar": "Guatemala",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Guatemala - Tiered OT"
    },
    ("Hong Kong", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Guatemala - Tiered OT"
    },
    ("Hong Kong", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Hong Kong"
    },
    ("Hong Kong", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|HK Vacation Leave|HK Sick Leave|HK Other (Manager Approved)",
        "holiday_calendar": "Hong Kong",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Hong Kong"
    },
    ("India - Gurgaon", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Haryana"
    },
    ("India - Gurgaon", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday| Gu Other(Manager Approved)",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "India Gurgaon - CC"
    },
    ("India - Gurgaon", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday| Gu Other(Manager Approved)",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "India Gurgaon - CC"
    },
    ("India - Gurgaon", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GU Sick Leave|GU Casual Leave|GU Vacation Leave|GU Statutory Holiday| Gu Other(Manager Approved)",
        "holiday_calendar": "India - Gurgaon",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "India Gurgaon - CC"
    },
    ("India - Mumbai", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Maharashtra"
    },
    ("India - Mumbai", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|MU Vacation Leave| MU Maternity Leave",
        "holiday_calendar": "India - Mumbai",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Maharashtra"
    },
    ("Indonesia", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Indonesia"
    },
    ("Indonesia", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Indonesia"
    },
    ("Indonesia", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Indonesia"
    },
    ("Indonesia", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|RI Vacation Leave|RI Sick Leave",
        "holiday_calendar": "Indonesia",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Indonesia"
    },
    ("Japan", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Japan"
    },
    ("Japan", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Japan"
    },
    ("Japan", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Japan"
    },
    ("Japan", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|JA Vacation Leave|JA Bereavement Leave|JA Nursing Care Leave|JA Maternity Leave|JA Paternity Leave",
        "holiday_calendar": "Japan",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Japan"
    },
    ("Malaysia", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Malaysia"
    },
    ("Malaysia", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Malaysia"
    },
    ("Malaysia", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Malaysia"
    },
    ("Malaysia", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|MAL Vacation Leave|MAL Sick Leave|MAL Maternity Leave|MAL Unpaid Leave",
        "holiday_calendar": "Malaysia",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Malaysia"
    },
    ("Malta", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Blank"
    },
    ("Malta", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Blank"
    },
    ("Malta", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Blank"
    },
    ("Malta", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Blank",
        "holiday_calendar": "Blank",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Blank"
    },
    ("Mexico", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Mexico"
    },
    ("Mexico", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Mexico"
    },
    ("Mexico", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Mexico"
    },
    ("Mexico", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|MAX Sick Leave|MEX Vacation Leave|MEX Statutory Holiday| Days in Lieu",
        "holiday_calendar": "Mexico",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Mexico"
    },
    ("N'lands", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Netherlands"
    },
    ("N'lands", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Netherlands"
    },
    ("N'lands", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Netherlands"
    },
    ("N'lands", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|NL Vacation Leave|NL Sick Leave|NL Other(Manager Approved)",
        "holiday_calendar": "",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Netherlands"
    },
    ("Philippines", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Philippines"
    },
    ("Philippines", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Philippines"
    },
    ("Philippines", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Philippines"
    },
    ("Philippines", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|RP Vacation Leave|RP Sick Leave",
        "holiday_calendar": "Philippines",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Philippines"
    },
    ("Poland", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Poland - Standard"
    },
    ("Poland", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Poland - Standard"
    },
    ("Poland", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Poland - Standard"
    },
    ("Poland", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|PL Vacation Leave Junior/Urlop|Pl Sick Leave/Chorobowy|PL Special Leave/Okolicznosciowy|PL Family Member Care Leave/Bezplatna Opieka|PL Force Majeure Leave/Sila Wyzsza|Pl Vacation Leave Senior/Urlop",
        "holiday_calendar": "Poland",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Poland - Standard"
    },
    ("Singapore", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Singapore"
    },
    ("Singapore", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Singapore"
    },
    ("Singapore", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Singapore"
    },
    ("Singapore", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|SGP Vacation Leave|SGP Sick Leave|SGP Other (Manager Approved)",
        "holiday_calendar": "Singapore",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Singapore"
    },
    ("Spain", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Europe Timesheet (Spain & Poland)",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Spain"
    },
    ("Spain", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Spain"
    },
    ("Spain", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Spain"
    },
    ("Spain", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|ES Vacation Leave/Vacaciones Retribuidas|ES Sick Leave/Baja por Enfermedad",
        "holiday_calendar": "Spain",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Spain"
    },
    ("Thailand", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Thailand"
    },
    ("Thailand", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Thailand - Call Center"
    },
    ("Thailand", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Thailand - Call Center"
    },
    ("Thailand", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|TH Vacation Leave|TH Sick Leave|TH Unpaid Leave|TH Other (Manager Approved)|TH Statutory Holiday",
        "holiday_calendar": "Thailand",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Thailand - Call Center"
    },
    ("Turkey", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "Turkey - Standard"
    },
    ("Turkey", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey CC Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Turkey - CC"
    },
    ("Turkey", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey CC Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Turkey - CC"
    },
    ("Turkey", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Turkey CC Standard Timesheet",
        "time_off_types": "Holiday|TR Vacation Leave|TR Sick Leave|TR Unpaid Leave|TR Other (Manager Approved)|TR Statutory Holiday",
        "holiday_calendar": "Turkey",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "Turkey - CC"
    },
    ("UK", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "United Kingdom"
    },
    ("UK", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "United Kingdom"
    },
    ("UK", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "United Kingdom"
    },
    ("UK", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|GB Vacation Leave|GB Sick Leave",
        "holiday_calendar": "UK",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "United Kingdom"
    },
    ("USA", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("USA", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6374"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5050"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5107"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "7020"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6190"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "7000"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5026"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5200"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6870"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6872"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5030"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5040"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6878"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "7018"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "7019"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5022"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5017"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5019"): {
        "license": "TimeOff Enterprise|Workforce Management|TimeBill Plus",
        "timesheet_template": "Project Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6370"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6570"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6580"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6372"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6470"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "6476"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "7233"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5023"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5024"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "8 Hours/day; Mon-Fri",
        "overtime_template": None,
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5025"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5027"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
    ("Aruba", "5028"): {
        "license": "TimeOff Enterprise|Workforce Management",
        "timesheet_template": "CC Standard Timesheet",
        "time_off_types": "Holiday|USA Bereavement Leave|USA Sick Leave Days|USA Vacation Leave|USA Voting|USA Statutory Leave",
        "holiday_calendar": "USA",
        "schedule_type": "Shift Schedule",
        "overtime_template": "Overtime Request Template",
        "payrule": "U.S. Federal Overtime (FLSA Coverage)"
    },
}

# Default settings when no mapper match found (should log exception)
DEFAULT_SETTINGS = {
    "license": "TimeOff Enterprise",
    "timesheet_template": "Standard Timesheet",
    "time_off_types": "Holiday|Vacation",
    "holiday_calendar": "Canada",
    "schedule_type": "8 Hours/day; Mon-Fri",
    "overtime_template": None,
    "payrule": "Canada"
}


def get_mapper_settings(location_name, department_code):
    """
    Get Replicon settings for a location/department combination.
    
    Args:
        location_name (str): Location name from Talent API (user_location_name)
        department_code (str): Department code from Talent API (org_level_code)
        
    Returns:
        dict: Replicon settings dict or None if not found
        
    Note:
        Returns None if combination not found - caller should log exception and skip user
    """
    if not location_name or not department_code:
        return None
    
    # Try exact match
    key = (location_name, department_code)
    if key in LOCATION_DEPARTMENT_MAPPER:
        return LOCATION_DEPARTMENT_MAPPER[key]
    
    # Try case-insensitive match
    location_lower = location_name.lower().strip()
    dept_lower = department_code.lower().strip()
    
    for (loc, dept), settings in LOCATION_DEPARTMENT_MAPPER.items():
        if loc.lower().strip() == location_lower and dept.lower().strip() == dept_lower:
            return settings
    
    # No match found
    return None


def is_valid_mapper_key(location_name, department_code):
    """
    Check if a location/department combination exists in the mapper.
    
    Args:
        location_name (str): Location name from Talent API
        department_code (str): Department code from Talent API
        
    Returns:
        bool: True if combination exists in mapper
    """
    return get_mapper_settings(location_name, department_code) is not None


def get_licenses_for_user(location_name, department_code):
    """
    Get license URIs to assign for a user based on mapper settings.
    
    Args:
        location_name (str): Location name from Talent API
        department_code (str): Department code from Talent API
        
    Returns:
        list: List of license URIs to assign
    """
    settings = get_mapper_settings(location_name, department_code)
    if not settings:
        return []
    
    license_str = settings.get("license", "")
    licenses = []
    
    if "TimeOff Enterprise" in license_str:
        licenses.append("urn:replicon-saas:product:time-off-enterprise")
    if "Workforce Management" in license_str:
        licenses.append("urn:replicon-saas:product:wfm-enterprise")
    
    return licenses


def get_time_off_types_list(location_name, department_code):
    """
    Get time off types as a list for a user.
    
    Args:
        location_name (str): Location name from Talent API
        department_code (str): Department code from Talent API
        
    Returns:
        list: List of time off type names
    """
    settings = get_mapper_settings(location_name, department_code)
    if not settings:
        return []
    
    time_off_str = settings.get("time_off_types", "")
    if not time_off_str:
        return []
    
    return [t.strip() for t in time_off_str.split("|") if t.strip()]


def get_all_locations():
    """
    Get all unique location names from the mapper.
    
    Returns:
        set: Set of unique location names
    """
    return set(loc for loc, _ in LOCATION_DEPARTMENT_MAPPER.keys())


def get_all_departments():
    """
    Get all unique department codes from the mapper.
    
    Returns:
        set: Set of unique department codes
    """
    return set(dept for _, dept in LOCATION_DEPARTMENT_MAPPER.keys())
