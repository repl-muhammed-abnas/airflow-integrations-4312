"""
Costa Rica Specific Time-offs Mapper
This mapper defines special time-off types for Costa Rica region that need special handling.
"""

# Special time-offs that should be preserved during Workday updates
COSTA_RICA_PRESERVED_TIMEOFFS = [
    {
        "name": "[CR] Senority Vacation day",
        "description": "Special vacation days for employees hired on or before 01/11/2009",
        "eligibility_criteria": {
            "hire_date_threshold": "2009-11-01",  # Format: YYYY-MM-DD
            "applies_to": "existing_users_only",  # Not assigned to new users
            "annual_credit": 5,  # 5 days credited annually
            "credit_frequency": "anniversary",  # Credited on hire date anniversary
            "carryover": True,  # Unused days carry over to next year
        },
        "preserve_during_update": True,  # Don't remove during Workday updates
        "auto_assign_to_new_users": False,  # NEVER assign to new users
    }
]

def get_preserved_timeoff_names():
    return [timeoff["name"] for timeoff in COSTA_RICA_PRESERVED_TIMEOFFS]

def should_preserve_timeoff(timeoff_name):
    return timeoff_name in get_preserved_timeoff_names()

def get_seniority_vacation_config():
    for timeoff in COSTA_RICA_PRESERVED_TIMEOFFS:
        if timeoff["name"] == "[CR] Senority Vacation day":
            return timeoff
    return None