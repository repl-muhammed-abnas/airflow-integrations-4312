# Version 1.6 - Updated Location Codes, Time Off Types and Timesheet Template Logic
# Date: 9th October 2025
# Version 1.7 - Phase 2 Netherlands implementation: added location code 0835_1000
# Date: 30th July 2026

# External employee type codes for determining template assignment
EXTERNAL_EMPLOYEE_TYPE_CODES = ["C", "8", "7", "9"]

location_wise_data = {
    "0800_D101": {  # Ireland
        "timeoff_types": [
            "Vacation_A_0010_IE",
            "Unassigned_A_0035_IE",
            "Flex workingtime schedule _A_0039_IE",
            "Employee Training_A_0049_IE",
            "Sick - w/o certificate_A_0210_IE",
            "Chargeable Hours_A_0800_IE",
            "Office Closure_A_0017_IE",
            "Paid Leave_A_0040_IE",
            "Sick - with certificate_A_0200_IE",
            "Doctor visits_A_0230_IE",
            "Expatriation_A_0306_IE",
            "Unpaid leave_A_0601_IE",
            "Parents leave_A_0602_IE",
            "Parental leave_A_0604_IE",
            "Maternity Leave_A_0605_IE",
            "Maternity leave (unpaid)_A_0606_IE",
            "Paternity leave_A_I600_IE",
            "Sick - Long Term (Paid)_A_I612_IE",
            "Sick - Long Term (Unpaid)_A_I613_IE",
            "Jury Service_A_I614_IE",
            "Compassionate Leave_A_I615_IE",
            "Force Majeure leave_A_I620_IE",
            "Time in Lieu_A_I621_IE"
        ],
        "timesheet_template_internal": "[Ireland] Agile-In/Out with Time Distribution_Internal",
        "timesheet_template_external": "[Ireland] Agile-In/Out with Time Distribution_External"
    },
    "0870_N101": {  # Norway
        "timeoff_types": [
            "Holiday_A_0010_NO",
            "Birth of Child_A_0024_NO",
            "Reduction Overtime_A_0030 _NO",
            "Unassigned_A_0035_NO",
            "Short Term Paid LOA (40%)_A_0039_NO",
            "Illness with certificate_A_0200_NO",
            "Illness w/o certificate_A_0210_NO",
            "Doctor visits_A_0230_NO",
            "Child Illness_A_0236_NO",
            "Child Illness unpaid_A_0290_NO",
            "Short term Unpaid Leave_A_0600_NO",
            "Chargeable Hours_A_0800_NO",
            "Approved absence_A_N028_NO",
            "Special leave_A_0304_NO",
            "Expatriation_A_0306_NO",
            "Unpaid absences_A_0601_NO",
            "Parental leave_A_0603_NO",
            "Pregnancy holiday_A_0605_NO"
        ],
        "timesheet_template_internal": "[Norway] Agile-In/Out with Time Distribution_Internal",
        "timesheet_template_external": "[Norway] Agile-In/Out with Time Distribution_External"
    },
    "0835_1000": {  # Netherlands
        "timeoff_types": [],
        "timesheet_template_internal": "[Netherland] Agile-In/Out with Time Distribution_Internal",
        "timesheet_template_external": "[Netherland] Agile-In/Out with Time Distribution_External"
    }
}
