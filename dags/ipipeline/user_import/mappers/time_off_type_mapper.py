"""
iPipeline User Import - Time Off Type Mapper

Complete time off policies matrix from Excel file with exact values preserved.
All 34 policies with accrual conditions, location/level filtering, and approval paths.

INCLUDE/EXCLUDE LIST LOGIC:
==========================

Field Structure:
- location_level_1: Exact match required (USA, UK, Canada, Japan, ALL)
- location_level_2_to_include: List of locations to include (empty = match all)
- location_level_2_to_exclude: List of locations to exclude (empty = exclude "")
- employee_levels_to_include: List of specific employee levels to include
- employee_levels_to_exclude: List of employee levels to exclude

Matching Logic:
- Empty include list = "match all values"
- Empty exclude list = "exclude nothing" 
- If include list has values, actual value MUST be in include list
- If exclude list has values, actual value must NOT be in exclude list

ACCRUAL CONDITIONS:
==================
- tenure_rules: List of service year tiers with min_years, max_years, rate, limitation_hours
- fte_prorated: Boolean for FTE proration
- policy_updates_required: Boolean for policy updates
"""

# Complete Time Off Types Matrix - ALL 34 policies with exact Excel values
TIME_OFF_TYPE_MAPPER = [
    # USA Vacation - Excluding California, All Levels
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": ["California"],
        "employee_levels_to_include": ["M1", "M2", "M3", "M4", "P1", "P2", "P3", "P4", "P5", "S1", "S2", "S3", "S4", "M5", "M6", "E7", "E9"],
        "employee_levels_to_exclude": [],
        "time_off_type": "USA _Vacation",
        "time_off_type_group": "Vacation/Holiday",
        "time_off_type_approval_path": "iPipeline US",
        "paycode": "VACPY",
        "visible_to_employees": True,
        "accrual_conditions": {
            "tenure_rules": [
                {"min_years": 0, "max_years": 7, "rate": 5, "limitation_hours": 120},
                {"min_years": 7, "max_years": 999, "rate": 6.67, "limitation_hours": 160}
            ],
            "fte_prorated": True,
            "policy_updates_required": True
        },
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "1-Jan",
        "over_draw": "Over Draw validation if the expected yearly entitlement is coming in negetive",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # USA California Vacation
    {
        "location_level_1": "United States",
        "location_level_2_to_include": ["California"],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "USA_California _Vacation",
        "time_off_type_group": "Vacation/Holiday",
        "time_off_type_approval_path": "iPipeline US",
        "paycode": "VACPY",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "1-Jan",
        "over_draw": "Over Draw validation if the expected yearly entitlement is coming in negetive",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # UK Holiday
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "UK _Holiday",
        "time_off_type_group": "Vacation/Holiday",
        "time_off_type_approval_path": "iPipeline UK",
        "paycode": "UKVAC",
        "visible_to_employees": True,
        "accrual_conditions": {
            "tenure_rules": [{"min_years": 0, "max_years": 999, "rate": 15.625, "limitation_hours": 187.5}],
            "fte_prorated": True,
            "policy_updates_required": True
        },
        "carry_forward": "3",
        "carry_forward_expiry": "1-Apr",
        "time_off_reset": "1-Jan",
        "over_draw": "Over Draw validation if the expected yearly entitlement is coming in negetive",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Canada Vacation - Complex service year tiers
    {
        "location_level_1": "Canada",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Canada_Vacation",
        "time_off_type_group": "Vacation/Holiday",
        "time_off_type_approval_path": "iPipeline Canada",
        "paycode": "CAVAC",
        "visible_to_employees": True,
        "accrual_conditions": {
            "tenure_rules": [
                {"min_years": 0, "max_years": 3, "rate": 5, "limitation_hours": 120},
                {"min_years": 3, "max_years": 4, "rate": 5.66, "limitation_hours": 136},
                {"min_years": 4, "max_years": 5, "rate": 6, "limitation_hours": 144},
                {"min_years": 5, "max_years": 6, "rate": 6.33, "limitation_hours": 152},
                {"min_years": 6, "max_years": 7, "rate": 6.66, "limitation_hours": 160},
                {"min_years": 7, "max_years": 8, "rate": 7, "limitation_hours": 168},
                {"min_years": 8, "max_years": 9, "rate": 7.33, "limitation_hours": 176},
                {"min_years": 9, "max_years": 10, "rate": 7.66, "limitation_hours": 184},
                {"min_years": 10, "max_years": 11, "rate": 8, "limitation_hours": 192},
                {"min_years": 11, "max_years": 999, "rate": 8.33, "limitation_hours": 200}
            ],
            "fte_prorated": True,
            "policy_updates_required": True
        },
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "1-Jan",
        "over_draw": "Over Draw validation if the expected yearly entitlement is coming in negetive",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Japan Vacation
    {
        "location_level_1": "Japan",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Japan_Vacation",
        "time_off_type_group": "Vacation/Holiday",
        "time_off_type_approval_path": "iPipeline Japan",
        "paycode": "JPVAC",
        "visible_to_employees": True,
        "accrual_conditions": {
            "tenure_rules": [{"min_years": 0, "max_years": 999, "rate": 13.33, "limitation_hours": 160}],
            "fte_prorated": True,
            "policy_updates_required": True
        },
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "1-Jan",
        "over_draw": "Over Draw validation if the expected yearly entitlement is coming in negetive",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # USA Illness/Sick
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "USA_Illness/Sick",
        "time_off_type_group": "Illness/Sick",
        "time_off_type_approval_path": "iPipeline US",
        "paycode": "SICK",
        "visible_to_employees": True,
        "accrual_conditions": {
            "tenure_rules": [
                {"min_years": 0, "max_years": 2, "rate": 1.66, "limitation_hours": 40},
                {"min_years": 2, "max_years": 999, "rate": 2, "limitation_hours": 48}
            ],
            "fte_prorated": True,
            "policy_updates_required": True
        },
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "1-Jan",
        "over_draw": "Over Draw validation if the expected yearly entitlement is coming in negetive",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # UK Illness/Sick - Complex service year tiers
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "UK_Illness/Sick",
        "time_off_type_group": "Illness/Sick",
        "time_off_type_approval_path": "iPipeline UK",
        "paycode": "UKSICK",
        "visible_to_employees": True,
        "accrual_conditions": {
            "tenure_rules": [
                {"min_years": 0, "max_years": 2, "rate": 5, "limitation_hours": 45},
                {"min_years": 2, "max_years": 3, "rate": 7.5, "limitation_hours": 90},
                {"min_years": 3, "max_years": 999, "rate": 12.5, "limitation_hours": 120}
            ],
            "fte_prorated": True,
            "policy_updates_required": True
        },
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "1-Jan",
        "over_draw": "Over Draw validation if the expected yearly entitlement is coming in negetive",
        "validation": "Probation 119 days",
        "leave_type": "Hours"
    },
    
    # Global Volunteer
    {
        "location_level_1": "ALL",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Global_Volunteer",
        "time_off_type_group": "Volunteer",
        "time_off_type_approval_path": "TS_SUPERVISOR_APPROVAL",
        "paycode": "VOLUN",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "1-Jan",
        "over_draw": "",
        "validation": "",
        "leave_type": "Day"
    },
    
    # USA Personal (excluding California)
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": ["California"],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "USA_Personal",
        "time_off_type_group": "Personal",
        "time_off_type_approval_path": "iPipeline US",
        "paycode": "PERSN",
        "visible_to_employees": True,
        "accrual_conditions": {
            "tenure_rules": [{"min_years": 1, "max_years": 999, "rate": 0.66, "limitation_hours": 16}],
            "fte_prorated": True,
            "policy_updates_required": True
        },
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Canada Personal
    {
        "location_level_1": "Canada",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Canada_Personal",
        "time_off_type_group": "Personal",
        "time_off_type_approval_path": "iPipeline Canada",
        "paycode": "CADPERSN",
        "visible_to_employees": True,
        "accrual_conditions": {
            "tenure_rules": [{"min_years": 1, "max_years": 999, "rate": 3.33, "limitation_hours": 80}],
            "fte_prorated": True,
            "policy_updates_required": True
        },
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # UK Time off for Dependants
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "UK_Time off for Dependants",
        "time_off_type_group": "Time off for Dependants",
        "time_off_type_approval_path": "iPipeline UK",
        "paycode": "UKDEP",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "No Carry over",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # UK Bank Holidays Owed
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "UK_Bank Holidays Owed",
        "time_off_type_group": "Bank Holidays Owed",
        "time_off_type_approval_path": "iPipeline UK",
        "paycode": "UKHOL",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Canada Bereavement
    {
        "location_level_1": "Canada",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Canada_Berevement",
        "time_off_type_group": "Berevement",
        "time_off_type_approval_path": "iPipeline Canada",
        "paycode": "CABER",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Japan Bereavement
    {
        "location_level_1": "Japan",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Japan_Berevement",
        "time_off_type_group": "Berevement",
        "time_off_type_approval_path": "iPipeline Japan",
        "paycode": "JPBER",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "No Carry over",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # USA Bereavement
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "USA_Berevement",
        "time_off_type_group": "Berevement",
        "time_off_type_approval_path": "iPipeline US",
        "paycode": "BEREA",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "No Carry over",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # UK Compassionate leave
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "UK_Compassionate leave",
        "time_off_type_group": "Compassionate leave",
        "time_off_type_approval_path": "iPipeline UK",
        "paycode": "UKCOMP",
        "visible_to_employees": False,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Global Early Office Closing
    {
        "location_level_1": "ALL",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Global_Early Office Closing",
        "time_off_type_group": "Early Office Closing",
        "time_off_type_approval_path": "TS_SUPERVISOR_APPROVAL",
        "paycode": "EARLY",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "No Carry over",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Canada Summer Hours
    {
        "location_level_1": "Canada",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Canada_Summer Hours",
        "time_off_type_group": "Summer Hours",
        "time_off_type_approval_path": "iPipeline Canada",
        "paycode": "CADSUMM",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # UK Flexible Time Off
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "UK_Flexible Time Off",
        "time_off_type_group": "Flexible Time Off",
        "time_off_type_approval_path": "iPipeline UK",
        "paycode": "UKFLEX",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Global Jury Duty
    {
        "location_level_1": "ALL",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Global_Jury Duty",
        "time_off_type_group": "Jury Duty",
        "time_off_type_approval_path": "TS_SUPERVISOR_APPROVAL",
        "paycode": "JURY",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "No Carry over",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # USA Unpaid Hourly
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "USA_Unpaid_Hourly",
        "time_off_type_group": "Unpaid",
        "time_off_type_approval_path": "TS_SUPERVISOR_APPROVAL",
        "paycode": "UNPTO-H",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_USA_Unpaid Leave (HR-managed)
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_USA_Unpaid Leave",
        "time_off_type_group": "Unpaid",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "UNPTO",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_USA_Short Term Disability (HR-managed)
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_USA_Short Term Disability",
        "time_off_type_group": "Short Term Disability",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "STD",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_UK_Parental Leave (HR-managed)
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_UK_Parental Leave",
        "time_off_type_group": "Parental Leave",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "SMP",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_USA_Bonding Leave (HR-managed)
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_USA_Bonding Leave",
        "time_off_type_group": "Bonding Leave",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "BOND",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_UK_Adoptive Leave (HR-managed)
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_UK_Adoptive Leave",
        "time_off_type_group": "Adoptive Leave",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "UKADOPT",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_UK_Antenatal (HR-managed)
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_UK_Antenatal",
        "time_off_type_group": "Antenatal",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "UKANT",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # UK Appointment
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "UK_Appointment",
        "time_off_type_group": "Appointment",
        "time_off_type_approval_path": "iPipeline UK",
        "paycode": "UKAPPT",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_UK_Career Break (HR-managed)
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_UK_Career Break",
        "time_off_type_group": "Career Break",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "UKCAREER",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_UK_Paternity Leave (HR-managed)
    {
        "location_level_1": "United Kingdom",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_UK_Paternity Leave",
        "time_off_type_group": "Paternity Leave",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "UPAT",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_USA_Maternity Leave (HR-managed)
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_USA_Maternity Leave",
        "time_off_type_group": "Maternity Leave",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "MAT",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # USA Summer Hours
    {
        "location_level_1": "United States",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "USA_Summer Hours",
        "time_off_type_group": "Summer Hours",
        "time_off_type_approval_path": "iPipeline US",
        "paycode": "SUMMER",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_Canada_Pregnancy/Parental/Maternity Leave (HR-managed)
    {
        "location_level_1": "Canada",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_Canada_Pregnancy/Parental/Maternity Leave",
        "time_off_type_group": "Maternity Leave",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "OFFPAYROLL",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    },
    
    # Z_Canada_Leave Other (HR-managed)
    {
        "location_level_1": "Canada",
        "location_level_2_to_include": [],
        "location_level_2_to_exclude": [],
        "employee_levels_to_include": [],
        "employee_levels_to_exclude": [],
        "time_off_type": "Z_Canada_Leave Other",
        "time_off_type_group": "Other",
        "time_off_type_approval_path": "HR Approval",
        "paycode": "OFFPAYROLL",
        "visible_to_employees": True,
        "accrual_conditions": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "time_off_reset": "",
        "over_draw": "",
        "validation": "",
        "leave_type": "Hours"
    }
]