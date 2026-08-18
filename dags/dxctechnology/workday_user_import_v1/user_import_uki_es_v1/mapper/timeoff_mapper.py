# UK&I CSC TimeOff Mapper
# Defines timeoff sets based on location, FTE, and shift type

TIMEOFF_SETS = {
    "UK_FT_NO_SHIFT": [
        "[UK] Annual Leave",
        "[UK] New Parents Appointment",
        "[UK] New Parents Appointment (unpaid)",
        "[UK] Time off for Dependents",
        "[UK] Jury Service",
        "[UK] Compassionate Leave",
        "[UK] Carers leave",
        "[UK] Civic activities",
        "[UK] Time off in lieu",
        "[UK] Competing in a major sporting event (Paid)",
        "[UK] Reserve Forces- Training & Volunatry (Paid)",
        "[UK] A/L Carryover",
        "[UK] Sickness Carry Over",
        "[UK] Leave of Absence carryover",
        "[UK] Bought A/L",
        "[UK] Term time type",
        "[UK] Charitable leave",
        "[UK] At Risk Leave",
        "[UK] Birthing Parent (Mat) Leave",
        "[UK] Parental Leave",
        "[UK] Adoption Leave",
        "[UK] Shared Parental Leave",
        "[UK] Sabbatical Leave",
        "[UK] Co-Parent (Pat) Leave",
        "[UK] Extended Sickness Absence",
        "[UK] Statutory Sick pay",
        "[UK] Nil Pay",
        "[UK] Leave for Medical Purpose",
        "[UK] Public Holiday",
        "[UK] Garden Leave",
        "[UK] Contractual Days",
        "[UK] Other Paid Leave",
        "[UK] Other Unpaid Leave",
        "[UK] Long term sickness Absence (OSP 50%)",
        "[UK] Reserve Forces-Military Service (Unpaid)",
        "[UK] Time off for Employee Representatives",
        "[UK] Trade Union Representatives",
        "[UK] Voluntary Public Duties",
        "[UK] Sold A/L",
        "[UK] Sickness Absence"
    ],
    
    "UK_PT_NO_SHIFT": [
        "[UK] P/T Bought A/L Hrs",
        "[UK] P/T Public Holiday Hrs",
        "[UK] P/T Annual Leave Hrs",
        "[UK] PT General CarryOver Hrs",
        "[UK] P/T Carry Over Hrs",
        "[UK] P/T Sickness Carry Over Hrs",
        "[UK] New Parents Appointment",
        "[UK] New Parents Appointment (unpaid)",
        "[UK] Time off for Dependents",
        "[UK] Jury Service",
        "[UK] Compassionate Leave",
        "[UK] Carers leave",
        "[UK] Civic activities",
        "[UK] Time off in lieu",
        "[UK] Competing in a major sporting event (Paid)",
        "[UK] Reserve Forces- Training & Volunatry (Paid)",
        "[UK] P/T Sold A/L Hrs",
        "[UK] Term time type",
        "[UK] Charitable leave",
        "[UK] At Risk Leave",
        "[UK] Birthing Parent (Mat) Leave",
        "[UK] Parental Leave",
        "[UK] Adoption Leave",
        "[UK] Shared Parental Leave",
        "[UK] Sabbatical Leave",
        "[UK] Co-Parent (Pat) Leave",
        "[UK] Extended Sickness Absence",
        "[UK] Statutory Sick pay",
        "[UK] Nil Pay",
        "[UK] Leave for Medical Purpose",
        "[UK] Public Holiday",
        "[UK] Garden Leave",
        "[UK] Contractual Days",
        "[UK] Other Paid Leave",
        "[UK] Other Unpaid Leave",
        "[UK] Long term sickness Absence (OSP 50%)",
        "[UK] Reserve Forces-Military Service (Unpaid)",
        "[UK] Time off for Employee Representatives",
        "[UK] Trade Union Representatives",
        "[UK] Voluntary Public Duties",
        "[UK] Sickness Absence"
    ],
    
    "UK_SHIFT": [
        "[UK] Annual Leave",
        "[UK] New Parents Appointment",
        "[UK] New Parents Appointment (unpaid)",
        "[UK] Time off for Dependents",
        "[UK] Jury Service",
        "[UK] Compassionate Leave",
        "[UK] Carers leave",
        "[UK] Civic activities",
        "[UK] Time off in lieu",
        "[UK] Competing in a major sporting event (Paid)",
        "[UK] Reserve Forces- Training & Volunatry (Paid)",
        "[UK] A/L Carryover",
        "[UK] Sickness Carry Over",
        "[UK] Leave of Absence carryover",
        "[UK] Bought A/L",
        "[UK] Shift Public Holiday",  # Note: Different from non-shift
        "[UK] Term time type",
        "[UK] Charitable leave",
        "[UK] At Risk Leave",
        "[UK] Birthing Parent (Mat) Leave",
        "[UK] Parental Leave",
        "[UK] Adoption Leave",
        "[UK] Shared Parental Leave",
        "[UK] Sabbatical Leave",
        "[UK] Co-Parent (Pat) Leave",
        "[UK] Extended Sickness Absence",
        "[UK] Statutory Sick pay",
        "[UK] Nil Pay",
        "[UK] Leave for Medical Purpose",
        "[UK] Public Holiday",
        "[UK] Garden Leave",
        "[UK] Contractual Days",
        "[UK] Other Paid Leave",
        "[UK] Other Unpaid Leave",
        "[UK] Long term sickness Absence (OSP 50%)",
        "[UK] Reserve Forces-Military Service (Unpaid)",
        "[UK] Time off for Employee Representatives",
        "[UK] Trade Union Representatives",
        "[UK] Voluntary Public Duties",
        "[UK] Sold A/L",
        "[UK] Sickness Absence"
    ],
    
    "IRL_FT_NO_SHIFT": [
        "[IRL] Time off in Lieu",
        "[IRL] Jury Leave",
        "[IRL] DV Leave",
        "[IRL] Compassionate Leave",
        "[IRL] Annual Leave",
        "[IRL] Medical Care Leave (Unpaid)",
        "[IRL] Bought A/L",
        "[IRL] General CarryOver",
        "[IRL] A/L Carryover",
        "[IRL] Sickness Carry Over",
        "[IRL] Witness Summons",
        "[IRL] Army Reserve - Training & Volunteering",
        "[IRL] Trade Union",
        "[IRL] Competing in Major Sporting Event",
        "[IRL] Special Leave (Unpaid)",
        "[IRL] Public Holiday",
        "[IRL] Parents Leave",
        "[IRL] Parental Leave",
        "[IRL] Nil Pay",
        "[IRL] Mobilisation of Army Reserves",
        "[IRL] Long Term Sickness Absence",
        "[IRL] Leave for Medical Purposes",
        "[IRL] Health and Safety Leave",
        "[IRL] Garden Leave",
        "[IRL] Force Majeure Leave",
        "[IRL] Co-Parent (Pat) Leave",
        "[IRL] Contractual Days",
        "[IRL] Charitable Leave",
        "[IRL] Carers Leave",
        "[IRL] Birthing Parent (Mat) Leave",
        "[IRL] Adoption Leave",
        "[IRL] Employee Representative Duties",
        "[IRL] Other Paid Leave",
        "[IRL] Other Unpaid Leave",
        "[IRL] Competing in Major Sporting Event",
        "[IRL] Sold A/L",
        "[IRL] Sickness Absence"
    ],
    
    "IRL_PT_NO_SHIFT": [
        "[IRL] P/T Bought A/L Hrs",
        "[IRL] P/T Public Holiday Hrs",
        "[IRL] P/T Annual Leave Hrs",
        "[IRL] PT General CarryOver Hrs",
        "[IRL] P/T Carry Over Hrs",
        "[IRL] P/T Sickness Carry Over Hrs",
        "[IRL] Time off in Lieu",
        "[IRL] Jury Leave",
        "[IRL] DV Leave",
        "[IRL] Compassionate Leave",
        "[IRL] Medical Care Leave (Unpaid)",
        "[IRL] Witness Summons",
        "[IRL] Army Reserve - Training & Volunteering",
        "[IRL] Trade Union",
        "[IRL] Competing in Major Sporting Event",
        "[IRL] Special Leave (Unpaid)",
        "[IRL] Public Holiday",
        "[IRL] Parents Leave",
        "[IRL] Parental Leave",
        "[IRL] Nil Pay",
        "[IRL] Mobilisation of Army Reserves",
        "[IRL] Long Term Sickness Absence",
        "[IRL] Leave for Medical Purposes",
        "[IRL] Health and Safety Leave",
        "[IRL] Garden Leave",
        "[IRL] Force Majeure Leave",
        "[IRL] Co-Parent (Pat) Leave",
        "[IRL] Contractual Days",
        "[IRL] Charitable Leave",
        "[IRL] Carers Leave",
        "[IRL] Birthing Parent (Mat) Leave",
        "[IRL] Adoption Leave",
        "[IRL] Employee Representative Duties",
        "[IRL] Other Paid Leave",
        "[IRL] Other Unpaid Leave",
        "[IRL] Competing in Major Sporting Event",
        "[IRL] P/T Sold A/L Hrs",
        "[IRL] Sickness Absence"
    ],
    
    "IRL_SHIFT": [
        "[IRL] Time off in Lieu",
        "[IRL] Jury Leave",
        "[IRL] DV Leave",
        "[IRL] Compassionate Leave",
        "[IRL] Annual Leave",
        "[IRL] Medical Care Leave (Unpaid)",
        "[IRL] Bought A/L",
        "[IRL] General CarryOver",
        "[IRL] A/L Carryover",
        "[IRL] Sickness Carry Over",
        "[IRL] Shift Public Holiday",  # Note: Different from non-shift
        "[IRL] Witness Summons",
        "[IRL] Army Reserve - Training & Volunteering",
        "[IRL] Trade Union",
        "[IRL] Competing in Major Sporting Event",
        "[IRL] Special Leave (Unpaid)",
        "[IRL] Public Holiday",
        "[IRL] Parents Leave",
        "[IRL] Parental Leave",
        "[IRL] Nil Pay",
        "[IRL] Mobilisation of Army Reserves",
        "[IRL] Long Term Sickness Absence",
        "[IRL] Leave for Medical Purposes",
        "[IRL] Health and Safety Leave",
        "[IRL] Garden Leave",
        "[IRL] Force Majeure Leave",
        "[IRL] Co-Parent (Pat) Leave",
        "[IRL] Contractual Days",
        "[IRL] Charitable Leave",
        "[IRL] Carers Leave",
        "[IRL] Birthing Parent (Mat) Leave",
        "[IRL] Adoption Leave",
        "[IRL] Employee Representative Duties",
        "[IRL] Other Paid Leave",
        "[IRL] Other Unpaid Leave",
        "[IRL] Competing in Major Sporting Event",
        "[IRL] Sold A/L",
        "[IRL] Sickness Absence"
    ]
}

def get_timeoffs_for_user(location, fte_percent, work_shift):
    # Normalize location
    location_key = "UK" if "kingdom" in location.lower() or location.upper() in ["UK", "GB"] else "IRL"
    
    # Determine if shift schedule
    is_shift_schedule = "shift schedule" in work_shift.lower()
    
    # Build timeoff set key
    if is_shift_schedule:
        timeoff_key = f"{location_key}_SHIFT"
    else:
        try:
            fte_value = float(fte_percent)
            fte_key = "FT" if fte_value >= 100 else "PT"
            timeoff_key = f"{location_key}_{fte_key}_NO_SHIFT"
        except:
            timeoff_key = f"{location_key}_FT_NO_SHIFT"  # Default to full-time
    
    return TIMEOFF_SETS.get(timeoff_key, [])

def get_timeoff_set_name(location, fte_percent, work_shift):
    # Normalize location
    location_key = "UK" if "kingdom" in location.lower() or location.upper() in ["UK", "GB"] else "IRL"
    
    # Determine if shift schedule
    is_shift_schedule = "shift schedule" in work_shift.lower()
    
    # Build timeoff set key
    if is_shift_schedule:
        return f"{location_key}_SHIFT"
    else:
        try:
            fte_value = float(fte_percent)
            fte_key = "FT" if fte_value >= 100 else "PT"
            return f"{location_key}_{fte_key}_NO_SHIFT"
        except:
            return f"{location_key}_FT_NO_SHIFT"  # Default to full-time