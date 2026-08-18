"""
iPipeline User Import - Time Off Accrual Mapper

Accrual policies for time off types with seniority-based tiers.
Maps leave types to their accrual rates, caps, and carry forward rules.

REFERENCE LOGIC TYPES:
=====================
Type1-A1: Accruing vacation policies with tenure-based tiers (USA, Canada)
Type1-A2: Accruing vacation policies with tenure-based tiers (Japan)
Type1-B: Accruing vacation policies with fixed rate and carry forward (UK Holiday)
Type2-A: Non-accruing policies with full balance at year start, tenure-based (Sick, Personal)
Type2-B: Non-accruing policies with fixed balance, no carry over (Bereavement, Compassionate)
Type3: UK Illness/Sick-C with complex tenure tiers
Type4/Type 4: Unlimited/HR-managed policies - no accrual tracking
Type5/Type 5: No balance tracking policies (Summer Hours, Holiday Carry Over)

SENIORITY CONDITION OPERATORS:
=============================
- ">=X": Greater than or equal to X years of service
- "<=X": Less than or equal to X years of service  
- ">X": Greater than X years of service
- "<X": Less than X years of service

Each policy entry contains:
- leave_type: The time off type name (matches assignment mapper)
- seniority_condition: Service year condition for this tier
- yearly_accrual_rate_unit: "Hours" or "Days"
- cap_accruals_for_year_hours: Maximum hours that can accrue (null if no cap)
- yearly_accrual_rate: Rate value in days or hours per year
- reset: When balance resets (null if no reset)
- carry_forward: Days/hours that can carry forward (null if none)
- carry_forward_expiry: Date when carry forward expires (null if N/A)
- reference_logic_type: Type classification for accrual logic handling

The USA_Vacation & Canada_Vacation will not be maintained in this mapper as a constant formula will be used
"""

TIME_OFF_ACCRUAL_MAPPER = [
    # ===========================================
    # UK Holiday - 25 days
    # Type1-B: Accruing with carry forward
    # ===========================================
    {
        "leave_type": "UK _Holiday-25 days",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": 187.5,
        "yearly_accrual_rate": 25,
        "reset": "",
        "carry_forward": 3,
        "carry_forward_expiry": "1-Apr",
        "reference_logic_type": "Type1-B"
    },

    # ===========================================
    # UK Holiday - 27 days
    # Type1-B: Accruing with carry forward
    # ===========================================
    {
        "leave_type": "UK _Holiday-27 days",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": 216,
        "yearly_accrual_rate": 27,
        "reset": "",
        "carry_forward": 3,
        "carry_forward_expiry": "1-Apr",
        "reference_logic_type": "Type1-B"
    },

    # ===========================================
    # UK Holiday - 28 days
    # Type1-B: Accruing with carry forward
    # ===========================================
    {
        "leave_type": "UK _Holiday-28 days",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": 224,
        "yearly_accrual_rate": 28,
        "reset": "",
        "carry_forward": 3,
        "carry_forward_expiry": "1-Apr",
        "reference_logic_type": "Type1-B"
    },

    # ===========================================
    # Japan Vacation
    # Type1-A: Accruing with fixed rate
    # ===========================================
    {
        "leave_type": "Japan_Vacation",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Hours",
        "cap_accruals_for_year_hours": 160,
        "yearly_accrual_rate": 160,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type1-A2"
    },

    # ===========================================
    # USA Illness/Sick
    # Type2-A: Non-accruing, full balance at year start
    # ===========================================
    {
        "leave_type": "USA_Illness/Sick",
        "seniority_condition": "<1",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 5,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-A"
    },
    {
        "leave_type": "USA_Illness/Sick",
        "seniority_condition": ">=1",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 6,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-A"
    },

    # ===========================================
    # UK Illness/Sick-C
    # Type3: Complex tenure tiers for UK sick
    # ===========================================
    {
        "leave_type": "UK_Illness/Sick-C",
        "seniority_condition": "<1",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 6,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type3"
    },
    {
        "leave_type": "UK_Illness/Sick-C",
        "seniority_condition": ">=1",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 12,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type3"
    },
    {
        "leave_type": "UK_Illness/Sick-C",
        "seniority_condition": ">=2",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 20,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type3"
    },

    # ===========================================
    # UK Illness/Sick-A
    # Type2-A: Non-accruing, tenure-based
    # ===========================================
    {
        "leave_type": "UK_Illness/Sick-A",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 130,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-A"
    },

    # ===========================================
    # UK Illness/Sick-B
    # Type2-A: Non-accruing, fixed rate
    # ===========================================
    {
        "leave_type": "UK_Illness/Sick-B",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 60,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-A"
    },

    # ===========================================
    # USA Personal
    # Type2-A: Non-accruing, full balance
    # ===========================================
    {
        "leave_type": "USA_Personal",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 2,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-A"
    },

    # ===========================================
    # Canada Personal
    # Type2-A: Non-accruing, full balance
    # ===========================================
    {
        "leave_type": "Canada_Personal",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 10,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-A"
    },

    # ===========================================
    # Canada Bereavement
    # Type2-B: Non-accruing, no carry over
    # ===========================================
    {
        "leave_type": "Canada_Bereavement",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 3,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-B"
    },

    # ===========================================
    # Japan Bereavement
    # Type2-B: Non-accruing, no carry over
    # ===========================================
    {
        "leave_type": "Japan_Bereavement",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 3,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-B"
    },

    # ===========================================
    # USA Bereavement
    # Type2-B: Non-accruing, no carry over
    # ===========================================
    {
        "leave_type": "USA_Bereavement",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 3,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-B"
    },

    # ===========================================
    # UK Compassionate leave
    # Type2-B: Non-accruing, no carry over
    # ===========================================
    {
        "leave_type": "UK_Compassionate leave",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 5,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-B"
    },

    # ===========================================
    # UK Time off for Dependants
    # Type2-B: Non-accruing, no carry over
    # ===========================================
    {
        "leave_type": "UK_Time off for Dependants",
        "seniority_condition": ">=0",
        "yearly_accrual_rate_unit": "Days",
        "cap_accruals_for_year_hours": "",
        "yearly_accrual_rate": 3,
        "reset": "",
        "carry_forward": "",
        "carry_forward_expiry": "",
        "reference_logic_type": "Type2-B"
    }
]
