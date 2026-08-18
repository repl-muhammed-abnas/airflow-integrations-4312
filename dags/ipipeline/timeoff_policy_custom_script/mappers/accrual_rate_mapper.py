accrual_rate_mapper = [
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
