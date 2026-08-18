# V2.7 - Proration of [CAN] Jour personnel/Personal Days
#
# Source-of-truth mapper for the Personal Days proration logic. All business
# rules live here so that future changes (e.g. a new unpaid-leave reason code,
# a new bucket, an updated yearly cap) are pure data edits with no code change
# required in the DAG, callables, or instance files.
#
# Adding a NEW unpaid-leave reason code in the future:
#   1. Add an entry to LEAVE_OUT_IMPACT_RULES below.
#   2. Set "action" to one of the existing action strings:
#        "zero_immediately"             - balance set to 0 at change-effective-date
#        "buffer_then_zero"             - balance preserved for N weeks, then 0
#        "prorate_at_leave_start"       - use the Start of Leaves table column
#        "prorate_then_buffer_then_zero"- prorate at change_eff AND zero at
#                                         change_eff + N weeks (two policy lines)
#      and (for buffer_then_zero and prorate_then_buffer_then_zero) set
#      "buffer_weeks".
#   3. Done. No Python changes are required.
#
# Adding a NEW bucket (Reg/Temp, std_hrs, job-code family combination):
#   1. Add a row to BUCKET_TABLE.
#   2. Add the corresponding 12-month start_of_leaves + returns table to
#      PERSONAL_DAYS_PRORATION_TABLES if a new table key is used.

PERSONAL_DAYS_TIMEOFF_TYPE_NAME = "[CAN] Jour personnel/Personal Days"

# 26 weeks gating threshold for return-time proration (strict >).
LONG_LEAVE_THRESHOLD_DAYS = 182

# Event code that identifies a Leave of Absence transaction in the SAP payload.
LEAVE_OF_ABSENCE_EVENT_CODE = "10"

# Outbound impact per event-reason-code. Codes NOT in this dict are no-ops
# (no outbound action, AND no return-time proration eligibility either).
#
# UNPSTD <-> UNPLTD continuation (per CRL call 2026-05-15 + CR(4) row 4):
# CRL never sends UNPLTD as a fresh leave. The user always starts on UNPSTD
# (17 weeks, no impact on Personal Days), and after 17 weeks the CRL SAP
# system auto-converts the event to UNPLTD. The 9-week buffer on UNPLTD is
# therefore a CONTINUATION of the 17 weeks already served on UNPSTD:
#   17 weeks (UNPSTD, no impact) + 9 weeks (UNPLTD buffer-then-zero) = 26 weeks.
# Because the integration writes the "Leave Start Date" UDF ONLY on the
# Active->Unpaid Leave transition (i.e. on UNPSTD arrival), and the UDF is
# preserved across the UNPSTD->UNPLTD transition (still Unpaid Leave on both
# sides), the 26-week duration check naturally measures from the original
# UNPSTD start date. UNPLTD itself is listed below as buffer_then_zero/9w
# so its OWN outbound write places the zero-balance line 9 weeks after the
# UNPLTD effective date - which lines up with 26 weeks from UNPSTD start.
# UNPSTD is intentionally absent from LEAVE_OUT_IMPACT_RULES (no-op).
LEAVE_OUT_IMPACT_RULES = {
    "UNPLTD": {"action": "buffer_then_zero", "buffer_weeks": 9},
    "UNPMED": {"action": "buffer_then_zero", "buffer_weeks": 26},
    "UNPMIL": {"action": "buffer_then_zero", "buffer_weeks": 26},
    "UNPWCL": {"action": "buffer_then_zero", "buffer_weeks": 26},
    # UNPADP / UNPMPA per xlsx Sheet1 rows 3-4 column H: write TWO outbound
    # policy lines - prorated balance at change_eff (from Start of Leaves
    # table) AND zero at change_eff + 26 weeks. Captures the "user has
    # prorated amount from change effective date... balance set to 0 after
    # 26 weeks" two-part rule.
    "UNPADP": {"action": "prorate_then_buffer_then_zero", "buffer_weeks": 26},
    "UNPMPA": {"action": "prorate_then_buffer_then_zero", "buffer_weeks": 26},
    "UNPPAR": {"action": "zero_immediately"},
    "UNPPER": {"action": "zero_immediately"},
    "UNPEDU": {"action": "zero_immediately"},
    "UNPSBC": {"action": "zero_immediately"},
    "UNPSBP": {"action": "zero_immediately"},
}

# Reg/Temp + Standard Hours + Job Code family letter (penultimate char of
# job_code per V2.7 docx image32 "Job Code ending letter (Last but 2nd one)")
# resolves to a proration table key and the yearly max hours.
#
# TEMP is intentionally absent - non-Regular employees are excluded.
BUCKET_TABLE = {
    ("Regular", 37.5, "A"): ("10d_FT",      75.0),
    ("Regular", 37.5, "T"): ("10d_FT",      75.0),
    ("Regular", 37.5, "M"): ("10d_FT",      75.0),
    ("Regular", 30.0, "A"): ("10d_PT_30",   52.5),
    ("Regular", 30.0, "T"): ("10d_PT_30",   52.5),
    ("Regular", 30.0, "M"): ("10d_PT_30",   52.5),
    ("Regular", 22.5, "A"): ("10d_PT_22.5", 37.5),
    ("Regular", 22.5, "T"): ("10d_PT_22.5", 37.5),
    ("Regular", 22.5, "M"): ("10d_PT_22.5", 37.5),
    ("Regular", 37.5, "S"): ("10d_PT_22.5", 37.5),
    ("Regular", 30.0, "S"): ("5d_PT_30",    30.0),
    ("Regular", 22.5, "S"): ("5d_PT_22.5",  22.5),
}

# Five proration tables. Each bucket has two 12-month sub-tables:
#   start_of_leaves  - used for "prorate_at_leave_start" outbound action
#   returns          - used for return-time proration (with 15-day rounding)
# Values are hours (transcribed from Pro-Rated Personal Days-Replicon - TB.pdf).
PERSONAL_DAYS_PRORATION_TABLES = {
    "10d_FT": {
        "start_of_leaves": {1: 15.00, 2: 15.00, 3: 18.75, 4: 22.50, 5: 30.00, 6: 37.50,
                            7: 45.00, 8: 52.50, 9: 56.25, 10: 60.00, 11: 67.50, 12: 75.00},
        "returns":         {1: 75.00, 2: 67.50, 3: 60.00, 4: 56.25, 5: 52.50, 6: 45.00,
                            7: 37.50, 8: 30.00, 9: 22.50, 10: 18.75, 11: 15.00, 12: 15.00},
    },
    "10d_PT_30": {
        "start_of_leaves": {1: 15.00, 2: 15.00, 3: 15.00, 4: 15.00, 5: 22.50, 6: 26.25,
                            7: 30.00, 8: 35.62, 9: 39.38, 10: 45.00, 11: 48.75, 12: 52.50},
        "returns":         {1: 52.50, 2: 48.75, 3: 45.00, 4: 39.38, 5: 35.62, 6: 30.00,
                            7: 26.25, 8: 22.50, 9: 15.00, 10: 15.00, 11: 15.00, 12: 15.00},
    },
    "10d_PT_22.5": {
        "start_of_leaves": {1: 15.00, 2: 15.00, 3: 15.00, 4: 15.00, 5: 15.00, 6: 18.75,
                            7: 22.50, 8: 24.38, 9: 28.12, 10: 30.00, 11: 33.75, 12: 37.50},
        "returns":         {1: 37.50, 2: 33.75, 3: 30.00, 4: 28.12, 5: 24.38, 6: 22.50,
                            7: 18.75, 8: 15.00, 9: 15.00, 10: 15.00, 11: 15.00, 12: 15.00},
    },
    "5d_PT_30": {
        "start_of_leaves": {1: 15.00, 2: 15.00, 3: 15.00, 4: 15.00, 5: 15.00, 6: 15.00,
                            7: 17.50, 8: 20.00, 9: 22.50, 10: 25.00, 11: 27.50, 12: 30.00},
        "returns":         {1: 30.00, 2: 27.50, 3: 25.00, 4: 22.50, 5: 20.00, 6: 17.50,
                            7: 15.00, 8: 15.00, 9: 15.00, 10: 15.00, 11: 15.00, 12: 15.00},
    },
    "5d_PT_22.5": {
        "start_of_leaves": {1: 15.00, 2: 15.00, 3: 15.00, 4: 15.00, 5: 15.00, 6: 15.00,
                            7: 15.00, 8: 15.00, 9: 15.00, 10: 15.00, 11: 18.75, 12: 22.50},
        "returns":         {1: 22.50, 2: 18.75, 3: 15.00, 4: 15.00, 5: 15.00, 6: 15.00,
                            7: 15.00, 8: 15.00, 9: 15.00, 10: 15.00, 11: 15.00, 12: 15.00},
    },
}


# ---------------------------------------------------------------------------
# Module-level validation - runs at DAG-parse time so typos in this mapper
# fail fast in CI rather than at user-payload time with a wrong proration.
# ---------------------------------------------------------------------------

_VALID_ACTIONS = {
    "zero_immediately",
    "buffer_then_zero",
    "prorate_at_leave_start",
    "prorate_then_buffer_then_zero",
}
_ACTIONS_REQUIRING_BUFFER_WEEKS = {"buffer_then_zero", "prorate_then_buffer_then_zero"}
_EXPECTED_MONTHS = set(range(1, 13))

for _code, _rule in LEAVE_OUT_IMPACT_RULES.items():
    assert isinstance(_rule, dict), f"LEAVE_OUT_IMPACT_RULES[{_code!r}] must be a dict"
    _action = _rule.get("action")
    assert _action in _VALID_ACTIONS, (
        f"LEAVE_OUT_IMPACT_RULES[{_code!r}].action={_action!r} not in {_VALID_ACTIONS}"
    )
    if _action in _ACTIONS_REQUIRING_BUFFER_WEEKS:
        _bw = _rule.get("buffer_weeks")
        assert isinstance(_bw, int) and _bw > 0, (
            f"LEAVE_OUT_IMPACT_RULES[{_code!r}] action={_action} requires "
            f"positive int buffer_weeks; got {_bw!r}"
        )

for _key, _tbl in PERSONAL_DAYS_PRORATION_TABLES.items():
    for _col in ("start_of_leaves", "returns"):
        assert _col in _tbl, f"PERSONAL_DAYS_PRORATION_TABLES[{_key!r}] missing {_col!r}"
        assert set(_tbl[_col].keys()) == _EXPECTED_MONTHS, (
            f"PERSONAL_DAYS_PRORATION_TABLES[{_key!r}][{_col!r}] must have months 1-12; "
            f"got {sorted(_tbl[_col].keys())}"
        )

for _bucket_key, _bucket_val in BUCKET_TABLE.items():
    assert isinstance(_bucket_val, tuple) and len(_bucket_val) == 2, (
        f"BUCKET_TABLE[{_bucket_key!r}] must be (table_key, yearly_max); got {_bucket_val!r}"
    )
    _table_key, _yearly_max = _bucket_val
    assert _table_key in PERSONAL_DAYS_PRORATION_TABLES, (
        f"BUCKET_TABLE[{_bucket_key!r}] references unknown table {_table_key!r}"
    )
    assert isinstance(_yearly_max, (int, float)) and _yearly_max > 0, (
        f"BUCKET_TABLE[{_bucket_key!r}] yearly_max must be positive; got {_yearly_max!r}"
    )

# Clean up loop temporaries so they don't leak as module globals.
del _code, _rule, _action, _key, _tbl, _col, _bucket_key, _bucket_val, _table_key, _yearly_max
del _VALID_ACTIONS, _EXPECTED_MONTHS
try:
    del _bw
except NameError:
    pass
