# Payroll Calendar Mapper for UK & Ireland Payroll Export
# Export runs on the day AFTER the payroll cutoff date at 12:00 AM GMT
# Extraction window: Current payroll period + 12 weeks PTA (rolling)
#
# NOTE: Calendar entries are intentionally limited to the current business cycle.
# When calendar expires, DAG will skip execution (expected behavior for non-export days).
# CR Required: Add 2027+ entries before Dec 2026 based on business-provided cutoff dates.
# Contact: DXC payroll team for future cutoff dates.

LCSC_PAYROLL_CALENDAR = [
    {"year": 2026, "month": 6, "payroll_cutoff_day": 5},
    {"year": 2026, "month": 7, "payroll_cutoff_day": 3},
    {"year": 2026, "month": 8, "payroll_cutoff_day": 7},
    {"year": 2026, "month": 9, "payroll_cutoff_day": 4},
    {"year": 2026, "month": 10, "payroll_cutoff_day": 2},
    {"year": 2026, "month": 11, "payroll_cutoff_day": 6},
    {"year": 2026, "month": 12, "payroll_cutoff_day": 4},
]

LES_PAYROLL_CALENDAR = [
    {"year": 2026, "month": 6, "payroll_cutoff_day": 5},
    {"year": 2026, "month": 7, "payroll_cutoff_day": 3},
    {"year": 2026, "month": 8, "payroll_cutoff_day": 7},
    {"year": 2026, "month": 9, "payroll_cutoff_day": 4},
    {"year": 2026, "month": 10, "payroll_cutoff_day": 2},
    {"year": 2026, "month": 11, "payroll_cutoff_day": 6},
    {"year": 2026, "month": 12, "payroll_cutoff_day": 4},
]
