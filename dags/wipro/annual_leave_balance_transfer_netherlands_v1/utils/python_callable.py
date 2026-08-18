import math
from datetime import datetime
from wipro.annual_leave_balance_transfer_netherlands_v1 import config


def _truncate_2(number):
    """Truncate (do not round) a number to 2 decimal places."""
    return math.floor(number * 100) / 100


def get_balance_to_transfer(balance, onsite_direct_recruit, user_start_date, report_run_date):
    """Split the unused Annual Leave balance into Additional (5-yr) and Carried Over (6-mo).

    Resource group (from the "Onsite Direct Recruit" report field) sets the full yearly
    entitlement: LOCAL_HIRE -> ODR (25), anything else -> LTA/ASSIGNEE (22).

    Proration applies only when the user joined within the report year (service < 1 year as
    of 31 Dec): entitlement = full * months_worked / 12 (truncated to 2 decimals), where
    months_worked counts whole calendar months from the joining month through December.

    The extra-legal cap = max(0, entitlement - LEGAL_DAYS). The Additional bucket takes up to
    that cap; the remainder goes to Carried Over.
    """
    group = (onsite_direct_recruit or "").strip().upper()
    full_entitlement = config.ODR_ENTITLEMENT \
        if group == config.ONSITE_DIRECT_RECRUIT_LOCAL_HIRE else config.LTA_ENTITLEMENT

    start_date = datetime.strptime(user_start_date, config.DATE_DEFAULT_FORMAT)
    report_year = int(report_run_date.split("/")[0])

    if start_date.year == report_year:
        months_worked = 13 - start_date.month
        entitlement = _truncate_2(full_entitlement * months_worked / 12)
    else:
        entitlement = float(full_entitlement)

    extra_cap = max(0.0, round(entitlement - config.LEGAL_DAYS, 2))
    additional = round(min(extra_cap, balance), 2)
    carried_over = round(balance - additional, 2)

    return [
        {
            'name': config.ANNUAL_LEAVE_ADDITIONAL,
            'balance': str(additional)
        },
        {
            'name': config.ANNUAL_LEAVE_CARRIED_OVER,
            'balance': str(carried_over)
        }
    ]
