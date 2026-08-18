"""Replicon to Xero field value translations for the Singapore invoice export."""

# Replicon billing type -> Xero inventory ItemCode. Ad-hoc and blank types carry no ItemCode.
ITEM_CODE_BY_BILLING_TYPE = {
    'timesheet': 'SVCS-TM',
    'expense': 'EXP',
    'fixed-bid': 'SVCS-FC',
    'adhoc': '',
}

# Recipe sets LineAmountTypes per create action: NoTax for fixed-bid, Exclusive otherwise.
# A Xero invoice has a single LineAmountTypes, resolved from the first line item's type.
LINE_AMOUNT_TYPE_BY_BILLING_TYPE = {
    'timesheet': 'Exclusive',
    'expense': 'Exclusive',
    'fixed-bid': 'NoTax',
    'adhoc': 'Exclusive',
}
DEFAULT_LINE_AMOUNT_TYPE = 'Exclusive'

# Replicon invoice item types the recipe handles.
ALLOWED_ITEM_TYPES = (
    'urn:replicon:invoice-item-type:timesheet',
    'urn:replicon:invoice-item-type:expense',
    'urn:replicon:invoice-item-type:fixed-bid',
    'urn:replicon:invoice-item-type:adhoc',
)
