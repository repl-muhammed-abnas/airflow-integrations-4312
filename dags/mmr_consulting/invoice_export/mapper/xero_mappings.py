"""Replicon to Xero field value translations."""

# Replicon billing type -> Xero inventory ItemCode.
ITEM_CODE_BY_BILLING_TYPE = {
    'timesheet': 'SVCS-TM',
    'expense': 'EXP',
    'fixed-bid': 'SVCS-FC',
    'adhoc': '',
}

# Replicon invoice item types allowed
ALLOWED_ITEM_TYPES = (
    'urn:replicon:invoice-item-type:timesheet',
    'urn:replicon:invoice-item-type:expense',
    'urn:replicon:invoice-item-type:fixed-bid',
    'urn:replicon:invoice-item-type:adhoc',
)

# Project "PO Type" extension field values
PO_TYPE_MONTHLY_BILLING = 'Fixed Bid – Monthly Billing'
PO_TYPE_TM = 'T&M'
PO_TYPE_PAYMENT_TERMS = 'Fixed Bid – Payment Terms'
PO_TYPE_PAYMENT_TERMS_EXP = 'Fixed Bid - Payment Terms (EXP)'