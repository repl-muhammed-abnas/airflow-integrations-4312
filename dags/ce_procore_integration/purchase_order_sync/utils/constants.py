class InputSource:
    SFTP = 'sftp'
    EMAIL = 'email'


class ProcorePurchaseOrderStatus:
    DRAFT = 'Draft'
    PROCESSING = 'Processing'
    APPROVED = 'Approved'
    CLOSED = 'Closed'


class CE_Fields:
    """CSV column names from Computerease purchase order export"""
    PO_NUMBER = 'P.O. Number'
    PO_DATE = 'P.O. Date'
    VENDOR_NUMBER = 'Vendor Number'
    VENDOR_NAME = 'Vendor Name'
    VENDOR_PHONE = 'Vendor Phone Number'
    BUYER = 'Buyer'
    ITEM_NUMBER = 'Item Number'
    ITEM_NAME = 'Item Name'
    ITEM_CLASS = 'Item Class'
    UNIT_PRICE = 'Unit Price'
    ORDERED = 'Ordered'
    RECEIVED = 'Received'
    BALANCE = 'Balance'
    VALUE_ORDERED = 'Value Ordered'
    VALUE_RECEIVED = 'Value of Received'
    VALUE_BALANCE = 'Value of Balance'
    RECEIPT_DATE = 'Receipt Date'
    EXPECTED_DATE = 'Expected Date'
    JOB_NAME = 'Job Name'
    JOB_CODE = 'Job Code'
    PHASE_CODE = 'Phase Code'
    CATEGORY_CODE = 'Category Code'
    COST_TYPE = 'Cost Type'
    EQUIPMENT_NUMBER = 'Equipment Number'
    EQUIPMENT_CODE = 'Equipment Code'
    APPROVED = 'Approved'
    VENDOR_ITEM_NUMBER = 'Vendor Item #'
    DATE_REQUIRED = 'Date Required'
    LOCATION = 'Location'
