"""Constants for Procore to ComputerEase Purchase Order Synchronization."""

class ErrorType:
    ERROR = 'error' # Blocks Sync, Email Alert
    WARNING = 'warning' # Does not block sync, Email Alert
    SKIP = 'skip' # Does not block sync, No Email Alert

RESOURCE_PURCHASE_ORDER = 'Purchase Order Contracts'
EVENT_TYPE_UPDATE = 'update'
APPROVED = 'Approved'

PHASE_CODE_MAX_LENGTH = 4
CATEGORY_CODE_MAX_LENGTH = 10

MAX_DESCRIPTION_LINES = 10
DESCRIPTION_LINE_LENGTH = 30

MAX_ADDRESS_LINES = 4
ADDRESS_LINE_LENGTH = 30

CE_IMPORT_TYPE = 'icpos'

JSON_FILENAME = 'purchase_orders.json'
JSON_INDENT_SPACES = 2

DOWNLOAD_LINK_EXPIRY_SECONDS = 7 * 24 * 60 * 60
