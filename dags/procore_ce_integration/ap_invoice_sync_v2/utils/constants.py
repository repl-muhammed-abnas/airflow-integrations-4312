RESOURCE_DRAW_REQUESTS = 'Draw Requests'

class ErrorType:
    API_ERROR = 'api_error'

class AccountingMethod:
    UNIT = 'unit'
    AMOUNT = 'amount'

class InvoiceLineItemType:
    CONTRACT_ITEM = 'contract_item'
    CHANGE_ORDER_ITEM = 'change_order_item'
    CONTRACT_DETAIL_ITEM = 'contract_detail_item'

class CommitmentType:
    SUBCONTRACT = 'WorkOrderContract'
    PURCHASE_ORDER = 'PurchaseOrderContract'
