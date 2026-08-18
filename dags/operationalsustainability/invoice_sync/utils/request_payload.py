import uuid
null = None
import rail
from datetime import datetime
from operationalsustainability.invoice_sync import config


def format_date_for_qbo(date_value):
    """
    Convert various date formats to QBO format (YYYY-MM-DD).

    Args:
        date_value: Date string or datetime object

    Returns:
        str: Date in YYYY-MM-DD format or None if invalid
    """
    if not date_value:
        return None
    if isinstance(date_value, str):
        # Parse common Replicon date formats
        try:
            dt = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        except (ValueError, AttributeError):
            # Already in correct format or unparseable
            return date_value
    return date_value



def get_queuedforsync_invoice():
    return {
        'page': 1,
        'pagesize': 10000,
        'columnUris': [
            'urn:replicon:invoice2-list-column:invoice',
            'urn:replicon:invoice2-list-column:invoice-number-text',
            'urn:replicon:invoice2-list-column:client',
            'urn:replicon:invoice2-list-column:creation-date-time',
            'urn:replicon:invoice2-list-column:last-modified-date-time',
            'urn:replicon:invoice2-list-column:invoice-status',
            'urn:replicon:invoice2-list-column:payment-due-date',
            'urn:replicon:invoice2-list-column:invoice-date',
            'urn:replicon:invoice2-list-column:total-invoice-amount',
            'urn:replicon:invoice2-list-column:invoice-currency',
            'urn:replicon:invoice2-list-column:payment-term',
            'urn:replicon:invoice2-list-column:invoice-amount-in-base-currency',
            'urn:replicon:invoice2-list-column:description',
            'urn:replicon:invoice2-list-column:invoice-items'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': rail.result('get_sync_status_filter_definition')    #sync status
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': config.invoice_sync_status_to_process
                }
            }
        }
    }


def get_invoice_item_request(dag_run):
    return {
        'page': 1,
        'pageSize': 10000,
        'invoice': {
            'uri': dag_run.conf['invoice']
        },
        'invoiceItemColumnOptions': [
            "urn:replicon:invoice-item-column-option:project",
            "urn:replicon:invoice-item-column-option:user",
            "urn:replicon:invoice-item-column-option:billing-rate",
            "urn:replicon:invoice-item-column-option:task",
            "urn:replicon:invoice-item-column-option:entry-date"
        ]
    }


def create_customer_qbo_request(dag_run):
    billing_address = rail.result('get_client_details').get('billingAddress')
    client_address = rail.result('get_client_details').get('clientAddress')
    primary_email_address = billing_address.get('email') if billing_address else None
    primary_phone = billing_address.get('phoneNumber') if billing_address else None
    fax_number = billing_address.get('faxNumber') if billing_address else None

    payload = {
        'PrimaryEmailAddr': {
            'Address': primary_email_address
        } if primary_email_address else null,
        'DisplayName': dag_run.conf['client']['textValue'],
        'PrimaryPhone': {
            'FreeFormNumber': primary_phone
        } if primary_phone else null,
        'Active': True,
        'BillAddr': {
            'City': billing_address['city'],
            'Line1': billing_address['address'],
            'PostalCode': billing_address['zipPostalCode'],
            'Country': billing_address['country']['name'] if billing_address['country'] else null,
            'CountrySubDivisionCode': billing_address['stateProvince']
        } if billing_address else null,
        'ShipAddr': {
            'City': client_address['city'],
            'Line1': client_address['address'],
            'PostalCode': client_address['zipPostalCode'],
            'Country': client_address['country']['name'] if client_address['country'] else null,
            'CountrySubDivisionCode': client_address['stateProvince']
        } if client_address else null,
        'CompanyName': rail.result('get_client_details')['name']
    }

    # Add optional fax field only if present
    if fax_number:
        payload['Fax'] = {'FreeFormNumber': fax_number}

    return payload


def create_invoice_with_multiline_item_request(dag_run, invoice_data):
    invoice_date = format_date_for_qbo(dag_run.conf.get('creation_datetime'))
    statement_memo = dag_run.conf['invoice']
    customer_id = rail.result('create_customer_qbo').get('Customer', {}).get('Id') if rail.result('create_customer_qbo') else \
                    rail.result('parse_qbo_customer').get('Id')
    display_message = config.invoice_customer_message
    due_date = format_date_for_qbo(dag_run.conf.get('payment_due_date'))
    invoice_number = f"REP-{dag_run.conf['invoice_number']}"

    if not invoice_date or not due_date:
        raise ValueError("Invoice date and due date are required fields")
    return {
        'CustomerRef': {
            'value': customer_id
        },
        'PrivateNote': statement_memo,
        'TxnDate': invoice_date,
        'DocNumber': invoice_number,
        'CustomerMemo': {
            'value': display_message
        },
        'DueDate': due_date,
        'Line': [
            {
                'DetailType': 'SalesItemLineDetail',
                "Description": item["item_description"],
                "Amount": item["item_amount"],
                "SalesItemLineDetail": {
                    "UnitPrice": item["rate"],
                    "Qty": item["quantity"],
                    "ServiceDate": (
                        (rail.result('invoice_items').get('d', [{}])[-1].get('dateRange', {}).get('startDate') or 
                        rail.result('invoice_items').get('d', [{}])[-1].get('dateRange', {}).get('endDate'))
                        if rail.result('invoice_items').get('d') else None
                    ),
                    'ItemRef': {
                        'name': item[0]['products_standard_price_book'],
                        'value': item[0]['item_id']
                    },
                }
            }
            for item in invoice_data
        ]
    }


def get_update_invoice_sync_status(dag_run):
    return {
        'objectUri': dag_run.conf['uri'],
        'value': {
            'definition': {
                'name': 'Sync Status'
            },
            'tag': {
                'tagName': {
                    'name': 'Synced',
                    'tagDefinitionUri': null
                }
            }
        }
    }


def get_update_invoice_external_system_number(dag_run):
    text_value = rail.result('create_invoice_qbo').get('QueryResponse').get('Invoice').get('DocNumber') \
        if (rail.result('create_invoice_qbo') and rail.result('create_invoice_qbo').get('QueryResponse') and \
            rail.result('create_invoice_qbo').get('QueryResponse').get('Invoice')) else null
    return {
        'objectUri': dag_run.conf['uri'],
        'value': {
            'definition': {
                'name': 'External System Invoice #'
            },
            'textValue': f'QBO Invoice #{text_value}' if text_value else null
        }
    }


def get_update_invoice_sync_note(dag_run):
    return {
        'objectUri': dag_run.conf['uri'],
        'value': {
            'definition': {
                'name': 'Sync Note'
            },
            'textValue': 'Success'
        }
    }
