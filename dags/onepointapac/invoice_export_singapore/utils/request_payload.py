import rail
from onepointapac.invoice_export_singapore import config


null = None


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
            'urn:replicon:invoice2-list-column:total-invoice-amount',
            'urn:replicon:invoice2-list-column:invoice-currency',
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': rail.result('get_sync_status_filter_definition')
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': 'Queued for Sync'
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
            'urn:replicon:invoice-item-column-option:project',
            'urn:replicon:invoice-item-column-option:user',
            'urn:replicon:invoice-item-column-option:billing-rate',
            'urn:replicon:invoice-item-column-option:task',
            'urn:replicon:invoice-item-column-option:entry-date',
        ]
    }


def get_create_contact_payload():
    """Xero Contact from Replicon client details: STREET from clientAddress, POBOX from
    billingAddress, name split into First/Last from billingContact (recipe create_contact1)."""
    client_details = rail.result('get_client_details_in_replicon')
    billing_contact = client_details.get('billingContact')
    billing_address = client_details.get('billingAddress') or {}
    client_address = client_details.get('clientAddress') or {}

    def country_name(address):
        return (address.get('country') or {}).get('name') if address.get('country') else ''

    return {
        "Name": client_details.get('name'),
        "ContactStatus": 'ACTIVE',
        "FirstName": " ".join(billing_contact.split()[:-1]) if billing_contact else " ",
        "LastName": billing_contact.split()[-1] if billing_contact else " ",
        "EmailAddress": client_address.get('email'),
        "Addresses": [
            {
                "AddressType": "STREET",
                "AddressLine1": client_address.get('address'),
                "City": client_address.get('city'),
                "Region": client_address.get('stateProvince'),
                "PostalCode": client_address.get('zipPostalCode'),
                "Country": country_name(client_address),
            },
            {
                "AddressType": "POBOX",
                "AddressLine1": billing_address.get('address'),
                "City": billing_address.get('city'),
                "Region": billing_address.get('stateProvince'),
                "PostalCode": billing_address.get('zipPostalCode'),
                "Country": country_name(billing_address),
            }
        ],
        "Phones": [
            {
                "PhoneType": "DEFAULT",
                "PhoneNumber": client_address.get('phoneNumber'),
                "PhoneAreaCode": "",
                "PhoneCountryCode": ""
            },
            {
                "PhoneType": "FAX",
                "PhoneNumber": client_address.get('faxNumber'),
                "PhoneAreaCode": "",
                "PhoneCountryCode": ""
            }
        ],
    }


def _billing_type(item):
    """Replicon billing type slug, e.g. 'timesheet' / 'expense' / 'fixed-bid' / 'adhoc'."""
    return (item['billingType'].split(":"))[-1] if item.get('billingType') else ''


def _item_quantity(item):
    # GetPageOfInvoiceItemsForInvoice3 omits quantity on non-time items; default to 1.
    quantity = item.get('quantity')
    quantity = quantity if quantity is not None else 1
    return quantity / 8


def _unit_amount(item):
    # Time-based items expose an hourly rate; fixed-bid/expense/adhoc items have no rate,
    # so the line total lives in totalAmount (same fallback as dags/xero/invoice_export).
    amount = (item.get('rate') or {}).get('amount')
    if amount is None:
        amount = (item.get('totalAmount') or {}).get('amount')
    return amount * 8 if amount is not None else None


def _standard_description(item):
    """project.name/ + user + /task + /rate + /startDate (parts omitted when absent)."""
    project = (item.get('project') or {}).get('name')
    user = (item.get('user') or {}).get('displayText')
    task = (item.get('task') or {}).get('displayText')
    billing_rate = (item.get('billingRate') or {}).get('displayText')
    start_date = (item.get('dateRange') or {}).get('startDate')

    description = f"{project}/" if project else ""
    description += user if user else ""
    description += f"/{task}" if task else ""
    description += f"/{billing_rate}" if billing_rate else ""
    description += f"/{start_date}" if start_date else ""
    return description


def _adhoc_description(item, item_description):
    """project.name/ + the item-level metadata description (recipe create action dc5ea9b2)."""
    project = (item.get('project') or {}).get('name')
    description = f"{project}/" if project else ""
    description += item_description if item_description else ""
    return description


def _blank_description(item):
    """project.name/ + 'Ad-hoc' for items with no billing type (recipe create_invoice4)."""
    project = (item.get('project') or {}).get('name')
    description = f"{project}/" if project else ""
    return description + "Ad-hoc"


def _line_description(item, item_descriptions):
    billing_type = _billing_type(item)
    if billing_type == 'adhoc':
        return _adhoc_description(item, item_descriptions.get(item.get('invoiceItemUri')))
    if not billing_type:
        return _blank_description(item)
    return _standard_description(item)


def _make_line_item(item, item_descriptions):
    billing_type = _billing_type(item)
    line_item = {
        "Description": _line_description(item, item_descriptions),
        "Quantity": _item_quantity(item),
        "UnitAmount": _unit_amount(item),
    }
    item_code = config.ITEM_CODE_BY_BILLING_TYPE.get(billing_type, '')
    if item_code:
        line_item["ItemCode"] = item_code
    return line_item


def get_invoice_payload_with_line_items(dag_run):
    """Build the Xero create-invoice payload with one line item per Replicon invoice item."""
    replicon_line_items = [
        item for item in (rail.result('get_all_invoice_items') or [])
        if not item.get('billingType') or item['billingType'] in config.ALLOWED_ITEM_TYPES
    ]
    if not replicon_line_items:
        return {}

    client_details = rail.result('get_client_details_in_replicon')
    item_descriptions = {
        d['invoiceItemUri']: d.get('description')
        for d in (rail.result('enrich_invoice_items') or []) if d and d.get('invoiceItemUri')
    }

    line_items = [_make_line_item(item, item_descriptions) for item in replicon_line_items]

    # A Xero invoice has a single LineAmountTypes; the recipe's first foreach item sets the
    # header, so resolve it from the first line item's billing type (NoTax for fixed-bid).
    first_billing_type = _billing_type(replicon_line_items[0])
    line_amount_type = config.LINE_AMOUNT_TYPE_BY_BILLING_TYPE.get(
        first_billing_type, config.DEFAULT_LINE_AMOUNT_TYPE)

    return {
        "Invoices": [
            {
                "Type": 'ACCREC',
                "Status": 'DRAFT',
                "Contact": {
                    "Name": client_details['name'],
                    "FirstName": client_details.get('billingContact'),
                    "EmailAddress": (client_details.get('clientAddress') or {}).get('email'),
                },
                "LineItems": line_items,
                "Date": dag_run.conf['creation_datetime'],
                "DueDate": dag_run.conf['payment_due_date'],
                "CurrencyCode": dag_run.conf.get('currency_code', config.CURRENCY_CODE),
                "LineAmountTypes": line_amount_type,
                # Recipe does not set InvoiceNumber (Xero auto-numbers). Reference is the
                # Replicon invoice number prefixed with "Proforma Invoice #".
                "Reference": f"{config.REFERENCE_PREFIX}{dag_run.conf['invoice_number']}",
            }
        ]
    }


def get_update_invoice_sync_status(dag_run):
    return {
        'objectUri': dag_run.conf['invoice'],
        'value': {
            'definition': {
                'name': config.SYNC_STATUS_FIELD_NAME
            },
            'tag': {
                'tagName': {
                    'name': config.SYNC_STATUS_SYNCED_TAG,
                    'tagDefinitionUri': null
                }
            }
        }
    }


def get_update_invoice_external_system_number(dag_run):
    # Recipe stores the Xero InvoiceNumber (auto-generated), e.g. "Xero Invoice # INV-0042".
    result = rail.result('create_invoice_in_xero')
    invoice_number = (result['Invoices'][0].get('InvoiceNumber')
                      if result and result.get('Invoices') else None)
    return {
        'objectUri': dag_run.conf['invoice'],
        'value': {
            'definition': {
                'name': config.EXTERNAL_SYSTEM_INVOICE_FIELD_NAME
            },
            'textValue': f'Xero Invoice # {invoice_number}' if invoice_number else null
        }
    }


def get_update_invoice_sync_note(dag_run):
    return {
        'objectUri': dag_run.conf['invoice'],
        'value': {
            'definition': {
                'name': config.SYNC_NOTE_FIELD_NAME
            },
            'textValue': 'Success'
        }
    }
