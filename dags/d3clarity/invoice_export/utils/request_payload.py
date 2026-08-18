from datetime import datetime
import rail

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
            'urn:replicon:invoice2-list-column:invoice-date',
            'urn:replicon:invoice2-list-column:total-invoice-amount',
            'urn:replicon:invoice2-list-column:invoice-currency',
            'urn:replicon:invoice2-list-column:payment-term',
            'urn:replicon:invoice2-list-column:invoice-amount-in-base-currency',
            'urn:replicon:invoice2-list-column:description'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': rail.result('get_sync_status_filter_definition')
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': 'Queued for Synced'
                }
            }
        }
    }


def get_create_contact_payload():
    client_details = rail.result('get_client_details_in_replicon')
    billing_contact = client_details['billingContact']
    billing_address = client_details['billingAddress']
    client_address = client_details['clientAddress']
    return {
        "ContactStatus": 'ACTIVE',
        "FirstName": " ".join((billing_contact).split()[:-1]) if billing_contact else " ",
        "LastName": (((billing_contact).split())[-1]) if billing_contact else " ",
        "EmailAddress": client_address.get('email'),
        "Addresses": [
            {
                "AddressType": "POBOX",
                "AddressLine1": billing_address.get('address'),
                "City": billing_address.get('city'),
                "Region": billing_address.get('stateProvince'),
                "PostalCode": billing_address.get('zipPostalCode'),
                "Country": billing_address.get('country').get('name') if billing_address.get('country') else ''
            },
            {
                "AddressType": "STREET",
                "AddressLine1": client_address.get('address'),
                "City": client_address.get('city'),
                "Region": client_address.get('stateProvince'),
                "PostalCode": client_address.get('zipPostalCode'),
                "Country": client_address.get('country').get('name') if client_address.get('country') else ''
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
        "Name": client_details.get('name')
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


def get_date_string(dateobj):
    day = dateobj.get('day')
    month = dateobj.get('month')
    year = dateobj.get('year')
    return (datetime.strptime(f'{year}-{month}-{day}',"%Y-%m-%d")).strftime("%Y-%m-%d")

def parse_line_items(replicon_line_items, item_mapper):
    line_items = []
    allowed_item_types = ['urn:replicon:invoice-item-type:timesheet', 'urn:replicon:invoice-item-type:expense', 'urn:replicon:invoice-item-type:fixed-bid',
                          'urn:replicon:invoice-item-type:adhoc']
    for item in replicon_line_items:
        billing_type = (item['billingType'].split(
            ":"))[-1] if item['billingType'] else ''
        line_items.append(
            {
                "Description": (((item['project'].get('name') + "/") if (item['project'] and item['project'].get('name')) else "") +
                                "Ad-hoc") if (not billing_type or billing_type == 'adhoc') else (
                    ((item['project'].get('name') + "/") if (item['project'] and item['project'].get('name')) else "") +
                    (item['user'].get('displayText') if (item['user'] and item['user'].get('displayText')) else "") +
                    (("/" + item['task'].get('displayText')) if (item['task'] and item['task'].get('displayText')) else "") +
                    (("/" + item['billingRate'].get('displayText')) if (item['billingRate'] and item['billingRate'].get('displayText')) else "") +
                    (("/" + get_date_string(item['dateRange'].get('startDate'))) if (item['dateRange'] and item['dateRange'].get('startDate')) else "")),
                "ItemCode": "" if not billing_type else item_mapper[billing_type]['item_code'],
                "Quantity": item['quantity'] if item.get('quantity') != None else 1,
                "AccountCode": "200" if not billing_type else item_mapper[billing_type]['account_code'],
                "UnitAmount": item['rate'].get('amount') if item['rate'] and item['rate'].get('amount') else item['totalAmount'].get('amount'),
            } if ( not(item['billingType']) or (item['billingType'] in allowed_item_types))  else {}
        )
    return list(filter(None, line_items))

def get_ymd_dateformat(dt):
    try:
        dt = datetime.strptime(dt, '%b %d, %Y').strftime('%Y-%m-%d')
    except:
        dt = dt
    return dt


def get_invoice_payload_with_line_items(dag_run):
    replicon_invoice_line_items = rail.result('get_all_invoice_items')
    xero_inventory_items = list(map(lambda item: {
        'name': item['Code'],
        'accountcode': item['SalesDetails'].get('AccountCode', '200') if item['SalesDetails'] else '200'
    }, (rail.result('get_inventory_items_from_xero'))['Items']))
    client_details = rail.result('get_client_details_in_replicon')
    invoice_currency = {
        "US Dollar": "USD",
        "Australian Dollar": "AUD",
        "British Pound": "GBP",
        "Canadian Dollar": "CAD",
        "Euro": "EUR",
        "Japanese Yen": "JPY",
        "Swiss Franc": "CHF",
        "Singapore Dollar": "SGD"
    }
    item_mapper = {
        'timesheet': {
            'item_code': 'SVCS-TM',
            'account_code': rail.find_first_by_attr_and_get_attr(xero_inventory_items, 'name', 'SVCS-TM', 'accountcode', '200')
        },
        'expense': {
            'item_code': 'EXP',
            'account_code': rail.find_first_by_attr_and_get_attr(xero_inventory_items, 'name', 'EXP', 'accountcode', '200')
        },
        'fixed-bid': {
            'item_code': 'SVCS-FC',
            'account_code': rail.find_first_by_attr_and_get_attr(xero_inventory_items, 'name', 'SVCS-FC', 'accountcode', '200')
        },
        'adhoc': {
            'item_code': '',
            'account_code': '200'
        }
    }
    invoice_line_items = parse_line_items(
        replicon_invoice_line_items, item_mapper)
    payload = {
        "Invoices": [
            {
                "Type": 'ACCREC',
                "Contact": {
                    "Name": client_details['name'],
                    "FirstName": client_details['billingContact'],
                    "EmailAddress": client_details['clientAddress'].get('email')
                },
                "LineItems": invoice_line_items,
                "Date": get_ymd_dateformat(dag_run.conf['invoice_date']),
                "DueDate": dag_run.conf['payment_due_date'],
                "CurrencyCode": invoice_currency.get(dag_run.conf['invoice_currency'].get('textValue'), dag_run.conf['invoice_currency'].get('textValue')),
                "Status": "DRAFT",
                "LineAmountTypes": "NoTax",
                "Reference": dag_run.conf['invoice_number'],
            }
        ]
    } if invoice_line_items else {}
    return payload


def get_update_invoice_sync_status(dag_run):
    return {
        'objectUri': dag_run.conf['invoice'],
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
    text_value = rail.result('create_invoice_in_xero')['Invoices'][0]['InvoiceNumber'] if rail.result(
        'create_invoice_in_xero') and rail.result(
        'create_invoice_in_xero')['Invoices'] else null
    return {
        'objectUri': dag_run.conf['invoice'],
        'value': {
            'definition': {
                'name': 'External System Invoice #'
            },
            'textValue': f'Xero Invoice # {text_value}' if text_value else null
        }
    }


def get_update_invoice_sync_note(dag_run):
    return {
        'objectUri': dag_run.conf['invoice'],
        'value': {
            'definition': {
                'name': 'Sync Note'
            },
            'textValue': 'Success'
        }
    }
