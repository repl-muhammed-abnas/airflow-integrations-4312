"""Replicon request bodies and Xero payload builders for the MMR Consulting invoice export.

Builds per-billing-type Xero line items (expense, timesheet, fixed-bid, adhoc) and
assembles the final invoice payload from enrichment data in sibling child DAG tasks.
"""
import rail
from mmr_consulting.invoice_export import config
from mmr_consulting.invoice_export.mapper import countries, xero_mappings 

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
    billing_contact = client_details.get('billingContact')
    billing_address = client_details.get('billingAddress') or {}
    client_address = client_details.get('clientAddress') or {}
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


def _billing_type(item):
    """Replicon billing type slug, e.g. 'timesheet' / 'expense' / 'fixed-bid' / 'adhoc'."""
    return (item['billingType'].split(":"))[-1] if item.get('billingType') else ''


def _user_name(item):
    """Reformat Replicon's 'Last, First' user display text to 'First Last'."""
    display_text = (item.get('user') or {}).get('displayText')
    if not display_text:
        return None
    parts = display_text.split(",")
    if len(parts) >= 2:
        return f"{parts[-1].strip()} {parts[0].strip()}"
    return display_text.strip()


def _project_name(item):
    return (item.get('project') or {}).get('name')


def _expense_tracking_number(item):
    """tracking# for an expense line = expenseDescription split on '|', first part."""
    expense_description = item.get('expenseDescription')
    if not expense_description:
        return None
    return expense_description.split("|")[0].strip()


def _or_null(value):
    # blank description becomes the literal string "null" to match expected output
    return value if value else "null"


def _expense_description(item):
    return (f"{_user_name(item)}, \r\nExpense Report for {_project_name(item)} "
            f"with tracking# {_expense_tracking_number(item)}, as per attached Expense Report")


def _timesheet_description(item, is_monthly_billing):
    if is_monthly_billing:
        return f"Consulting Services on \n{_project_name(item)},\nProgress billing"
    return (f"{_user_name(item)}, \r\nConsulting Services on {_project_name(item)}, "
            f"as per attached Timesheet Report")


def _adhoc_description(item_description, project_name):
    return f"Consulting Services on \n{project_name}\n{item_description}"


def _item_quantity(item):
    return item.get('quantity') if item.get('quantity') is not None else 1


def _rate_amount(item):
    return (item.get('rate') or {}).get('amount')


def _total_amount(item):
    return (item.get('totalAmount') or {}).get('amount')


def _sum_total_amounts(items):
    """Sum totalAmount.amount across a group; returns None if all amounts are None."""
    total = 0
    seen_value = False
    for item in items:
        amount = _total_amount(item)
        if amount is not None:
            total += amount
            seen_value = True
    return total if seen_value else None


def _is_monthly_billing(item, project_po_types):
    """True when the item's project PO Type contains PO_TYPE_MONTHLY_BILLING (substring match)."""
    project = item.get('project') or {}
    project_uri = project.get('uri')
    po_type = project_po_types.get(project_uri) if project_uri else None
    return bool(po_type) and xero_mappings.PO_TYPE_MONTHLY_BILLING in po_type


def _make_line_item(country_config, description, quantity, unit_amount, item_code):
    """Assemble a single Xero line item with the fields common to every line."""
    line_item = {
        "Description": _or_null(description),
        "Quantity": quantity,
        "AccountCode": config.DEFAULT_ACCOUNT_CODE,
        "UnitAmount": unit_amount,
        "TaxType": country_config['tax_type'],
    }
    if item_code:
        line_item["ItemCode"] = item_code

    # BC tracking attached to every line that has a description.
    tracking_name = country_config.get('tracking_category_name')
    if tracking_name and description:
        line_item["Tracking"] = [
            {"Name": tracking_name, "Option": config.TRACKING_OPTION}]
    return line_item


def parse_line_items(replicon_line_items, country_config, item_descriptions, project_po_types):
    """Build Xero line items for one invoice using three passes:
    1. Per-item lines for adhoc, fixed-bid, and non-monthly timesheet items.
    2. One aggregated EXP line per distinct expenseDescription (summed totalAmount).
    3. One aggregated SVCS-TM line per project for monthly-billing timesheet items.
    """
    line_items = []

    expense_groups = {}      # expenseDescription -> [items] (first-seen order)
    monthly_groups = {}      # project uri       -> [items] (first-seen order)

    # Pass 1: per-item lines (adhoc / fixed-bid / non-monthly timesheet). Expense
    # and monthly-billing timesheet items are deferred to the aggregation passes.
    for item in replicon_line_items:
        if item.get('billingType') and item['billingType'] not in xero_mappings.ALLOWED_ITEM_TYPES:
            continue

        billing_type = _billing_type(item)
        item_code = xero_mappings.ITEM_CODE_BY_BILLING_TYPE.get(billing_type, '')

        if billing_type == 'expense':
            # Deferred to pass 2 (aggregated by expenseDescription).
            key = item.get('expenseDescription')
            expense_groups.setdefault(key, []).append(item)
            continue

        if billing_type == 'timesheet':
            if _is_monthly_billing(item, project_po_types):
                # Deferred to pass 3 (aggregated by project).
                project = item.get('project') or {}
                monthly_groups.setdefault(project.get('uri'), []).append(item)
                continue
            description = _timesheet_description(item, is_monthly_billing=False)
            line_items.append(_make_line_item(
                country_config, description, _item_quantity(item),
                _rate_amount(item), item_code))
            continue

        if billing_type == 'fixed-bid':
            description = item_descriptions.get(item.get('invoiceItemUri'))
            line_items.append(_make_line_item(
                country_config, description, _item_quantity(item),
                _rate_amount(item), item_code))
            continue

        # adhoc (and any other allowed type): no ItemCode, newline description.
        item_description = item_descriptions.get(item.get('invoiceItemUri')) or ''
        description = _adhoc_description(item_description, _project_name(item))
        line_items.append(_make_line_item(
            country_config, description, _item_quantity(item),
            _rate_amount(item), item_code))

    # Pass 2: aggregated expense lines (one per distinct expenseDescription).
    for group in expense_groups.values():
        first = group[0]
        description = _expense_description(first)
        line_items.append(_make_line_item(
            country_config, description, 1, _sum_total_amounts(group),
            xero_mappings.ITEM_CODE_BY_BILLING_TYPE.get('expense', '')))

    # Pass 3: aggregated monthly-billing timesheet lines (one per distinct project).
    for group in monthly_groups.values():
        first = group[0]
        description = _timesheet_description(first, is_monthly_billing=True)
        line_items.append(_make_line_item(
            country_config, description, 1, _sum_total_amounts(group),
            xero_mappings.ITEM_CODE_BY_BILLING_TYPE.get('timesheet', '')))

    return line_items


def get_invoice_payload_with_line_items(dag_run):
    country_config = countries.COUNTRIES[dag_run.conf['country']]
    replicon_invoice_line_items = rail.result('get_all_invoice_items') or []
    client_details = rail.result('get_client_details_in_replicon')

    # invoiceItemUri -> item-level description (from GetInvoiceItem).
    item_descriptions = {
        d['invoiceItemUri']: d.get('description')
        for d in (rail.result('enrich_invoice_items') or []) if d and d.get('invoiceItemUri')
    }
    # project uri -> PO Type (from GetProjectDetails2).
    project_po_types = {
        p['uri']: p.get('po_type')
        for p in (rail.result('get_project_po_types') or []) if p and p.get('uri')
    }

    invoice_line_items = parse_line_items(
        replicon_invoice_line_items, country_config, item_descriptions, project_po_types)
    if not invoice_line_items:
        return {}
    return {
        "Invoices": [
            {
                "Type": 'ACCREC',
                "Contact": {
                    "Name": client_details['name'],
                    "FirstName": client_details['billingContact'],
                    "EmailAddress": client_details['clientAddress'].get('email')
                },
                "LineItems": invoice_line_items,
                "Date": dag_run.conf['invoice_date'],
                "DueDate": dag_run.conf['payment_due_date'],
                "CurrencyCode": dag_run.conf.get('currency_code'),
                "Status": "DRAFT",
                "LineAmountTypes": country_config['line_amount_types'],
                # Xero InvoiceNumber = Replicon invoice number; Reference = PO number.
                "InvoiceNumber": dag_run.conf['invoice_number'],
                "Reference": dag_run.conf.get('po_number'),
            }
        ]
    }


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
    # Stores the Xero InvoiceID (GUID), not the InvoiceNumber.
    text_value = rail.result('create_invoice_in_xero')['Invoices'][0]['InvoiceID'] if rail.result(
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
