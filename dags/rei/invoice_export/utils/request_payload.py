from datetime import datetime
from rail import result, find_first_by_attr_and_get_attr

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
                'filterDefinitionUri': result('get_sync_status_filter_definition')
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': 'Queued for Synced'
                }
            }
        }
    }


def get_client_address(dag_run):
    return {
        'address': {
            'value': dag_run.conf['client_address']
        },
        'city': {'value': dag_run.conf['client_city']} if dag_run.conf['client_city'] else null,
        'stateProvince': {'value': dag_run.conf['client_state']} if dag_run.conf['client_state'] else null,
        'country': {'value': {'uri': dag_run.conf['client_country']}} if dag_run.conf['client_country'] else null,
        'zipPostalCode': {'value': dag_run.conf['client_zip']} if dag_run.conf['client_zip'] else null,
        'phoneNumber': {'value': dag_run.conf['client_phone_number']} if dag_run.conf['client_phone_number'] else null,
        'email': {'value': dag_run.conf['client_email']} if dag_run.conf['client_email'] else null
    }


def get_billing_address(dag_run):
    return {
        'address': {
            'value': dag_run.conf['billing_address']
        },
        'city': {'value': dag_run.conf['billing_city']} if dag_run.conf['billing_city'] else null,
        'stateProvince': {'value': dag_run.conf['billing_state']} if dag_run.conf['billing_state'] else null,
        'country': {'value': {'uri': dag_run.conf['billing_country']}} if dag_run.conf['billing_country'] else null,
        'zipPostalCode': {'value': dag_run.conf['billing_zip']} if dag_run.conf['billing_zip'] else null
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
            'urn:replicon:invoice-item-column-option:expense-code',
            'urn:replicon:invoice-item-column-option:timesheet-period'
        ]
    }


def create_customer_qbo_request(dag_run):
    billing_address = result('get_client_details')['billingAddress']
    client_address = result('get_client_details')['clientAddress']
    company_name = result('get_client_details')['displayText']
    primary_email_address = billing_address['email']
    primary_phone = billing_address['phoneNumber']
    fax_number = billing_address['faxNumber']
    return {
        'PrimaryEmailAddr': {
            'Address': primary_email_address
        } if primary_email_address else null,
        'DisplayName': dag_run.conf['client']['textValue'],
        'FullyQualifiedName': dag_run.conf['client']['textValue'],
        'PrimaryPhone': {
            'FreeFormNumber': primary_phone
        } if primary_phone else null,
        'Fax': {
            'FreeFormNumber': fax_number
        } if fax_number else null,
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
        'CompanyName': company_name
    }


def get_date_in_str(replicon_date, empty_delim):
    return f"{replicon_date['year']}-{replicon_date['month']}-{replicon_date['day']}" if replicon_date else empty_delim


def get_description(invoice):
    billing_type = invoice['billingType'].split(
        ':')[-1] if invoice['billingType'] else ''
    empty_delim = ' '
    desc = ''
    if billing_type == 'expense':
        desc = invoice['project']['displayText']
    else:
        project_name = f"{invoice['project']['name']}/" if invoice['project'] else empty_delim
        task_display_text = f"/{invoice['task']['displayText']}" if invoice['task'] else empty_delim
        desc = f'{project_name}{task_display_text}'
    return desc


def get_sales_item(invoice_qbo_items):
    invoice_items = result('get_invoice_items')
    sales_line_items = []
    for invoice in invoice_items:
        billing_type = invoice['billingType'].split(
            ':')[-1] if invoice['billingType'] else ''
        description = get_description(invoice)
        total_amount = invoice['totalAmount']['amount'] if invoice['totalAmount'] else null
        rate_amount = invoice['rate']['amount'] if invoice['rate'] else total_amount
        quantity = invoice['quantity'] if invoice.get(
            'quantity') != None else 1
        if billing_type == 'expense':
            sales_items_ref = [x for x in invoice_qbo_items if x['Name'] == invoice['expenseCode']['displayText']]
        else:
            sales_items_ref = [x for x in invoice_qbo_items if x['Name'] == invoice['billingRate']['name']]

        if billing_type == 'expense' and invoice['dateRange']:
            service_date = invoice['dateRange']['startDate']
        elif billing_type == 'timesheet' and invoice['timesheetPeriod']:
            service_date = invoice['timesheetPeriod']['startDate']
        else:
            service_date = null
        if service_date:
            service_date = f"{service_date['year']}-{service_date['month']:02d}-{service_date['day']:02d}"
        sales_line_items.append({
            'DetailType': 'SalesItemLineDetail',
            'Amount': total_amount,
            'Description': description,
            'SalesItemLineDetail': {
                'ItemRef': {
                    'name': sales_items_ref[0]['Name'],
                    'value': sales_items_ref[0]['Id']
                },
                'UnitPrice': rate_amount,
                'Qty': quantity,
                'ServiceDate': service_date
            }
        })
    return sales_line_items


def create_invoice_with_multiline_item_request(dag_run):
    invoice_date = dag_run.conf['creation_datetime']
    statement_memo = dag_run.conf['invoice']
    customer_id = result('parse_qbo_customer').get('Id') if result(
        'parse_qbo_customer') else (result('create_customer_qbo').get('Customer', {}).get('Id') if result(
            'create_customer_qbo') else null)
    sales_term = result('look_for_pay_terms_in_qbo')['QueryResponse']['Term'] if result(
        'look_for_pay_terms_in_qbo')['QueryResponse'] else ''
    sale_term_ref = find_first_by_attr_and_get_attr(sales_term, 'DueDays', int(dag_run.conf['payment_term']['numberValue']), 'Id')
    display_message = 'Thank you for your business and have a great day!'
    due_date = dag_run.conf['payment_due_date']
    invoice_number = f"{dag_run.conf['invoice_number']}"
    invoice_qbo_items = result('search_items_qbo')['QueryResponse']['Item'] if result(
        'search_items_qbo')['QueryResponse'] else ''


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
        'SalesTermRef': {
            'value': sale_term_ref
        } if sale_term_ref else {},
        'Line': get_sales_item(invoice_qbo_items),
        'DueDate': due_date
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
    text_value = result('create_invoice_with_multiline_item')['Invoice']['DocNumber'] if result(
        'create_invoice_with_multiline_item')['Invoice'] else null
    return {
        'objectUri': dag_run.conf['invoice'],
        'value': {
            'definition': {
                'name': 'External System Invoice #'
            },
            'textValue': f'QBO Invoice #{text_value}' if text_value else null
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

def get_dates_in_datetime(dates):
    return [datetime.strptime(date, '%Y-%m-%d') for date in dates]

def get_start_end_date(invoice_items):
    start_date_list = []
    end_date_list = []
    
    for rec in invoice_items:
        billing_type = rec.get('billingType', '')
        if billing_type and billing_type.split(':')[-1] == 'timesheet':
            timesheet_period = rec.get('timesheetPeriod')
            if timesheet_period:
                start_date = timesheet_period.get('startDate')
                end_date = timesheet_period.get('endDate')
                if start_date:
                    start_date_list.append(get_date_in_str(start_date, ''))
                if end_date:
                    end_date_list.append(get_date_in_str(end_date, ''))
    
    start_dates_in_datetime = get_dates_in_datetime(start_date_list) if start_date_list else []
    end_dates_in_datetime = get_dates_in_datetime(end_date_list) if end_date_list else []
    
    start_date = datetime.strftime(min(start_dates_in_datetime), '%Y-%m-%d') if start_dates_in_datetime else None
    end_date = datetime.strftime(max(end_dates_in_datetime), '%Y-%m-%d') if end_dates_in_datetime else None
    return start_date, end_date

def get_report_extract_child_dag_payload(dag_run):
    invoice_items = result('get_invoice_items')
    start_date, end_date = get_start_end_date(invoice_items)
    project_list = []
    for invoice in invoice_items:
        project_list.append({
            "project":invoice['project']['name']
        })
    invoice_number = dag_run.conf['invoice_number']
    return {
        "timesheetperiodstartdate": start_date,
        "timesheetperiodenddate": end_date,
        "projectlists": project_list,
        "invoicenumber": invoice_number,
        "notification_email": dag_run.conf['notification_email']
    }

def get_project_filters(enablefilter, project_list):
    filter_list = []
    all_projects = result('get_all_projects')
    filter_uri = find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'ProjectFilter', 'uri')
    for project in project_list:
        value = find_first_by_attr_and_get_attr(all_projects, 'displayText', project['project'], 'uri', '')
        filter_list.append({
            "reportFilterUri": filter_uri,
            "value": value.split(":")[-1]
        })
    return filter_list

def get_generate_report_batch_param(dag_run):
    enablefilter = result('get_report_details')['filterConfiguration']['enabledFilters']

    filter_values = get_project_filters(enablefilter, dag_run.conf['projectlists'])
    filter_values.append({
        "reportFilterUri": find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'TimesheetPeriodFilter', 'uri'),
        "value": None
    })
    filter_values.append({
        "reportFilterUri": find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'TimesheetPeriodFilter', 'uri'),
        "value": dag_run.conf['timesheetperiodstartdate']
    })
    filter_values.append({
        "reportFilterUri": find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'TimesheetPeriodFilter', 'uri'),
        "value": dag_run.conf['timesheetperiodenddate']
    })
    return {"reportParameters": [
                {
                    "reportUri": result('get_report_details')['uri'],
                    "filterValues": filter_values,
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
            ]
        }

def prepare_variables(dag_run):
    invoice_items = result('get_invoice_items')
    billingrate = list(set(map(lambda x: x["billingRate"]["displayText"], filter(lambda x: bool(x["billingRate"]), invoice_items))))
    expensecode = list(set(map(lambda x: x["expenseCode"]["displayText"], filter(lambda x: bool(x["expenseCode"]), invoice_items))))
    items = billingrate + expensecode
    formatted_items = ', '.join(f"'{item}'" for item in set(items))
    query_to_use = f"SELECT DISTINCT qboitems itemname FROM items WHERE itemname IN ({formatted_items})"

    return {
        "billingrate": billingrate,
        "expensecode": expensecode,
        "billingratecount": len(billingrate),
        "expensecodecount": len(expensecode),
        "billing_expense_rate_count": len(billingrate) + len(expensecode),
        "billingrate_query_val": str(tuple(billingrate)),
        "expensecode_query_val": str(tuple(expensecode)),
        "query_to_use": query_to_use,
    }

def qbo_items_list(dag_run):
    invoice_qbo_items = result('search_items_qbo')['QueryResponse']['Item'] if result(
        'search_items_qbo')['QueryResponse'] else ''
    qboitems = [{"qboitems":x['Name']} for x in invoice_qbo_items]
    return qboitems
