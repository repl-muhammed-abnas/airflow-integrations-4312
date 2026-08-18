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
    primary_email_address = billing_address['email']
    primary_phone = billing_address['phoneNumber']
    return {
        'PrimaryEmailAddr': {
            'Address': primary_email_address
        } if primary_email_address else null,
        'DisplayName': dag_run.conf['client']['textValue'],
        'FullyQualifiedName': dag_run.conf['client']['textValue'],
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
        'CompanyName': dag_run.conf['client']['textValue']
    }


def get_date_in_str(replicon_date, empty_delim):
    return f"{replicon_date['year']}-{replicon_date['month']}-{replicon_date['day']}" if replicon_date else empty_delim


def get_description(invoice):
    empty_delim = ' '
    project_name = f"{invoice['project']['name']}/" if invoice['project'] else empty_delim
    user_display_text = invoice['user']['displayText'] if invoice['user'] else empty_delim
    task_display_text = f"/{invoice['task']['displayText']}" if invoice['task'] else empty_delim
    billingrate_display_text = f"/{invoice['billingRate']['displayText']}" if invoice['billingRate'] else empty_delim
    expensecode_display_text = f"/{invoice['expenseCode']['displayText']}" if invoice['expenseCode'] else empty_delim
    startdate = f"/{get_date_in_str(invoice['dateRange']['startDate'], empty_delim)}" if invoice['dateRange'] else empty_delim
    timesheetperiod_startdate = get_date_in_str(
        invoice['timesheetPeriod']['startDate'], empty_delim) if invoice['timesheetPeriod'] else empty_delim
    timesheetperiod_enddate = get_date_in_str(
        invoice['timesheetPeriod']['endDate'], empty_delim) if invoice['timesheetPeriod'] else empty_delim
    # pylint: disable=line-too-long
    return f'{project_name}{user_display_text}{task_display_text}{billingrate_display_text}{expensecode_display_text}{startdate}{timesheetperiod_startdate} - {timesheetperiod_enddate}'


def get_sales_item(invoice_qbo_items,project_oef_values,invoice_qbo_taxcodes):
    invoice_items = result('get_invoice_items')
    sales_line_items = []
    for invoice in invoice_items:
        description = get_description(invoice)
        total_amount = invoice['totalAmount']['amount'] if invoice['totalAmount'] else null
        rate_amount = invoice['rate']['amount'] if invoice['rate'] else total_amount
        quantity = invoice['quantity'] if invoice.get(
            'quantity') != None else 1
        sales_items = [x for x in invoice_qbo_items if x['Name'] == project_oef_values['product_services_oef_val']]
        sales_line_items.append({
            'DetailType': 'SalesItemLineDetail',
            'Amount': total_amount,
            'Description': description,
            'SalesItemLineDetail': {
                'TaxCodeRef': {
                    'value': find_first_by_attr_and_get_attr(invoice_qbo_taxcodes, 'Name', 'Exempt', "Id")
                },
                'ItemRef': {
                    'name': sales_items[0]['Name'],
                    'value': sales_items[0]['Id']
                },
                'UnitPrice': rate_amount,
                'Qty': quantity
            }
        })
    return sales_line_items

def get_custom_fields(project_oef_values,customfield_data_details_qbo):
    return [
        {
            "DefinitionId": find_first_by_attr_and_get_attr(customfield_data_details_qbo,'name','Project Reference','definition_id'),
            "StringValue": project_oef_values['project_reference_oef_val'], 
            "Type": "StringType"
        },
        {
            "DefinitionId": find_first_by_attr_and_get_attr(customfield_data_details_qbo,'name','Project Lead','definition_id'),
            "StringValue": project_oef_values['project_lead_oef_val'],
            "Type": "StringType"
        },
        {
            "DefinitionId": find_first_by_attr_and_get_attr(customfield_data_details_qbo,'name','Purchase Order/SOW','definition_id'),
            "StringValue": project_oef_values['order_sow_oef_val'],
            "Type": "StringType"
        },
    ]


def create_invoice_with_multiline_item_request(dag_run):
    invoice_date = dag_run.conf['creation_datetime']
    statement_memo = dag_run.conf['invoice']
    customer_id = result('parse_qbo_customer').get('Id') if result(
        'parse_qbo_customer') else (result('create_customer_qbo').get('Customer', {}).get('Id') if result(
            'create_customer_qbo') else null)
    display_message = 'Thank you for your business and have a great day!'
    due_date = dag_run.conf['payment_due_date']
    invoice_number = dag_run.conf['invoice_number']
    invoice_qbo_items = result('search_items_qbo')['QueryResponse']['Item'] if result(
        'search_items_qbo')['QueryResponse'] else ''
    invoice_qbo_taxcodes = result('search_taxcodes_qbo')['QueryResponse']['TaxCode'] if result(
        'search_taxcodes_qbo')['QueryResponse'] else ''
    project_oef_values = result('get_project_oef_feild_values')
    customfield_data_details_qbo = result("get_customfield_data_values_qbo")

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
        'Line': get_sales_item(invoice_qbo_items, project_oef_values, invoice_qbo_taxcodes),
        'DueDate': due_date,
        "CustomField": get_custom_fields(project_oef_values,customfield_data_details_qbo)
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
            'textValue': f'QBO Invoice {text_value}' if text_value else null
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

def get_project_oef_values():
    project_oef_details = result('get_project_details')['extensionFieldValues']
    if project_oef_details:
        return {
            'product_services_oef_val':find_first_by_attr_and_get_attr(project_oef_details, 'definition.displayText', 'Product/Services', 'textValue', ''),
            'project_reference_oef_val':find_first_by_attr_and_get_attr(project_oef_details, 'definition.displayText', 'Project Reference', 'textValue', ''),
            'project_lead_oef_val':find_first_by_attr_and_get_attr(project_oef_details, 'definition.displayText', 'Project Lead', 'textValue', ''),
            'order_sow_oef_val':find_first_by_attr_and_get_attr(project_oef_details, 'definition.displayText', 'Order/SOW', 'textValue', ''),
        }
    return {
        'product_services_oef_val':'',
        'project_reference_oef_val':'',
        'project_lead_oef_val':'',
        'order_sow_oef_val':'',
    }
    
def get_customefield_data_val_qbo():
    query_for_salesform_preferences = result('search_custom_feilds')['QueryResponse']['Preferences'][0]['SalesFormsPrefs']
    customfields_lists_in_qbo = query_for_salesform_preferences['CustomField'][1]['CustomField']
    return list(map(lambda x:{
        'definition_id': x['Name'][-1],
        'name': x['StringValue'],
        'type': x['Type']
    }, customfields_lists_in_qbo))

def check_product_service_present_in_qbo():
    invoice_qbo_items = result('search_items_qbo')['QueryResponse']['Item'] if result(
        'search_items_qbo')['QueryResponse'] else ''
    project_oef_values = result('get_project_oef_feild_values')
    sales_items = [x for x in invoice_qbo_items if x['Name'] == project_oef_values['product_services_oef_val']]
    print(sales_items)
    if sales_items:
        return sales_items
    return None
