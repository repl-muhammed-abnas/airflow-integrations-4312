import decimal, uuid
import rail

null = None


#master dag

def get_queued_invoices_method():
     return {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:invoice2-list-column:invoice",
                    rail.result('get_sync_status_column_uri')
                ], 
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": rail.result('get_sync_status_filter_uri')
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "Queued for Synced"
                        },
                        "filterDefinitionUri": None
                    },
                    "value": None,
                    "filterDefinitionUri": None
                }
            }


#child dags

def calculate_vat_fun(dag_run):
        invoice_items = rail.result('get_invoice_item_details')
        
        # Variables to track
        total_amount = decimal.Decimal('0')
        vat_amount = decimal.Decimal('0')
        vat_line_uri = None
        
        # Process each invoice item
        for item in invoice_items:
            details = item.get('details', {})
            item_uri = details.get('invoiceItemUri')
            amount = decimal.Decimal(str(details.get('amount', 0)))
            
            # Check if this is a VAT line by examining custom metadata
            is_vat_line = False
            for metadata in details.get('customMetadata', []):
                if (metadata.get('keyUri') == 'urn:replicon:invoice-item-metadata-key:description' and 
                    metadata.get('value', {}).get('text') == 'VAT'):
                    is_vat_line = True
                    vat_amount = amount
                    vat_line_uri = item_uri
                    break
            
            # Add to total if not a VAT line
            if not is_vat_line:
                total_amount += amount
        
        # Calculate expected VAT (20% of total)
        vat_percentage = decimal.Decimal(str(dag_run.conf['vat_percentage']))
        expected_vat = (total_amount * vat_percentage / decimal.Decimal('100')).quantize(
            decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP
        )
        
        return {
            'total_amount': float(total_amount),
            'current_vat_amount': float(vat_amount),
            'expected_vat_amount': float(expected_vat),
            'vat_line_uri': vat_line_uri,
            'vat_correct': vat_amount == expected_vat
        }



def get_invoice_items_method(dag_run):
    return {
            'page': 1,
            'pageSize': 10000,
            'invoice': {
                'uri': dag_run.conf['invoice_uri']
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


def skip_processing_no_items_method(dag_run):
     return {
            'objectUri': dag_run.conf['invoice_uri'],
            'value': {
                'definition': {
                    'name': 'Sync Status'
                },
                'tag': {
                    'tagName': {
                        'name': 'Not Sync',
                        'tagDefinitionUri': null
                    }
                }
            }
        }


def skip_processing_correct_vat_method(dag_run):
     return {
        'objectUri': dag_run.conf['invoice_uri'],
        'value': {
            'definition': {
                'name': 'Sync Status'
            },
            'tag': {
                'tagName': {
                    'name': 'Not Sync',
                    'tagDefinitionUri': null
                }
            }
        }
    }


def add_vat_line_method(dag_run):
     return {
            'invoiceItem': {
                'target': None,
                'invoice': {
                    'uri': dag_run.conf['invoice_uri'],
                    'invoiceNumberText': None,
                    'parameterCorrelationId': None
                },
                'amount': str(rail.result('calculate_vat')['expected_vat_amount']),
                'customMetadata': [
                    {
                        'keyUri': 'urn:replicon:invoice-item-metadata-key:billing-type',
                        'value': {
                            'uri': 'urn:replicon:invoice-item-type:adhoc',
                            'slug': None,
                            'bool': None,
                            'date': None,
                            'number': None,
                            'text': None,
                            'time': None,
                            'calendarDayDurationValue': None,
                            'workdayDurationValue': None,
                            'dateRange': None,
                            'collection': []
                        }
                    },
                    {
                        'keyUri': 'urn:replicon:invoice-item-metadata-key:quantity',
                        'value': {
                            'uri': None,
                            'slug': None,
                            'bool': None,
                            'date': None,
                            'number': '1',
                            'text': None,
                            'time': None,
                            'calendarDayDurationValue': None,
                            'workdayDurationValue': None,
                            'dateRange': None,
                            'collection': []
                        }
                    },
                    {
                        'keyUri': 'urn:replicon:invoice-item-metadata-key:rate',
                        'value': {
                            'uri': None,
                            'slug': None,
                            'bool': None,
                            'date': None,
                            'number': str(rail.result('calculate_vat')['expected_vat_amount']),
                            'text': None,
                            'time': None,
                            'calendarDayDurationValue': None,
                            'workdayDurationValue': None,
                            'dateRange': None,
                            'collection': []
                        }
                    },
                    {
                        'keyUri': 'urn:replicon:invoice-item-metadata-key:description',
                        'value': {
                            'uri': None,
                            'slug': None,
                            'bool': None,
                            'date': None,
                            'number': None,
                            'text': 'VAT',
                            'time': None,
                            'calendarDayDurationValue': None,
                            'workdayDurationValue': None,
                            'dateRange': None,
                            'collection': []
                        }
                    }
                ]
            },
            'unitOfWorkId': str(uuid.uuid4())
            }
    


def update_invoice_sync_status_method(dag_run):
     return {
            'objectUri': dag_run.conf['invoice_uri'],
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