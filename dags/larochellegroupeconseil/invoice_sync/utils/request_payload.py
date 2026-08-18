import rail
import uuid
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
                    'text': 'Queued for Synced'
                }
            }
        }
    }


def update_invoice_details(dag_run):
    return {
            "invoice": {
            "target": {
            "uri": dag_run.conf['uri'],
            "invoiceNumberText": null,
            "parameterCorrelationId": null
            },
            "client": {
            "uri": dag_run.conf['client']['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
            },
            "invoiceNumberText": dag_run.conf['invoiceNumberText'],
            "invoiceCurrency": {
            "uri": dag_run.conf['invoiceCurrency']['uri'],
            "name": null,
            "symbol": null
            },
            "customMetadata": rail.result('custom_metadata_list'),
            "extensionFieldValues": []
            },
            "unitOfWorkId": f'unitOfWorkId{str(uuid.uuid4())}'
    }


def update_invoice_sync_status_success(dag_run):
    return {
        'objectUri': dag_run.conf['uri'],
        'value': {
            'definition': {
                'name': 'Sync Status'
            },
            'tag': {
                'tagName': {
                    'name': 'Invoice Update - success',
                    'tagDefinitionUri': null
                }
            }
        }
    }


def get_update_invoice_sync_status_fail(dag_run):
    return {
        'objectUri': dag_run.conf['uri'],
        'value': {
            'definition': {
                'name': 'Sync Status'
            },
            'tag': {
                'tagName': {
                    'name': 'Invoice Update - failed',
                    'tagDefinitionUri': null
                }
            }
        }
    }