# pylint: disable=line-too-long
import uuid
import rail
null = None


def get_client_details_payload(dag_run):
    return {
        "clientUri": dag_run.conf['client_uri']
    }


def get_create_invoice_payload(dag_run):
    return {
        "invoice": {
            "target": {
                "uri": null,
                "invoiceNumberText": null,
                "parameterCorrelationId": dag_run.conf['project_code'] + dag_run.conf['client_name']
            },
            "client": {
                "uri": null,
                "name": dag_run.conf['client_name']
            },
            "invoiceNumberText": dag_run.conf['project_code'],
            "invoiceCurrency": {
                "uri": null,
                "name": null,
                "symbol": "CAD$"
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:summarize-columns",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": [
                            {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": "project",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            },
                            {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": "user",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            },
                            {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": "billingRate",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            },
                            {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": "entryDate",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            },
                            {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": "task",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        ]
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:invoice-date",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": rail.result('get_todays_date'),
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:payment-terms",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": "15",
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:payment-due-date",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": rail.result('get_payment_date'),
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:billing-address",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": rail.result('get_client_details', {}).get('billingAddress', {}).get('address'),
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                }
            ],
            "extensionFieldValues": []
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_invoice_items_payload(dag_run):
    return {
        "invoiceUri": rail.result('create_invoice').get('invoiceReference').get('uri'),
        "invoiceBillingItemGroupUri": null,
        "billingItemSearch": {
            "invoiceDateRange": {
                "startDate": rail.result('get_start_date'),
                "endDate": rail.result('get_end_date'),
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "timesheetPeriodInRange": null,
            "billingItemColumnFilterOption": [
                "urn:replicon:billing-item-column-filter-option:project",
                "urn:replicon:billing-item-column-filter-option:user",
                "urn:replicon:billing-item-column-filter-option:billing-rate",
                "urn:replicon:billing-item-column-filter-option:entry-date",
                "urn:replicon:billing-item-column-filter-option:task"
            ],
            "projects": {
                "projectUris": [
                    dag_run.conf['project_uri']
                ]
            },
            "users": null,
            "billingRates": null,
            "billingItemTypeSearch": null,
            "textSearch": null
        },
        "groupingsExcludedUris": [],
        "billingItemsExcludedUris": [],
        "billingItemIncludesUris": [],
        "unitOfWorkId": dag_run.conf['project_code']+str(uuid.uuid4())
    }
