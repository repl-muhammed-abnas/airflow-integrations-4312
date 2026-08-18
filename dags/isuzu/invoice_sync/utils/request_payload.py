# pylint: disable=too-many-statements line-too-long
import uuid
import rail
null = None


def get_create_invoice_payload_24(dag_run):
    data = {
        "invoice": {
            "target": {
                "uri": null,
                "invoiceNumberText": null,
                "parameterCorrelationId": f"{dag_run.conf['Line_Item_Expense_Type_Name']}ab{str(uuid.uuid4())}c1"
            },
            "client": {
                "uri": null,
                "name": "Non CB Expense"
            },
            "invoiceNumberText": f"{dag_run.conf['Vendor_Invoice_Number']} ({str(uuid.uuid4())})",
            "invoiceCurrency": {
                "uri": null,
                "name": null,
                "symbol": "$"
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
                                "text": "description",
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
                        "date": rail.result('log_invoice_date_details'),
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
                        "number": "0",
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
                        "date": rail.result('log_invoice_date_details'),
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
                    "keyUri": "urn:replicon:invoice-metadata-key:description",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": f"{dag_run.conf['Line_Item_Description']}",
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:notes-for-customer",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": f"Posting Date:{dag_run.conf['Request_Custom_15_Posting_Date']}",
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:po-number",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": f"{dag_run.conf['PO_Number']}",
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:internal-notes",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": f"{dag_run.conf['Request_ID']}",
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
        "unitOfWorkId": f"{dag_run.conf['Line_Item_Expense_Type_Name'] }124{str(uuid.uuid4())}"
    }
    return data


def get_create_invoice_payload_48(dag_run):
    data = {
        "invoice": {
            "target": {
                "uri": null,
                "invoiceNumberText": null,
                "parameterCorrelationId": f"{dag_run.conf['Line_Item_Expense_Type_Name']}{str(uuid.uuid4())}abc1"
            },
            "client": {
                "uri": null,
                "name": f"{dag_run.conf['Line_Item_Custom_10_Client']}"
            },
            "invoiceNumberText": f"{dag_run.conf['Vendor_Invoice_Number']} ({str(uuid.uuid4())})",
            "invoiceCurrency": {
                "uri": null,
                "name": null,
                "symbol": "$"
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
                                "text": "description",
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
                        "date": rail.result('log_invoice_date_details'),
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
                        "number": "0",
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
                        "date": rail.result('log_invoice_date_details'),
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
                    "keyUri": "urn:replicon:invoice-metadata-key:description",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": f"{dag_run.conf['Line_Item_Description']}",
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:notes-for-customer",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": f"Posting Date:{dag_run.conf['Request_Custom_15_Posting_Date']}",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:po-number",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": f"{dag_run.conf['PO_Number']}",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-metadata-key:internal-notes",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": f"{dag_run.conf['Request_ID']}",
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
        "unitOfWorkId": f"{dag_run.conf['Line_Item_Expense_Type_Name']}1{str(uuid.uuid4())}24"
    }
    return data


def get_create_invoice_payload_15(dag_run):
    data = {
        "invoiceItem": {
            "target": null,
            "invoice": {
                "uri": rail.result('log_existinginvoiceuri_10'),
                "invoiceNumberText": null,
                "parameterCorrelationId": null
            },
            "amount": rail.result('log_invoice_line_amount'),
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:quantity",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": dag_run.conf['Line_Item_Quantity'],
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:rate",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": dag_run.conf['Line_Item_Unit_Price'],
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:description",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": rail.result('log_invoice_description'),
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:project",
                    "value": {
                        "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_uri')[0]['cells'], 'textValue', 'Non CB Expense project', 'uri'),
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                }
            ]
        },
        "unitOfWorkId": f"{dag_run.conf['Line_Item_Expense_Type_Name']}abc1{str(uuid.uuid4())}"
    }
    return data


def get_create_invoice_payload_26(dag_run):
    data = {
        "invoiceItem": {
            "target": null,
            "invoice": {
                "uri": rail.result('createinvoice_24').get('invoiceReference').get('uri'),
                "invoiceNumberText": null,
                "parameterCorrelationId": null
            },
            "amount": rail.result('log_invoice_line_amount'),
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:quantity",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": dag_run.conf['Line_Item_Quantity'],
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:rate",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": dag_run.conf['Line_Item_Unit_Price'],
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:description",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": rail.result('log_invoice_description'),
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:project",
                    "value": {
                        "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_uri')[0]['cells'], 'textValue', 'Non CB Expense project', 'uri'),
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                }
            ]
        },
        "unitOfWorkId": f"{dag_run.conf['Line_Item_Expense_Type_Name']}ab{str(uuid.uuid4())}c"
    }
    return data


def get_create_invoice_payload_39(dag_run):
    data = {
        "invoiceItem": {
            "target": null,
            "invoice": {
                "uri": rail.result('log_existinginvoiceuri_33'),
                "invoiceNumberText": null,
                "parameterCorrelationId": null
            },
            "amount": rail.result('log_invoice_line_amount'),
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:quantity",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": dag_run.conf['Line_Item_Quantity'],
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:rate",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": dag_run.conf['Line_Item_Unit_Price'],
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:description",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": rail.result('log_invoice_description'),
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:project",
                    "value": {
                        "uri": rail.result('search_projects_37_37_37')[0].get("cells")[0].get('uri') if rail.result('search_projects_37_37_37') else "",
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                }
            ]
        },
        "unitOfWorkId": f"{dag_run.conf['Line_Item_Expense_Type_Name']}ab{str(uuid.uuid4())}"
    }
    return data


def get_create_invoice_payload_51(dag_run):
    data = {
        "invoiceItem": {
            "target": null,
            "invoice": {
                "uri": rail.result('createinvoice_48').get('invoiceReference').get('uri'),
                "invoiceNumberText": null,
                "parameterCorrelationId": null
            },
            "amount": rail.result('log_invoice_line_amount'),
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:quantity",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": dag_run.conf['Line_Item_Quantity'],
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:rate",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": dag_run.conf['Line_Item_Unit_Price'],
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:description",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": rail.result('log_invoice_description'),
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:invoice-item-metadata-key:project",
                    "value": {
                        "uri": rail.result('search_projects_37_37_49')[0].get("cells")[0].get('uri') if rail.result('search_projects_37_37_49') else "",
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                }
            ]
        },
        "unitOfWorkId": f"{dag_run.conf['Line_Item_Expense_Type_Name']}abc{str(uuid.uuid4())}"
    }
    return data
