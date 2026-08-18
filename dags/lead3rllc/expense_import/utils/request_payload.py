import rail
from datetime import datetime as dt
from pendulum import now
from functools import lru_cache
from lead3rllc.expense_import.config import date_default_format

null = None


def required_expense_sheet_customfield_uri(response_rows):
    for item in response_rows:
        if 'textValue' in item['cells'][0].keys():
            if item['cells'][0]['textValue'] == "Reference Number":
                return item['cells'][0]['uri']
    return None


@lru_cache(maxsize=8)
def load_required_valid_records():
    return rail.load_all_records(rail.result('query_get_matching_records_from_valid_records_for_expense_sheet'))


def put_expense_sheet_payload_report(dag_run):
    feed_file_expense_entries = load_required_valid_records()
    entries = list(map(lambda x: {
        "target": {
            "uri": null,
            "parameterCorrelationId": null
        },
        "incurredDate": rail.get_replicon_date(
            dt.strptime(x['Transaction_Date'], date_default_format)),
        "description": x['Business_Purpose'],
        "expenseBillingOptionUri": "urn:replicon:expense-billing-option:bill-to-client" if "Yes" in x['Is_Billable'] else "urn:replicon:expense-billing-option:not-billed",
        "project": {
            "uri": x['project_uri']
        },
        "expenseCode": {
            "uri": x['expense_code_uri']
        },
        "flatAmountEntry": {
            "incurredAmountNet": {
                "amount": float(x['Amount'].replace(',', '')),
                "currency": {
                    "uri": dag_run.conf['default_currency_uri']
                }
            }
        },
        "customFieldValues": [
            {
                "customField": {
                    "uri": dag_run.conf['reference_number_customfield_uri']
                },
                "text": x['Expense_Report_Number']
            }
        ],
    }, feed_file_expense_entries))

    return {
        "parameter": {
            "target": {
                "uri": rail.result('publish_expense_sheet_draft')['uri']
            },
            "owner": {
                "uri": dag_run.conf['owner_uri']
            },
            "date": rail.get_replicon_date(dt.strptime(dag_run.conf['report_date'], date_default_format) if dag_run.conf['report_date'] else now().date()),
            "description": dag_run.conf['concur_username'],
            "reimbursementCurrency": {
                "uri": dag_run.conf['default_currency_uri']
            },
            "entries": entries,
            "noticeExplicitlyAccepted": "1"
        }
    }


def put_expense_sheet_payload_invoice(dag_run):
    feed_file_expense_entries = load_required_valid_records()
    entries = list(map(lambda x: {
        "target": {
            "uri": null,
            "parameterCorrelationId": null
        },
        "incurredDate": rail.get_replicon_date(
            dt.strptime(x['Date_Incurred'], date_default_format)),
        "description": x['Line_Description'],
        "expenseBillingOptionUri": "urn:replicon:expense-billing-option:bill-to-client" if "Yes" in x['Is_Billable'] else "urn:replicon:expense-billing-option:not-billed",
        "project": {
            "uri": x['project_uri']
        },
        "expenseCode": {
            "uri": x['expense_code_uri']
        },
        "flatAmountEntry": {
            "incurredAmountNet": {
                "amount": float(x['Amount'].replace(',', '')),
                "currency": {
                    "uri": dag_run.conf['default_currency_uri']
                }
            }
        },
        "customFieldValues": [
            {
                "customField": {
                    "uri": dag_run.conf['reference_number_customfield_uri']
                },
                "text": x['Invoice_Number']
            }
        ],
    }, feed_file_expense_entries))

    return {
        "parameter": {
            "target": {
                "uri": rail.result('publish_expense_sheet_draft')['uri']
            },
            "owner": {
                "uri": dag_run.conf['owner_uri']
            },
            "date": rail.get_replicon_date(dt.strptime(dag_run.conf['invoice_date'], date_default_format) if dag_run.conf['invoice_date'] else now().date()),
            "description": dag_run.conf['vendor_name'],
            "reimbursementCurrency": {
                "uri": dag_run.conf['default_currency_uri']
            },
            "entries": entries,
            "noticeExplicitlyAccepted": "1"
        }
    }
