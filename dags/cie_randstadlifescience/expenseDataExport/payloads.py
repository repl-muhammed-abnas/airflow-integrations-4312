import json
def get_expense_detail_payload(dag_run):
    return {
    "expenseSheetUris": json.loads(json.dumps(dag_run.conf["item"]))
    }

def get_report_payloads(report_uri):
    return  {
        "reportUri": report_uri,
        "filterValues": [],
        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
        }
