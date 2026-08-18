from airflow.exceptions import AirflowFailException
from datetime import datetime

null = None

def retrieve_export_uri(response):
    if response['error']:
        raise AirflowFailException(response)
    return response['timeDataExportUri']

def extract_download_url(response):
    """Extract download URL from time data download batch response."""
    if response and 'downloadUrl' in response:
        return response['downloadUrl']
    else:
        raise AirflowFailException("Failed to retrieve download URL from response")

def translate_rows(row):
    ignored_keys = ('billing_entry', 'billing_rate_name', 'transaction_id')
    if row:
        return {
            **{k: v for k, v in row.items() if k not in ignored_keys},
            **{
                'billing_entry': "Billable" if row['billing_rate_name'] else "Non-Billable",
                'billing_rate_name': row['billing_rate_name'][0] if (not row['billing_rate_name'] in ['Project Rate', 'Non-Billable']) and row['billing_rate_name'] else null,
                'transaction_id': '',
            }
        }
    return None
