from airflow.exceptions import AirflowFailException


def retrieve_export_uri(response):
    if response['error'] is not None:
        raise AirflowFailException('Export failed - ' + str(response))
    return response['timeDataExportUri']
