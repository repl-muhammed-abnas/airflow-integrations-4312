from airflow.exceptions import AirflowFailException


def retrieve_export_uri(response):
    """
    Retrieve export URI from response
    Raises AirflowFailException if export failed
    """
    if response['error'] is not None:
        raise AirflowFailException('Export failed - ' + str(response))
    return response['timeDataExportUri']