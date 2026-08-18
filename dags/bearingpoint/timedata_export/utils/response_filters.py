from airflow.exceptions import AirflowFailException


def get_value(item, index, pluck_key):
    return item[index].get(pluck_key)

def retrieve_export_uri(response):
    if response['error'] is not None:
        raise AirflowFailException('Export failed - ' + response)
    return response['timeDataExportUri']

def get_billing_rates_filter(response):
    if not response['rows']:
        return []
    return list(map(lambda billing_rate: {
        "name": get_value(billing_rate['cells'], 0, 'textValue'),
        "uri": get_value(billing_rate['cells'], 0, 'uri'),
        "description": get_value(billing_rate['cells'], 1, 'textValue'),
        "enabled": get_value(billing_rate['cells'], 2, 'textValue')
    }, response['rows']))
