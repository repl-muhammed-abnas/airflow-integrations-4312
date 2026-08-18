import rail
from airflow.exceptions import AirflowFailException


def retrieve_export_uri(response):
    if response['error'] is not None:
        raise AirflowFailException('Export failed - ' + response)
    return response['payRunUri']


def get_payroll_file_format_details_for_country(response, config):
    file_format = rail.find_first_by_attr_and_get_attr(
        response, "displayText", config.payroll_export_file_format
    )
    return file_format['uri']


def filter_create_export_result(response):
    rail.set_result(key="actual_response", val=response)
    return response['uri']


def get_payroll_location_uri_filter(response, config):
    if not response:
        raise AirflowFailException(
            f"No location details found for `{config.PAYROLL_LOCATION_NAME}` in Replicon instance")

    location_details = list(filter(lambda location:
                                   location['location']['displayText'] == config.PAYROLL_LOCATION_NAME and
                                   location['isEffectivelyEnabled'] in [True, 'true', 'True'], response))

    if not location_details:
        raise AirflowFailException(
            f"Location `{config.PAYROLL_LOCATION_NAME}` not present in Replicon instance")

    return location_details[0]


def get_location_uris(response):
    return list(map(lambda location: {'uri': location['cells'][0]['uri'],
                                      'name': location['cells'][0]['textValue']}, response['rows']))
