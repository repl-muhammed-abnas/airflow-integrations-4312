"""
Custom helper methods for T-Systems Project Billing Rate Import integration.

This module contains utility functions and helper methods that are shared across
different components of the billing rate assignment integration.
"""
import itertools
import rail
from datetime import datetime
from pendulum import now
import json

null = None

# Required/Mandatory Fields for Validation
mandatory_fields = [
    'Billing_Rate_ID',
    'Project_ID',
    'Rate_Type',
    'Billing_Rate_Value',
    'Billing_Rate_Currency'
]


def handle_api_error_504():
    """
    Handle 504 Service Unavailable errors as success (no data available)

    Returns:
        dict: Success status for 504 errors, failure status for others
    """
    if rail.result('fetch_billing_event'):
        return {
            'process': True,
            'message': 'API is Processed Successfully',
            'status_code': 200
        }
    try:
        fetch_result = rail.result('fetch_billing_event', 'error')

        status_code = fetch_result.get('status_code', 0)
        error_message = fetch_result.get('exc_message', '')

        if error_message == '504:Gateway Timeout':
            return {
                'process': True,
                'message': 'API is Processed Successfully',
                'status_code': 504
            }
        else:
            return {
                'process': False,
                'message': error_message,
                'status_code': status_code
            }
    except Exception as e:
        return {
            'process': False,
            'message': f'{str(e)}',
            'status_code': rail.result('fetch_billing_event', 'error').get('status_code', 0) if rail.result('fetch_billing_event') else 504
        }


def get_billing_rate_name_details(record, separator, length_billing_rate_name):
    """
    Extracts the billing rate name from the record using the specified pattern and separator.

    Args:
        record (dict): The billing rate record.
        pattern (str): The pattern to combine record fields to get the billing rate name.
        separator (str): The separator to use in the formatted name.

    Returns:
        str: The formatted billing rate name with patters as below:
        {Rate_Type}-{Billing_Text}-{Project_ID}-{CIAM_ID}
    """
    for field in mandatory_fields:
        if not record.get(field):
            return {
                'name_exceeds_length': False,
                'combined_fields_except_billing_text': '',
                'length_combined_fields_except_billing_text': '',
                'billing_text_with_remaining_length': '',
                'length_billing_text_with_remaining_length': '',
                'final_billing_rate_name': ''
            }

    combined_fields_except_billing_text = record['Rate_Type'] + separator + record['Project_ID'] + (
        (separator + record['CIAM_ID']) if record['CIAM_ID'] else '')
    length_combined_fields_except_billing_text = len(
        combined_fields_except_billing_text)
    if length_combined_fields_except_billing_text > length_billing_rate_name:
        return {
            'name_exceeds_length': True,
            'combined_fields_except_billing_text': '',
            'length_combined_fields_except_billing_text': '',
            'billing_text_with_remaining_length': '',
            'length_billing_text_with_remaining_length': '',
            'final_billing_rate_name': ''
        }

    billing_text = (
        separator + record['Billing_Text']) if record['Billing_Text'] else ''

    billing_text_with_remaining_length = billing_text[0:(
        length_billing_rate_name - length_combined_fields_except_billing_text)]

    split_combined_fields = combined_fields_except_billing_text.split(
        separator, 1)
    final_billing_rate_name = split_combined_fields[0] + \
        billing_text_with_remaining_length + \
        separator + split_combined_fields[1]

    return {
        'name_exceeds_length': False,
        'combined_fields_except_billing_text': combined_fields_except_billing_text,
        'length_combined_fields_except_billing_text': length_combined_fields_except_billing_text,
        'billing_text_with_remaining_length': billing_text_with_remaining_length,
        'length_billing_text_with_remaining_length': len(billing_text_with_remaining_length),
        'final_billing_rate_name': final_billing_rate_name
    }


def get_value_for_key(key, list_of_dictionaries):
    for item in list_of_dictionaries:
        if isinstance(item, dict) and item.get('key') == key:
            return item.get('value', '')
    return ''


def parse_and_transform_api_response_to_billing_rate_records_list(response_text, config):
    """
    Single task to parse API response and transform to billing rate list format.
    Combines parsing, validation, and transformation in one operation.

    Args:
        response_text: Raw API response with concatenated JSON objects

    Returns:
        list: Standardized billing rate records list
    """
    if not response_text or not response_text.strip():
        print(f"No billing rate data received from API")
        return []

    # Parse concatenated JSON objects
    json_objects = []
    decoder = json.JSONDecoder()
    response_text = response_text.strip()

    while response_text:
        response_text = response_text.lstrip()
        if not response_text:
            break

        try:
            obj, end_idx = decoder.raw_decode(response_text)
            json_objects.append(obj)
            response_text = response_text[end_idx:].lstrip()
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {str(e)}")
            break

    print(f"Parsed {len(json_objects)} billing rate events")

    if not json_objects:
        return []

    # Transform to list format
    billing_rate_records_list = []

    for event_obj in json_objects:
        try:
            # Validate event structure and extract product
            if not (isinstance(event_obj, dict) and
                    'data' in event_obj and
                    isinstance(event_obj['data'], dict) and
                    'product' in event_obj['data']):
                print(
                    f"Invalid event structure, skipping: {event_obj.get('id', 'unknown')}")
                continue

            project_billing_record = event_obj['data']['product']

            if not (project_billing_record):
                print(
                    f"Product key value not found, skipping: {event_obj.get('id', 'unknown')}")

            else:
                project_billing_details = {
                    'Billing_Rate_ID': project_billing_record.get('id', ''),
                    'Project_ID': get_value_for_key(
                        'costObject', project_billing_record.get('characteristic4Contract', [])),
                    'Rate_Type': rail.find_first_by_attr_and_get_attr(
                        project_billing_record.get('productCharacteristic', []), "valueType", "BillingRateType", "name", ''),
                    'Billing_Text': project_billing_record.get('description', ''),
                    'CIAM_ID': rail.find_first_by_attr_and_get_attr(
                        project_billing_record.get('relatedParty', []), "role", "ResourceAssignee", "id", ''),
                    'Billing_Rate_Value': project_billing_record['productPrice'][0]['price']['dutyFreeAmount']['value'] if project_billing_record.get('productPrice', []) else '',
                    'Billing_Rate_Currency': project_billing_record['productPrice'][0]['price']['dutyFreeAmount']['unit'] if project_billing_record.get('productPrice', []) else '',
                }

                project_billing_details.update(get_billing_rate_name_details(
                    project_billing_details, config.billing_rate_name_separator, config.length_billing_rate_name))

                billing_rate_records_list.append(project_billing_details)

        except Exception as e:
            print(f"Error transforming event to project record: {str(e)}")
            continue

    return billing_rate_records_list


def validate_integration_success():
    """
    Validate the overall integration success by checking API status results.
    Fails the DAG if either API had genuine errors (not 503 or success).

    Returns:
        dict: Integration status summary
    """
    create_status = rail.result('get_project_billing_rate_import_api_status')[
        'status_code']

    if create_status not in [200, 504]:
        raise Exception("Billing Event API encountered an error.")

    return {
        'message': 'Integration completed successfully.'
    }


def get_mandatory_field_validation_details(record):
    missing_field_log = []
    for item in mandatory_fields:
        if not (record[item].strip()):
            missing_field_log.append(
                f"{item.replace('_', ' ')} is missing from the record")
    if missing_field_log:
        return rail.smartjoin_by_delim(missing_field_log, ';')

    return ''


def get_each_billing_rate_payload(item):

    conf = {
        **item,
        'default_currency_uri': rail.result('get_default_currency_uri'),
        'existing_billing_rate_name_in_replicon': item['final_billing_rate_name'].lower() in rail.result('get_all_existing_billing_rate_names'),
        'log_job_start_time': rail.result('log_job_start_time'),
    }

    existing_billing_rates_in_replicon = rail.result(
        'get_existing_billing_rates')

    matching_billing_rate_based_on_description = rail.find_first_by_attr_and_get_attr(
        existing_billing_rates_in_replicon, 'description', item['Billing_Rate_ID'], null)

    conf.update({
        "existing_billing_rate_description": matching_billing_rate_based_on_description['description'] if matching_billing_rate_based_on_description else '',
        "existing_billing_rate_name":  matching_billing_rate_based_on_description['name'] if matching_billing_rate_based_on_description else '',
        "existing_billing_rate_uri":  matching_billing_rate_based_on_description['uri'] if matching_billing_rate_based_on_description else '',
        "existing_billing_rate_amount": matching_billing_rate_based_on_description['billing_rate_amount'] if matching_billing_rate_based_on_description else ''
    })

    return conf


def get_process_each_user_payload_dag_ids(parallel_count):

    child_dags_list = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'trigger_process_each_payload_dag_{x+1}') if rail.result(
            f'trigger_process_each_payload_dag_{x+1}') else []), range(parallel_count)))))

    return child_dags_list


def get_payload_data(dag_run):
    return {
        'Billing_Rate_ID': dag_run.conf.get('Billing_Rate_ID'),
        'Project_ID': dag_run.conf.get('Project_ID'),
        'Rate_Type': dag_run.conf.get('Rate_Type'),
        'Billing_Text': dag_run.conf.get('Rate_Type', ''),
        'CIAM_ID': dag_run.conf.get('CIAM_ID', ''),
        'Billing_Rate_Value': dag_run.conf.get('Billing_Rate_Value'),
        'Billing_Rate_Currency': dag_run.conf.get('Billing_Rate_Currency'),
    }


def get_required_project_details(response):
    if response['results']:
        return {
            'project_name': response['results'][0]['project']['name'],
            'project_uri': response['results'][0]['project']['uri'],
            'project_billing_type': response['results'][0]['project']['billingType']['displayText'],
            'resources_assigned_to_project': list(map(lambda x: {
                'name': x['resource']['displayText'],
                'uri': x['resource']['uri'],
            }, response['results'][0]['team'])) if bool(response['results'][0]['team']) else [],
            'existing_billing_rates': list(map(lambda x: {
                'name': x['billingRate']['name'],
                'uri': x['billingRate']['uri'],
            }, response['results'][0]['timeAndMaterials']['projectBillingRates'])) if bool(response['results'][0]['timeAndMaterials']) else []
        }
    return {}


def get_project_billing_rate_schedule(res):
    if not res:
        return {}
    intial_and_schedule_entries = {}
    schedule_entries = []
    for item in res:
        if not (item['effectiveDate']):
            intial_and_schedule_entries.update({
                "initialRate": {
                    "amount": item['rate']['amount'],
                    "currencyUri": item['rate']['currency']['uri']
                }
            })
        else:
            schedule_entries.append({
                "effectiveDate": item['effectiveDate'],
                "rate": {
                    "amount": item['rate']['amount'],
                    "currencyUri": item['rate']['currency']['uri']
                }
            })
    if schedule_entries:
        intial_and_schedule_entries.update({
            "scheduleEntries": schedule_entries
        })

    return intial_and_schedule_entries


def get_update_billing_rate_amount_in_project_payload(dag_run, existing_intial_and_schedule_entries):
    payload = {
        "projectUri": rail.result('get_project_details')["project_uri"],
        "billingRateUri": dag_run.conf["billing_rate_uri"]
    }
    payload.update(existing_intial_and_schedule_entries)

    run_date = datetime.strptime(
        dag_run.conf['run_date_time'], "%Y-%m-%dT%H:%M:%S%z")

    if payload.get('scheduleEntries'):
        payload['scheduleEntries'].append({
            "effectiveDate": rail.get_replicon_date(run_date),
            "rate": {
                "amount": dag_run.conf['Billing_Rate_Value'],
                "currencyUri": dag_run.conf['default_currency_uri']
            }
        })
    else:
        payload.update({
            "scheduleEntries": [{
                "effectiveDate": rail.get_replicon_date(run_date),
                "rate": {
                    "amount": dag_run.conf['Billing_Rate_Value'],
                    "currencyUri": dag_run.conf['default_currency_uri']
                }
            }]
        })

    return payload


def get_billing_rate_add_update_project_and_resource_log_details(dag_run):
    status = "Success"
    details = [
        f"Billing Rate is {'added to' if dag_run.conf['operation_type'] == 'Add' else 'updated in'} Replicon successfully"]

    if rail.result('get_project_details'):
        if rail.result('log_project_is_not_time_and_materials'):
            status = "Exception"
            details.append(f"Billing Type of Project is not Time & Materials")
        elif rail.result('log_billing_rate_assigned_to_project_successfully'):
            details.append(
                "Billing Rate is assigned to the project successfully")
        elif rail.result('log_billing_rate_updated_in_project_successfully'):
            details.append(
                "Billing Rate is updated in project successfully")
        else:
            details.append("Billing Rate is already assigned to project")
    else:
        status = "Exception"
        details.append(f"Project not found in Replicon")

    if dag_run.conf['CIAM_ID']:
        if rail.result('log_user_not_found_in_replicon'):
            status = "Exception"
            details.append(
                f"User with Employee ID: '{dag_run.conf['CIAM_ID']}' not found in Replicon")
        elif rail.result('log_user_not_assigned_to_project'):
            status = "Exception"
            details.append(
                f"User with Employee ID: '{dag_run.conf['CIAM_ID']}' is not a resource assigned to the project")
        elif rail.result('log_billing_rate_assigned_to_resource_successfully'):
            details.append(
                "Billing Rate is assigned to the resource successfully")
        else:
            details.append("Billing Rate is already assigned to the resource")

    return {
        "billing_rate_id": dag_run.conf['Billing_Rate_ID'],
        "billing_rate_name": dag_run.conf['Billing_Rate_Name'],
        "project_id": dag_run.conf['Project_ID'],
        "ciam_id": dag_run.conf['CIAM_ID'],
        "action": dag_run.conf['operation_type'],
        "status": status,
        "details": rail.smartjoin_by_delim(details, " ; ")
    }


def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    master_log = dag_run.conf['master_log']
    child_logs = dag_run.conf['child_logs']

    if master_log:
        if isinstance(master_log, list):
            log_artifacts.extend(master_log)
        else:
            log_artifacts.append(master_log)

    if child_logs:
        if isinstance(child_logs, list):
            log_artifacts.extend(child_logs)
        else:
            log_artifacts.append(child_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    rail.set_result(key="skipped_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Skipped', final_log_records))))
    rail.set_result(key="total_record_count",
                    val=dag_run.conf['total_records'])

    return final_log_records


def get_email_details_callable(dag_run, time_zone):
    _now = now(time_zone)
    return {
        "job_end_time": _now.isoformat(),
        "job_duration": (((_now - datetime.strptime(dag_run.conf['job_start_time'], "%Y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"Log_project_billing_rate_import_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }
