from datetime import datetime
from pendulum import now
import rail
import itertools
from uuid import uuid4


def page_handler(request, result):
    """Handle pagination for Replicon API responses."""
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_process_dag_ids(parallel_count, trigger_task_id):
    """Collect all DAG run IDs from parallel trigger tasks."""
    dag_ids = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'{trigger_task_id}_{x+1}') if rail.result(
            f'{trigger_task_id}_{x+1}') else []), range(parallel_count)))))

    return dag_ids


def validate_fields(dag_run, mandatory_fields_list):
    """Validate that all mandatory fields are present in record"""
    exception_log = []
    for item in mandatory_fields_list:
        if not dag_run.conf.get(item):
            exception_log.append(f"Mandatory field '{item}' is blank")

    return exception_log


def get_processing_details(type, key, feed_uri):
    status = "Success"
    details = f"{type} Processed Successfully"
    exceptions = []

    if feed_uri:
        exceptions = rail.result(f"update_{type.lower()}", "update_exceptions")
        update_logs = rail.result(f"update_{type.lower()}", "update_logs")
        if exceptions:
            status = 'Exception'
            details = " ; ".join(
                [f"{type} Processed with Exceptions"] + exceptions + update_logs)
        else:
            details = " ; ".join(
                [f"{type} Processed Successfully"] + update_logs)

    else:
        exceptions = rail.result(f"create_{type.lower()}", "create_exceptions")
        if exceptions:
            status = 'Exception'
            details = " ; ".join(
                [f"{type} Processed with Exceptions"] + exceptions)
        else:
            details = f"{type} Processed Successfully"

    if key == 'status':
        return status

    return details


def client_representatives_list_to_assign():
    cr_list = rail.result(
        'get_existing_client_representatives_list_for_client')
    cr_list.append(rail.result(
        'get_client_representative_details_in_replicon').get('uri'))

    return cr_list


def get_cr_assignment_exception(dag_run):
    if not rail.result('get_client_representative_details_in_replicon').get('uri'):
        return "Client Representative not found in Replicon"
    if not rail.result('get_client_representative_details_in_replicon').get('status'):
        return "Client Representative is disabled in Replicon"
    return ''


def do_format_logs(dag_run):
    """Format and consolidate logs from master and child DAGs for reporting."""
    log_artifacts = []
    log_records = []

    # Process and consolidate client logs from master and child DAGs
    client_master_logs = dag_run.conf['master_client_log']
    client_child_logs = dag_run.conf['client_logs']

    # Process and consolidate project logs from master and child DAGs
    project_master_logs = dag_run.conf['master_project_log']
    project_child_logs = dag_run.conf['project_logs']

    if client_child_logs:
        if isinstance(client_child_logs, list):
            log_artifacts.extend(client_child_logs)
        else:
            log_artifacts.append(client_child_logs)

    if client_master_logs:
        if isinstance(client_master_logs, list):
            log_artifacts.extend(client_master_logs)
        else:
            log_artifacts.append(client_master_logs)

    if project_child_logs:
        if isinstance(project_child_logs, list):
            log_artifacts.extend(project_child_logs)
        else:
            log_artifacts.append(project_child_logs)

    if project_master_logs:
        if isinstance(project_master_logs, list):
            log_artifacts.extend(project_master_logs)
        else:
            log_artifacts.append(project_master_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_client_log_records = rail.load_all_records(log)
            if each_client_log_records:
                log_records.extend(each_client_log_records)

    final_log_records = []
    if log_records:
        final_log_records = list(map(lambda log: {
            **{
                'jobid': log['ecid']
            },
            **log['properties'],
        }, log_records))

    # Calculate statistics for logs
    error_records_length = len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records)))
    exception_records_length = len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records)))

    # Set statistics
    rail.set_result(key="error_record_count",
                    val=error_records_length)
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count",
                    val=exception_records_length)
    rail.set_result(key="total_record_count",
                    val=dag_run.conf['total_records_accounts'] + dag_run.conf['total_records_opportunities'])

    return {
        'final_log_records': final_log_records,
        'total_error_logs_count': error_records_length,
        'total_exception_logs_count': exception_records_length,
    }


def get_email_details_callable(dag_run, time_zone):
    """Generate email metadata including timestamps and file names."""
    _now = now(time_zone)
    return {
        "job_end_time": _now.isoformat(),
        "job_duration": (((_now - datetime.strptime(dag_run.conf['job_start_time'], "%Y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"log_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }


def get_contract_date_from_collection(script_details, keyuri):
    collection = rail.find_first_by_attr_and_get_attr(
        script_details['parameters'], 'keyUri', keyuri, 'value.collection', '')
    if not collection:
        return {}
    for data in collection:
        if not data:
            continue
        for k, v in data.items():
            if k == 'date':
                return v
    return {}


def get_project_contract_details(resp):
    if not resp.get('contract') or not resp.get('scripts'):
        return {}
    script_details = resp['scripts'][0]
    contract_details = resp['contract']
    return {
        'script_name': script_details.get('script', {}).get('name', ''),
        'script_uri': script_details.get('script', {}).get('uri', ''),
        'project_scriptable_contract_script_uri': script_details.get('projectContractScriptUri', ''),
        'contract_script_uri': contract_details.get('uri', ''),
        'contract_end_date': get_contract_date_from_collection(script_details, 'urn:replicon:script-key:parameter:contract-end-date'),
        'contract_start_date': get_contract_date_from_collection(script_details, 'urn:replicon:script-key:parameter:contract-start-date'),
        'additional_data_points': rail.find_first_by_attr_and_get_attr(script_details['parameters'], 'keyUri', 'urn:replicon:script-key:revenue-contract-clause-script:additional-data-points', 'value.collection', []),
    }


def get_revenue_contract_payload(dag_run):
    additional_data_points = rail.result(
        'get_revenue_contract_details_for_project')['additional_data_points']
    project_default_currency_displaytext = rail.result(
        'get_project_details')['defaultBillingCurrency']['displayText']
    project_default_currency_uri = rail.result(
        'get_project_details')['defaultBillingCurrency']['uri']
    settings_payload = [
        {
            "keyUri": "urn:replicon:script-key:parameter:contract-end-date",
            "value": {
                "text": "Contract End Date",
                "collection": [
                    {
                        "text": "date"
                    },
                    {
                        "date": rail.result('get_revenue_contract_details_for_project')['contract_end_date']
                    }
                ]
            }
        },
        {
            "keyUri": "urn:replicon:script-key:parameter:contract-start-date",
            "value": {
                "text": "Contract Start Date",
                "collection": [
                    {
                        "text": "date"
                    },
                    {
                        "date": rail.result('get_revenue_contract_details_for_project')['contract_start_date']
                    }
                ]
            }
        },
        {
            "keyUri": "urn:replicon:script-key:parameter:total-contract-value",
            "value": {
                "text": "Total Contract Value (Base Currency)",
                "collection": [
                    {
                        "text": "money"
                    },
                    {
                        "uri": project_default_currency_uri,
                        "number": dag_run.conf.get('Total_Contract_Value__c'),
                        "text": project_default_currency_displaytext
                    }
                ]
            }
        },
        {
            "keyUri": "urn:replicon:script-key:parameter:total-contract-value-in-reference-currency",
            "value": {
                "text": "Total Contract Value (Project Currency)",
                "collection": [
                    {
                        "text": "money"
                    },
                    {
                        "uri": project_default_currency_uri,
                        "number": dag_run.conf.get('Total_Contract_Value__c'),
                        "text": project_default_currency_displaytext
                    }
                ]
            }
        },
        {
            "keyUri": "urn:replicon:script-key:script-type",
            "value": {
                "uri": "urn:replicon:script-key:script-type:revenue-contract-clause"
            }
        }
    ]

    if additional_data_points:
        settings_payload.append({
            "keyUri": "urn:replicon:script-key:revenue-contract-clause-script:additional-data-points",
            "value": {
                "collection": additional_data_points
            }
        })

    return {
        "project": {
            "uri": dag_run.conf.get('project_uri')
        },
        "contract": {
            "contract": {
                "uri": rail.result('get_revenue_contract_details_for_project')['contract_script_uri']
            },
            "projectScripts": [
                {
                    "projectContractScriptUri": rail.result('get_revenue_contract_details_for_project')['project_scriptable_contract_script_uri'],
                    "script": {
                        "uri": rail.result('get_revenue_contract_details_for_project')['script_uri']
                    },
                    "settings": settings_payload
                }
            ]
        },
        "unitOfWorkId": str(uuid4())
    }
