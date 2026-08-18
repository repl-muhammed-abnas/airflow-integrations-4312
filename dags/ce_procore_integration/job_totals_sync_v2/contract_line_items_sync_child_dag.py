from datetime import timedelta
import rail
from ce_procore_integration.job_totals_sync.utils.constants import SyncType, ContractLevel


def create_dag_instance(config): # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.contract_line_items_sync_child_dag_id,
        description='Computerease to Procore Prime Contract SOV Line Item Sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_project_exists',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_project_exists = rail.IfOperator(
            task_id='check_project_exists',
            test=lambda dag_run: dag_run.conf.get('procore_project_id') is not None,
            yes_task='search_prime_contract',
            no_task='log_project_not_found'
        )

        search_prime_contract = rail.ProcoreApiOperator(
            task_id='search_prime_contract',
            endpoint='/prime_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['procore_project_id'],
                'filters[origin_id]': f'CE_{dag_run.conf["job_code"]}'
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'origin_id', f'CE_{dag_run.conf["job_code"]}', 'id', None)
        )

        check_prime_contract_exists = rail.IfOperator(
            task_id='check_prime_contract_exists',
            test='{{ result("search_prime_contract") is not none }}',
            yes_task='fetch_job_details',
            no_task='log_prime_contract_not_found'
        )

        fetch_job_details = rail.ComputereaseAPIOperator(
            task_id='fetch_job_details',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda dag_run: {
                'code': dag_run.conf['job_code']
            },
            data_handler=lambda response, dag_run: response['data'][0] if response.get(
                'data') else None
        )

        def parse_contract_data_for_sync(dag_run):
            line_items = dag_run.conf['contract_line_items']
            job_code = dag_run.conf['job_code']
            job_detail = rail.result('fetch_job_details')
            if not job_detail:
                raise ValueError(
                    f"Job details not found for job code: {job_code}")
            contract_by = ContractLevel.CATEGORY if job_detail.get(
                'contract_by_cat') else ContractLevel.PHASE

            parsed_data = []
            for item in line_items:
                if item.get('level') != contract_by:
                    continue
                phase_code = item.get('phase_code', '')
                category_code = item.get('category_code', '')
                if contract_by == ContractLevel.CATEGORY and not category_code:
                    continue
                if contract_by == ContractLevel.PHASE and not phase_code:
                    continue
                contract_amount = str(item.get('contract_amount', '0.00'))
                if float(contract_amount) == 0:
                    continue
                entry = {
                    'job_code': job_code,
                    'phase_code': phase_code,
                    'contract_amount': contract_amount,
                    'cost_type': config.revenue_cost_type,
                    'flat_code': item['flat_code']
                }
                if contract_by == ContractLevel.CATEGORY:
                    entry['category_code'] = category_code
                parsed_data.append(entry)
            return parsed_data

        parse_contract_data = rail.PythonOperator(
            task_id='parse_contract_data',
            python_callable=parse_contract_data_for_sync
        )

        fetch_existing_line_items = rail.ProcoreApiOperator(
            task_id='fetch_existing_line_items',
            endpoint=lambda: f'/prime_contracts/{rail.result("search_prime_contract")}/line_items',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['procore_project_id']
            },
            data_handler=lambda response: {
                item.get('wbs_code', {}).get('id'): {
                    'line_item_id': item.get('id'),
                    'contract_amount': item.get('amount', 0),
                    'wbs_code_id': item.get('wbs_code', {}).get('id'),
                    'flat_code': item.get('wbs_code', {}).get('flat_code', ''),
                    'origin_id': item.get('origin_id', '')
                } for item in response if item.get('wbs_code', {}).get('id')
            }
        )

        def get_line_item_actions(dag_run):
            parsed_contract_data = rail.result('parse_contract_data')
            existing_line_items = rail.result('fetch_existing_line_items')
            wbs_codes_lookup = dag_run.conf.get('wbs_codes_lookup') or {}

            updates = []
            wbs_creation_errors = []
            line_items_to_delete = []

            # Track incoming WBS code IDs for deletion comparison
            incoming_wbs_ids = set()

            # Process contract data to build updates payload
            for contract_item in parsed_contract_data:
                flat_code = contract_item['flat_code']
                wbs_lookup_value = wbs_codes_lookup.get(flat_code)
                contract_amount = str(
                    contract_item.get('contract_amount', '0.00'))

                # If lookup value exists and is not a string (i.e., it's a valid WBS code ID)
                if wbs_lookup_value and not isinstance(wbs_lookup_value, str):
                    incoming_wbs_ids.add(str(wbs_lookup_value))
                    updates.append({
                        "amount": contract_amount,
                        "description": config.revenue_cost_type,
                        "quantity": "1",
                        "origin_id": f"CE_{flat_code}",
                        "unit_cost": contract_amount,
                        "wbs_code_id": wbs_lookup_value
                    })
                else:
                    # Form error message
                    if wbs_lookup_value and isinstance(wbs_lookup_value, str):
                        error_message = wbs_lookup_value
                    else:
                        error_message = f"WBS code not found for {flat_code}"

                    wbs_creation_errors.append({
                        'flat_code': flat_code,
                        'error_message': error_message
                    })

            # Identify line items to delete (existing in Procore but not in incoming CE data)
            existing_wbs_ids = set(existing_line_items.keys())
            wbs_ids_to_delete = existing_wbs_ids - incoming_wbs_ids

            for wbs_id in wbs_ids_to_delete:
                existing_item = existing_line_items[wbs_id]
                line_items_to_delete.append({
                    'line_item_id': existing_item['line_item_id'],
                    'wbs_code_id': wbs_id,
                    'flat_code': existing_item['flat_code']
                })

            return {
                'updates': updates,
                'wbs_creation_errors': wbs_creation_errors,
                'line_items_to_delete': line_items_to_delete
            }

        identify_line_item_actions = rail.PythonOperator(
            task_id='identify_line_item_actions',
            python_callable=get_line_item_actions
        )

        if_wbs_creation_errors = rail.IfOperator(
            task_id='if_wbs_creation_errors',
            test='{{ result("identify_line_item_actions")["wbs_creation_errors"] | length > 0 }}',
            yes_task='log_wbs_creation_errors',
            no_task='if_line_items_to_delete'
        )

        log_wbs_creation_errors = rail.WriteLogOperator(
            task_id='log_wbs_creation_errors',
            message='na',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'entity_code': dag_run.conf.get('job_code', ''),
                    'error_message': f'Prime Contract SOV not synced for {err["flat_code"]} as ' + err['error_message'],
                    'sync_type': SyncType.CONTRACT,
                    'reset_retry_count': dag_run.conf.get('reset_retry_count', False)
                }
                for err in rail.result('identify_line_item_actions').get('wbs_creation_errors', [])
            ],
            properties=lambda item: item
        )

        if_line_items_to_delete = rail.IfOperator(
            task_id='if_line_items_to_delete',
            test='{{ result("identify_line_item_actions")["line_items_to_delete"] | length > 0 }}',
            yes_task='trigger_line_items_deletion',
            no_task='if_line_items_to_sync'
        )

        trigger_line_items_deletion = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_line_items_deletion',
            items='{{ result("identify_line_item_actions")["line_items_to_delete"] | to_json }}',
            trigger_dag_id=config.contract_line_items_deletion_child_dag_id,
            execution_timeout=timedelta(minutes=30),
            conf=lambda item, dag_run: {
                'prime_contract_id': rail.result("search_prime_contract"),
                'project_id': dag_run.conf['procore_project_id'],
                'line_item_id': item['line_item_id'],
                'wbs_code_id': item['wbs_code_id'],
                'flat_code': item['flat_code'],
                'job_code': dag_run.conf.get('job_code', ''),
                'reset_retry_count': dag_run.conf.get('reset_retry_count', False)
            }
        )

        wait_for_deletions = rail.WaitForDagRunsSensor(
            task_id='wait_for_deletions',
            dag_runs='{{ result("trigger_line_items_deletion") }}',
            execution_timeout=timedelta(minutes=30)
        )

        if_line_items_to_sync = rail.IfOperator(
            task_id='if_line_items_to_sync',
            test='{{ result("identify_line_item_actions")["updates"] | length > 0 }}',
            yes_task='sync_line_items_to_procore',
            no_task='catch_error'
        )

        sync_line_items_to_procore = rail.ProcoreApiOperator(
            task_id='sync_line_items_to_procore',
            endpoint=lambda: f'/prime_contracts/{rail.result("search_prime_contract")}/line_items/sync',
            method='PATCH',
            data=lambda: {
                'updates': rail.result('identify_line_item_actions')['updates']
            },
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['procore_project_id'],
                'run_configurable_validations': 'false'
            }
        )

        check_if_sync_has_errors = rail.IfOperator(
            task_id='check_if_sync_has_errors',
            test='{{ result("sync_line_items_to_procore").errors | length > 0 }}',
            yes_task='log_sync_failure',
            no_task='catch_error'
        )

        def get_sync_error_message(dag_run, error_object):
            try:
                lookup = dag_run.conf.get('wbs_codes_lookup') or {}
                wbs_codes_reverse_map = {v: k for k, v in lookup.items() if isinstance(v, int)}

                errors = error_object.get('errors', {})
                if not errors or not isinstance(errors, dict):
                    return "Unknown sync error"

                messages = []
                for key, msgs in errors.items():
                    if isinstance(msgs, list):
                        msg_str = ", ".join(str(m) for m in msgs)
                    else:
                        msg_str = str(msgs)
                    messages.append(f"{key}: {msg_str}")

                wbs_code_id = error_object.get('wbs_code_id')
                flat_code = wbs_codes_reverse_map.get(wbs_code_id, 'unknown')
                return f"SOV not synced for {flat_code} due to - " + "; ".join(messages)
            except Exception as e:
                return f"Error parsing sync error: {str(e)}"

        log_sync_failure = rail.WriteLogOperator(
            task_id='log_sync_failure',
            message='Contract line item sync failed',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'entity_code': dag_run.conf.get('job_code', ''),
                    'error_message': get_sync_error_message(dag_run, err),
                    'sync_type': SyncType.CONTRACT,
                    'reset_retry_count': dag_run.conf.get('reset_retry_count', False)
                }
                for err in rail.result('sync_line_items_to_procore').get('errors', [])
            ],
            properties=lambda item: item
        )

        log_project_not_found = rail.WriteLogOperator(
            task_id='log_project_not_found',
            message='Project not found in Procore, skipping contract sync',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_code': dag_run.conf.get('job_code', ''),
                'error_message': f'Prime Contract SOVs not synced for job : {dag_run.conf.get("job_code", "")}, since project doesn\'t exist in Procore',
                'sync_type': SyncType.CONTRACT,
                'reset_retry_count': dag_run.conf.get('reset_retry_count', False)
            }
        )

        log_prime_contract_not_found = rail.WriteLogOperator(
            task_id='log_prime_contract_not_found',
            message='Prime contract not found in Procore, skipping contract sync',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_code': dag_run.conf.get('job_code', ''),
                'error_message': f'Prime Contract SOVs not synced for job : {dag_run.conf.get("job_code", "")}, since prime contract doesn\'t exist in Procore',
                'sync_type': SyncType.CONTRACT,
                'reset_retry_count': dag_run.conf.get('reset_retry_count', False)
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_code': dag_run.conf.get('job_code', ''),
                'error_message': f"Prime Contract SOVs not synced succesfully for job - {dag_run.conf.get('job_code', '')} due to error: {{{{ get_error_message() }}}}",
                'sync_type': SyncType.CONTRACT,
                'reset_retry_count': dag_run.conf.get('reset_retry_count', False)
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> check_project_exists
        check_project_exists >> rail.Label(
            'Yes') >> search_prime_contract >> check_prime_contract_exists
        check_project_exists >> rail.Label(
            'No') >> log_project_not_found >> catch_error

        check_prime_contract_exists >> rail.Label(
            'Yes') >> fetch_job_details >> parse_contract_data >> fetch_existing_line_items >> identify_line_item_actions >> if_wbs_creation_errors
        check_prime_contract_exists >> rail.Label(
            'No') >> log_prime_contract_not_found >> catch_error

        if_wbs_creation_errors >> rail.Label(
            'Yes') >> log_wbs_creation_errors >> if_line_items_to_delete
        if_wbs_creation_errors >> rail.Label('No') >> if_line_items_to_delete

        if_line_items_to_delete >> rail.Label(
            'Yes') >> trigger_line_items_deletion >> wait_for_deletions >> if_line_items_to_sync
        if_line_items_to_delete >> rail.Label('No') >> if_line_items_to_sync

        if_line_items_to_sync >> rail.Label(
            'Yes') >> sync_line_items_to_procore >> check_if_sync_has_errors
        if_line_items_to_sync >> rail.Label('No') >> catch_error

        check_if_sync_has_errors >> rail.Label(
            'Yes') >> log_sync_failure >> catch_error
        check_if_sync_has_errors >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
