from datetime import date, timedelta
import base64
import io
import zipfile
import json
import rail
from procore_ce_integration.productivity_unit_sync.utils.util import (
    extract_ce_code,
    format_ce_date,
    truncate_notes,
    validate_field_length
)
from procore_ce_integration.initial_setup_sync.shared_utils import build_import_file_description, normalize_ce_identifier, parse_wbs_flat_code
from procore_ce_integration.productivity_unit_sync.utils.constants import (
    CE_FIELD_LENGTHS,
    CE_IMPORT_TYPE,
    JSON_FILENAME,
    JSON_INDENT_SPACES,
    RESOURCE_PRODUCTIVITY_LOG
)
from procore_ce_integration.job_structure_sync.utils.constants import WBSType


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='Procore CE Productivity Unit Sync - Sync Productivity Logs to ComputerEase',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'aws_conn_id': config.aws_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_productivity_logs',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def filter_and_store_logs(response, dag_run):
            subcontract_ids = set()
            purchase_order_ids = set()
            logs_to_sync = dag_run.conf['item']['log_ids']
            logs = []

            for log in response:
                if str(log['id']) not in logs_to_sync:
                    continue
                holder = log.get('line_item_holder', {})
                holder_id = str(holder.get('id'))
                holder_type = holder.get('type')

                if holder_type == 'WorkOrderContract':
                    subcontract_ids.add(holder_id)
                elif holder_type == 'PurchaseOrderContract':
                    purchase_order_ids.add(holder_id)

                logs.append({
                    'line_item_id': log.get('line_item_id'),
                    'created_at': log.get('created_at'),
                    'notes': log.get('notes', ''),
                    'quantity_used': log.get('quantity_used')
                })

            return {
                'logs': logs,
                'subcontract_ids': list(subcontract_ids),
                'purchase_order_ids': list(purchase_order_ids)
            }

        fetch_productivity_logs = rail.ProcoreApiOperator(
            task_id='fetch_productivity_logs',
            endpoint='/projects/{{ dag_run.conf.item.project_id }}/productivity_logs',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['item']['project_id'],
                'start_date': (date.today() - timedelta(days=30)).isoformat()
            },
            data_handler=lambda response, dag_run: filter_and_store_logs(response, dag_run)
        )

        check_if_po_exists = rail.IfOperator(
            task_id='check_if_po_exists',
            test='{{ result("fetch_productivity_logs").purchase_order_ids | length > 0 }}',
            yes_task='fetch_purchase_order_sovs',
            no_task='check_if_sc_exists'
        )

        def flatten_line_items(response):
            if not response:
                return []
            return [
                line_item
                for contract in response
                for line_item in contract.get('line_items', [])
            ]

        fetch_purchase_order_sovs = rail.ProcoreApiOperator(
            task_id='fetch_purchase_order_sovs',
            endpoint='/purchase_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'view': 'extended',
                'project_id': dag_run.conf['item']['project_id'],
                'filters[id]': f"[{','.join(rail.result('fetch_productivity_logs')['purchase_order_ids'])}]"
            },
            data_handler=flatten_line_items
        )


        check_if_sc_exists = rail.IfOperator(
            task_id='check_if_sc_exists',
            test='{{ result("fetch_productivity_logs").subcontract_ids | length > 0 }}',
            yes_task='fetch_subcontract_sovs',
            no_task='process_unit_details'
        )

        fetch_subcontract_sovs = rail.ProcoreApiOperator(
            task_id='fetch_subcontract_sovs',
            endpoint='/work_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'view': 'extended',
                'project_id': dag_run.conf['item']['project_id'],
                'filters[id]': f"[{','.join(rail.result('fetch_productivity_logs')['subcontract_ids'])}]"
            },
            data_handler=flatten_line_items
        )

        def extract_job_and_line_items():
            po_items = rail.result('fetch_purchase_order_sovs') or []
            sc_items = rail.result('fetch_subcontract_sovs') or []
            contract_line_items = po_items + sc_items
            logs = rail.result('fetch_productivity_logs')['logs']

            if not contract_line_items:
                raise ValueError("No line items found for project")

            job_code = normalize_ce_identifier(extract_ce_code(
                contract_line_items[0].get('project', {}).get('origin_id', '')
            ))

            line_item_lookup = {}
            errors = []

            for line_item in contract_line_items:
                line_item_id = line_item.get('id')
                flat_code = line_item.get('wbs_code', {}).get('flat_code')

                if line_item_id and not flat_code:
                    errors.append({
                        'entity_code': f'Line Item: {line_item_id} ({line_item["description"]})',
                        'error_message': 'Line item has no Budget code assigned in Procore - cannot sync to ComputerEase'
                    })
                    continue

                if line_item_id and flat_code:
                    cost_code = {
                        'code': line_item['cost_code']['code'],
                        'parent': line_item['cost_code']['parent']
                    }
                    line_item_lookup[line_item_id] = {
                        'flat_code': flat_code,
                        'cost_code': cost_code
                    }

            logs_to_process = []

            for log in logs:
                line_item_id = log.get('line_item_id')
                item_info = line_item_lookup.get(line_item_id)

                if not item_info:
                    errors.append({
                        'entity_code': f'Line Item: {line_item_id}',
                        'error_message': 'Productivity unit not found in Procore - cannot sync to ComputerEase'
                    })
                    continue

                logs_to_process.append({
                    'flat_code': item_info['flat_code'],
                    'cost_code': item_info['cost_code'],
                    'date': log.get('created_at'),
                    'notes': log.get('notes', ''),
                    'newunits': log.get('quantity_used')
                })

            return {
                'errors': errors,
                'job_code': job_code,
                'logs_to_process': logs_to_process
            }

        process_unit_details = rail.PythonOperator(
            task_id='process_unit_details',
            python_callable=extract_job_and_line_items
        )


        def get_ce_job_details(response):
            if not response.get('data'):
                raise ValueError(
                    "Job not found in ComputerEase — sync job first before syncing productivity units"
                )
            return {
                'wbs_type': response['data'][0].get('wbs_type'),
                'job_found': True
            }

        fetch_ce_job_details = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_job_details',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda: {
                'code': rail.result('process_unit_details')['job_code']
            },
            data_handler=get_ce_job_details
        )


        def transform_all_logs():
            contract_data = rail.result('process_unit_details')
            job_code = contract_data['job_code']
            logs_to_process = contract_data['logs_to_process']
            contract_errors = contract_data.get('errors', [])

            wbs_type = rail.result('fetch_ce_job_details').get('wbs_type')

            ce_entries = []
            errors = list(contract_errors)

            for log_entry in logs_to_process:
                flat_code = log_entry['flat_code']
                cost_code = log_entry['cost_code']

                phase_code, category_code, _ = parse_wbs_flat_code(flat_code, cost_code, wbs_type)

                if not category_code:
                    errors.append({
                        'entity_code': flat_code,
                        'error_message': f"Category code missing from flat_code '{flat_code}'"
                    })
                    continue

                if not log_entry.get('date') or not log_entry.get('newunits'):
                    errors.append({
                        'entity_code': flat_code,
                        'error_message': f"Missing required fields (date or newunits) for flat_code '{flat_code}'"
                    })
                    continue

                ctx = {'entity_code': flat_code}
                valid, _ = validate_field_length(job_code, 'job', CE_FIELD_LENGTHS['job'], errors, ctx)
                if not valid:
                    continue
                valid, _ = validate_field_length(category_code, 'cat', CE_FIELD_LENGTHS['cat'], errors, ctx)
                if not valid:
                    continue
                if phase_code and wbs_type == WBSType.JOB_PHASE_CAT:
                    valid, _ = validate_field_length(phase_code, 'phase', CE_FIELD_LENGTHS['phase'], errors, ctx)
                    if not valid:
                        continue

                ce_entry = {
                    'date': format_ce_date(log_entry['date']),
                    'job': job_code,
                    'cat': category_code,
                    'newunits': str(log_entry['newunits'] or '')[:CE_FIELD_LENGTHS['newunits']],
                    'notes': truncate_notes(log_entry.get('notes', ''), CE_FIELD_LENGTHS['notes'])
                }

                if wbs_type == WBSType.JOB_PHASE_CAT and phase_code:
                    ce_entry['phase'] = phase_code

                ce_entries.append(ce_entry)

            ce_import_data = {
                "type": CE_IMPORT_TYPE,
                "data": ce_entries,
                "comments": f"{len(ce_entries)} new record(s)"
            }
            return {
                'errors': errors,
                'ce_import_data': ce_import_data,
                'is_valid': bool(ce_import_data['data'])
            }

        prepare_ce_payload = rail.PythonOperator(
            task_id='prepare_ce_payload',
            python_callable=transform_all_logs
        )

        check_if_valid_units_present = rail.IfOperator(
            task_id='check_if_valid_units_present',
            test='{{ result("prepare_ce_payload").is_valid }}',
            yes_task='create_zip_and_encode',
            no_task='has_validation_errors'
        )

        def create_import_zip_file(dag_run):
            payload_result = rail.result('prepare_ce_payload')
            ce_import_data = payload_result.get('ce_import_data', [])
            project_id = dag_run.conf['item']['project_id']

            json_content = json.dumps(ce_import_data, indent=JSON_INDENT_SPACES)

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr(JSON_FILENAME, json_content)

            zip_bytes = zip_buffer.getvalue()
            base64_encoded = base64.b64encode(zip_bytes).decode('utf-8')

            return {
                'project_id': project_id,
                'base64_data': base64_encoded,
                'entry_count': len(ce_import_data['data'])
            }

        create_zip_and_encode = rail.PythonOperator(
            task_id='create_zip_and_encode',
            python_callable=create_import_zip_file
        )

        import_to_ce = rail.ComputereaseAPIOperator(
            task_id='import_to_ce',
            endpoint='/import/',
            request_method='POST',
            request_body=lambda: {
                "import_type": "Units Complete",
                "description": build_import_file_description(
                    RESOURCE_PRODUCTIVITY_LOG,
                    f"{rail.result('process_unit_details')['job_code']} ({rail.result('create_zip_and_encode')['entry_count']} new logs)"
                ),
                "import_data": rail.result('create_zip_and_encode')['base64_data']
            }
        )

        has_validation_errors = rail.IfOperator(
            task_id='has_validation_errors',
            test='{{ result("prepare_ce_payload").errors | length > 0 }}',
            yes_task='write_validation_errors',
            no_task='catch_error'
        )

        write_validation_errors = rail.WriteLogOperator(
            task_id='write_validation_errors',
            items=lambda: rail.result('prepare_ce_payload')['errors'],
            message='na',
            severity='Error/Exception',
            properties=lambda item, dag_run: {
                'entity_type': RESOURCE_PRODUCTIVITY_LOG,
                'entity_code': item.get('entity_code'),
                'project_id': dag_run.conf['item']['project_id'],
                'error_message': item.get('error_message')
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_code': dag_run.conf['item']['project_id'],
                'project_id': dag_run.conf['item']['project_id'],
                'log_ids': ','.join(dag_run.conf['item']['log_ids']),
                'error_message': f"Productivity unit sync failed for project {dag_run.conf['item']['project_id']} - {{{{ get_error_message() }}}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error >> log_to_sumo
        batch_task >> fetch_productivity_logs >> check_if_po_exists

        check_if_po_exists >> rail.Label('Yes') >> fetch_purchase_order_sovs >> check_if_sc_exists
        check_if_po_exists >> rail.Label('No') >> check_if_sc_exists

        check_if_sc_exists >> rail.Label('Yes') >> fetch_subcontract_sovs >> process_unit_details
        check_if_sc_exists >> rail.Label('No') >> process_unit_details >> fetch_ce_job_details

        fetch_ce_job_details >> prepare_ce_payload >> check_if_valid_units_present

        check_if_valid_units_present >> rail.Label('No') >> has_validation_errors
        check_if_valid_units_present >> rail.Label('Yes') >> create_zip_and_encode >> import_to_ce >> has_validation_errors

        has_validation_errors >> rail.Label('Yes') >> write_validation_errors >> catch_error
        has_validation_errors >> rail.Label('No') >> catch_error

        return dag


rail.for_each_instance(create_dag_instance)
