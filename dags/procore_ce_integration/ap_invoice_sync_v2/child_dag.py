from datetime import timedelta
import json
import rail

from procore_ce_integration.ap_invoice_sync_v2.utils import util
from procore_ce_integration.initial_setup_sync.shared_utils import extract_ce_code, normalize_ce_identifier, parse_wbs_flat_code
from procore_ce_integration.ap_invoice_sync_v2.utils.constants import AccountingMethod, CommitmentType, ErrorType, InvoiceLineItemType


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.ap_invoice_child_dag_id,
        description='Procore to Computerease AP Invoice Sync CHILD DAG - Bulk fetch & prepare invoices by project',
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
            start_task='fetch_invoices',
            end_task='prepare_invoice_data',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def process_invoices_response(response, dag_run):
            invoices = []
            subcontract_ids = []
            purchase_order_ids = []
            for inv in response:
                if inv.get('id') in dag_run.conf['invoice_ids']:
                    invoices.append(inv)
                    if inv.get('commitment_type') == CommitmentType.SUBCONTRACT and inv.get('commitment_id'):
                        subcontract_ids.append(str(inv['commitment_id']))

                    elif inv.get('commitment_type') == CommitmentType.PURCHASE_ORDER and inv.get('commitment_id'):
                        purchase_order_ids.append(str(inv['commitment_id']))

            return {
                'response': response,
                'invoices': invoices,
                'subcontract_ids': subcontract_ids,
                'purchase_order_ids': purchase_order_ids
            }

        fetch_invoices = rail.ProcoreApiOperator(
            task_id='fetch_invoices',
            endpoint='/requisitions',
            method='GET',
            version='1.1',
            query_params=lambda dag_run: {
                'view': 'extended',
                'project_id': dag_run.conf['project_id']
                # 'filters[id]': ','.join(str(i) for i in dag_run.conf['invoice_ids']), # use this once filters is supported
            },
            data_handler=lambda response, dag_run: process_invoices_response(response, dag_run)
        )

        def process_commitment_response(response):
            if not response:
                return None
            vendor_origin_id = response.get('vendor', {}).get('origin_id', '') if response.get('vendor') else ''
            vendor_code = normalize_ce_identifier(extract_ce_code(vendor_origin_id))
            vendor_name = response.get('vendor', {}).get('company', '') if response.get('vendor') else ''
            project_origin_id = response.get('project', {}).get('origin_id', '') if response.get('project') else ''
            job_code = normalize_ce_identifier(extract_ce_code(project_origin_id))
            return {
                'po_number': response.get('number'),
                'commitment_origin_id': response.get('origin_id', ''),
                'accounting_method': response.get('accounting_method', ''),
                'vendor_code': vendor_code,
                'vendor_name': vendor_name,
                'job_code': job_code,
                'commitment_title': response.get('title', ''),
                'raw_response': response
            }

        has_purchase_orders = rail.IfOperator(
            task_id='has_purchase_orders',
            test='{{ result("fetch_invoices").purchase_order_ids | length > 0 }}',
            yes_task='fetch_purchase_orders',
            no_task='has_subcontracts'
        )

        fetch_purchase_orders = rail.ProcoreApiOperator(
            task_id='fetch_purchase_orders',
            endpoint='/purchase_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'filters[id]': f"[{','.join(rail.result('fetch_invoices')['purchase_order_ids'])}]"
            },
            data_handler=lambda response: {
                str(c['id']): process_commitment_response(c) for c in (response or [])
            }
        )

        has_subcontracts = rail.IfOperator(
            task_id='has_subcontracts',
            test='{{ result("fetch_invoices").subcontract_ids | length > 0 }}',
            yes_task='fetch_subcontracts',
            no_task='fetch_cost_codes'
        )

        fetch_subcontracts = rail.ProcoreApiOperator(
            task_id='fetch_subcontracts',
            endpoint='/work_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'filters[id]': f"[{','.join(rail.result('fetch_invoices')['subcontract_ids'])}]"
            },
            data_handler=lambda response: {
                str(c['id']): process_commitment_response(c) for c in (response or [])
            }
        )

        fetch_cost_types = rail.ComputereaseAPIOperator(
            task_id='fetch_cost_types',
            endpoint='/catalog/cost-type',
            request_method='GET',
            data_handler=lambda resp: {
                item['reference']: item['code']
                for item in resp.get('data', [])
                if item.get('reference') and item.get('code')
            }
        )

        def group_ce_subcontracts_by_code(response):
            relevant_codes = {
                c['po_number']
                for c in (rail.result('fetch_subcontracts') or {}).values()
            }
            grouped = {}
            for item in (response.get('data') or []):
                code = item.get('code')
                if code not in relevant_codes:
                    continue
                line_items = [
                    {
                        'unique_id': x.get('unique_id'),
                        'sequence_id': x.get('sequence_id'),
                        'description': x.get('description'),
                        'amount': x.get('amount'),
                        'phase_code': x.get('phase_code'),
                        'category_code': x.get('category_code'),
                        'costtype': x.get('costtype')
                    }
                    for x in (item.get('subcontract_item') or [])
                ]
                entry = grouped.setdefault(code, {
                    'subcontract_items': None,
                    'change_order_items': {}
                })
                rfc_code = item.get('rfc_code')
                if rfc_code:
                    entry['change_order_items'][rfc_code] = line_items
                else:
                    entry['subcontract_items'] = line_items
            return grouped

        fetch_ce_subcontracts = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_subcontracts',
            endpoint='/catalog/subcontract',
            request_method='GET',
            page_limit=1000,
            data_handler=group_ce_subcontracts_by_code
        )

        fetch_cost_codes = rail.ProcoreApiOperator(
            task_id='fetch_cost_codes',
            endpoint='/cost_codes',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=lambda response: {
                cc['id']: cc for cc in (response or [])
            }
        )

        def _get_first_job_code():
            all_commitments = {}
            all_commitments.update(rail.result('fetch_purchase_orders', {}) or {})
            all_commitments.update(rail.result('fetch_subcontracts', {}) or {})
            for c in all_commitments.values():
                if c and c.get('job_code'):
                    return c['job_code']
            return ''

        fetch_ce_job_wbs_type = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_job_wbs_type',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda: {'code': _get_first_job_code()},
            paginate=False,
            data_handler=lambda response: {
                response['data'][0].get('code', ''): {
                    'wbs_type': response['data'][0].get('wbs_type', ''),
                    'job_code': response['data'][0].get('code', '')
                }
            } if response and len(response.get('data') or []) > 0 else {}
        )

        fetch_all_ce_imports = rail.ComputereaseAPIOperator(
            task_id='fetch_all_ce_imports',
            endpoint='/import/',
            request_method='GET',
            query_params={'import_type': 'Payable Invoices'},
            paginate=True,
            data_handler=lambda response: {
                imp.get('uuid'): imp
                for imp in (response.get('data', []) if isinstance(response, dict) else (response or []))
            }
        )

        def prepare_invoice_data_for_xml():  # pylint: disable=too-many-return-statements,too-many-branches,too-many-statements
            bulk_result = rail.result('fetch_invoices', {})
            invoices = bulk_result.get('invoices', [])

            all_commitments = {}
            all_commitments.update(rail.result('fetch_purchase_orders', {}) or {})
            all_commitments.update(rail.result('fetch_subcontracts', {}) or {})

            all_ce_imports = rail.result('fetch_all_ce_imports', {}) or {}
            cost_codes_lookup = rail.result('fetch_cost_codes', {}) or {}
            wbs_type_by_job_code = rail.result('fetch_ce_job_wbs_type', {}) or {}
            cost_types_lookup = rail.result('fetch_cost_types', None)

            results = []

            for invoice in invoices:
                invoice_id = invoice.get('id')
                invoice_number = invoice.get('invoice_number', '')

                try:
                    # Status check
                    if invoice.get('status', '').lower() != 'approved':
                        results.append({
                            'invoice_id': invoice_id,
                            'skipped': True,
                            'reason': f"Invoice status is {invoice.get('status', 'unknown')}, not approved"
                        })
                        continue

                    # Dedup check via bulk CE import list
                    raw_origin = invoice.get('origin_data')
                    origin_data = {}
                    if raw_origin and isinstance(raw_origin, str):
                        try:
                            origin_data = json.loads(raw_origin)
                        except (json.JSONDecodeError, ValueError):
                            pass
                    elif isinstance(raw_origin, dict):
                        origin_data = raw_origin

                    import_uuid = origin_data.get('import_uuid')
                    if import_uuid:
                        ce_import = all_ce_imports.get(import_uuid)
                        # Only skip if CE actually has this import and it is already done.
                        # A uuid missing from CE (deleted/stale origin_data) falls through
                        # and gets re-imported rather than being silently marked accepted.
                        if ce_import and ce_import.get('status') in config.SKIP_STATUSES:
                            results.append({
                                'invoice_id': invoice_id,
                                'skipped': True,
                                'ce_status': ce_import.get('status'),
                                'import_uuid': import_uuid
                            })
                            continue

                    # Commitment lookup
                    commitment_id = str(invoice.get('commitment_id', ''))
                    commitment_type = invoice.get('commitment_type', '')
                    commitment = all_commitments.get(commitment_id)

                    if not invoice_number:
                        results.append({'invoice_id': invoice_id, 'invoice_number': '', 'error': 'Invoice number is required for CE integration', 'error_type': 'Missing Info'})
                        continue
                    if not commitment:
                        results.append({'invoice_id': invoice_id, 'invoice_number': invoice_number, 'error': 'Failed to fetch commitment details', 'error_type': 'Data Fetch'})
                        continue
                    if not commitment.get('vendor_code'):
                        results.append({'invoice_id': invoice_id, 'invoice_number': invoice_number, 'error': 'Vendor missing CE code (origin_id not set)', 'error_type': 'Missing Info'})
                        continue
                    if not commitment.get('job_code'):
                        results.append({'invoice_id': invoice_id, 'invoice_number': invoice_number, 'error': 'Project missing CE code (origin_id not set)', 'error_type': 'Missing Info'})
                        continue

                    # CE job WBS type lookup
                    job_code = commitment.get('job_code', '')
                    ce_job_details = wbs_type_by_job_code.get(job_code)
                    if not ce_job_details:
                        results.append({'invoice_id': invoice_id, 'invoice_number': invoice_number, 'error': 'Job not found in Computerease for the related Invoice', 'error_type': ErrorType.API_ERROR})
                        continue
                    wbs_type = ce_job_details.get('wbs_type', '')

                    # Subcontract data lookup
                    is_subcontract = commitment_type == CommitmentType.SUBCONTRACT
                    ce_subcontract = {}
                    if is_subcontract:
                        ce_subcontracts_map = rail.result('fetch_ce_subcontracts', {}) or {}
                        ce_subcontract = ce_subcontracts_map.get(commitment.get('po_number', ''), {})

                    # Build invoice_data
                    payment_summary = invoice.get('payment_summary', {})
                    amount_less_retention = float(payment_summary.get('invoiced_amount_due', 0) or 0)
                    project_name = invoice.get('summary_text', {}).get('project_name', '') if invoice.get('summary_text') else ''

                    invoice_data = {
                        'invoice_id': invoice_id,
                        'invoice_number': invoice_number,
                        'invoice_date': invoice.get('billing_date'),
                        'payment_due_date': invoice.get('due_date'),
                        'amount_less_retention': amount_less_retention,
                        'description': f"INV #{invoice.get('number', '')} for {util.clean_contract_name(invoice.get('contract_name', ''))}"[-30:],
                        'status': invoice.get('status'),
                        'vendor_code': commitment.get('vendor_code'),
                        'vendor_name': commitment.get('vendor_name'),
                        'job_code': job_code,
                        'project_name': project_name,
                        'po_number': commitment.get('po_number'),
                        'commitment_origin_id': commitment.get('commitment_origin_id'),
                        'commitment_title': commitment.get('commitment_title'),
                        'commitment_id': invoice.get('commitment_id'),
                        'is_subcontract': is_subcontract,
                        'line_items': []
                    }

                    # Validate subcontract data before processing line items
                    line_items = invoice.get('items', [])
                    invoice_has_change_order_item = any(
                        item.get('item_type') == InvoiceLineItemType.CHANGE_ORDER_ITEM for item in line_items
                    ) if is_subcontract else False
                    ce_subcontract_items = ce_subcontract.get('subcontract_items')
                    ce_change_order_items = ce_subcontract.get('change_order_items')

                    if is_subcontract:
                        if not ce_subcontract_items:
                            results.append({'invoice_id': invoice_id, 'subcontract': commitment.get('po_number'), 'invoice_number': invoice_number, 'error': 'Subcontract not found in CE', 'error_type': ErrorType.API_ERROR})
                            continue
                        if invoice_has_change_order_item and not ce_change_order_items:
                            results.append({'invoice_id': invoice_id, 'subcontract': commitment.get('po_number'), 'invoice_number': invoice_number, 'error': 'Change order not found in CE', 'error_type': ErrorType.API_ERROR})
                            continue

                    # Process line items
                    change_order_rfc_lookup = {
                        pkg['change_order_id']: pkg['number']
                        for pkg in invoice.get('item_packages', [])
                        if pkg.get('change_order_id')
                    }
                    is_unit_based = commitment.get('accounting_method') == AccountingMethod.UNIT
                    sum_of_gross = 0.0
                    line_item_error = None

                    for item in line_items:
                        wbs_flat_code = item.get('wbs_code', {}).get('flat_code', '') if item.get('wbs_code') else ''
                        cost_code_id = str(item.get('cost_code_id'))
                        cost_code = cost_codes_lookup.get(cost_code_id) if cost_code_id else None
                        phase_code, category_code, cost_type = parse_wbs_flat_code(wbs_flat_code, cost_code, wbs_type)

                        gross_amount = float(item.get('gross_amount', 0) or 0)
                        if gross_amount == 0:
                            continue
                        sum_of_gross += gross_amount

                        line_item_data = {
                            'description': item.get('description_of_work', ''),
                            'amount': gross_amount,
                            'phase_code': phase_code,
                            'category_code': category_code,
                            'cost_type': cost_type,
                            'wbs_code': wbs_flat_code,
                            'line_number': item.get('line_number')
                        }

                        if is_subcontract:
                            ce_costtype = (cost_types_lookup or {}).get(cost_type, '')
                            item_type = item.get('item_type', '')
                            line_item_data['subitemnum'] = None

                            if item_type in [InvoiceLineItemType.CONTRACT_ITEM, InvoiceLineItemType.CONTRACT_DETAIL_ITEM]:
                                line_item_data['subitemnum'] = util.get_ce_sequence_id(
                                    ce_subcontract_items, phase_code, category_code, ce_costtype)
                            elif item_type == InvoiceLineItemType.CHANGE_ORDER_ITEM:
                                pco_id = item.get('potential_change_order_id')
                                rfc_number = change_order_rfc_lookup.get(pco_id, '')
                                line_item_data['subrfcnum'] = rfc_number
                                ce_co_items = ce_change_order_items.get(rfc_number) if ce_change_order_items else None
                                line_item_data['subitemnum'] = util.get_ce_sequence_id(
                                    ce_co_items, phase_code, category_code, ce_costtype) if ce_co_items else None

                            if line_item_data['subitemnum'] is None:
                                line_item_error = {
                                    'invoice_id': invoice_id,
                                    'phase': phase_code,
                                    'category': category_code,
                                    'ce_costtype': ce_costtype,
                                    'invoice_number': invoice_number,
                                    'error': f'Sequence id not matched for {item_type}: ({line_item_data["line_number"]}) - {wbs_flat_code}',
                                    'error_type': 'Data Mismatch'
                                }
                                break

                            if is_unit_based:
                                line_item_data['subbillqty'] = float(item.get('work_completed_this_period_quantity', 0) or 0)

                        invoice_data['line_items'].append(line_item_data)

                    if line_item_error:
                        results.append(line_item_error)
                        continue

                    if invoice_data['line_items']:
                        invoice_data['amount'] = sum_of_gross
                        invoice_data['retention_amount'] = sum_of_gross - amount_less_retention
                    else:
                        invoice_data['amount'] = amount_less_retention
                        invoice_data['retention_amount'] = 0.0

                    validation_warnings = util.validate_field_lengths(invoice_data, config.CE_FIELD_VALIDATIONS)
                    if validation_warnings:
                        results.append({'invoice_id': invoice_id, 'invoice_number': invoice_number, 'error': f"CE character limit violations: {'; '.join(validation_warnings)}", 'error_type': 'Field Length Validation'})
                        continue

                    results.append({'invoice_id': invoice_id, 'data': invoice_data})

                except Exception as e:  # pylint: disable=broad-except
                    results.append({'invoice_id': invoice_id, 'error': f'Unexpected error processing invoice: {str(e)}', 'error_type': 'Unknown'})

            return results

        prepare_invoice_data = rail.PythonOperator(
            task_id='prepare_invoice_data',
            python_callable=prepare_invoice_data_for_xml,
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> prepare_invoice_data
        batch_task >> fetch_invoices >> has_purchase_orders

        has_purchase_orders >> rail.Label('Yes') >> fetch_purchase_orders >> has_subcontracts
        has_purchase_orders >> rail.Label('No') >> has_subcontracts

        has_subcontracts >> rail.Label('Yes') >> fetch_subcontracts >> fetch_cost_types >> fetch_ce_subcontracts >> fetch_cost_codes
        has_subcontracts >> rail.Label('No') >> fetch_cost_codes

        fetch_cost_codes >> fetch_ce_job_wbs_type >> fetch_all_ce_imports >> prepare_invoice_data

        return dag


rail.for_each_instance(create_dag_instance)
