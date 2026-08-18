from datetime import timedelta
import json
import rail

from procore_ce_integration.ap_invoice_sync.utils import util
from procore_ce_integration.initial_setup_sync.shared_utils import extract_ce_code, normalize_ce_identifier, parse_wbs_flat_code
from procore_ce_integration.ap_invoice_sync.utils.constants import AccountingMethod, CommitmentType, ErrorType, InvoiceLineItemType


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.ap_invoice_child_dag_id,
        description='Procore to Computerease AP Invoice Sync CHILD DAG - Fetch Invoice Details',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='validate_input',
            end_task='prepare_invoice_data',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def validate_input_fields():
            conf = rail.get_dag_run_conf()
            required_fields = ['project_id', 'invoice_id']
            missing_fields = [f for f in required_fields if not conf.get(f)]

            if missing_fields:
                return {
                    'is_valid': False,
                    'error': f"Missing required fields: {', '.join(missing_fields)}",
                    'error_type': 'Input Validation'
                }

            return {
                'is_valid': True
            }

        validate_input = rail.PythonOperator(
            task_id='validate_input',
            python_callable=validate_input_fields
        )

        check_input_valid = rail.IfOperator(
            task_id='check_input_valid',
            test='{{ result("validate_input").is_valid }}',
            yes_task='fetch_invoice_details',
            no_task='prepare_invoice_data'
        )

        fetch_invoice_details = rail.ProcoreApiOperator(
            task_id='fetch_invoice_details',
            endpoint='/requisitions/{{ dag_run.conf.invoice_id }}',
            method='GET',
            version='1.1',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}',
                'view': 'extended'
            },
            paginate=False,
            data_handler=lambda response: {
                'invoice': response,
                'commitment_id': response.get('commitment_id') if response else None,
                'commitment_type': response.get('commitment_type') if response else None,
                'origin_data': (json.loads(response.get('origin_data')) if response and response.get(
                    'origin_data') and isinstance(response.get('origin_data'), str) else {})
            }
        )

        def process_commitment_response(response):
            if not response:
                return None

            # Extract vendor info
            vendor_origin_id = response.get('vendor', {}).get(
                'origin_id', '') if response.get('vendor') else ''
            vendor_code = normalize_ce_identifier(extract_ce_code(vendor_origin_id))
            vendor_name = response.get('vendor', {}).get(
                'company', '') if response.get('vendor') else ''

            # Extract project/job info
            project_origin_id = response.get('project', {}).get(
                'origin_id', '') if response.get('project') else ''
            job_code = normalize_ce_identifier(extract_ce_code(project_origin_id))

            return {
                'po_number': response.get('number'),
                'commitment_origin_id': response.get('origin_id', ''),
                'accounting_method': response.get('accounting_method', ''),
                'vendor_code': vendor_code,
                'vendor_name': vendor_name,
                'job_code': job_code,
                'raw_response': response
            }

        # Check invoice status - skip if not approved
        check_invoice_status = rail.IfOperator(
            task_id='check_invoice_status',
            test=lambda: rail.result('fetch_invoice_details', {}).get(
                'invoice', {}).get('status', '').lower() == 'approved',
            yes_task='fetch_commitment_details',
            no_task='prepare_invoice_data'
        )

        fetch_commitment_details = rail.ProcoreApiOperator(
            task_id='fetch_commitment_details',
            endpoint=lambda: (
                f'/purchase_order_contracts/{rail.result("fetch_invoice_details", {}).get("commitment_id")}'
                if rail.result("fetch_invoice_details", {}).get("commitment_type", "") == CommitmentType.PURCHASE_ORDER
                else f'/work_order_contracts/{rail.result("fetch_invoice_details", {}).get("commitment_id")}'
            ),
            method='GET',
            paginate=False,
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=process_commitment_response
        )

        # Check if invoice has already been synced to CE
        if_origindata_has_uuid = rail.IfOperator(
            task_id='if_origindata_has_uuid',
            test=lambda: bool(
                rail.result('fetch_invoice_details', {}).get(
                    'origin_data', {}).get('import_uuid')
            ),
            yes_task='fetch_ce_import_status',
            no_task='fetch_ce_job_wbs_type'
        )

        fetch_ce_import_status = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_import_status',
            endpoint=lambda: f"/import/{rail.result('fetch_invoice_details', {}).get('origin_data', {}).get('import_uuid')}",
            request_method='GET',
            paginate=False,
            data_handler=lambda response: {
                'status': (
                    response.get('data', [{}])[0].get('status', '')
                    if response and response.get('data')
                    else 'NotFound'
                ),
                'import_uuid': rail.result('fetch_invoice_details', {}).get('origin_data', {}).get('import_uuid')
            }
        )

        should_skip_invoice = rail.IfOperator(
            task_id='should_skip_invoice',
            test=lambda: rail.result('fetch_ce_import_status', {}).get(
                'status', 'NotFound') in config.SKIP_STATUSES,
            yes_task='prepare_invoice_data',
            no_task='fetch_ce_job_wbs_type'
        )

        fetch_ce_job_wbs_type = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_job_wbs_type',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda: {
                'code': rail.result('fetch_commitment_details', {}).get('job_code', '')
            },
            paginate=False,
            data_handler=lambda response: {
                'wbs_type': response['data'][0].get('wbs_type', ''),
                'job_code': response['data'][0].get('code', '')
            } if response and len(response.get('data') or []) > 0 else None
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

        def validate_fetched_data():
            invoice_result = rail.result('fetch_invoice_details')
            commitment = rail.result('fetch_commitment_details')

            if not invoice_result or not invoice_result.get('invoice'):
                return {
                    'is_valid': False,
                    'error': 'Failed to fetch invoice details',
                    'error_type': 'Data Fetch'
                }

            # Check if invoice has invoice number (mandatory for this integration)
            invoice = invoice_result.get('invoice', {})
            if not invoice.get('invoice_number'):
                return {
                    'is_valid': False,
                    'invoice_number': '',
                    'error': 'Invoice number is required for CE integration',
                    'error_type': 'Missing Info'
                }

            if not commitment:
                return {
                    'is_valid': False,
                    'invoice_number': invoice.get('invoice_number'),
                    'error': 'Failed to fetch commitment details',
                    'error_type': 'Data Fetch'
                }

            if not commitment.get('vendor_code'):
                return {
                    'is_valid': False,
                    'invoice_number': invoice.get('invoice_number'),
                    'error': 'Vendor missing CE code (origin_id not set)',
                    'error_type': 'Missing Info'
                }

            if not commitment.get('job_code'):
                return {
                    'is_valid': False,
                    'invoice_number': invoice.get('invoice_number'),
                    'error': 'Project missing CE code (origin_id not set)',
                    'error_type': 'Missing Info'
                }

            return {
                'is_valid': True
            }

        if_subcontract = rail.IfOperator(
            task_id='if_subcontract',
            test=lambda: rail.result('fetch_invoice_details')['commitment_type'] == CommitmentType.SUBCONTRACT,
            yes_task='fetch_ce_subcontract',
            no_task='prepare_invoice_data'
        )


        def get_filtered_items(line_items):
            return list(map(lambda x: {
                'unique_id': x.get('unique_id'),
                'sequence_id': x.get('sequence_id'),
                'description': x.get('description'),
                'amount': x.get('amount'),
                'phase_code': x.get('phase_code'),
                'category_code': x.get('category_code'),
                'costtype': x.get('costtype')
            }, line_items)) if line_items else []

        def get_line_items(response):
            change_orders = {}
            subcontract_items = None

            for item in response.get('data', []) or []:
                line_items = get_filtered_items(item.get('subcontract_item', []))
                rfc_code = item.get('rfc_code')
                if rfc_code:
                    change_orders[rfc_code] = line_items
                else:
                    subcontract_items = line_items

            return {
                'response': response,
                'subcontract_items': subcontract_items,
                'change_order_items': change_orders
            }

        fetch_ce_subcontract = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_subcontract',
            endpoint='/catalog/subcontract',
            request_method='GET',
            page_limit=1000,
            query_params=lambda: {
                'code': rail.result('fetch_commitment_details', {}).get('po_number', '')
            },
            data_handler=get_line_items
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


        def prepare_invoice_data_for_xml():  # pylint: disable=too-many-return-statements
            conf = rail.get_dag_run_conf()
            invoice_id = conf.get('invoice_id')
            try:
                # Check if input validation failed
                input_validation = rail.result('validate_input', {})
                if not input_validation.get('is_valid'):
                    return {
                        'invoice_id': invoice_id,
                        'error': input_validation.get('error', 'Invalid input'),
                        'error_type': input_validation.get('error_type', 'Input Validation')
                    }

                # Check if invoice status is not approved
                if rail.result('check_invoice_status') == 'prepare_invoice_data':
                    invoice_status = rail.result('fetch_invoice_details', {}).get(
                        'invoice', {}).get('status', 'unknown')
                    return {
                        'invoice_id': invoice_id,
                        'skipped': True,
                        'reason': f'Invoice status is {invoice_status}, not approved'
                    }

                # Check if invoice was skipped (should_skip_invoice went to yes_task)
                if rail.result('should_skip_invoice') == 'prepare_invoice_data':
                    return {
                        'invoice_id': invoice_id,
                        'skipped': True,
                        'ce_status': rail.result('fetch_ce_import_status', {}).get('status', 'Unknown'),
                        'import_uuid': rail.result('fetch_ce_import_status', {}).get('import_uuid')
                    }

                # Check if data validation failed
                data_validation = validate_fetched_data()
                if not data_validation.get('is_valid'):
                    return {
                        'invoice_id': invoice_id,
                        'invoice_number': data_validation.get('invoice_number', ''),
                        'error': data_validation.get('error', 'Data validation failed'),
                        'error_type': data_validation.get('error_type', 'Data Validation')
                    }

                # All validations passed, prepare the data
                invoice_result = rail.result('fetch_invoice_details', {})
                invoice = invoice_result.get('invoice', {})
                commitment = rail.result('fetch_commitment_details', {})

                # Get project name from invoice summary_text
                project_name = invoice.get(
                    'summary_text', {}).get('project_name', '')

                # Calculate amount_less_retention from payment_summary
                payment_summary = invoice.get('payment_summary', {})
                amount_less_retention = float(
                    payment_summary.get('invoiced_amount_due', 0) or 0)

                # Detect subcontract commitment
                is_subcontract = invoice_result['commitment_type'] == CommitmentType.SUBCONTRACT

                # Prepare invoice data structure
                invoice_data = {
                    'invoice_id': invoice.get('id'),
                    'invoice_number': invoice.get('invoice_number'),
                    'invoice_date': invoice.get('billing_date'),
                    'payment_due_date': invoice.get('due_date'),
                    'amount_less_retention': amount_less_retention,
                    'description': f"INV #{invoice.get('number', '')} for {util.clean_contract_name(invoice.get('contract_name', ''))}"[-30:],
                    'status': invoice.get('status'),

                    'vendor_code': commitment.get('vendor_code'),
                    'vendor_name': commitment.get('vendor_name'),

                    'job_code': commitment.get('job_code'),
                    'project_name': project_name,

                    'po_number': commitment.get('po_number'),
                    'commitment_origin_id': commitment.get('commitment_origin_id'),
                    'commitment_title': commitment.get('commitment_title'),
                    'commitment_id': invoice.get('commitment_id'),

                    'is_subcontract': is_subcontract,
                    'line_items': []
                }

                # Process invoice line items
                line_items = invoice.get('items', [])
                sum_of_gross = 0.0  # Track total gross amount across all line items

                ce_job_details = rail.result('fetch_ce_job_wbs_type', {})
                if not ce_job_details:
                    return {
                        'invoice_id': invoice_id,
                        'invoice_number': invoice_data.get('invoice_number', ''),
                        'error': 'Job not found in Computerease for the related Invoice',
                        'error_type': ErrorType.API_ERROR
                    }

                wbs_type = ce_job_details.get('wbs_type', '')
                cost_codes_lookup = rail.result('fetch_cost_codes', {})
                ce_subcontract = rail.result('fetch_ce_subcontract', {}) if is_subcontract else {}
                invoice_has_change_order_item = any(
                    item.get('item_type') == InvoiceLineItemType.CHANGE_ORDER_ITEM for item in line_items
                ) if is_subcontract else False
                ce_subcontract_items = ce_subcontract.get('subcontract_items')
                ce_change_order_items = ce_subcontract.get('change_order_items')
                if is_subcontract:
                    if not ce_subcontract_items:
                        return {
                            'invoice_id': invoice_id,
                            'subcontract': commitment.get('po_number'),
                            'invoice_number': invoice_data.get('invoice_number', ''),
                            'error': f"Subcontract not found in CE",
                            'error_type': ErrorType.API_ERROR
                        }
                    if invoice_has_change_order_item and not ce_change_order_items:
                        return {
                            'invoice_id': invoice_id,
                            'subcontract': commitment.get('po_number'),
                            'invoice_number': invoice_data.get('invoice_number', ''),
                            'error': f"Change order not found in CE",
                            'error_type': ErrorType.API_ERROR
                        }

                change_order_rfc_lookup = {
                    pkg['change_order_id']: pkg['number']
                    for pkg in invoice.get('item_packages', [])
                    if pkg.get('change_order_id')
                }
                is_unit_based = commitment.get('accounting_method') == AccountingMethod.UNIT

                for item in line_items:
                    wbs_flat_code = item.get('wbs_code', {}).get(
                        'flat_code', '') if item.get('wbs_code') else ''

                    cost_code_id = str(item.get('cost_code_id'))
                    cost_code = cost_codes_lookup.get(cost_code_id) if cost_code_id else None
                    phase_code, category_code, cost_type = parse_wbs_flat_code(
                        wbs_flat_code, cost_code, wbs_type)

                    # Calculate gross amount and accumulate for invoice total
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
                        ce_costtype = rail.result('fetch_cost_types').get(cost_type, '')
                        item_type = item.get('item_type', '')
                        line_item_data['subitemnum'] = None
                        if item_type in [InvoiceLineItemType.CONTRACT_ITEM, InvoiceLineItemType.CONTRACT_DETAIL_ITEM]:
                            line_item_data['subitemnum'] = util.get_ce_sequence_id(
                                ce_subcontract_items,
                                phase_code,
                                category_code,
                                ce_costtype
                            )

                        elif item_type == InvoiceLineItemType.CHANGE_ORDER_ITEM:
                            pco_id = item.get('potential_change_order_id')
                            rfc_number = change_order_rfc_lookup.get(pco_id, '')
                            line_item_data['subrfcnum'] = rfc_number

                            ce_co_items = ce_change_order_items.get(rfc_number) if ce_change_order_items else None
                            line_item_data['subitemnum'] = util.get_ce_sequence_id(
                                ce_co_items,
                                phase_code,
                                category_code,
                                ce_costtype
                            ) if ce_co_items else None

                        if line_item_data['subitemnum'] is None:
                            return {
                            'invoice_id': invoice_id,
                            'phase': phase_code,
                            'category': category_code,
                            'ce_costtype': ce_costtype,
                            'invoice_number': invoice_data.get('invoice_number', ''),
                            'error': f'Sequence id not matched for {item_type}: ({line_item_data["line_number"]}) - {wbs_flat_code}',
                            'error_type': 'Data Mismatch'
                        }

                        if is_unit_based:
                            billing_qty = float(
                                item.get('work_completed_this_period_quantity', 0) or 0
                            )
                            line_item_data['subbillqty'] = billing_qty

                    invoice_data['line_items'].append(line_item_data)

                # Calculate final amounts based on whether we have line items or not
                if invoice_data['line_items']:
                    # Sum of gross amounts becomes the invoice amount
                    invoice_data['amount'] = sum_of_gross
                    invoice_data['retention_amount'] = sum_of_gross - \
                        amount_less_retention
                else:
                    # No line items - use amount_less_retention as invoice amount
                    invoice_data['amount'] = amount_less_retention
                    invoice_data['retention_amount'] = 0.0

                # Validate field lengths against CE limits
                validation_warnings = util.validate_field_lengths(invoice_data, config.CE_FIELD_VALIDATIONS)

                if validation_warnings:
                    # Skip sync if any field exceeds character limits
                    return {
                        'invoice_id': invoice_id,
                        'invoice_number': invoice_data.get('invoice_number', ''),
                        'error': f"CE character limit violations: {'; '.join(validation_warnings)}",
                        'error_type': 'Field Length Validation'
                    }

                return {
                    'invoice_id': invoice_id,
                    'data': invoice_data
                }
            except Exception as e:
                return {
                    'invoice_id': invoice_id,
                    'error': f'Unexpected error in child DAG: {str(e)}',
                    'error_type': 'Unknown'
                }

        prepare_invoice_data = rail.PythonOperator(
            task_id='prepare_invoice_data',
            python_callable=prepare_invoice_data_for_xml,
            trigger_rule='all_done'
        )

        batch_task >> validate_input >> check_input_valid
        batch_task >> prepare_invoice_data

        check_input_valid >> rail.Label(
            'Yes') >> fetch_invoice_details >> check_invoice_status
        check_input_valid >> rail.Label('No') >> prepare_invoice_data

        check_invoice_status >> rail.Label(
            'Yes') >> fetch_commitment_details >> if_origindata_has_uuid
        check_invoice_status >> rail.Label('No') >> prepare_invoice_data

        if_origindata_has_uuid >> rail.Label(
            'Yes') >> fetch_ce_import_status >> should_skip_invoice
        should_skip_invoice >> rail.Label('Yes') >> prepare_invoice_data
        should_skip_invoice >> rail.Label('No') >> fetch_ce_job_wbs_type
        if_origindata_has_uuid >> rail.Label('No') >> fetch_ce_job_wbs_type

        fetch_ce_job_wbs_type >> fetch_cost_codes >> if_subcontract

        if_subcontract >> rail.Label('Yes') >> fetch_ce_subcontract >> fetch_cost_types >> prepare_invoice_data
        if_subcontract >> rail.Label('No') >> prepare_invoice_data

        return dag


rail.for_each_instance(create_dag_instance)
