from datetime import timedelta
import json
import rail
from procore_ce_integration.initial_setup_sync.shared_utils import normalize_ce_identifier


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.ar_invoice_child_dag_id,
        description='Procore to Computerease AR Invoice Sync CHILD DAG - Fetch AR Invoice Details',
        max_active_runs=config.child_dag_max_active_runs,
        integration_type='generic',
        company_key=config.instance,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='validate_input',
            end_task='prepare_ar_invoice_data',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def validate_ar_input_fields():
            conf = rail.get_dag_run_conf()
            required_fields = ['project_id', 'invoice_id']
            missing_fields = [f for f in required_fields if not conf.get(f)]

            if missing_fields:
                return {
                    'is_valid': False,
                    'error': f"Missing required fields: {', '.join(missing_fields)}",
                    'error_type': 'Input Validation'
                }
            return {'is_valid': True}

        validate_input = rail.PythonOperator(
            task_id='validate_input',
            python_callable=validate_ar_input_fields
        )

        check_input_valid = rail.IfOperator(
            task_id='check_input_valid',
            test='{{ result("validate_input").is_valid }}',
            yes_task='fetch_ar_invoice_details',
            no_task='prepare_ar_invoice_data'
        )

        fetch_ar_invoice_details = rail.ProcoreApiOperator(
            task_id='fetch_ar_invoice_details',
            endpoint='/payment_applications/{{ dag_run.conf.invoice_id }}',
            method='GET',
            query_params={'project_id': '{{ dag_run.conf.project_id }}'},
            paginate=False,
            data_handler=lambda response: {
                'invoice': response,
                'contract_id': response.get('contract', {}).get('id') if response and response.get('contract') else None,
                'origin_data': (json.loads(response.get('origin_data')) if response and response.get(
                    'origin_data') and isinstance(response.get('origin_data'), str) else {})
            }
        )

        # Check invoice status - skip if not approved
        check_invoice_status = rail.IfOperator(
            task_id='check_invoice_status',
            test=lambda: rail.result('fetch_ar_invoice_details', {}).get(
                'invoice', {}).get('status', '').lower() == 'approved',
            yes_task='fetch_prime_contract_details',
            no_task='prepare_ar_invoice_data'
        )

        def extract_ce_code(origin_id):
            if origin_id and str(origin_id).startswith('CE_CUS_'):
                return str(origin_id)[7:]
            if origin_id and str(origin_id).startswith('CE_'):
                return str(origin_id)[3:]
            return None

        def identify_project_from_invoice():
            inv = rail.result('fetch_ar_invoice_details', {})
            inv_detail = inv.get('invoice', {})
            details = inv_detail.get('g703', [])

            if details:
                first_item = details[0]
                cost_code = first_item.get('cost_code', {})
                biller_type = cost_code.get('biller_type', '')
                biller_origin_id = cost_code.get('biller_origin_id', '')

                if biller_type == 'Project' and biller_origin_id:
                    return extract_ce_code(biller_origin_id)
            return None

        def is_prime_contract_eligible(pc, project_code):
            if not project_code:
                return False
            expected_number = project_code
            expected_title = f"Prime Contract - {project_code}"
            number = pc.get('number')
            title = pc.get('title')
            return (number == expected_number and title == expected_title)

        def process_prime_contract_response(response):
            if not response:
                return None

            # Extract client info from vendor key
            client_info = response.get('vendor', {}) if response else {}
            client_origin_id = client_info.get(
                'origin_id', '') if client_info else ''
            client_code = normalize_ce_identifier(extract_ce_code(
                client_origin_id) if client_origin_id else client_info.get('abbreviated_name', ''))
            client_name = client_info.get('name', '') if client_info else ''

            # Check if Prime Contract has origin_id
            has_origin_id = bool(response.get('origin_id'))

            # Identify project code using Prime Contract origin_id or Invoice Detail
            project_code = normalize_ce_identifier(extract_ce_code(response.get(
                'origin_id')) if has_origin_id else identify_project_from_invoice())

            return {
                'contract_id': response.get('id'),
                'contract_number': response.get('number'),
                'client_code': client_code,
                'client_name': client_name,
                'project_code': project_code,
                'is_prime_contract_eligible': is_prime_contract_eligible(response, project_code) if not has_origin_id else True,
                'raw_response': response
            }

        fetch_prime_contract_details = rail.ProcoreApiOperator(
            task_id='fetch_prime_contract_details',
            endpoint=lambda: f'/prime_contract/{rail.result("fetch_ar_invoice_details", {}).get("contract_id")}',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            paginate=False,
            data_handler=process_prime_contract_response
        )

        # Check if AR invoice has already been synced to CE
        if_origindata_has_uuid = rail.IfOperator(
            task_id='if_origindata_has_uuid',
            test=lambda: bool(
                rail.result('fetch_ar_invoice_details', {}).get(
                    'origin_data', {}).get('import_uuid')
            ),
            yes_task='fetch_ce_import_status',
            no_task='validate_ar_data'
        )

        fetch_ce_import_status = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_import_status',
            endpoint=lambda: f"/import/{rail.result('fetch_ar_invoice_details', {}).get('origin_data', {}).get('import_uuid')}",
            request_method='GET',
            paginate=False,
            data_handler=lambda response: {
                'status': (
                    response.get('data', [{}])[0].get('status', '')
                    if response and response.get('data')
                    else 'NotFound'
                ),
                'import_uuid': rail.result('fetch_ar_invoice_details', {}).get('origin_data', {}).get('import_uuid')
            }
        )

        should_skip_invoice = rail.IfOperator(
            task_id='should_skip_invoice',
            test=lambda: rail.result('fetch_ce_import_status', {}).get(
                'status', 'NotFound') in config.SKIP_STATUSES,
            yes_task='prepare_ar_invoice_data',
            no_task='validate_ar_data'
        )

        def validate_ar_fetched_data():
            invoice_result = rail.result('fetch_ar_invoice_details')
            prime_contract = rail.result('fetch_prime_contract_details')

            if not invoice_result or not invoice_result.get('invoice'):
                return {
                    'is_valid': False,
                    'error': 'Failed to fetch AR invoice details',
                    'error_type': 'Data Fetch'
                }

            invoice = invoice_result.get('invoice', {})
            if not invoice.get('invoice_number'):
                return {
                    'is_valid': False,
                    'invoice_number': '',
                    'error': 'Invoice number is required for CE integration',
                    'error_type': 'Missing Info'
                }

            if not prime_contract:
                return {
                    'is_valid': False,
                    'invoice_number': invoice.get('invoice_number'),
                    'error': 'Failed to fetch prime contract details',
                    'error_type': 'Data Fetch'
                }

            if not prime_contract.get('project_code'):
                return {
                    'is_valid': False,
                    'invoice_number': invoice.get('invoice_number'),
                    'error': 'Project could not be identified using Prime Contract or the invoice does not have line items.',
                    'error_type': 'Missing Info'
                }

            if not prime_contract.get('is_prime_contract_eligible'):
                return {
                    'is_valid': False,
                    'invoice_number': invoice.get('invoice_number'),
                    'error': 'Prime Contract is ineligible for syncing invoice',
                    'error_type': 'Ineligible record'
                }

            if not prime_contract.get('client_code'):
                return {
                    'is_valid': False,
                    'invoice_number': invoice.get('invoice_number'),
                    'error': 'Customer could not be identified',
                    'error_type': 'Missing Info'
                }

            return {'is_valid': True}

        validate_ar_data = rail.PythonOperator(
            task_id='validate_ar_data',
            python_callable=validate_ar_fetched_data
        )

        if_validation_passed = rail.IfOperator(
            task_id='if_validation_passed',
            test=lambda: rail.result(
                'validate_ar_data', {}).get('is_valid', False),
            yes_task='fetch_ce_job_categories',
            no_task='prepare_ar_invoice_data'
        )

        fetch_ce_job_categories = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_job_categories',
            endpoint='/catalog/category',
            request_method='GET',
            query_params=lambda: {
                'job_code': rail.result('fetch_prime_contract_details', {}).get('project_code', '')
            },
            data_handler=lambda response: [
                c for c in response.get('data', []) if c.get('code', '')]
        )

        def identify_wbs_type(job_wbs_type):
            # CE has 3 wbs_types:
            # 1.T&M(when Time and Material check box is marked)
            # 2.Job/Phase/Cat(when phases exist)
            # 3.Job/Cat(when just categories exist or neither phases nor categories exist)
            # Because Job/Cat wbs_type is used in 2 scenarios, we need to identify if categories exist in CE or not to determine correct wbs_type to use while generating XML
            categories = rail.result('fetch_ce_job_categories')
            if job_wbs_type == 'Job/Cat':
                # If categories exist that means its not empty Job and we can rely on wbs_type value
                if categories and len(categories) > 0:
                    return job_wbs_type
                else:  # But if no categories exist in CE, that means its empty Job and we need to use default wbs_type from config
                    return config.default_wbs_type
            return job_wbs_type  # Return as is for other wbs_types

        fetch_ce_job_details = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_job_details',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda: {
                'code': rail.result('fetch_prime_contract_details', {}).get('project_code', '')
            },
            data_handler=lambda response: {
                'wbs_type': identify_wbs_type(response.get('data', [{}])[0].get('wbs_type', config.default_wbs_type)) if response.get('data') else config.default_wbs_type,
                'wbs_returned_by_ce': response.get('data', [{}])[0].get('wbs_type'),
                'job_found': len(response.get('data', [])) > 0
            }
        )

        get_invoice_line_items = rail.ProcoreApiOperator(
            task_id='get_invoice_line_items',
            endpoint='/payment_applications/{{ dag_run.conf.invoice_id }}',
            method='GET',
            version='1.1',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}',
                'view': 'extended'
            },
            paginate=False,
            data_handler=lambda response: {
                'items': response.get('items', []),
                'summary': response.get('summary', {}),
                'raw_response': response
            }
        )

        def parse_wbs_flat_code(flat_code, job_wbs_type):
            if not flat_code:
                return '', ''

            if '.' in flat_code:
                cost_code = flat_code.split('.')[0]
            else:
                cost_code = flat_code

            # Split cost_code by - to get phase and category
            if '-' in cost_code:  # Has both phase and category: "phase-category"
                parts = cost_code.split('-', 1)
                phase_code = parts[0]
                category_code = parts[1]
            else:  # Only one part, determine if it's phase or category based on WBS type
                if job_wbs_type == 'Job/Cat':  # It's a category
                    phase_code = ''
                    category_code = cost_code
                else:  # It's a phase
                    phase_code = cost_code
                    category_code = ''

            return phase_code, category_code

        def validate_ar_field_lengths(invoice_data):
            warnings = []

            # Create arrays for each field type from config
            invoice_fields = [
                (k, v) for k, v in config.CE_AR_FIELD_VALIDATIONS.items() if v['field_type'] == 'invoice']
            distribution_fields = [(k, v) for k, v in config.CE_AR_FIELD_VALIDATIONS.items(
            ) if v['field_type'] == 'distribution']

            # Validate invoice-level fields
            for field_key, validation_config in invoice_fields:
                value = invoice_data.get(field_key)
                if value and len(str(value)) > validation_config['char_limit']:
                    if validation_config['truncate']:
                        invoice_data[field_key] = str(value)[:validation_config['char_limit']]
                    else:
                        warnings.append(
                            f"{validation_config['display_name']} exceeds CE limit ({len(str(value))} > {validation_config['char_limit']} chars)")

            # Validate distribution fields
            for idx, distribution in enumerate(invoice_data.get('distributions', [])):
                for field_key, validation_config in distribution_fields:
                    value = distribution.get(field_key)
                    if value and len(str(value)) > validation_config['char_limit']:
                        if validation_config['truncate']:
                            invoice_data['distributions'][idx][field_key] = str(value)[:validation_config['char_limit']]
                        else:
                            warnings.append(
                                f"Distribution {idx+1} {validation_config['display_name']} exceeds CE limit ({len(str(value))} > {validation_config['char_limit']} chars)")

            return warnings

        def prepare_ar_invoice_data_for_xml():  # pylint: disable=too-many-return-statements
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
                if rail.result('check_invoice_status') == 'prepare_ar_invoice_data':
                    invoice_status = rail.result('fetch_ar_invoice_details', {}).get(
                        'invoice', {}).get('status', 'unknown')
                    return {
                        'invoice_id': invoice_id,
                        'skipped': True,
                        'reason': f'Invoice status is {invoice_status}, not approved'
                    }

                # Check if invoice was skipped due to CE import status
                if rail.result('should_skip_invoice') == 'prepare_ar_invoice_data':
                    return {
                        'invoice_id': invoice_id,
                        'skipped': True,
                        'ce_status': rail.result('fetch_ce_import_status', {}).get('status', 'Unknown'),
                        'import_uuid': rail.result('fetch_ce_import_status', {}).get('import_uuid')
                    }

                # Check if data validation failed
                data_validation = rail.result('validate_ar_data', {})
                if not data_validation.get('is_valid'):
                    return {
                        'invoice_id': invoice_id,
                        'invoice_number': data_validation.get('invoice_number', ''),
                        'error': data_validation.get('error', 'Data validation failed'),
                        'error_type': data_validation.get('error_type', 'Data Validation')
                    }

                # All validations passed, prepare AR invoice data
                invoice_result = rail.result('fetch_ar_invoice_details', {})
                invoice = invoice_result.get('invoice', {})
                prime_contract = rail.result(
                    'fetch_prime_contract_details', {})

                # Extract summary and detail data
                summary = rail.result('get_invoice_line_items')['summary']
                details = rail.result('get_invoice_line_items')['items']
                current_payment_due = float(summary.get(
                    'current_payment_due', '0.00') or 0)

                ar_invoice_data = {
                    'invoice_id': invoice.get('id'),
                    'invoice_number': invoice.get('invoice_number'),
                    'client_code': prime_contract.get('client_code'),
                    'description': invoice.get('invoice_number'),
                    'client_name': prime_contract.get('client_name'),
                    'job_code': prime_contract.get('project_code'),
                    'contract_id': prime_contract.get('contract_id'),
                    'contract_number': prime_contract.get('contract_number'),
                    'billing_date': invoice.get('billing_date'),
                    'status': invoice.get('status'),
                    'current_payment_due': str(current_payment_due),
                    'distributions': []
                }

                sum_of_gross = 0.0
                for detail_item in details:
                    amount = float(detail_item.get(
                        'gross_amount', 0) or 0)
                    sum_of_gross += amount

                    # Skip distribution if gross amount is 0
                    if amount == 0:
                        continue

                    wbs_code = detail_item.get('wbs_code', {})
                    flat_code = wbs_code.get('flat_code', '')
                    job_wbs_type = rail.result(
                        'fetch_ce_job_details')['wbs_type']
                    phase_code, category_code = parse_wbs_flat_code(
                        flat_code, job_wbs_type)

                    distribution = {
                        'wbs_flat_code': flat_code,
                        'phase_code': phase_code,
                        'category_code': category_code,
                        'amount': amount,
                        'description': wbs_code.get('description', '')
                    }
                    ar_invoice_data['distributions'].append(distribution)

                # Skip invoice if no distributions added
                if not ar_invoice_data['distributions']:
                    return {
                        'invoice_id': invoice_id,
                        'invoice_number': invoice.get('invoice_number', ''),
                        'skipped': True,
                        'should_log': True,
                        'reason': 'Invoice Skipped: Invoice is missing line items or has a 0 amount'
                    }

                ar_invoice_data.update({
                    'retainage_amount': str(sum_of_gross - current_payment_due),
                    'items': {
                        'qty': 1,
                        'unit_price': sum_of_gross,
                        'description': config.default_item_description
                    }
                })

                # Validate field lengths against CE limits
                validation_warnings = validate_ar_field_lengths(
                    ar_invoice_data)
                if validation_warnings:
                    # Skip sync if any field exceeds character limits
                    return {
                        'invoice_id': invoice_id,
                        'invoice_number': ar_invoice_data.get('invoice_number', ''),
                        'error': f"CE character limit violations: {'; '.join(validation_warnings)}",
                        'error_type': 'Field Length Validation'
                    }

                return {
                    'invoice_id': invoice_id,
                    'data': ar_invoice_data
                }

            except Exception as e:
                return {
                    'invoice_id': invoice_id,
                    'error': f'Unexpected error in AR child DAG: {str(e)}',
                    'error_type': 'Unknown'
                }

        prepare_ar_invoice_data = rail.PythonOperator(
            task_id='prepare_ar_invoice_data',
            python_callable=prepare_ar_invoice_data_for_xml,
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> validate_input >> check_input_valid
        batch_task >> prepare_ar_invoice_data

        check_input_valid >> rail.Label(
            'Yes') >> fetch_ar_invoice_details >> check_invoice_status
        check_input_valid >> rail.Label('No') >> prepare_ar_invoice_data

        check_invoice_status >> rail.Label(
            'Yes') >> fetch_prime_contract_details >> if_origindata_has_uuid
        check_invoice_status >> rail.Label('No') >> prepare_ar_invoice_data

        if_origindata_has_uuid >> rail.Label(
            'Yes') >> fetch_ce_import_status >> should_skip_invoice
        should_skip_invoice >> rail.Label('Yes') >> prepare_ar_invoice_data
        should_skip_invoice >> rail.Label('No') >> validate_ar_data
        if_origindata_has_uuid >> rail.Label('No') >> validate_ar_data

        validate_ar_data >> if_validation_passed
        if_validation_passed >> rail.Label(
            'Yes') >> fetch_ce_job_categories >> fetch_ce_job_details >> get_invoice_line_items >> prepare_ar_invoice_data
        if_validation_passed >> rail.Label('No') >> prepare_ar_invoice_data

        return dag


rail.for_each_instance(create_dag_instance)
