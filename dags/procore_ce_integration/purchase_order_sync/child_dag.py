from datetime import timedelta
import base64
import io
import zipfile
import json
import rail
from procore_ce_integration.purchase_order_sync.utils.util import (
    format_ce_date,
    strip_html_tags,
    split_description,
    split_address,
    validate_field_length,
    validate_computerease_record
)
from procore_ce_integration.initial_setup_sync.shared_utils import (
    build_import_file_description,
    extract_ce_code,
    normalize_ce_identifier,
    parse_wbs_flat_code
)
from procore_ce_integration.purchase_order_sync.utils.constants import (
    APPROVED,
    ErrorType,
    PHASE_CODE_MAX_LENGTH,
    CATEGORY_CODE_MAX_LENGTH,
    MAX_DESCRIPTION_LINES,
    DESCRIPTION_LINE_LENGTH,
    MAX_ADDRESS_LINES,
    ADDRESS_LINE_LENGTH,
    CE_IMPORT_TYPE,
    JSON_FILENAME,
    JSON_INDENT_SPACES
)
from procore_ce_integration.purchase_order_sync.config import CE_FIELD_LENGTHS
from procore_ce_integration.job_structure_sync.utils.constants import WBSType


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='Procore to ComputerEase Purchase Order Sync - Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs_child,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_po_details',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_validated_purchase_order(response, dag_run):
            po_id = dag_run.conf['resource_id']
            po_data = response[0] if len(response) > 0 else {}

            job_code = normalize_ce_identifier(extract_ce_code(po_data['project'].get(
                'origin_id', ''))) if po_data.get('project') else None
            vendor_code = normalize_ce_identifier(extract_ce_code(po_data['vendor'].get(
                'origin_id', ''))) if po_data.get('vendor') else None
            project_name = po_data['project']['name'] if po_data.get('project') else 'unknown'

            po_details = {
                'po_data': po_data,
                'purchase_order_id': po_id,
                'project_name': project_name,
                'job_code': job_code,
                'vendor_code': vendor_code
            }

            if not po_data:
                raise ValueError('Failed to retrieve purchase order from Procore')

            if po_data.get('status') != APPROVED:
                return {'is_valid': False, **po_details}

            # Case-insensitive "already originated" skip (both sides via the helper): recognize a
            # legacy mixed-case CE_<code> so we do NOT re-sync it, since PO sync has no update path.
            # Differs intentionally from vendors_sync's exact compare (which self-migrates); legacy
            # PO origin_ids stay mixed and are corrected by a separate origin_id-only backfill.
            if normalize_ce_identifier(po_data.get('origin_id', '')) == f"CE_{normalize_ce_identifier(po_data.get('number', ''))}":
                return {'is_valid': False, **po_details}

            if not vendor_code:
                vendor_name = po_data['vendor'].get('company', 'unknown')
                raise ValueError(f'Vendor: {vendor_name} is not synced from ComputerEase')

            if not job_code:
                raise ValueError(f'Project: {project_name} is not synced from ComputerEase')

            return {'is_valid': True, **po_details}

        fetch_po_details = rail.ProcoreApiOperator(
            task_id='fetch_po_details',
            endpoint='/purchase_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'filters[id]': dag_run.conf['resource_id'],
                'view': 'extended'
            },
            data_handler=lambda response, dag_run: get_validated_purchase_order(
                response, dag_run)
        )

        check_po_valid = rail.IfOperator(
            task_id='check_po_valid',
            test='{{ result("fetch_po_details").is_valid }}',
            yes_task='fetch_ce_job_details',
            no_task='catch_error'
        )

        def get_ce_job_details(response):
            if not response.get('data'):
                return {'wbs_type': None, 'job_found': False}
            return {
                'wbs_type': response['data'][0].get('wbs_type'),
                'job_found': True
            }

        fetch_ce_job_details = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_job_details',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda: {
                'code': rail.result('fetch_po_details', {}).get('job_code', '')
            },
            data_handler=get_ce_job_details
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

        def extract_work_breakdown_structure_codes(line_item):
            flat_code = line_item['wbs_code'].get('flat_code') if line_item.get('wbs_code') else ''
            wbs_type = rail.result('fetch_ce_job_details').get('wbs_type')
            cost_code = line_item.get('cost_code')
            phase_code, category_code, cost_type_code = parse_wbs_flat_code(
                flat_code, cost_code, wbs_type)

            return {
                'phase_code': phase_code,
                'category_code': category_code,
                'cost_type_code': cost_type_code
            }

        def transform_purchase_order_header(purchase_order_data, errors_list):
            ship_to_address = purchase_order_data.get('ship_to_address', {})
            po_id = purchase_order_data.get('purchase_order_id', 'unknown')
            address_fields = split_address(
                ship_to_address,
                max_lines=MAX_ADDRESS_LINES,
                line_length=ADDRESS_LINE_LENGTH
            )

            computerease_header = {
                'ponum': purchase_order_data.get('po_number', ''),
                'povennum': purchase_order_data.get('vendor_code', ''),
                'podate': format_ce_date(purchase_order_data.get('po_date', '')),
                **address_fields,
                'poshipvia': validate_field_length(purchase_order_data.get('ship_via', ''), 'poshipvia', CE_FIELD_LENGTHS['poshipvia'], errors_list, po_id),
                'poreqdate': format_ce_date(purchase_order_data.get('required_date', '')),
                'poretpcnt': purchase_order_data.get('retention_percent', 0),
                'posalestaxnum': validate_field_length(purchase_order_data.get('sales_tax_number', ''), 'posalestaxnum', CE_FIELD_LENGTHS['posalestaxnum'], errors_list, po_id),
                'ponotes': (purchase_order_data.get('title', '') + '\n' + purchase_order_data.get('description', '')).strip()
            }
            return computerease_header

        def transform_purchase_order_line_item(line_item, purchase_order_header_data, errors_list):
            po_id = purchase_order_header_data.get('purchase_order_id', 'unknown')
            wbs_codes = line_item.get('extracted_wbs', {})
            line_item_id = line_item.get('id', 'unknown')

            phase_code = wbs_codes.get('phase_code', '')
            category_code = wbs_codes.get('category_code', '')
            cost_type_letter = wbs_codes.get('cost_type_code', '')

            if phase_code and len(phase_code) > PHASE_CODE_MAX_LENGTH:
                errors_list.append({
                    'purchase_order_id': po_id,
                    'error_message': f"Line item {line_item_id}: Phase code '{phase_code}' exceeds maximum length of {PHASE_CODE_MAX_LENGTH} characters.",
                    'error_type': ErrorType.ERROR
                })
                phase_code = ''

            if category_code and len(category_code) > CATEGORY_CODE_MAX_LENGTH:
                errors_list.append({
                    'purchase_order_id': po_id,
                    'error_message': f"Line item {line_item_id}: Category code '{category_code}' exceeds maximum length of {CATEGORY_CODE_MAX_LENGTH} characters.",
                    'error_type': ErrorType.ERROR
                })
                category_code = ''

            cost_type_numeric = rail.result('fetch_cost_types').get(cost_type_letter)
            item_cost_type = int(float(cost_type_numeric)) if str(cost_type_numeric).isdigit() else cost_type_letter
            if cost_type_numeric is None:
                errors_list.append({
                    'purchase_order_id': po_id,
                    'error_message': f"Line item {line_item_id}: Cost type code '{cost_type_letter}' not found in ComputerEase.",
                    'error_type': ErrorType.ERROR
                })
            elif len(cost_type_letter) > 1:
                errors_list.append({
                    'purchase_order_id': po_id,
                    'error_message': f"Line item {line_item_id}: Cost type code '{cost_type_letter}' exceeds maximum length of 1 character.",
                    'error_type': ErrorType.ERROR
                })

            description_text = line_item.get(
                'description', '') or line_item.get('title', '')
            description_fields = split_description(
                description_text,
                max_lines=MAX_DESCRIPTION_LINES,
                line_length=DESCRIPTION_LINE_LENGTH
            )

            job_code = line_item.get(
                'job_code') or purchase_order_header_data.get('job_code', '')
            if job_code and len(job_code) > CE_FIELD_LENGTHS['itemjob']:
                errors_list.append({
                    'purchase_order_id': po_id,
                    'error_message': f"Line item {line_item_id}: Job code '{job_code}' exceeds maximum length of {CE_FIELD_LENGTHS['itemjob']} characters.",
                    'error_type': ErrorType.ERROR
                })

            accounting_method = purchase_order_header_data.get(
                'accounting_method')
            itemprice = line_item.get(
                'unit_cost', 0) if accounting_method == 'unit' else line_item.get('total_amount', 0)
            quantity = line_item.get('quantity', 0) if accounting_method == 'unit' else (
                1 if float(itemprice) > 0 else 0)

            computerease_line_item = {
                **description_fields,
                'itemqty': float(quantity or 0),
                'itemprice': float(itemprice or 0),
                'itemlocation': validate_field_length(line_item.get('location', ''), 'itemlocation', CE_FIELD_LENGTHS['itemlocation'], errors_list, po_id, line_item_id),
                'itemjob': job_code,
                'itemphase': phase_code,
                'itemcat': category_code,
                'itemcosttype': item_cost_type
            }

            if not phase_code and not category_code:
                computerease_line_item['itemjob'] = ''
                computerease_line_item['itemphase'] = ''
                computerease_line_item['itemcat'] = ''

            wbs_type = rail.result('fetch_ce_job_details').get('wbs_type')
            if wbs_type == WBSType.JOB_CAT:
                computerease_line_item['itemphase'] = ''
                computerease_line_item['itemcat'] = category_code
            else:
                computerease_line_item['itemphase'] = phase_code
                computerease_line_item['itemcat'] = category_code

            return computerease_line_item

        def process_line_items(line_item, computerease_header, purchase_order_data, records, errors):
            computerease_line_item = transform_purchase_order_line_item(
                line_item,
                purchase_order_data,
                errors
            ) if line_item else {}

            record = {
                **computerease_header,
                **computerease_line_item
            }
            records.append(record)

        def raise_combined_errors(errors):
            blocking = [e['error_message'] for e in errors if e.get('error_type') == ErrorType.ERROR]
            if blocking:
                raise ValueError('\n'.join(blocking))

        def get_ce_payload():
            data = rail.result('fetch_po_details')
            po_id = data.get('purchase_order_id', '')
            po_data = data.get('po_data', {})
            job_code = data.get('job_code', '')
            vendor_code = data.get('vendor_code', '')

            errors = []

            if not rail.result('fetch_ce_job_details', {}).get('job_found'):
                raise ValueError('Job not found in ComputerEase — sync job first before syncing purchase orders')

            enriched_line_items = []
            for line_item in po_data.get('line_items', []):
                wbs_codes = extract_work_breakdown_structure_codes(line_item)
                item = dict(line_item)
                item['extracted_wbs'] = wbs_codes
                enriched_line_items.append(item)

            structured_data = {
                'purchase_order_id': po_id,
                'accounting_method': po_data.get('accounting_method', ''),
                'po_number': po_data.get('number'),
                'vendor_code': vendor_code,
                'job_code': job_code,
                'po_date': po_data.get('issued_on_date', ''),
                'required_date': po_data.get('delivery_date'),
                'sales_tax_number': po_data.get('tax_code_id'),
                'title': strip_html_tags(po_data.get('title', '')),
                'description': strip_html_tags(po_data.get('description', '')),
                'ship_to_address': po_data.get('ship_to_address', ''),
                'ship_via': po_data.get('ship_via', ''),
                'retention_percent': po_data.get('retainage_percent', 0),
                'line_items': enriched_line_items
            }

            records = []
            computerease_header = transform_purchase_order_header(structured_data, errors)
            is_valid, error_message = validate_computerease_record(computerease_header)
            if not is_valid:
                errors.append({
                    'purchase_order_id': po_id,
                    'error_message': f'Validation failed: {error_message}',
                    'error_type': ErrorType.ERROR
                })
                raise_combined_errors(errors)

            procore_line_items = structured_data.get('line_items', [])
            if not procore_line_items:
                process_line_items(
                    {},
                    computerease_header,
                    structured_data,
                    records,
                    errors
                )
            else:
                for line_item in procore_line_items:
                    process_line_items(
                        line_item,
                        computerease_header,
                        structured_data,
                        records,
                        errors
                    )

            # Any blocking line-item / field errors collected above abort the sync here.
            raise_combined_errors(errors)

            computerease_import_data = {
                "type": CE_IMPORT_TYPE,
                "comments": f"{len(records)} new record(s)",
                "data": records
            }

            return {
                'po_count': len(records),
                'ce_import_data': computerease_import_data
            }

        prepare_ce_payload = rail.PythonOperator(
            task_id='prepare_ce_payload',
            python_callable=get_ce_payload
        )

        check_if_pos_exist = rail.IfOperator(
            task_id='check_if_pos_exist',
            test=lambda: rail.result('prepare_ce_payload')['po_count'] > 0,
            yes_task='create_zip_and_encode',
            no_task='catch_error'
        )

        def create_import_zip_file():
            transformation_result = rail.result('prepare_ce_payload')
            computerease_import_data = transformation_result.get(
                'ce_import_data')

            po_number = rail.result('fetch_po_details', {}).get('po_data', {}).get('number', 'unknown')

            json_content = json.dumps(
                computerease_import_data, indent=JSON_INDENT_SPACES)

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr(JSON_FILENAME, json_content)

            zip_bytes = zip_buffer.getvalue()
            base64_encoded = base64.b64encode(zip_bytes).decode('utf-8')

            return {
                'po_number': po_number,
                'base64_data': base64_encoded
            }

        create_zip_and_encode = rail.PythonOperator(
            task_id='create_zip_and_encode',
            python_callable=create_import_zip_file
        )

        import_to_computerease = rail.ComputereaseAPIOperator(
            task_id='import_to_computerease',
            endpoint='/import/',
            request_method='POST',
            request_body=lambda: {
                "import_type": "Purchase Orders",
                "description": build_import_file_description('Purchase Order', rail.result('create_zip_and_encode')['po_number']),
                "import_data": rail.result('create_zip_and_encode')['base64_data']
            }
        )

        # Defer: enqueue the link for the mark-erp DAG instead of PATCHing origin_id now.
        if_defer_origin_id = rail.IfOperator(
            task_id='if_defer_origin_id',
            test=lambda: config.defer_origin_id_until_accepted,
            yes_task='build_pending_rows',
            no_task='mark_po_as_synced'
        )

        def get_ce_import_uuid():
            # Raw /import/ POST response carries the uuid under data.
            response = rail.result('import_to_computerease') or {}
            return (response.get('data') or {}).get('uuid', '')

        def build_pending_rows(dag_run):
            po_number = rail.result('create_zip_and_encode')['po_number']
            return [{
                'purchase_order_id': str(dag_run.conf['resource_id']),
                'project_id': str(dag_run.conf['project_id']),
                # origin_id must be CE's canonical UPPERCASE form to match CE->PC lookups.
                'origin_id': f"CE_{normalize_ce_identifier(po_number)}",
                'import_uuid': get_ce_import_uuid(),
                'queued_at': rail.render_template('{{ current_time() }}')
            }]

        build_pending_rows_task = rail.PythonOperator(
            task_id='build_pending_rows',
            python_callable=build_pending_rows
        )

        # Upsert keyed on purchase_order_id so a re-queue overwrites the stale pending row.
        enqueue_pending = rail.S3UpsertCollectionOperator(
            task_id='enqueue_pending',
            integration=config.s3_collection['integration'],
            customer=config.instance,
            collection_name=config.origin_id_update_table['name'],
            key_columns=config.origin_id_update_table['unique_columns'],
            rows=build_pending_rows_task.output
        )

        mark_po_as_synced = rail.ProcoreApiOperator(
            task_id='mark_po_as_synced',
            endpoint='/purchase_order_contracts/{{ dag_run.conf["resource_id"] }}',
            method='PATCH',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data=lambda: {
                "purchase_order_contract": {
                    "origin_id": f"CE_{normalize_ce_identifier(rail.result('create_zip_and_encode')['po_number'])}"
                }
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            severity='Error/Exception',
            message='{{ get_error_message() }}',
            properties={
                'purchase_order_id': '{{ dag_run.conf["resource_id"] }}',
                'company_id': '{{ dag_run.conf["company_id"] }}',
                'project_id': '{{ dag_run.conf["project_id"] }}',
                'error_type': 'error',
                'error_message': '{{ get_error_message() }}'
            }
        )

        batch_task >> catch_error
        batch_task >> fetch_po_details >> check_po_valid

        check_po_valid >> rail.Label(
            'Yes') >> fetch_ce_job_details >> fetch_cost_types >> prepare_ce_payload >> check_if_pos_exist
        check_po_valid >> rail.Label('No') >> catch_error

        check_if_pos_exist >> rail.Label('No') >> catch_error
        check_if_pos_exist >> rail.Label(
            'Yes') >> create_zip_and_encode >> import_to_computerease >> if_defer_origin_id

        if_defer_origin_id >> rail.Label('No') >> mark_po_as_synced >> catch_error
        if_defer_origin_id >> rail.Label(
            'Yes') >> build_pending_rows_task >> enqueue_pending >> catch_error

        return dag


rail.for_each_instance(create_dag_instance)
