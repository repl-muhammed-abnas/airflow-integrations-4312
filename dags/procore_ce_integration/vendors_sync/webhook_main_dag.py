from datetime import timedelta, datetime, timezone
import rail
import json
import zipfile
import base64
import io
from airflow.models import Variable
from procore_ce_integration.initial_setup_sync.shared_utils import (
    build_import_file_description,
    get_tenant_email,
    is_self_originated_event,
    normalize_ce_identifier,
    parse_date,
)
from procore_ce_integration.vendors_sync.config import (
    field_validations,
    vennum_max_length,
    resource_company_vendors,
    supported_event_types,
    vendor_bulk_sync_trigger_date_var_prefix,
)


def create_dag_instance(config):

    # Per-instance mode switch: empty -> webhook mode, a date -> one-time bulk sync.
    bulk_sync_trigger_date_var = f'{vendor_bulk_sync_trigger_date_var_prefix}_{config.instance}'

    with rail.create_airflow_dag(
        dag_id=config.vendor_webhook_dag_id,
        description='Procore to ComputerEase Vendor Sync (Webhook + Bulk)',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs,
        webhook_conf=rail.WebhookConf(bearer_token_var=config.bearer_token_var),
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='detect_mode_and_extract',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        procore_company_id_template = "{{conn." + \
            config.procore_conn_id + ".extra_dejson.company_id}}"

        def detect_mode_and_extract(dag_run):
            # Date set -> bulk; empty/unset/invalid -> webhook. Seed empty on first run so it
            # shows in the UI.
            trigger_date = Variable.get(bulk_sync_trigger_date_var, default_var=None)
            if trigger_date is None:
                Variable.set(bulk_sync_trigger_date_var, '')
                trigger_date = ''
            from_ts = parse_date(trigger_date.strip(), config.procore_datetime_format) if trigger_date.strip() else None
            if from_ts:
                # Upper bound must carry a time, else it resolves to midnight and drops same-day edits.
                to_ts = datetime.now(timezone.utc).strftime(config.procore_datetime_format)
                return {
                    'is_bulk': True,
                    'skip_processing': False,
                    'updated_at_to': to_ts,
                    'updated_at_from': from_ts
                }

            webhook_data = (dag_run.conf or {}).get('webhook', {}).get('data', {})
            resource_name = webhook_data.get('resource_name')
            event_type = webhook_data.get('event_type')
            resource_id = webhook_data.get('resource_id')

            if resource_name != resource_company_vendors:
                return {'is_bulk': False, 'skip_processing': True, 'reason': f'Not a supported resource: {resource_name}'}

            if event_type not in supported_event_types:
                return {'is_bulk': False, 'skip_processing': True, 'reason': f'Event type {event_type} not supported (only create/update)'}

            # Loopback guard: skip the echo of our own writes; UI/other-app changes still sync.
            if is_self_originated_event(webhook_data):
                return {'is_bulk': False, 'skip_processing': True, 'reason': 'Skipping self-originated change (loopback)'}

            # Cascade guard: project-triggered vendor changes carry source_project_id; direct user edits don't.
            source_project_id = (webhook_data.get('metadata') or {}).get('source_project_id')
            if source_project_id:
                return {'is_bulk': False, 'skip_processing': True, 'reason': f'Skipping cascaded vendor change from project {source_project_id}'}

            return {
                'is_bulk': False,
                'skip_processing': False,
                'vendor_id': resource_id
            }

        detect_mode_and_extract_task = rail.PythonOperator(
            task_id='detect_mode_and_extract',
            python_callable=detect_mode_and_extract
        )

        should_process = rail.IfOperator(
            task_id='should_process',
            test=lambda: not rail.result('detect_mode_and_extract').get('skip_processing', False),
            yes_task='fetch_vendors',
            no_task='reset_bulk_date'
        )

        def is_customer(vendor):
            # Customers (vendors with CE_CUS in origin_id or the CUS- name prefix) are not synced.
            origin_id = vendor.get('origin_id', '') or ''
            return ('CE_CUS' in origin_id) or (vendor.get('name') or '').startswith(config.cus_identifier)

        def exclude_customers(vendors):
            return [
                vendor for vendor in (vendors or [])
                if isinstance(vendor, dict) and not is_customer(vendor)
            ]

        def classify_duplicate_vendors(company_vendors, sync_vendor_ids):
            # Duplicate = the same abbreviated_name (vennum) is present on 2+ Procore vendors;
            # origin_id is not consulted. Returns the ids of this run's vendors that must be skipped,
            # split so each bucket gets its own error message.
            vendors_by_vennum = {}
            for vendor in company_vendors:
                vennum = (vendor.get('abbreviated_name') or '').strip()
                if not vennum:
                    continue
                vendors_by_vennum.setdefault(vennum, []).append(vendor)

            intra_run_duplicate_ids, existing_duplicate_ids = [], []
            for vennum, same_name_vendors in vendors_by_vennum.items():
                if len(same_name_vendors) < 2:
                    continue  # unique name -> nothing to flag
                in_run_vendors = [v for v in same_name_vendors if v.get('id') in sync_vendor_ids]
                collides_with_existing = any(v.get('id') not in sync_vendor_ids for v in same_name_vendors)
                if collides_with_existing:
                    # another Procore vendor outside this run holds the name -> skip all in-run ones
                    existing_duplicate_ids.extend(v.get('id') for v in in_run_vendors)
                else:
                    # name clashes only within this run -> keep the first, skip the rest
                    intra_run_duplicate_ids.extend(v.get('id') for v in in_run_vendors[1:])
            return intra_run_duplicate_ids, existing_duplicate_ids

        def _vendor_in_updated_at_range(vendor, detect_result):
            # Keep vendors whose updated_at falls in the bulk window; ISO-8601 timestamps in
            # procore_datetime_format compare lexicographically.
            from_ts = detect_result['updated_at_from']
            to_ts = detect_result['updated_at_to']
            vendor_ts = parse_date(vendor.get('updated_at'), config.procore_datetime_format)
            return bool(vendor_ts) and from_ts <= vendor_ts <= to_ts

        def filter_vendors(vendors):
            # Both modes fetch ALL vendors so duplicate detection sees the whole company, then pick
            # the sync targets: bulk -> the updated_at window; webhook -> the single event vendor.
            company_vendors = exclude_customers(vendors)
            detect_result = rail.result('detect_mode_and_extract')
            if detect_result.get('is_bulk'):
                vendors_to_sync = [v for v in company_vendors if _vendor_in_updated_at_range(v, detect_result)]
            else:
                webhook_vendor_id = str(detect_result.get('vendor_id'))
                vendors_to_sync = [v for v in company_vendors if str(v.get('id')) == webhook_vendor_id]

            sync_vendor_ids = {v.get('id') for v in vendors_to_sync}
            intra_run_duplicate_ids, existing_duplicate_ids = classify_duplicate_vendors(
                company_vendors, sync_vendor_ids)

            return {
                'vendors_to_sync': vendors_to_sync,
                'existing_duplicate_ids': existing_duplicate_ids,
                'intra_run_duplicate_ids': intra_run_duplicate_ids
            }

        fetch_vendors = rail.ProcoreApiOperator(
            task_id='fetch_vendors',
            endpoint='/vendors',
            method='GET',
            query_params={
                'view': 'normal',
                'company_id': procore_company_id_template
            },
            data_handler=filter_vendors
        )

        has_vendors_to_sync = rail.IfOperator(
            task_id='has_vendors_to_sync',
            test=lambda: len(rail.result('fetch_vendors')['vendors_to_sync']) > 0,
            yes_task='validate_vendor_data',
            no_task='reset_bulk_date'
        )

        def build_bulk_sync_payload():
            valid_vendors = rail.result('validate_vendor_data')['valid_vendors']
            updates_payload = [
                {
                    "id": vendor['id'],
                    # origin_id must carry CE's canonical UPPERCASE code (CE uppercases the vennum
                    # on import); a raw mixed-case vennum here breaks CE->PC lookups that build CE_<UPPER>.
                    "origin_id": f"CE_{normalize_ce_identifier(vendor['vennum'])}"
                }
                for vendor in valid_vendors
            ]
            return {"updates": updates_payload}

        build_origin_id_payload = rail.PythonOperator(
            task_id='build_origin_id_payload',
            python_callable=build_bulk_sync_payload
        )

        has_origin_id_updates = rail.IfOperator(
            task_id='has_origin_id_updates',
            test=lambda: len(rail.result('build_origin_id_payload')['updates']) > 0,
            yes_task='update_originid_in_procore',
            no_task='if_validation_errors_present'
        )

        update_originid_in_procore = rail.ProcoreApiOperator(
            task_id='update_originid_in_procore',
            procore_conn_id=config.procore_conn_id,
            endpoint='/vendors/sync',
            method='PATCH',
            query_params={
                'company_id': procore_company_id_template,
                'run_configurable_validations': 'false'
            },
            data=lambda: rail.result('build_origin_id_payload')
        )

        def split_address_for_ce(address):
            if not address:
                return {'address1': ''}
            if len(address) <= 30:
                return {'address1': address}
            else:
                # Find last space before position 30 to avoid breaking words
                split_pos = address.rfind(' ', 0, 30)
                if split_pos == -1:  # No space found, hard split at 30
                    split_pos = 30
                addr2 = address[split_pos:].strip()
                result = {'address1': address[:split_pos]}
                if addr2:  # Only include address2 if not blank
                    result['address2'] = addr2
                return result

        def get_vennum_and_validate(vendor, existing_ids, intra_run_ids):
            vennum = (vendor.get('abbreviated_name') or '').strip()
            vendor_id = vendor.get('id', '')

            if not vennum:
                error = "Vendor skipped: Abbreviated Name is missing. Please set Abbreviated Name in Procore to sync this vendor."
            elif len(vennum) > vennum_max_length:
                error = (
                    f"Vendor skipped: Abbreviated Name '{vennum}' exceeds {vennum_max_length} character limit "
                    f"({len(vennum)} chars). Please update Abbreviated Name in Procore to {vennum_max_length} characters or fewer."
                )
            elif vendor_id in existing_ids:
                error = (
                    f"Vendor skipped: Abbreviated Name '{vennum}' is already in use by another vendor in Procore. "
                    f"Assign a unique Abbreviated Name in Procore to sync this vendor."
                )
            elif vendor_id in intra_run_ids:
                error = (
                    f"Vendor skipped: Abbreviated Name '{vennum}' is duplicated within this sync; "
                    f"only the first vendor with this Abbreviated Name is synced. "
                    f"Assign a unique Abbreviated Name in Procore to sync this vendor."
                )
            else:
                error = None

            return vennum, error

        def validate_vendors():
            fetch_result = rail.result('fetch_vendors')
            procore_vendors = fetch_result['vendors_to_sync'] or []
            existing_ids = fetch_result['existing_duplicate_ids'] or []
            intra_run_ids = fetch_result['intra_run_duplicate_ids'] or []
            valid_vendors = []
            invalid_vendors = []

            for vendor in procore_vendors:
                vendor_id = vendor.get('id', '')
                errors = []
                is_valid = True

                vennum, vennum_error = get_vennum_and_validate(
                    vendor,
                    existing_ids,
                    intra_run_ids
                )
                if vennum_error:
                    errors.append(vennum_error)
                    is_valid = False

                vendor_name = vendor.get('name') or ''
                vendor_detail = {
                    'id': vendor_id,
                    'vennum': vennum,
                    'name': vendor_name[:30],
                    'remit_check_name': vendor_name[:60],
                    **split_address_for_ce(vendor.get('address') or ''),
                    'city': vendor.get('city', ''),
                    'state': vendor.get('state_code', ''),
                    'zip': vendor.get('zip', ''),
                    'phone': ''.join(filter(str.isdigit, vendor.get('business_phone') or '')),
                    'email': vendor.get('email_address', ''),
                    'fax': ''.join(filter(str.isdigit, vendor.get('fax_number') or '')),
                    'web': vendor.get('website', ''),
                    'status': config.vendor_status_active if vendor.get('is_active') is True else config.vendor_status_inactive
                }

                if not vendor_name.strip():
                    errors.append("Missing vendor name - mandatory field")
                    is_valid = False

                for validation in field_validations:
                    field_key = validation['field']
                    field_name = validation['display_name']
                    max_length = validation['max_length']

                    if field_key in vendor_detail:
                        if validation['truncate']:
                            vendor_detail[field_key] = str(vendor_detail[field_key])[:max_length]
                        else:
                            field_value = vendor_detail[field_key]
                            if field_value and len(field_value) > max_length:
                                errors.append(
                                    f"{field_name} exceeds {max_length} character limit: {len(field_value)} characters")
                                is_valid = False

                if is_valid:
                    valid_vendors.append(vendor_detail)
                else:
                    invalid_vendors.append({
                        "vendor_id": vendor_detail['id'],
                        "vendor_name": vendor_detail['name'],
                        "errors": ', '.join(errors)
                    })

            return {
                "valid_vendors": valid_vendors,
                "invalid_vendors": invalid_vendors,
                "total_vendors": len(procore_vendors),
                "valid_count": len(valid_vendors),
                "invalid_count": len(invalid_vendors)
            }

        validate_vendor_data = rail.PythonOperator(
            task_id='validate_vendor_data',
            python_callable=validate_vendors
        )

        if_valid_vendors_present = rail.IfOperator(
            task_id='if_valid_vendors_present',
            test=lambda: rail.result('validate_vendor_data')['valid_count'] > 0,
            yes_task='transform_vendor_data',
            no_task='if_validation_errors_present'
        )

        def transform_valid_vendors_to_ce_format():
            vendors_data = []

            for vendor_detail in rail.result('validate_vendor_data')['valid_vendors']:
                ce_vendor = {
                    "vennum": vendor_detail['vennum'],
                    "name": vendor_detail['name'],
                    "checkname": vendor_detail['remit_check_name'],
                    "address1": vendor_detail['address1'],
                    "city": vendor_detail['city'],
                    "zip": vendor_detail['zip'],
                    "phone": vendor_detail['phone'],
                    "email": vendor_detail['email'],
                    "fax": vendor_detail['fax'],
                    "status": vendor_detail['status'],
                    "web": vendor_detail['web'],
                }

                # Only include address2 if it exists
                if 'address2' in vendor_detail:
                    ce_vendor["address2"] = vendor_detail['address2']

                # Only include state if it's exactly 2 characters, could be 3 for non US Canadian addresses
                if vendor_detail['state'] and len(vendor_detail['state']) == 2:
                    ce_vendor["state"] = vendor_detail['state']

                vendors_data.append(ce_vendor)

            ce_import_data = {
                "type": "vendor",
                "layout": "JSON",
                "begin": "data",
                "data": vendors_data
            }

            return ce_import_data

        transform_vendor_data = rail.PythonOperator(
            task_id='transform_vendor_data',
            python_callable=transform_valid_vendors_to_ce_format
        )

        def create_vendor_zip_file(ce_import_data):
            json_content = json.dumps(ce_import_data, indent=2)

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr('vendors.json', json_content)

            zip_bytes = zip_buffer.getvalue()
            base64_encoded = base64.b64encode(zip_bytes).decode('utf-8')

            vendors = ce_import_data.get('data', [])
            return {
                'base64_data': base64_encoded,
                'vendor_count': len(vendors),
                # Webhook dedup identifier — only meaningful for a single vendor; '' otherwise (bulk).
                'vennum': vendors[0]['vennum'] if len(vendors) == 1 else ''
            }

        create_zip_and_encode = rail.PythonOperator(
            task_id='create_zip_and_encode',
            python_callable=lambda: create_vendor_zip_file(
                rail.result('transform_vendor_data')
            )
        )

        is_bulk_import = rail.IfOperator(
            task_id='is_bulk_import',
            test=lambda: rail.result('detect_mode_and_extract').get('is_bulk', False),
            yes_task='import_bulk',
            no_task='computerease_import_sync.get_import_file_id'
        )

        # Bulk mode: one import file for all vendors, no dedup.
        import_bulk = rail.ComputereaseAPIOperator(
            task_id='import_bulk',
            computerease_conn_id=config.computerease_conn_id,
            endpoint='/import/',
            request_method='POST',
            request_body=lambda: {
                "import_type": "Vendors",
                "description": build_import_file_description("Vendor", f"{rail.result('create_zip_and_encode')['vendor_count']} vendors"),
                "import_data": rail.result('create_zip_and_encode')['base64_data']
            }
        )

        # Webhook mode: dedup per vendor by vennum (a webhook may refire for the same vendor).
        ce_import_entry, ce_import_finish = rail.computerease_import_sync(
            group_id='computerease_import_sync',
            import_type='Vendors',
            description=lambda: build_import_file_description("Vendor", rail.result('create_zip_and_encode')['vennum']),
            import_data=lambda: rail.result('create_zip_and_encode')['base64_data'],
            computerease_conn_id=config.computerease_conn_id
        )

        # After the import: defer -> enqueue for the mark-erp-sync DAG if not already deferred; else set origin_id now.
        if_defer_origin_id = rail.IfOperator(
            task_id='if_defer_origin_id',
            test=lambda: config.defer_origin_id_until_accepted,
            yes_task='is_already_deferred',
            no_task='build_origin_id_payload'
        )

        is_already_deferred = rail.IfOperator(
            task_id='is_already_deferred',
            test=lambda: (not rail.result('detect_mode_and_extract').get('is_bulk', False))
                and rail.result('computerease_import_sync.get_import_file_id'),
            yes_task='if_validation_errors_present',
            no_task='build_pending_rows'
        )

        def get_ce_import_uuid():
            if rail.result('detect_mode_and_extract').get('is_bulk', False):
                response = rail.result('import_bulk') or {}
                return response['data'].get('uuid', '') if response.get('data') else ''

            import_uuid = rail.result('computerease_import_sync.get_import_file_id')
            if import_uuid:
                return import_uuid

            response = rail.result('computerease_import_sync.create_import_file') or {}
            return response['data'].get('uuid', '') if response.get('data') else ''

        def build_pending_rows():
            import_uuid = get_ce_import_uuid()
            fetched = {v.get('id'): v for v in rail.result('fetch_vendors')['vendors_to_sync']}
            queued_at = rail.render_template('{{ current_time() }}')
            rows, seen = [], set()
            for vendor in rail.result('validate_vendor_data')['valid_vendors']:
                vennum = vendor['vennum']
                vendor_id = vendor['id']
                # Deferred path: the enqueued origin_id (patched into Procore later by mark_erp_sync)
                # must be CE's canonical UPPERCASE form. The compare below stays exact on the stored
                # value so a legacy mixed-case link re-queues for re-stamping (self-migration).
                origin_id = f'CE_{normalize_ce_identifier(vennum)}'
                if (fetched.get(vendor_id) or {}).get('origin_id') == origin_id:
                    continue  # already linked — nothing to defer
                if vennum in seen:
                    continue
                seen.add(vennum)
                rows.append({
                    'vennum': vennum,
                    'procore_vendor_id': str(vendor_id),
                    'origin_id': origin_id,
                    'import_uuid': import_uuid,
                    'queued_at': queued_at
                })
            return rows

        build_pending_rows_task = rail.PythonOperator(
            task_id='build_pending_rows',
            python_callable=build_pending_rows
        )

        enqueue_pending = rail.S3UpsertCollectionOperator(
            task_id='enqueue_pending',
            integration=config.s3_collection['integration'],
            customer=config.instance,
            collection_name=config.origin_id_update_table['name'],
            key_columns=config.origin_id_update_table['unique_columns'],
            rows=build_pending_rows_task.output
        )

        if_validation_errors_present = rail.IfOperator(
            task_id='if_validation_errors_present',
            test=lambda: rail.result('validate_vendor_data')[
                'invalid_count'] > 0,
            yes_task='create_csv_for_validation_errors',
            no_task='reset_bulk_date'
        )

        create_csv_for_validation_errors = rail.WriteCSVFileOperator(
            task_id='create_csv_for_validation_errors',
            source='{{ result("validate_vendor_data")["invalid_vendors"] | tojson }}',
            header=['Vendor ID', 'Vendor Name', 'Validation Error', 'Status'],
            row=[
                "{{ item.vendor_id }}",
                "{{ item.vendor_name }}",
                "{{ item.errors }}",
                "Excluded from sync"
            ]
        )

        generate_logs_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_logs_download_link',
            artifact_name='{{ result("create_csv_for_validation_errors") }}',
            output_file_name='ProcoreComputerEase_VendorSyncLogs_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_error_email = rail.EmailOperator(
            task_id='send_error_email',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: Vendor Sync completed with errors - {{ current_time() }}',
            html_content='email_templates/error_mail.html',
        )

        # Clear the date on bulk success (runs once); no-op in webhook mode, skipped on failure.
        def reset_bulk_date_if_bulk():
            if rail.result('detect_mode_and_extract').get('is_bulk', False):
                Variable.set(bulk_sync_trigger_date_var, '')

        reset_bulk_date = rail.PythonOperator(
            task_id='reset_bulk_date',
            python_callable=reset_bulk_date_if_bulk,
            trigger_rule='none_failed_min_one_success'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> detect_mode_and_extract_task >> should_process

        should_process >> rail.Label('No') >> reset_bulk_date
        should_process >> rail.Label('Yes') >> fetch_vendors >> has_vendors_to_sync

        has_vendors_to_sync >> rail.Label('Yes') >> validate_vendor_data >> if_valid_vendors_present
        has_vendors_to_sync >> rail.Label('No') >> reset_bulk_date

        if_valid_vendors_present >> rail.Label('Yes') >> transform_vendor_data >> create_zip_and_encode >> is_bulk_import
        if_valid_vendors_present >> rail.Label('No') >> if_validation_errors_present

        is_bulk_import >> rail.Label('Yes') >> import_bulk >> if_defer_origin_id
        is_bulk_import >> rail.Label('No') >> ce_import_entry
        ce_import_finish >> if_defer_origin_id

        if_defer_origin_id >> rail.Label('No') >> build_origin_id_payload >> has_origin_id_updates
        if_defer_origin_id >> rail.Label('Yes') >> is_already_deferred

        is_already_deferred >> rail.Label('Yes') >> if_validation_errors_present
        is_already_deferred >> rail.Label('No') >> build_pending_rows_task >> enqueue_pending >> if_validation_errors_present

        has_origin_id_updates >> rail.Label('Yes') >> update_originid_in_procore >> if_validation_errors_present
        has_origin_id_updates >> rail.Label('No') >> if_validation_errors_present

        if_validation_errors_present >> rail.Label('No') >> reset_bulk_date
        if_validation_errors_present >> rail.Label('Yes') >> create_csv_for_validation_errors
        create_csv_for_validation_errors >> generate_logs_download_link >> send_error_email >> reset_bulk_date

        reset_bulk_date >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
