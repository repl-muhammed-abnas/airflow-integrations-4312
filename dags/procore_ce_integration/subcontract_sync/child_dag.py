from datetime import timedelta, datetime
import json
import time
import rail
from procore_ce_integration.initial_setup_sync.shared_utils import (
    extract_ce_code,
    normalize_ce_identifier,
    parse_date,
    parse_wbs_flat_code
)



def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.subcontract_child_dag_id,
        description='Procore Subcontract Sync - Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs_child,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_subcontract_details',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def enrich_with_attachment_diff(subcontract_data):
            if not subcontract_data:
                return subcontract_data
            origin_data_str = subcontract_data.get('origin_data', '') or ''
            try:
                origin_data = json.loads(origin_data_str) if origin_data_str else {}
            except (json.JSONDecodeError, TypeError):
                origin_data = {}
            already_synced_ids = set(origin_data.get('synced_attachment_ids', []))
            new_attachments = [
                {'attachment_id': a['id'], 'filename': a['filename'], 'url': a['url']}
                for a in (subcontract_data.get('attachments', []) or [])
                if a.get('id') and a.get('filename') and a.get('url')
                and a['id'] not in already_synced_ids
            ]
            subcontract_data['new_attachments'] = new_attachments
            subcontract_data['already_synced_ids'] = list(already_synced_ids)
            return subcontract_data

        fetch_subcontract_details = rail.ProcoreApiOperator(
            task_id='fetch_subcontract_details',
            endpoint='/work_order_contracts/{{ dag_run.conf.subcontract_id }}',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}',
                'view': 'extended'
            },
            data_handler=lambda response: enrich_with_attachment_diff(
                response if isinstance(response, dict) else (
                    response[0] if isinstance(response, list) and len(response) > 0 else None
                )
            )
        )

        check_if_subcontract_found = rail.IfOperator(
            task_id='check_if_subcontract_found',
            test=lambda: bool(rail.result('fetch_subcontract_details')),
            yes_task='check_subcontract_status',
            no_task='log_subcontract_not_found'
        )

        check_subcontract_status = rail.IfOperator(
            task_id='check_subcontract_status',
            test=lambda: ((rail.result('fetch_subcontract_details') or {}).get('status') or '').lower() in config.syncable_subcontract_statuses,
            yes_task='fetch_wbs_type_from_ce',
            no_task='catch_error'
        )

        def get_wbs_type_from_ce(response):
            if not response.get('data'):
                raise ValueError(
                    "Job not found in ComputerEase — sync job first before syncing subcontracts"
                )
            return response['data'][0].get('wbs_type')

        fetch_wbs_type_from_ce = rail.ComputereaseAPIOperator(
            task_id='fetch_wbs_type_from_ce',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda: {
                'code': normalize_ce_identifier(extract_ce_code(
                    (rail.result('fetch_subcontract_details').get('project') or {}).get('origin_id', '')
                ))
            },
            data_handler=get_wbs_type_from_ce
        )

        if_new_attachments = rail.IfOperator(
            task_id='if_new_attachments',
            test=lambda: bool((rail.result('fetch_subcontract_details') or {}).get('new_attachments')),
            yes_task='trigger_attachment_upload_dags',
            no_task='prepare_ce_payload'
        )

        trigger_attachment_upload_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_attachment_upload_dags',
            items='{{ result("fetch_subcontract_details").new_attachments | to_json }}',
            trigger_dag_id=config.attachment_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'attachment_id': item['attachment_id'],
                'filename': item['filename'],
                'url': item['url']
            }
        )

        wait_for_attachment_uploads = rail.WaitForDagRunsSensor(
            task_id='wait_for_attachment_uploads',
            dag_runs='{{ result("trigger_attachment_upload_dags") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_attachment_uuids = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_attachment_uuids',
            dagrun_task_id='set_dag_result',
            dag_runs='{{ result("trigger_attachment_upload_dags") }}'
        )

        def build_ce_subcontract_payload(dag_run):
            subcontract_data = rail.result('fetch_subcontract_details')

            subcontract_id = str(subcontract_data.get('id', ''))
            subcontract_number = subcontract_data.get('number', '') or ''

            if not subcontract_number:
                raise ValueError(
                    f"Subcontract with id:{subcontract_id} has no number assigned in Procore — cannot sync to CE"
                )

            if len(subcontract_number) > 10:
                raise ValueError(
                    f"Subcontract number '{subcontract_number}' exceeds the 10-character limit for CE code field"
                )

            job_code = normalize_ce_identifier(extract_ce_code(
                (subcontract_data.get('project') or {}).get('origin_id', '')))
            vendor_code = normalize_ce_identifier(extract_ce_code(
                (subcontract_data.get('vendor') or {}).get('origin_id', '')))
            if not job_code:
                raise ValueError(
                    f"Project does not exist in ComputerEase"
                )
            if not vendor_code:
                raise ValueError(
                    f"Vendor does not exist in ComputerEase or not assigned in Procore"
                )
            line_items = subcontract_data.get('line_items', []) or []
            procore_status = subcontract_data.get('status', '')

            origin_id = subcontract_data.get('origin_id', '') or ''
            action = 'update' if origin_id.startswith('CE_') else 'create'

            ce_approval_status = config.approval_status_mapper.get(procore_status)

            ce_data = {
                'code': subcontract_number,
                'job_code': job_code,
                'vendor_code': vendor_code,
                'description': (subcontract_data.get('title') or '')[:config.MAX_CHAR_LEN_DESCRIPTION],
                'contract_date': parse_date(subcontract_data.get('contract_date')),
                'entered_date': parse_date(subcontract_data.get('issued_on_date')) or datetime.today().strftime('%Y-%m-%d'),
                'orig_start_date': parse_date(subcontract_data.get('contract_start_date')),
                'actual_start_date': parse_date(subcontract_data.get('contract_start_date')),
                'orig_finish_date': parse_date(subcontract_data.get('contract_estimated_completion_date')),
                'actual_finish_date': parse_date(subcontract_data.get('actual_completion_date')),
                'retention_percent': float(subcontract_data.get('retainage_percent') or 0),
                'format': config.subcontract_format
            }

            # Remove None and empty string values
            ce_data = {k: v for k, v in ce_data.items() if v is not None and v != ''}

            # Build line items from SOV
            ce_items = []
            cost_type_map = dag_run.conf.get('cost_type_map') or {}
            for line_item in line_items:
                amount = float(line_item.get('amount', 0) or 0)
                if config.skip_zero_amount_line_items and amount == 0:
                    continue
                flat_code = (
                    line_item.get('wbs_code', {}).get('flat_code', '')
                    if line_item.get('wbs_code') else ''
                )
                wbs_type = rail.result('fetch_wbs_type_from_ce')
                phase_code, category_code, cost_type_code = parse_wbs_flat_code(flat_code, line_item.get('cost_code'), wbs_type)

                costtype = cost_type_map.get(cost_type_code) if cost_type_code else None
                if costtype is None:
                    raise ValueError(
                        "Costtype could not be found in ComputerEase for one or more item(s)"
                    )
                cost_type_name = line_item['line_item_type']['name']
                description = (line_item.get('description') or '').strip()

                units = float(line_item.get('quantity', 0) or 0)
                unit_price = float(line_item.get('unit_cost', 0) or 0)
                has_units = units != 0
                has_flat_price = unit_price == 0 and amount != 0

                ce_item = {
                    'item_type': 'item',
                    'record_id': str(line_item.get('id') or ''),
                    'job_code': job_code,
                    'phase_code': phase_code,
                    'category_code': category_code,
                    'description': (description or cost_type_name)[:config.MAX_CHAR_LEN_DESCRIPTION],
                    'has_units': has_units,
                    'has_flat_price': has_flat_price,
                    **({'units': units} if has_units else {}),
                    **({'unit_price': unit_price} if not has_flat_price else {}),
                    'amount': amount,
                    'costtype': costtype
                }

                ce_item = {k: v for k, v in ce_item.items() if v is not None and v != ''}
                ce_items.append(ce_item)

            if ce_items:
                ce_data['items'] = ce_items

            if ce_approval_status == 'approved' or ce_approval_status == 'denied':                
                approved_or_denied_date = parse_date(subcontract_data.get('signed_contract_received_date'))
                if not approved_or_denied_date:
                    approved_or_denied_date = datetime.now().strftime('%Y-%m-%d')
                ce_data['events'] = [{'type': ce_approval_status, 'name': 'INTEGRATION', 'on_date': approved_or_denied_date}]

            attachment_uuids = [
                r.get('uuid') for r in (rail.result('gather_attachment_uuids') or [])
                if r and r.get('uuid')
            ]
            if attachment_uuids:
                ce_data['attachments'] = attachment_uuids

            return {
                'payload': {
                    'import_type': config.ce_import_type,
                    'action': action,
                    'data': ce_data
                },
                'subcontract_id': subcontract_id,
                'subcontract_number': subcontract_number
            }

        prepare_ce_payload = rail.PythonOperator(
            task_id='prepare_ce_payload',
            python_callable=build_ce_subcontract_payload
        )

        import_to_computerease = rail.ComputereaseAPIOperator(
            task_id='import_to_computerease',
            endpoint='/import/automated',
            request_method='POST',
            request_body=lambda: rail.result('prepare_ce_payload')['payload']
        )

        check_if_pending = rail.IfOperator(
            task_id='check_if_pending',
            test=lambda: (rail.result('import_to_computerease') or {}).get('data', {}).get('status') == 'pending',
            yes_task='fetch_import_status',
            no_task='check_final_import_status'
        )

        def poll_import_status():
            uuid = (rail.result('import_to_computerease') or {}).get('data', {}).get('uuid')
            if not uuid:
                raise ValueError("Subcontract processing status unknown. It may or not have been accepted by ComputerEase.")
            timeout = config.import_poll_timeout_minutes * 60
            interval = config.import_poll_interval_seconds
            deadline = time.time() + timeout

            while time.time() < deadline:
                result = rail.ComputereaseAPIOperator(
                    task_id='_poll_import_status',
                    endpoint=f'/import/automated/{uuid}',
                    request_method='GET',
                    computerease_conn_id=config.computerease_conn_id,
                    paginate=False,

                ).execute({})
                status = (result or {}).get('data', {}).get('status')
                print(f"Polled import status: {status.upper() if status else 'None'}")
                error_message = (result or {}).get('data', {}).get('error_message', '') or ''
                if status != 'pending':
                    return result
                time.sleep(interval)

            raise TimeoutError(
                f"Import status still pending after {config.import_poll_timeout_minutes} minute(s) — "
                "it may eventually be accepted or rejected in ComputerEase"
            )

        fetch_import_status = rail.PythonOperator(
            task_id='fetch_import_status',
            python_callable=poll_import_status
        )

        check_final_import_status = rail.IfOperator(
            task_id='check_final_import_status',
            test=lambda: (
                rail.result('fetch_import_status') or
                rail.result('import_to_computerease') or {}
            ).get('data', {}).get('status') == 'processed',
            yes_task='update_origin_id_in_procore',
            no_task='log_import_rejected'
        )

        def get_import_error_message():
            raw = (
                (rail.result('fetch_import_status') or {}).get('data', {}).get('error_message') or
                (rail.result('import_to_computerease') or {}).get('data', {}).get('error_message')
            )
            if 'is locked by another user' in (raw or '').lower():
                return (
                    'This subcontract could not be processed as it is locked by another user. '
                    'Please close the subcontract and it will get processed.'
                )
            return 'Import rejected by ComputerEase - ' + (raw or 'Due to unknown reasons')

        log_import_rejected = rail.WriteLogOperator(
            task_id='log_import_rejected',
            message='na',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'SUBCONTRACT',
                'entity_code': rail.result('prepare_ce_payload')['subcontract_number'],
                'procore_subcontract_id': dag_run.conf['subcontract_id'],
                'procore_project_id': dag_run.conf['project_id'],
                'procore_project_name': (
                    (rail.result('fetch_subcontract_details') or {})
                    .get('project', {}).get('name', '')
                ),
                'error_message': get_import_error_message()
            }
        )

        update_origin_id_in_procore = rail.ProcoreApiOperator(
            task_id='update_origin_id_in_procore',
            endpoint='/work_order_contracts/{{ dag_run.conf.subcontract_id }}',
            method='PATCH',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data=lambda: {
                'work_order_contract': {
                    'origin_id': f"CE_{normalize_ce_identifier(rail.result('prepare_ce_payload')['subcontract_number'])}",
                    'origin_data': json.dumps({
                        'synced_attachment_ids': (
                            (rail.result('fetch_subcontract_details') or {}).get('already_synced_ids', []) +
                            [
                                r['attachment_id']
                                for r in (rail.result('gather_attachment_uuids') or [])
                                if r and r.get('uuid') and r.get('attachment_id')
                            ]
                        )
                    })
                }
            }
        )

        if_attachment_errors = rail.IfOperator(
            task_id='if_attachment_errors',
            test=lambda: any(
                r and not r.get('uuid')
                for r in (rail.result('gather_attachment_uuids') or [])
            ),
            yes_task='log_attachment_errors',
            no_task='catch_error'
        )

        log_attachment_errors = rail.WriteLogOperator(
            task_id='log_attachment_errors',
            message='na',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'SUBCONTRACT',
                'entity_code': (
                    (rail.result('prepare_ce_payload') or {}).get('subcontract_number') or
                    (rail.result('fetch_subcontract_details') or {}).get('number', '')
                ),
                'procore_subcontract_id': dag_run.conf['subcontract_id'],
                'procore_project_id': dag_run.conf['project_id'],
                'procore_project_name': (
                    (rail.result('fetch_subcontract_details') or {})
                    .get('project', {}).get('name', '')
                ),
                'error_message': 'Some attachments could not be uploaded: ' + ', '.join(
                    f"{r['filename']} - {r.get('error', 'unknown error')}"
                    for r in (rail.result('gather_attachment_uuids') or [])
                    if r and not r.get('uuid')
                )
            }
        )

        log_subcontract_not_found = rail.WriteLogOperator(
            task_id='log_subcontract_not_found',
            message='na',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'SUBCONTRACT',
                'entity_code': '',
                'procore_subcontract_id': dag_run.conf['subcontract_id'],
                'procore_project_id': dag_run.conf['project_id'],
                'procore_project_name': '',
                'error_message': f"Subcontract {dag_run.conf['subcontract_id']} not found in Procore"
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'SUBCONTRACT',
                'entity_code': (
                    (rail.result('prepare_ce_payload') or {}).get('subcontract_number') or
                    (rail.result('fetch_subcontract_details') or {}).get('number', '')
                ),
                'procore_subcontract_id': dag_run.conf['subcontract_id'],
                'procore_project_id': dag_run.conf['project_id'],
                'procore_project_name': (
                    (rail.result('fetch_subcontract_details') or {})
                    .get('project', {}).get('name', '')
                ),
                'error_message': 'Subcontract could not be synced - {{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> catch_error
        batch_task >> fetch_subcontract_details >> check_if_subcontract_found

        check_if_subcontract_found >> rail.Label('Yes') >> check_subcontract_status
        check_subcontract_status >> rail.Label('syncable') >> fetch_wbs_type_from_ce >> if_new_attachments
        check_subcontract_status >> rail.Label('not syncable') >> catch_error

        if_new_attachments >> rail.Label('Yes') >> trigger_attachment_upload_dags >> wait_for_attachment_uploads >> gather_attachment_uuids >> prepare_ce_payload
        if_new_attachments >> rail.Label('No') >> prepare_ce_payload

        prepare_ce_payload >> import_to_computerease >> check_if_pending
        check_if_pending >> rail.Label('pending') >> fetch_import_status >> check_final_import_status
        check_if_pending >> rail.Label('processed/rejected') >> check_final_import_status
        check_final_import_status >> rail.Label('processed') >> update_origin_id_in_procore >> if_attachment_errors
        if_attachment_errors >> rail.Label('Yes') >> log_attachment_errors >> catch_error
        if_attachment_errors >> rail.Label('No') >> catch_error
        check_final_import_status >> rail.Label('rejected') >> log_import_rejected >> catch_error

        check_if_subcontract_found >> rail.Label(
            'No') >> log_subcontract_not_found >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
