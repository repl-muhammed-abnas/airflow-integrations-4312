from datetime import datetime, timedelta
import base64
import io
import zipfile
import rail
import json
from procore_ce_integration.subcontract_change_order_sync.utils.xml_generator import generate_rfc_xml
from procore_ce_integration.subcontract_change_order_sync.utils.constants import RESOURCE_CHANGE_ORDER_PACKAGE, SkipReason, SyncStatus
from procore_ce_integration.initial_setup_sync.shared_utils import (
    build_import_file_description,
    extract_ce_code,
    normalize_ce_identifier,
    parse_date,
    parse_wbs_flat_code
)


# config:
# https://github.com/replicon/airflow-integrations/blob/main/dags/procore_ce_integration/change_order_sync/config.py


# pylint: disable=too-many-statements
def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.cop_child_dag_id,
        description='Procore to ComputerEase Subcontract Change Order Sync - COP Child DAG',
        max_active_runs=config.max_active_runs_cop_child,
        integration_type='generic',
        company_key=config.instance,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_co',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_co = rail.ProcoreApiOperator(
            task_id='get_co',
            endpoint="/change_order_packages/{{ dag_run.conf.cop_id }}",
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'id', dag_run.conf['cop_id'], default={})
        )

        is_pcco = rail.IfOperator(
            task_id='is_pcco',
            test=lambda: bool(rail.result('get_co')['type'] == "PrimeContractChangeOrder" and config.sync_prime_contract_change_order == True),
            yes_task='transform_to_rfc',
            no_task='check_commitment_cco_sync'
        )

        check_commitment_cco_sync = rail.IfOperator(
            task_id='check_commitment_cco_sync',
            test=lambda: bool(config.sync_commitment_contract_change_order == True),
            yes_task='fetch_subcontract_details',
            no_task='catch_error'
        )

        fetch_subcontract_details = rail.ProcoreApiOperator(
            task_id='fetch_subcontract_details',
            endpoint=lambda: (f'/work_order_contracts/{rail.result("get_co", {}).get("contract_id")}'),
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}',
                'view': 'extended'
            },
            data_handler=lambda response: (
                response if isinstance(response, dict) else (
                    response[0] if isinstance(response, list) and len(response) > 0 else None
                )
            )
        )

        def get_attachments_data_to_sync(change_order_data):
            origin_data_str = change_order_data.get('origin_data', '') or ''
            try:
                origin_data = json.loads(origin_data_str) if origin_data_str else {}
            except (json.JSONDecodeError, TypeError):
                origin_data = {}
            already_synced_commitment_cco_ids = set(origin_data.get('synced_commitment_cco_ids', []))
            new_attachments = [
                {'attachment_id': a['id'], 'filename': a['filename'], 'url': a['url']}
                for a in (change_order_data.get('attachments', []) or [])
                if a.get('id') and a.get('filename') and a.get('url')
                and a['id'] not in already_synced_commitment_cco_ids
            ]
            attachments = {
                'already_synced_commitment_cco_ids': list(already_synced_commitment_cco_ids),
                'new_attachments': new_attachments
            }
            return attachments
        
        get_attachments_to_sync = rail.PythonOperator(
            task_id='get_attachments_to_sync',
            python_callable=lambda: get_attachments_data_to_sync(rail.result('get_co'))
        )

        if_new_attachments = rail.IfOperator(
            task_id='if_new_attachments',
            test=lambda: bool(len(rail.result('get_attachments_to_sync', {}).get('new_attachments', [])) > 0),
            yes_task='trigger_attachment_upload_dags',
            no_task='prepare_ce_payload'
        )

        trigger_attachment_upload_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_attachment_upload_dags',
            items='{{ result("get_attachments_to_sync").new_attachments | to_json }}',
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

        def getCCOStatus(status):
            status_to_return = 'pending'
            if status.lower() == 'approved':
                status_to_return = "approved"
            elif status.lower() == 'rejected':
                status_to_return = "denied"
            return status_to_return

        def build_ce_subcontract_co_payload(dag_run):
            subcontract_data = rail.result('fetch_subcontract_details')
            change_order_package_data = rail.result('get_co')
            origin_id = change_order_package_data.get('origin_id', '')
            subcontract_code = subcontract_data.get('number', '') or ''
            job_code = normalize_ce_identifier(extract_ce_code(
                (subcontract_data.get('project') or {}).get('origin_id', '')))
            vendor_code = normalize_ce_identifier(extract_ce_code(
                (subcontract_data.get('vendor') or {}).get('origin_id', '')))
            line_items = change_order_package_data.get('line_items', []) or []
            ce_status = getCCOStatus(change_order_package_data.get('status', ''))
            action = 'update' if origin_id.startswith('CE_') else 'create'

            ce_data = {
                'code': subcontract_code,
                'rfc_code': change_order_package_data.get('number',''),
                'job_code': job_code,
                'vendor_code': vendor_code,
                'description': (change_order_package_data.get('title') or '')[:config.MAX_CHAR_LEN_DESCRIPTION],
                'entered_date': parse_date(change_order_package_data.get('invoiced_date')) or datetime.today().strftime('%Y-%m-%d')
            }

            if ce_status == "approved":
                ce_data['change_order_code'] = change_order_package_data.get('number','')


            if config.subcontract_format:
                ce_data['format'] = config.subcontract_format

            # Remove None and empty string values
            ce_data = {k: v for k, v in ce_data.items() if v is not None and v != ''}

            # Build line items from SOV
            ce_items = []
            cost_type_map = dag_run.conf['cost_type_map']
            for line_item in line_items:
                amount = float(line_item.get('amount', 0) or 0)
                flat_code = (
                    line_item.get('wbs_code', {}).get('flat_code', '')
                    if line_item.get('wbs_code') else ''
                )

                cost_type_name = line_item['line_item_type']['name']
                description = (line_item.get('description') or '').strip()

                phase_code, category_code, cost_type_code = parse_wbs_flat_code(
                    flat_code,
                    line_item.get('cost_code'),
                    dag_run.conf['wbs_type']
                )
                costtype = cost_type_map[cost_type_code] if cost_type_code in cost_type_map else None

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

                ce_item = {k: v for k, v in ce_item.items() if v != ''}
                ce_items.append(ce_item)
            
            if ce_status == "approved" or ce_status == "denied":
                approved_or_denied_date = parse_date(change_order_package_data.get('signed_change_order_received_date'))
                if not approved_or_denied_date:
                    approved_or_denied_date = datetime.now().strftime('%Y-%m-%d')
            
                approver_name = change_order_package_data.get('creator').get('name')

                events = [
                    {
                        "type": ce_status,
                        "name": approver_name,
                        "on_date": approved_or_denied_date
                    }
                ]
                ce_data["events"] = events
            
            attachment_uuids = [
                r.get('uuid') for r in (rail.result('gather_attachment_uuids') or [])
                if r and r.get('uuid')
            ]
            if attachment_uuids:
                ce_data['attachments'] = attachment_uuids

            if ce_items:
                ce_data['items'] = ce_items

            return {
                'import_type': 'subcontract',
                'action': action,
                'data': ce_data
            }

        prepare_ce_payload = rail.PythonOperator(
            task_id='prepare_ce_payload',
            python_callable=build_ce_subcontract_co_payload
        )

        import_to_computerease = rail.ComputereaseAPIOperator(
            task_id='import_to_computerease',
            endpoint='/import/automated',
            request_method='POST',
            request_body=lambda: rail.result('prepare_ce_payload')
        )

        check_for_attachments = rail.IfOperator(
            task_id='check_for_attachments',
            test=lambda: bool(rail.result('get_co')['type'] == "CommitmentContractChangeOrder" and config.sync_commitment_contract_change_order == True and len(rail.result('get_attachments_to_sync').get('new_attachments', [])) > 0),
            yes_task='update_origin_data_in_procore',
            no_task='update_origin_id'
        )

        update_origin_data_in_procore = rail.ProcoreApiOperator(
            task_id='update_origin_data_in_procore',
            endpoint="/change_order_packages/{{ dag_run.conf.cop_id }}",
            method='PATCH',
            data=lambda dag_run: {
                'change_order': {
                    "origin_id": f"CE_COP_{normalize_ce_identifier(dag_run.conf['job_code'])}_{dag_run.conf['cop_id']}",
                    'origin_data': json.dumps({
                        'synced_commitment_cco_ids': (
                            (rail.result('get_attachments_to_sync') or {}).get('already_synced_commitment_cco_ids', [])
                             +
                            [
                                r['attachment_id']
                                for r in (rail.result('gather_attachment_uuids') or [])
                                if r and r.get('uuid') and r.get('attachment_id')
                            ]
                        )
                    })
                },
                "project_id": dag_run.conf['project_id'],
                "contract_id": rail.result('get_co')['contract_id']
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
                'cop_id': '{{ dag_run.conf.cop_id }}',
                'project_id': '{{ dag_run.conf.project_id }}',
                'job_code': '{{ dag_run.conf.job_code }}',
                'reason': 'Unknown Error',                
                'message': 'Some Subcontract Change Order attachments could not be uploaded: ' + ', '.join(
                    f"{r['filename']} - {r.get('error', 'unknown error')}"
                    for r in (rail.result('gather_attachment_uuids') or [])
                    if r and not r.get('uuid')
                )
            }
        )

        def transform_to_rfc_xml():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            cop_data = rail.result('get_co')
            line_items = cop_data.get('line_items', [])
            job_code = dag_run_conf['job_code']
            cost_type_map = dag_run_conf['cost_type_map']
            wbs_type = dag_run_conf['wbs_type']

            try:
                xml_content = generate_rfc_xml(
                    cop_data, line_items, job_code, wbs_type, config, cost_type_map)
                return {
                    'success': True,
                    'xml_content': xml_content,
                    'cop_id': cop_data['id']
                }
            except Exception as e:  # pylint: disable=broad-except
                return {
                    'success': False,
                    'error': str(e),
                    'cop_id': cop_data['id']
                }

        transform_to_rfc = rail.PythonOperator(
            task_id='transform_to_rfc',
            python_callable=transform_to_rfc_xml
        )

        is_cost_type_not_present = rail.IfOperator(
            task_id='is_cost_type_not_present',
            test="{{ result('transform_to_rfc', 'invalid_cost_types') | length > 0 }}",
            yes_task='log_cost_type_exception',
            no_task='is_xml_generated'
        )

        log_cost_type_exception = rail.WriteLogOperator(
            task_id='log_cost_type_exception',
            message='Cost Type(s) not present in ComputerEase',
            severity='Error/Exception',
            properties={
                'cop_id': "{{ result('transform_to_rfc').cop_id }}",
                'project_id': '{{ dag_run.conf.project_id }}',
                'job_code': '{{ dag_run.conf.job_code }}',
                'reason': SkipReason.COST_TYPE_NOT_PRESENT,
                'message': "Line items could not be synced due to missing cost \
                    types in CE: {{ result('transform_to_rfc', 'invalid_cost_types') | smartjoin_by_delim(', ') }}",
                'sync_status': SyncStatus.SKIPPED
            }
        )

        is_xml_generated = rail.IfOperator(
            task_id='is_xml_generated',
            test="{{ result('transform_to_rfc').success }}",
            yes_task='zip_and_encode_xml',
            no_task='log_transform_error'
        )

        log_transform_error = rail.WriteLogOperator(
            task_id='log_transform_error',
            message='Failed to generate RFC XML',
            severity='Error/Exception',
            properties={
                'cop_id': "{{ result('transform_to_rfc').cop_id }}",
                'project_id': '{{ dag_run.conf.project_id }}',
                'job_code': '{{ dag_run.conf.job_code }}',
                'reason': SkipReason.XML_TRANSFORM_FAILURE,
                'message': "{{ result('transform_to_rfc').error }}",
                'sync_status': SyncStatus.ERROR
            }
        )

        def zip_and_encode(cop_id):

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr(f'rfc_{cop_id}.xml', rail.result(
                    'transform_to_rfc')['xml_content'])

            zip_bytes = zip_buffer.getvalue()
            base64_encoded = base64.b64encode(zip_bytes).decode('utf-8')

            description = build_import_file_description(RESOURCE_CHANGE_ORDER_PACKAGE, cop_id)

            return {
                'import_data': base64_encoded,
                'description': description,
                'cop_id': cop_id
            }

        zip_and_encode_xml = rail.PythonOperator(
            task_id='zip_and_encode_xml',
            python_callable=zip_and_encode,
            op_args=["{{ result('transform_to_rfc').cop_id }}"]
        )

        send_to_computerease, import_sync_finish = rail.computerease_import_sync(
            group_id='send_to_computerease',
            import_type='Job Costing RFC',
            description=lambda: rail.result('zip_and_encode_xml')['description'],
            import_data=lambda: rail.result('zip_and_encode_xml')['import_data']
        )

        update_origin_id = rail.ProcoreApiOperator(
            task_id='update_origin_id',
            endpoint="/change_order_packages/{{ dag_run.conf.cop_id }}",
            method='PATCH',
            data=lambda dag_run: {
                "change_order": {
                    "origin_id": f"CE_COP_{normalize_ce_identifier(dag_run.conf['job_code'])}_{dag_run.conf['cop_id']}"
                },
                "project_id": dag_run.conf['project_id'],
                "contract_id": rail.result('get_co')['contract_id']
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties={
                'cop_id': '{{ dag_run.conf.cop_id }}',
                'project_id': '{{ dag_run.conf.project_id }}',
                'job_code': '{{ dag_run.conf.job_code }}',
                'reason': 'Unknown Error',
                'message': '{{ get_error_message() }}',
                'sync_status': SyncStatus.ERROR
            }
        )

        batch_task >> get_co >> is_pcco
        is_pcco >> rail.Label(
            'Yes') >> transform_to_rfc >> is_cost_type_not_present
        is_pcco >> rail.Label(
            'No') >> check_commitment_cco_sync
        
        check_commitment_cco_sync >> rail.Label(
            'Yes') >> fetch_subcontract_details >> get_attachments_to_sync >> if_new_attachments
        check_commitment_cco_sync >> rail.Label(
            'No') >> catch_error
        
        if_new_attachments >> rail.Label(
            'Yes') >> trigger_attachment_upload_dags >> wait_for_attachment_uploads >> gather_attachment_uuids >> prepare_ce_payload        
        if_new_attachments >> rail.Label(
            'No') >> prepare_ce_payload
        
        prepare_ce_payload >> import_to_computerease >> check_for_attachments

        check_for_attachments >> rail.Label(
            'Yes') >> update_origin_data_in_procore >> if_attachment_errors
        check_for_attachments >> rail.Label(
            'No') >> update_origin_id >> catch_error
        
        if_attachment_errors >> rail.Label(
            'Yes') >> log_attachment_errors >> catch_error
        if_attachment_errors >> rail.Label(
            'No') >> catch_error

        is_cost_type_not_present >> rail.Label(
            'Yes') >> log_cost_type_exception >> is_xml_generated
        is_cost_type_not_present >> rail.Label(
            'No') >> is_xml_generated

        is_xml_generated >> rail.Label(
            'Yes') >> zip_and_encode_xml >> send_to_computerease
        import_sync_finish >> update_origin_id >> catch_error

        is_xml_generated >> rail.Label(
            'No') >> log_transform_error >> catch_error

        batch_task >> catch_error

    return dag


rail.for_each_instance(create_dag_instance)
