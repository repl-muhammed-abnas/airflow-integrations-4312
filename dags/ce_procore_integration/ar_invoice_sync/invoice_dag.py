import rail
from datetime import timedelta
from ce_procore_integration.ar_invoice_sync.utils.constants import ProcoreInvoiceStatus
from ce_procore_integration.util_dags.utils import normalize_ce_identifier


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.invoice_dag_id,
        description='Create Owner Invoices in Procore',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.invoice_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_owner_invoice',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        create_owner_invoice = rail.ProcoreApiOperator(
            task_id='create_owner_invoice',
            method='POST',
            endpoint=lambda dag_run: f'/prime_contracts/{dag_run.conf["prime_contract_id"]}/payment_applications',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']}
        )

        fetch_owner_invoice_line_items = rail.ProcoreApiOperator(
            task_id='fetch_owner_invoice_line_items',
            method='GET',
            endpoint='/payment_applications/{{ result("create_owner_invoice").id }}',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']},
            data_handler=lambda response: response[0].get(
                'g703', []) if response and len(response) == 1 else []
        )

        def get_prepared_line_item_updates(dag_run):
            invoice = dag_run.conf['invoice_data']['invoice']
            ce_line_items = invoice.get('line_items', [])
            procore_line_items = rail.result('fetch_owner_invoice_line_items')

            if not ce_line_items or not procore_line_items:
                return {'updates': [], 'exceptions': []}

            # Create lookup by flat_code
            ce_lookup = {}
            exceptions = []

            for ce_item in ce_line_items:
                phase = ce_item.get('phase', '')
                category = ce_item.get('category', '')
                cost_type = config.default_cost_type

                ce_flat_code = ''
                if phase and category:
                    ce_flat_code = f"{phase}-{category}.{cost_type}"
                elif phase or category:
                    ce_flat_code = f"{phase or category}.{cost_type}"

                if ce_flat_code not in ce_lookup:
                    ce_lookup[ce_flat_code] = ce_item.copy()
                else:
                    # Sum amounts if multiple items with same flat_code
                    ce_lookup[ce_flat_code]['billed_amount'] += ce_item.get(
                        'billed_amount', 0)

            updates = []
            matched_ce_codes = set()

            for pc_item in procore_line_items:
                wbs_code = pc_item.get('wbs_code', {})
                procore_flat_code = normalize_ce_identifier(wbs_code.get(
                    'flat_code', '')) if wbs_code else ''

                line_item_id = pc_item.get('id')

                if procore_flat_code in ce_lookup and procore_flat_code not in matched_ce_codes:

                    matched_ce_item = ce_lookup[procore_flat_code]
                    matched_ce_codes.add(procore_flat_code)

                    current_work = float(
                        matched_ce_item.get('billed_amount', 0))

                    updates.append({
                        'line_item_id': line_item_id,
                        'work_completed_this_period': current_work
                    })

            if len(updates) == 0:
                for ce_flat_code, ce_item in ce_lookup.items():
                    exceptions.append({
                        'ce_flat_code': ce_flat_code,
                        'budget_code': ce_item.get('budget_code', ''),
                        'billed_amount': ce_item.get('billed_amount', 0),
                        'reason': f"No matching Procore SOV line item found with flat_code '{ce_flat_code}'"
                    })

            return {'updates': updates, 'exceptions': exceptions}

        prepare_line_item_updates = rail.PythonOperator(
            task_id='prepare_line_item_updates',
            python_callable=get_prepared_line_item_updates
        )

        has_line_item_exceptions = rail.IfOperator(
            task_id='has_line_item_exceptions',
            test='{{ result("prepare_line_item_updates").exceptions | length > 0 }}',
            yes_task='write_line_item_exceptions',
            no_task='check_has_line_items'
        )

        write_line_item_exceptions = rail.WriteLogOperator(
            task_id='write_line_item_exceptions',
            message='Line Item Matching Exception',
            severity='Error/Exception',
            properties=lambda item: item,
            items=lambda dag_run: [
                {
                    'code': dag_run.conf['invoice_data']['invoice'].get('invoice_number', 'unknown'),
                    'job_code': dag_run.conf['invoice_data']['invoice'].get('job_code', 'unknown'),
                    'customer_code': dag_run.conf['invoice_data']['invoice'].get('customer_code', 'unknown'),
                    'invoice_number': dag_run.conf['invoice_data']['invoice'].get('invoice_number', 'unknown'),
                    'company_id': dag_run.conf.get('company_id', 'unknown'),
                    'status': 'Exception',
                    'reason': item.get('reason', '') + (f" (wbs_code: {item['ce_flat_code']})" if item.get('ce_flat_code') else '')
                } for item in rail.result('prepare_line_item_updates').get('exceptions', [])
            ]
        )

        check_has_line_items = rail.IfOperator(
            task_id='check_has_line_items',
            test='{{ result("prepare_line_item_updates").updates | length > 0 }}',
            yes_task='update_line_items_bulk',
            no_task='delete_draft_invoice'
        )

        delete_draft_invoice = rail.ProcoreApiOperator(
            task_id='delete_draft_invoice',
            method='DELETE',
            endpoint='/payment_applications/{{ result("create_owner_invoice").id }}',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']}
        )

        def build_bulk_line_items_payload(dag_run):
            """Build payload for bulk updating all line items in payment application"""
            updates = rail.result(
                'prepare_line_item_updates').get('updates', [])

            items = []
            for update in updates:
                items.append({
                    'id': update['line_item_id'],
                    'type': 'payment_application_line_item',
                    'description_override': None,
                    'new_materials': 0,
                    'stored_materials': 0,
                    'materials_stored_retainage_percent_this_period': 0,
                    'materials_stored_retainage_released_this_period': 0,
                    'materials_stored_retainage_retained_this_period': 0,
                    'work_completed_this_period': update['work_completed_this_period'],
                    'work_completed_retainage_percent_this_period': 0,
                    'work_completed_retainage_released_this_period': 0,
                    'work_completed_retainage_retained_this_period': 0
                })

            return {
                'project_id': dag_run.conf['project_id'],
                'payment_application': {
                    'description_type': 'custom',
                    'status': ProcoreInvoiceStatus.DRAFT,
                    'tax_summary': [],
                    'items': items
                }
            }

        update_line_items_bulk = rail.ProcoreApiOperator(
            task_id='update_line_items_bulk',
            method='PATCH',
            endpoint='/payment_applications/{{ result("create_owner_invoice").id }}',
            version='1.1',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'prime_contract_id': dag_run.conf['prime_contract_id']
            },
            data=build_bulk_line_items_payload
        )

        def get_payment_application_id():
            return rail.result('create_owner_invoice')['id']

        def build_owner_invoice_payload(dag_run):
            payload = dag_run.conf['invoice_data']
            return {
                'project_id': dag_run.conf['project_id'],
                'payment_application': {
                    'invoice_number': payload['invoice']['invoice_number'],
                    'billing_date': payload['invoice']['invoice_date'],
                    'period_start': payload['period_start'],
                    'period_end': payload['period_end'],
                    'status': ProcoreInvoiceStatus.APPROVED,
                    'include_attachments': False
                }
            }
        update_owner_invoice = rail.ProcoreApiOperator(
            task_id='update_owner_invoice',
            method='PATCH',
            endpoint=lambda dag_run: f'/prime_contracts/{dag_run.conf["prime_contract_id"]}/payment_applications/{get_payment_application_id()}',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']},
            data=build_owner_invoice_payload
        )

        def get_error_details(dag_run):
            invoice_data = dag_run.conf.get('invoice_data', {})
            invoice = invoice_data.get('invoice', {})

            err = rail.render_template('{{ get_error_message() }}')
            if isinstance(err, str):
                status = 'Error'
                reason = err
            else:
                status = err.get('response', {}).get('status_code', 'Error')
                reason = err.get('response', {}).get('json', {}).get(
                    'error', {}).get('reason', str(err))

            return {
                'code': invoice.get('invoice_number', 'unknown'),
                'job_code': invoice.get('job_code', 'unknown'),
                'customer_code': invoice.get('customer_code', 'unknown'),
                'company_id': dag_run.conf.get('company_id', 'unknown'),
                'status': status,
                'reason': reason
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> create_owner_invoice >> fetch_owner_invoice_line_items >> prepare_line_item_updates

        # Handle exceptions and line item updates
        prepare_line_item_updates >> has_line_item_exceptions
        has_line_item_exceptions >> rail.Label(
            'Yes') >> write_line_item_exceptions >> check_has_line_items
        has_line_item_exceptions >> rail.Label('No') >> check_has_line_items

        # If we have line items, update them and set invoice to APPROVED
        # If no line items, delete the draft invoice
        check_has_line_items >> rail.Label(
            'Yes') >> update_line_items_bulk >> update_owner_invoice >> catch_error
        check_has_line_items >> rail.Label(
            'No') >> delete_draft_invoice >> catch_error

        batch_task >> catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
