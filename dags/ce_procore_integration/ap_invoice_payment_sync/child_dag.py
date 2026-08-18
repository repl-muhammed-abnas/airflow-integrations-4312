import rail
from datetime import timedelta
from ce_procore_integration.ap_invoice_payment_sync.utils.util import (
    get_error_message,
    build_unique_key
)


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='AP Invoice Payment Sync Child',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_purchase_orders',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        fetch_purchase_orders = rail.ProcoreApiOperator(
            task_id='fetch_purchase_orders',
            endpoint='/purchase_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'filter[status]': 'Approved',
                'project_id': dag_run.conf['batch']['project_id'],
            },
            data_handler=lambda response: {
                x['origin_id']: x['id'] for x in response if x.get('origin_id')
            } if response else {}
        )

        fetch_subcontracts = rail.ProcoreApiOperator(
            task_id='fetch_subcontracts',
            endpoint='/work_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'filter[status]': 'Approved',
                'project_id': dag_run.conf['batch']['project_id'],
            },
            data_handler=lambda response: {
                x['origin_id']: x['id'] for x in response if x.get('origin_id')
            } if response else {}
        )

        def build_payment_payloads(dag_run):
            def create_payload(payment):
                origin_data = f"CE_{payment['check_number']}_{payment['voucher_number']}"
                notes = f"Discount offered: {payment['discount']}" if float(payment['discount']) > 0 else ''
                return {
                    'notes': notes,
                    'date': payment['date'],
                    'payment_method': 'check',
                    'origin_data': origin_data,
                    'amount': float(payment['paid']),
                    'check_number': payment['check_number'],
                    'payment_number': payment['voucher_number'],
                    'invoice_number': payment['invoice_number']
                }
            batch = dag_run.conf['batch']
            commitments = {**rail.result('fetch_purchase_orders'), **rail.result('fetch_subcontracts')}

            valid = []
            invalid = []

            for operation in ['new', 'changed', 'removed']:
                for payment in batch.get(operation, []):
                    origin_data = f"CE_{payment['check_number']}_{payment['voucher_number']}"
                    commitment_id = commitments.get(f"CE_{payment['po_number']}")
                    if not commitment_id:
                        invalid.append({
                            'commitment': payment['voucher_number'],
                            'check_number': payment['check_number'],
                            'voucher_number': payment['voucher_number'],
                            'reason': f"Commitment {payment['po_number']} not found/approved/synced in Procore"
                        })
                        continue

                    if operation == 'removed':
                        valid.append({
                            'contract_id': commitment_id,
                            'operation': config.DELETE,
                            'payload': {
                                'origin_data': origin_data,
                                'check_number': payment['check_number'],
                                'payment_number': payment['voucher_number']
                            },
                        })

                    else:
                        valid.append({
                            'contract_id': commitment_id,
                            'payload': create_payload(payment),
                            'operation': config.CREATE if operation == 'new' else config.UPDATE,
                        })
            return {
                'valid': valid,
                'invalid': invalid
            }

        build_payloads = rail.PythonOperator(
            task_id='build_payloads',
            python_callable=build_payment_payloads
        )

        has_invalid_payments = rail.IfOperator(
            task_id='has_invalid_payments',
            test=lambda: len(rail.result('build_payloads').get('invalid', [])) > 0,
            yes_task='write_invalid_payment_exceptions',
            no_task='check_has_valid_payments'
        )

        write_invalid_payment_exceptions = rail.WriteLogOperator(
            task_id='write_invalid_payment_exceptions',
            message='Payment Exception',
            severity='Error/Exception',
            properties=lambda item: item,
            items=lambda dag_run: [
                {
                    'code': item.get('commitment', 'unknown'),
                    'job_code': dag_run.conf['batch']['job_number'],
                    'company_id': dag_run.conf['company_id'],
                    'unique_key': build_unique_key(
                        dag_run.conf['company_id'],
                        item['check_number'],
                        item['voucher_number']
                    ),
                    'status': 'Exception',
                    'reason': item.get('reason', 'Unknown error')
                } for item in rail.result('build_payloads')['invalid']
            ]
        )

        check_has_valid_payments = rail.IfOperator(
            task_id='check_has_valid_payments',
            test='{{ result("build_payloads").valid | length > 0 }}',
            yes_task='trigger_all_payments',
            no_task='collect_payment_failures'
        )

        trigger_all_payments = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_all_payments',
            items=lambda: rail.result('build_payloads')['valid'],
            trigger_dag_id=config.payment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'operation': item['operation'],
                'contract_id': item['contract_id'],
                'contract_payment': item['payload'],
                'project_id': dag_run.conf['batch']['project_id'],
                'job_number': dag_run.conf['batch']['job_number'],
                'company_id': dag_run.conf['company_id']
            }
        )

        wait_for_all_payments = rail.WaitForDagRunsSensor(
            task_id='wait_for_all_payments',
            dag_runs='{{ result("trigger_all_payments") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_failed_payment_sync = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_failed_payment_sync',
            dagrun_task_id='check_payment_error',
            dag_runs='{{ result("trigger_all_payments") }}'
        )


        def get_child_failures(dag_run):
            failed_keys = []
            company_id = dag_run.conf['company_id']

            # Scenario 1: Check for invalid payments (no commitment found)
            invalid_payments = rail.result('build_payloads').get('invalid', [])
            for payment in invalid_payments:
                check_number = payment['check_number']
                voucher_number = payment['voucher_number']
                unique_key = build_unique_key(company_id, check_number, voucher_number)
                failed_keys.append(unique_key)

            # Scenario 2: Check for individual payment sync failures
            gather_result = rail.result('gather_failed_payment_sync')
            if gather_result:
                for unique_key in gather_result:
                    if unique_key is not None:
                        failed_keys.append(unique_key)
            return failed_keys

        collect_payment_failures = rail.PythonOperator(
            task_id='collect_payment_failures',
            python_callable=get_child_failures
        )


        def get_error_details(dag_run):
            """Extract standardized error details for logging."""
            company_id = dag_run.conf.get('company_id', 'unknown')
            batch = dag_run.conf.get('batch', {})
            job_number = batch.get('job_number', 'unknown')

            # Extract payment identifiers from batch
            new_payments = batch.get('new', [])
            changed_payments = batch.get('changed', [])
            removed_payments = batch.get('removed', [])

            all_payment_ids = []
            for payment_list in [new_payments, changed_payments, removed_payments]:
                for payment in payment_list:
                    voucher = payment.get('voucher_number', '')
                    if voucher:
                        all_payment_ids.append(voucher)

            payment_codes = ','.join(all_payment_ids[:5])  # Limit to first 5
            if len(all_payment_ids) > 5:
                payment_codes += f' (+{len(all_payment_ids) - 5} more)'

            err = rail.render_template('{{ get_error_message() }}')
            error = get_error_message(err)

            return {
                'code': payment_codes or 'unknown',
                'job_code': job_number,
                'company_id': company_id,
                'status': error['status'],
                'reason': error['reason']
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

        def construct_child_dag_failures(dag_run):
            task_state = rail.render_template('{{ get_task_state("gather_failed_payment_sync") }}')
            if task_state == "success":
                return []
            failed_keys = []
            batch_conf = dag_run.conf['batch']
            company_id = dag_run.conf['company_id']

            # Mark ALL payments in this batch as failed
            for operation in ['new', 'changed', 'removed']:
                for payment in batch_conf.get(operation, []):
                    check_number = payment['check_number']
                    voucher_number = payment['voucher_number']
                    unique_key = build_unique_key(company_id, check_number, voucher_number)
                    failed_keys.append(unique_key)

            return failed_keys

        collect_child_failures = rail.PythonOperator(
            task_id='collect_child_failures',
            trigger_rule='all_done',
            python_callable=construct_child_dag_failures
        )

        batch_task >> catch_error >> collect_child_failures
        batch_task >> fetch_purchase_orders >> fetch_subcontracts >> build_payloads >> has_invalid_payments

        has_invalid_payments >> rail.Label('Yes') >> write_invalid_payment_exceptions >> check_has_valid_payments
        has_invalid_payments >> rail.Label('No') >> check_has_valid_payments

        check_has_valid_payments >> rail.Label('Yes') >> trigger_all_payments >> wait_for_all_payments >> gather_failed_payment_sync >> collect_payment_failures
        check_has_valid_payments >> rail.Label('No') >> collect_payment_failures >> catch_error

        return dag


rail.for_each_instance(create_dag_instance)
