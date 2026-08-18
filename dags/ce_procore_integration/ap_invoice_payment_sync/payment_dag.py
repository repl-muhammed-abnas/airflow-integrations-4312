import rail
from datetime import timedelta

from ce_procore_integration.ap_invoice_payment_sync.utils.util import (
    get_error_message,
    build_unique_key
)


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.payment_dag_id,
        description='Handles individual payment create/update/delete operations',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.payment_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_existing_payment',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        fetch_existing_payment = rail.ProcoreApiOperator(
            task_id='fetch_existing_payment',
            endpoint='/contract_payments',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'contract_id': dag_run.conf['contract_id']
            },
            data_handler=lambda response, dag_run: next(
                (x for x in response if x.get('origin_data') == dag_run.conf['contract_payment']['origin_data']),
                None
            ) if response else None
        )

        determine_sync_required = rail.IfOperator(
            task_id='determine_sync_required',
            test=lambda dag_run: dag_run.conf['operation'] != config.DELETE,
            yes_task='fetch_invoice_id',
            no_task='delete_payment'
        )

        def get_invoice_id(response, invoice_number, commitment_id):
            if response:
                for invoice in response:
                    if invoice['origin_id'] == f'CE_{invoice_number}':
                        return invoice['id']

            raise ValueError(
                f"Invoice {invoice_number} not found in Procore for commitment {commitment_id}"
            )

        fetch_invoice_id = rail.ProcoreApiOperator(
            task_id='fetch_invoice_id',
            endpoint='/requisitions',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'filters[commitment_id]': dag_run.conf['contract_id']
            },
            data_handler=lambda response, dag_run: get_invoice_id(
                response,
                dag_run.conf['contract_payment']['invoice_number'],
                dag_run.conf['contract_id']
            )
        )


        determine_create_or_update = rail.IfOperator(
            task_id='determine_create_or_update',
            test=lambda: rail.result('fetch_existing_payment') is None,
            yes_task='create_payment',
            no_task='update_payment'
        )

        create_payment = rail.ProcoreApiOperator(
            task_id='create_payment',
            endpoint='/contract_payments',
            method='POST',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'contract_id': dag_run.conf['contract_id'],
                'contract_payment': {
                    **dag_run.conf['contract_payment'],
                    'requisition_id': rail.result('fetch_invoice_id')
                }
            }
        )

        update_payment = rail.ProcoreApiOperator(
            task_id='update_payment',
            endpoint=lambda: f'/contract_payments/{rail.result("fetch_existing_payment")["id"]}',
            method='PATCH',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'contract_id': dag_run.conf['contract_id'],
                'contract_payment': {
                    **dag_run.conf['contract_payment'],
                    'requisition_id': rail.result('fetch_invoice_id')
                }
            }
        )


        def validate_and_get_delete_endpoint(existing_payment):
            """Validate payment exists and return delete endpoint."""
            if not existing_payment:
                raise ValueError(
                    f"Payment already deleted in Procore"
                )
            return f'/contract_payments/{existing_payment["id"]}'

        delete_payment = rail.ProcoreApiOperator(
            task_id='delete_payment',
            method='DELETE',
            endpoint=lambda: validate_and_get_delete_endpoint(
                rail.result('fetch_existing_payment')
            ),
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'contract_id': dag_run.conf['contract_id']
            }
        )

        def get_error_details(dag_run):
            """Extract standardized error details for logging."""
            company_id = dag_run.conf.get('company_id', 'unknown')
            job_number = dag_run.conf.get('job_number', 'unknown')
            contract_payment = dag_run.conf.get('contract_payment', {})
            origin_data = contract_payment.get('origin_data', 'unknown')

            err = rail.render_template('{{ get_error_message() }}')
            error = get_error_message(err)

            return {
                'code': f"Payment key - {origin_data}",
                'job_code': job_number,
                'company_id': company_id,
                'unique_key': build_unique_key(
                    company_id,
                    contract_payment.get('check_number', ''),
                    contract_payment.get('payment_number', '')
                ),
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

        def construct_failed_payment_unique_key(dag_run):
            """Return unique_key for failed payment, None for success."""
            if rail.result('catch_error'):
                error = rail.render_template('{{ get_error_message() }}')
                error_reason = get_error_message(error)['reason']
                if 'Payment already deleted in Procore' in error_reason:
                    return None
                company_id = dag_run.conf['company_id']
                contract_payment = dag_run.conf['contract_payment']
                check_number = contract_payment['check_number']
                voucher_number = contract_payment['payment_number']
                return build_unique_key(company_id, check_number, voucher_number)
            return None
        check_payment_error = rail.PythonOperator(
            task_id='check_payment_error',
            trigger_rule='all_done',
            python_callable=construct_failed_payment_unique_key
        )

        batch_task >> catch_error >> check_payment_error
        batch_task >> fetch_existing_payment >> determine_sync_required

        determine_sync_required >> rail.Label('No') >> delete_payment >> catch_error
        determine_sync_required >> rail.Label('Yes') >> fetch_invoice_id >> determine_create_or_update

        determine_create_or_update >> rail.Label('Yes') >> create_payment >> catch_error
        determine_create_or_update >> rail.Label('No') >> update_payment >> catch_error


        return dag


rail.for_each_instance(create_dag_instance)
