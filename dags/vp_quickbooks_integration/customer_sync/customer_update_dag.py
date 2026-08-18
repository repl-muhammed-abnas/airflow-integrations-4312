"""
Customer Update DAG for QBO -> VP Customer Sync.

Updates an existing firm in Vantagepoint from a QBO Customer record.
Triggered by the router DAG when the firm-map HAS an existing entry for
this QBOID; the router forwards the resolved FirmID as
`dag_run.conf['vp_client_id']`.

Update-path scope:
  POST /contact   (upsert contact details that may have changed in QBO)

NOT called on this path:
  - POST /firm/{ClientID}/address — VP rejects with `Record already exists`
    because the create DAG already created the billing/shipping rows. A
    real address-change flow would need GET addresses + PUT, mirroring
    the customer_sync_upsert recovery branch. Deferred.
  - POST /dvp/accounting/{ClientID} — vendor-side metadata; customers
    aren't vendors. Removed from create DAG for the same reason.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.customer_sync.utils.python_callable_method import (  # noqa: E501
    build_contact_body,
    capture_customer_dag_error,
)


def create_dag(config):
    """Per-record DAG: upsert contact in VP for an existing firm."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_customer_sync_update_{config.instance}',
        description='Update existing customer (firm) in Vantagepoint from QuickBooks',
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=[
            'vantagepoint_quickbooks', 'customer_sync', 'update_customer'
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            ),
        }
    ) as dag:

        upsert_contact = rail.VantagepointContactOperator(
            task_id='upsert_contact',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='POST',
            request_body=build_contact_body,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        catch_customer_dag_error = rail.PythonOperator(
            task_id='catch_customer_dag_error',
            trigger_rule='all_done',
            python_callable=capture_customer_dag_error,
            op_args=[
                '{{ dag_run.conf.QBOID }}',
                '{{ dag_run.conf.DisplayName }}',
                '{{ get_error_message() }}'
            ]
        )

        upsert_contact >> catch_customer_dag_error

        return dag


rail.for_each_instance(create_dag)
