"""
Customer Create DAG for QBO -> VP Customer Sync.

Creates a new firm in Vantagepoint from a QBO Customer record.
Triggered by the router DAG when the firm-map has NO existing entry for
this QBOID.

Steps:
  POST /firm                          (create firm; capture ClientID)
  Insert row into firm map            (Variable upsert; pooled)
  POST /firm/{ClientID}/address       (BILLING address)
  POST /firm/{ClientID}/address       (SHIPPING address)
  POST /contact                       (contact, linked via CLAddress)
  POST /dvp/accounting/{ClientID}     (VEAccounting)

Mirrors `vp_quickbooks_integration/vendor_sync/vendor_create_dag.py`.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.customer_sync.utils.python_callable_method import (  # noqa: E501
    upsert_firm_mapping_method,
    build_create_firm_body,
    build_billing_address_body,
    build_shipping_address_body,
    build_contact_body,
    capture_customer_dag_error,
)


def create_dag(config):
    """Per-record DAG: create firm + addresses + contact + accounting in VP."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_customer_sync_create_{config.instance}',
        description='Create customer (firm) in Vantagepoint from QuickBooks',
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=[
            'vantagepoint_quickbooks', 'customer_sync', 'create_customer'
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            ),
        }
    ) as dag:

        create_firm_in_vp = rail.VantagepointFirmOperator(
            task_id='create_firm_in_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='POST',
            request_body=build_create_firm_body,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # The firm-mapping Variable is a JSON blob; RMW is inherently racy.
        # max_active_runs=1 on this DAG serializes within an instance, and
        # the Airflow Pool 'psa_vp_qbo_firm_map_writer' (slots=1) is a
        # belt-and-suspenders against concurrent writers across DAGs/runs.
        # Pre-create the Pool in Admin -> Pools.
        update_firm_map_lookup = rail.PythonOperator(
            task_id='update_firm_map_lookup',
            pool='psa_vp_qbo_firm_map_writer',
            python_callable=lambda: (
                upsert_firm_mapping_method(config.instance)
            )
        )

        upsert_billing_address = rail.VantagepointFirmAddressOperator(
            task_id='upsert_billing_address',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='POST',
            client_id="{{ result('create_firm_in_vp')[0].ClientID }}",
            request_body=build_billing_address_body,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # trigger_rule='none_failed': run when billing succeeded OR was
        # skipped (no Billing block on the QBO Customer). Same idea for
        # contact below — a skipped address should not cascade-skip the
        # rest of the chain.
        upsert_shipping_address = rail.VantagepointFirmAddressOperator(
            task_id='upsert_shipping_address',
            trigger_rule='none_failed',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='POST',
            client_id="{{ result('create_firm_in_vp')[0].ClientID }}",
            request_body=build_shipping_address_body,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        upsert_contact = rail.VantagepointContactOperator(
            task_id='upsert_contact',
            trigger_rule='none_failed',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='POST',
            request_body=build_contact_body,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # VEAccounting intentionally NOT called here: that's vendor-side
        # accounting metadata (PayTerms/1099/etc.), and a QBO Customer is
        # not a VP Vendor. The vendor_sync direction handles VEAccounting;
        # customer_sync ends at the contact write.

        # trigger_rule='all_done': capture both hard failures (operator
        # raises -> upstream 'failed') AND clean runs (returns None,
        # gets filtered upstream).
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

        (
            create_firm_in_vp >> update_firm_map_lookup >>
            upsert_billing_address >> upsert_shipping_address >>
            upsert_contact
        )

        create_firm_in_vp >> catch_customer_dag_error
        update_firm_map_lookup >> catch_customer_dag_error
        upsert_billing_address >> catch_customer_dag_error
        upsert_shipping_address >> catch_customer_dag_error
        upsert_contact >> catch_customer_dag_error

        return dag


rail.for_each_instance(create_dag)
