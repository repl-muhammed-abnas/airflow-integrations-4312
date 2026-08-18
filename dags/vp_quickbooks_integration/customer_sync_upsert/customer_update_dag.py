"""
Customer Update DAG for VP -> QBO Customer Upsert.
Updates an existing Customer in QuickBooks Online from a Vantagepoint Firm
record. Falls back to create if the mapped QBO customer no longer exists
(stale firm map row).

Mirrors the Workato update branch of `014-503 PSA Vantagepoint Customer to
QuickBooks`:
  GET /firm/{ClientID}
  GET /firm/{ClientID}/address
  GET /contact?filterHash[ClientID]=...
  search Customer in QBO by mapped Id (need SyncToken for PUT)
    if found  -> POST /customer (update with Id+SyncToken)
    else      -> POST /customer (create — fallback) + patch firm map
  Refresh firm map row
"""
from datetime import timedelta
import rail
from vp_quickbooks_integration.customer_sync_upsert.utils.python_callable_method import (  # noqa: E501
    build_update_customer_search_query,
    build_create_customer_body,
    build_update_customer_body,
    has_existing_qbo_customer,
    capture_qbo_customer_id_from_create_fallback,
    add_customer_to_firm_map_fallback,
    refresh_firm_map_row,
    build_firm_contacts_filter,
    capture_update_error,
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """Update DAG for syncing existing VP firm changes to QBO customers."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_customer_upsert_update_{config.instance}',
        description='Update QuickBooks customer from Vantagepoint firm',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'customer_upsert', 'update_customer'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        get_firm_from_vp = rail.VantagepointFirmOperator(
            task_id='get_firm_from_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='GET',
            client_id="{{ dag_run.conf.ClientID }}",
            pagination=False
        )

        get_firm_addresses_from_vp = rail.VantagepointFirmAddressOperator(
            task_id='get_firm_addresses_from_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='GET',
            client_id="{{ dag_run.conf.ClientID }}"
        )

        get_firm_contact_from_vp = rail.VantagepointContactOperator(
            task_id='get_firm_contact_from_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='GET',
            filters=build_firm_contacts_filter
        )

        # Pre-compute the search SQL in Python so we can validate the
        # qbo_customer_id format (defends against SQL injection via
        # tampered firm-map rows) before the value reaches the QBO
        # operator. QuickBooksCustomerOperator.query is a Jinja
        # template_field (NOT a callable like VantagepointFirmOperator's
        # `filters`), so passing a Python function here would crash with
        # `function += str` in the operator's URL construction. Read the
        # SQL back via Jinja {{ result(...) }}.
        build_search_query = rail.PythonOperator(
            task_id='build_search_query',
            python_callable=build_update_customer_search_query
        )

        search_existing_qbo_customer = rail.QuickBooksCustomerOperator(
            task_id='search_existing_qbo_customer',
            intuit_conn_id=(
                "{{ dag_run.conf.connections.intuit }}"
            ),
            operation='search',
            query="{{ result('build_search_query') }}"
        )

        check_qbo_customer_found = rail.IfOperator(
            task_id='check_qbo_customer_found',
            test=has_existing_qbo_customer,
            yes_task='update_customer_in_qbo',
            no_task='create_customer_in_qbo_fallback'
        )

        update_customer_in_qbo = rail.QuickBooksCustomerOperator(
            task_id='update_customer_in_qbo',
            intuit_conn_id=(
                "{{ dag_run.conf.connections.intuit }}"
            ),
            operation='update',
            request_body=build_update_customer_body
        )

        refresh_firm_map = rail.PythonOperator(
            task_id='refresh_firm_map',
            python_callable=refresh_firm_map_row
        )

        # Fallback path: firm map row was stale. POST as create; patch map.
        create_customer_in_qbo_fallback = rail.QuickBooksCustomerOperator(
            task_id='create_customer_in_qbo_fallback',
            intuit_conn_id=(
                "{{ dag_run.conf.connections.intuit }}"
            ),
            operation='create',
            request_body=build_create_customer_body
        )

        capture_fallback_customer_id = rail.PythonOperator(
            task_id='capture_fallback_customer_id',
            python_callable=capture_qbo_customer_id_from_create_fallback
        )

        patch_firm_map_fallback = rail.PythonOperator(
            task_id='patch_firm_map_fallback',
            python_callable=add_customer_to_firm_map_fallback
        )

        catch_customer_dag_error = rail.PythonOperator(
            task_id='catch_customer_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_update_error,
            op_args=[
                '{{ dag_run.conf.ClientID }}',
                "{{ dag_run.conf.get('Name') or '' }}",
                '{{ get_error_message() }}'
            ]
        )

        (
            get_firm_from_vp >>
            get_firm_addresses_from_vp >>
            get_firm_contact_from_vp >>
            build_search_query >>
            search_existing_qbo_customer >>
            check_qbo_customer_found
        )

        (
            check_qbo_customer_found >>
            rail.Label('QBO customer found') >>
            update_customer_in_qbo >>
            refresh_firm_map >>
            catch_customer_dag_error
        )

        (
            check_qbo_customer_found >>
            rail.Label('QBO customer missing — fallback create') >>
            create_customer_in_qbo_fallback >>
            capture_fallback_customer_id >>
            patch_firm_map_fallback >>
            catch_customer_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
