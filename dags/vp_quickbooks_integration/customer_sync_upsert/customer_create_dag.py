"""
Customer Create DAG for VP -> QBO Customer Upsert.
Creates a new Customer in QuickBooks Online from a Vantagepoint Firm record.

Mirrors the Workato create branch of `014-503 PSA Vantagepoint Customer to
QuickBooks`:
  GET /firm/{ClientID}                          (full firm record)
  GET /firm/{ClientID}/address                  (billing + shipping addrs)
  GET /contact?filterHash[ClientID]=...         (primary contact)
  search QBO by CompanyName                     (recovery check)
    if found  -> capture existing Id, skip create  (duplicate prevention)
    else      -> POST /customer (create)
  Insert/update row into firm map
  PUT /firm/{ClientID}                          (write QBOID back)
"""
from datetime import timedelta
import rail
from vp_quickbooks_integration.customer_sync_upsert.utils.python_callable_method import (  # noqa: E501
    build_create_customer_body,
    build_qbo_search_by_company_name_query,
    capture_existing_qbo_customer_id_from_recovery,
    capture_qbo_customer_id_from_create,
    qbo_customer_already_exists,
    add_customer_to_firm_map,
    build_firm_contacts_filter,
    build_vp_firm_writeback_body,
    capture_create_error,
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """Create DAG for creating new customers in QuickBooks from VP firms."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_customer_upsert_create_{config.instance}',
        description='Create QuickBooks customer from Vantagepoint firm',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'customer_upsert', 'create_customer'],
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

        # Recovery search: pre-compute the SQL in Python (QBO query field
        # is a Jinja template_field, not a callable — see comment in
        # customer_update_dag.py).
        build_qbo_search_query = rail.PythonOperator(
            task_id='build_qbo_search_query',
            python_callable=build_qbo_search_by_company_name_query
        )

        search_qbo_customer_by_name = rail.QuickBooksCustomerOperator(
            task_id='search_qbo_customer_by_name',
            intuit_conn_id=(
                "{{ dag_run.conf.connections.intuit }}"
            ),
            operation='search',
            query="{{ result('build_qbo_search_query') }}"
        )

        check_qbo_already_exists = rail.IfOperator(
            task_id='check_qbo_already_exists',
            test=qbo_customer_already_exists,
            yes_task='capture_existing_qbo_customer_id',
            no_task='create_customer_in_qbo'
        )

        # Recovery branch: QBO already has a customer with this CompanyName.
        # Just capture its Id (no POST needed). Then carry on to update
        # the firm map + write back to VP, exactly like the create path.
        capture_existing_qbo_customer_id = rail.PythonOperator(
            task_id='capture_existing_qbo_customer_id',
            python_callable=capture_existing_qbo_customer_id_from_recovery
        )

        # Create branch: no existing QBO customer, do the POST.
        create_customer_in_qbo = rail.QuickBooksCustomerOperator(
            task_id='create_customer_in_qbo',
            intuit_conn_id=(
                "{{ dag_run.conf.connections.intuit }}"
            ),
            operation='create',
            request_body=build_create_customer_body
        )

        capture_qbo_customer_id = rail.PythonOperator(
            task_id='capture_qbo_customer_id',
            python_callable=capture_qbo_customer_id_from_create
        )

        # Converge: whichever branch ran, write the firm-map row.
        # trigger_rule='none_failed_min_one_success' fires when one of the
        # two preceding capture tasks succeeded and the other was
        # skipped (or both succeeded).
        update_firm_map_lookup = rail.PythonOperator(
            task_id='update_firm_map_lookup',
            trigger_rule='none_failed_min_one_success',
            python_callable=add_customer_to_firm_map
        )

        # Write the QBO Customer Id back onto the VP firm record.
        writeback_qboid_to_vp = rail.VantagepointFirmOperator(
            task_id='writeback_qboid_to_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='PUT',
            client_id="{{ dag_run.conf.ClientID }}",
            request_body=build_vp_firm_writeback_body
        )

        catch_customer_dag_error = rail.PythonOperator(
            task_id='catch_customer_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_create_error,
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
            build_qbo_search_query >>
            search_qbo_customer_by_name >>
            check_qbo_already_exists
        )
        (
            check_qbo_already_exists >>
            rail.Label('Already in QBO — recover Id') >>
            capture_existing_qbo_customer_id >>
            update_firm_map_lookup
        )
        (
            check_qbo_already_exists >>
            rail.Label('Not in QBO — create') >>
            create_customer_in_qbo >>
            capture_qbo_customer_id >>
            update_firm_map_lookup
        )
        (
            update_firm_map_lookup >>
            writeback_qboid_to_vp >>
            catch_customer_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
