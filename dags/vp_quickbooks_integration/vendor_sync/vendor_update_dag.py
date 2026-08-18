"""
Vendor Update DAG for VP QBO Vendor Sync.
Updates an existing firm in Vantagepoint from a QBO Vendor record.

Mirrors the Workato update branch (steps 12-22 of customer_vendor_to_vp):
  IF vp_client_id (FirmID) is present (from firm map row):
      PUT /firm/{ClientID}
  ELSE  (firm map row stale — FirmID empty):
      POST /firm  (recipe step 19)
      capture new ClientID
      patch firm map row with the new FirmID  (recipe step 21 — update_entry)
      GET firm to fetch the autonumbered Vendor code

  Both paths converge → resolve_firm_id_for_update
  GET /firm/{ClientID}/address                           (list existing addrs)
  -> if Billing address exists  -> PUT /firm/{ClientID} with CLAddress array
  -> else                       -> POST /firm/{ClientID}/address
  GET /api/firm/{ClientID}/vendorAccountingInfo          (existing VEAcc?)
  -> if any record exists       -> PUT /firm/{ClientID} with VEAccounting array
  -> else                       -> POST /vision/firm/VendorAccountingInfo/
"""
import logging
from datetime import timedelta
import rail
from vp_quickbooks_integration.vendor_sync.utils.python_callable_method import (
    has_vp_client_id_in_conf,
    build_update_firm_body,
    build_create_firm_body,
    capture_client_id_from_fallback,
    add_firm_to_firm_map_fallback,
    resolve_firm_id_for_update,
    find_billing_address_id,
    check_billing_address_exists,
    build_create_firm_address_body,
    build_update_firm_address_body,
    has_first_and_last_name,
    has_suffix_input,
    is_suffix_in_codetable,
    build_create_suffix_body,
    build_firm_contacts_filter,
    find_matching_contact_id,
    check_matching_contact_exists,
    build_create_contact_body,
    build_update_contact_body,
    check_veaccounting_exists,
    build_create_veaccounting_body,
    build_update_veaccounting_body,
    capture_update_error
)

logger = logging.getLogger(__name__)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """
    Create DAG for updating existing vendors (firms) in Vantagepoint.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_vendor_sync_update_{config.instance}',
        description='Update vendor (firm) in Vantagepoint from QuickBooks',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'vendor_sync', 'update_vendor'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        # --- Firm op (recipe steps 13-21): branch PUT vs POST-fallback ---

        has_vp_client_id = rail.IfOperator(
            task_id='has_vp_client_id',
            test=has_vp_client_id_in_conf,
            yes_task='update_firm_in_vp',
            no_task='create_firm_in_vp_fallback'
        )

        update_firm_in_vp = rail.VantagepointFirmOperator(
            task_id='update_firm_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='PUT',
            client_id="{{ dag_run.conf.vp_client_id }}",
            request_body=build_update_firm_body
        )

        # Fallback path: firm map row exists for this QBOID but FirmID is
        # empty (stale row). Recipe steps 17-21: POST firm, capture new
        # ClientID, then update_entry on the lookup row.
        create_firm_in_vp_fallback = rail.VantagepointFirmOperator(
            task_id='create_firm_in_vp_fallback',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='POST',
            request_body=lambda: build_create_firm_body(config.instance)
        )

        capture_new_firm_id = rail.PythonOperator(
            task_id='capture_new_firm_id',
            python_callable=capture_client_id_from_fallback
        )

        update_firm_map_lookup_fallback = rail.PythonOperator(
            task_id='update_firm_map_lookup_fallback',
            python_callable=add_firm_to_firm_map_fallback
        )

        get_firm_after_create_fallback = rail.VantagepointFirmOperator(
            task_id='get_firm_after_create_fallback',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='GET',
            client_id="{{ result('capture_new_firm_id') }}",
            pagination=False
        )

        # --- Converge: resolve the FirmID to use downstream ---

        resolve_firm_id = rail.PythonOperator(
            task_id='resolve_firm_id_for_update',
            trigger_rule='none_failed',
            python_callable=resolve_firm_id_for_update
        )

        # --- Address upsert (recipe: GET-then-PUT-or-POST) ---

        get_firm_addresses = rail.VantagepointFirmAddressOperator(
            task_id='get_firm_addresses',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='GET',
            client_id="{{ result('resolve_firm_id_for_update') }}",
            pagination=False
        )

        find_billing_address = rail.PythonOperator(
            task_id='find_billing_address_id',
            python_callable=find_billing_address_id
        )

        is_billing_address_exists = rail.IfOperator(
            task_id='is_billing_address_exists',
            test=check_billing_address_exists,
            yes_task='update_firm_address_in_vp',
            no_task='create_firm_address_in_vp'
        )

        update_firm_address_in_vp = rail.VantagepointFirmOperator(
            task_id='update_firm_address_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='PUT',
            client_id="{{ result('resolve_firm_id_for_update') }}",
            request_body=build_update_firm_address_body
        )

        create_firm_address_in_vp = rail.VantagepointFirmAddressOperator(
            task_id='create_firm_address_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='POST',
            client_id="{{ result('resolve_firm_id_for_update') }}",
            request_body=build_create_firm_address_body
        )

        # --- Contact upsert (recipe steps 10-14) ---

        has_contact_name = rail.IfOperator(
            task_id='has_first_and_last_name',
            test=has_first_and_last_name,
            yes_task='has_suffix_input',
            no_task='log_no_contact',
            trigger_rule='none_failed'
        )

        def log_no_contact_present():
            qbo_id = (
                rail.get_current_context()['dag_run'].conf.get('Id')
            )
            logger.info(
                "No first/last name present for QBO vendor %s "
                "— skipping contact upsert", qbo_id
            )

        log_no_contact = rail.PythonOperator(
            task_id='log_no_contact',
            python_callable=log_no_contact_present
        )

        check_has_suffix = rail.IfOperator(
            task_id='has_suffix_input',
            test=has_suffix_input,
            yes_task='get_suffix_codes_from_vp',
            no_task='get_firm_contacts'
        )

        get_suffix_codes_from_vp = rail.VantagepointSettingsListOperator(
            task_id='get_suffix_codes_from_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            endpoint='/codeTable/CFGSuffix',
            request_method='GET'
        )

        check_suffix_in_codetable = rail.IfOperator(
            task_id='is_suffix_in_codetable',
            test=is_suffix_in_codetable,
            yes_task='get_firm_contacts',
            no_task='create_suffix_in_vp'
        )

        create_suffix_in_vp = rail.VantagepointSettingsListOperator(
            task_id='create_suffix_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            endpoint='/codeTable/CFGSuffix',
            request_method='POST',
            request_body=build_create_suffix_body
        )

        get_firm_contacts = rail.VantagepointContactOperator(
            task_id='get_firm_contacts',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='GET',
            filters=build_firm_contacts_filter,
            pagination=False,
            trigger_rule='none_failed'
        )

        find_matching_contact = rail.PythonOperator(
            task_id='find_matching_contact_id',
            python_callable=find_matching_contact_id
        )

        is_matching_contact_exists = rail.IfOperator(
            task_id='is_matching_contact_exists',
            test=check_matching_contact_exists,
            yes_task='update_firm_contact_in_vp',
            no_task='create_firm_contact_in_vp'
        )

        update_firm_contact_in_vp = rail.VantagepointContactOperator(
            task_id='update_firm_contact_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='PUT',
            contact_id="{{ result('find_matching_contact_id') }}",
            request_body=build_update_contact_body
        )

        create_firm_contact_in_vp = rail.VantagepointContactOperator(
            task_id='create_firm_contact_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='POST',
            request_body=build_create_contact_body
        )

        # --- VEAccounting upsert (recipe: GET-then-PUT-or-POST) ---

        get_firm_veaccounting = rail.VantagepointAPIOperator(
            task_id='get_firm_veaccounting',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            endpoint=(
                "/firm/{{ result('resolve_firm_id_for_update') }}"
                "/vendorAccountingInfo"
            ),
            request_method='GET',
            pagination=False,
            trigger_rule='none_failed'
        )

        is_veaccounting_exists = rail.IfOperator(
            task_id='is_veaccounting_exists',
            test=check_veaccounting_exists,
            yes_task='update_vendor_accounting_in_vp',
            no_task='create_vendor_accounting_in_vp'
        )

        update_vendor_accounting_in_vp = rail.VantagepointFirmOperator(
            task_id='update_vendor_accounting_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='PUT',
            client_id="{{ result('resolve_firm_id_for_update') }}",
            request_body=build_update_veaccounting_body
        )

        # Vision endpoints bypass /api. Use VantagepointCustomOperator
        # (base_path='') so URL is {host}/vision/firm/VendorAccountingInfo
        # rather than {host}/api/vision/firm/VendorAccountingInfo.
        create_vendor_accounting_in_vp = rail.VantagepointCustomOperator(
            task_id='create_vendor_accounting_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            endpoint='/vision/firm/VendorAccountingInfo',
            request_method='POST',
            pagination=False,
            request_body=build_create_veaccounting_body
        )

        catch_vendor_dag_error = rail.PythonOperator(
            task_id='catch_vendor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_update_error,
            op_args=[
                '{{ dag_run.conf.Id }}',
                (
                    "{{ dag_run.conf.get('CompanyName') or "
                    "dag_run.conf.get('DisplayName') or '' }}"
                ),
                '{{ get_error_message() }}'
            ]
        )

        # --- Wiring ---

        # Firm op: PUT vs POST-fallback
        (
            has_vp_client_id >>
            rail.Label('FirmID present in lookup') >>
            update_firm_in_vp
        )
        (
            has_vp_client_id >>
            rail.Label('FirmID missing — create fallback') >>
            create_firm_in_vp_fallback >>
            capture_new_firm_id >>
            update_firm_map_lookup_fallback >>
            get_firm_after_create_fallback
        )

        # Converge both paths at resolve_firm_id
        update_firm_in_vp >> resolve_firm_id
        get_firm_after_create_fallback >> resolve_firm_id

        # Address upsert
        (
            resolve_firm_id >>
            get_firm_addresses >>
            find_billing_address >>
            is_billing_address_exists
        )
        (
            is_billing_address_exists >>
            rail.Label('Billing address exists') >>
            update_firm_address_in_vp
        )
        (
            is_billing_address_exists >>
            rail.Label('Billing address not found') >>
            create_firm_address_in_vp
        )

        # Address branches converge at has_contact_name (was get_firm_veaccounting)
        update_firm_address_in_vp >> has_contact_name
        create_firm_address_in_vp >> has_contact_name

        # Contact upsert flow
        (
            has_contact_name >>
            rail.Label('Contact name present') >>
            check_has_suffix
        )
        (
            has_contact_name >>
            rail.Label('No contact name') >>
            log_no_contact
        )

        (
            check_has_suffix >>
            rail.Label('Suffix supplied') >>
            get_suffix_codes_from_vp >>
            check_suffix_in_codetable
        )
        (
            check_has_suffix >>
            rail.Label('No Suffix') >>
            get_firm_contacts
        )

        (
            check_suffix_in_codetable >>
            rail.Label('Suffix already in CFGSuffix') >>
            get_firm_contacts
        )
        (
            check_suffix_in_codetable >>
            rail.Label('Suffix missing — add it') >>
            create_suffix_in_vp >>
            get_firm_contacts
        )

        (
            get_firm_contacts >>
            find_matching_contact >>
            is_matching_contact_exists
        )
        (
            is_matching_contact_exists >>
            rail.Label('Contact match found') >>
            update_firm_contact_in_vp
        )
        (
            is_matching_contact_exists >>
            rail.Label('No contact match') >>
            create_firm_contact_in_vp
        )

        # All contact paths converge at get_firm_veaccounting
        update_firm_contact_in_vp >> get_firm_veaccounting
        create_firm_contact_in_vp >> get_firm_veaccounting
        log_no_contact >> get_firm_veaccounting

        # VEAccounting upsert
        get_firm_veaccounting >> is_veaccounting_exists
        (
            is_veaccounting_exists >>
            rail.Label('VEAccounting exists') >>
            update_vendor_accounting_in_vp
        )
        (
            is_veaccounting_exists >>
            rail.Label('VEAccounting not found') >>
            create_vendor_accounting_in_vp
        )

        update_vendor_accounting_in_vp >> catch_vendor_dag_error
        create_vendor_accounting_in_vp >> catch_vendor_dag_error

        return dag


rail.for_each_instance(create_dag)
