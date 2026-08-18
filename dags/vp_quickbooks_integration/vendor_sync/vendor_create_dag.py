"""
Vendor Create DAG for VP QBO Vendor Sync.
Creates a new firm in Vantagepoint from a QBO Vendor record.

Mirrors the Workato create branch:
  POST /firm                                  (create firm)
  Insert row into firm map (lookup table)
  POST /firm/{ClientID}/address               (create BILLING address)
  POST /vision/firm/VendorAccountingInfo/     (create VEAccounting record)
"""
import logging
from datetime import timedelta
import rail
from vp_quickbooks_integration.vendor_sync.utils.python_callable_method import (
    build_create_firm_body,
    capture_client_id_from_create,
    add_firm_to_firm_map,
    has_any_billing_address,
    build_create_firm_address_body,
    has_first_and_last_name,
    has_suffix_input,
    is_suffix_in_codetable,
    build_create_suffix_body,
    build_create_contact_body,
    build_create_veaccounting_body,
    has_pay_terms_input,
    capture_create_error
)

logger = logging.getLogger(__name__)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """
    Create DAG for creating new vendors (firms) in Vantagepoint.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_vendor_sync_create_{config.instance}',
        description='Create vendor (firm) in Vantagepoint from QuickBooks',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'vendor_sync', 'create_vendor'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        create_firm_in_vp = rail.VantagepointFirmOperator(
            task_id='create_firm_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='POST',
            request_body=lambda: build_create_firm_body(config.instance)
        )

        capture_client_id = rail.PythonOperator(
            task_id='capture_client_id',
            python_callable=capture_client_id_from_create
        )

        update_firm_map_lookup = rail.PythonOperator(
            task_id='update_firm_map_lookup',
            python_callable=add_firm_to_firm_map
        )

        # Fetch the firm back from VP to read the actual stored Vendor code.
        # We don't assume firm.Vendor == QBOID — we read what VP has on
        # the firm, in case any tenant workflow rewrites it after create.
        get_firm_after_create = rail.VantagepointFirmOperator(
            task_id='get_firm_after_create',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='GET',
            client_id="{{ result('capture_client_id') }}",
            pagination=False
        )

        has_billing_address = rail.IfOperator(
            task_id='has_billing_address',
            test=has_any_billing_address,
            yes_task='create_firm_address_in_vp',
            no_task='log_no_billing_address'
        )

        def log_no_billing_address_present():
            qbo_id = (
                rail.get_current_context()['dag_run'].conf.get('Id')
            )
            logger.info(
                "No billing address fields present for QBO vendor %s "
                "— skipping firm address POST", qbo_id
            )

        log_no_billing_address = rail.PythonOperator(
            task_id='log_no_billing_address',
            python_callable=log_no_billing_address_present
        )

        create_firm_address_in_vp = rail.VantagepointFirmAddressOperator(
            task_id='create_firm_address_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='POST',
            client_id="{{ result('capture_client_id') }}",
            request_body=build_create_firm_address_body
        )

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
                "— skipping contact POST", qbo_id
            )

        log_no_contact = rail.PythonOperator(
            task_id='log_no_contact',
            python_callable=log_no_contact_present
        )

        # --- CFGSuffix lookup-and-add (recipe steps 4-9) ---

        check_has_suffix = rail.IfOperator(
            task_id='has_suffix_input',
            test=has_suffix_input,
            yes_task='get_suffix_codes_from_vp',
            no_task='create_firm_contact_in_vp'
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
            yes_task='create_firm_contact_in_vp',
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

        create_firm_contact_in_vp = rail.VantagepointContactOperator(
            task_id='create_firm_contact_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='POST',
            request_body=build_create_contact_body,
            trigger_rule='none_failed'
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
            trigger_rule='none_failed',
            request_body=build_create_veaccounting_body
        )

        has_pay_terms = rail.IfOperator(
            task_id='has_pay_terms_input',
            test=has_pay_terms_input,
            yes_task='finalize_ready_for_processing',
            no_task='log_no_pay_terms'
        )

        finalize_ready_for_processing = rail.VantagepointFirmOperator(
            task_id='finalize_ready_for_processing',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='PUT',
            client_id="{{ result('capture_client_id') }}",
            request_body={'ReadyForProcessing': 'Y'}
        )

        def log_no_pay_terms_present():
            qbo_id = (
                rail.get_current_context()['dag_run'].conf.get('Id')
            )
            logger.info(
                "No PayTerms in QBO TermRef for vendor %s — skipping "
                "ReadyForProcessing finalization (firm stays as 'N' until "
                "payment terms are configured)", qbo_id
            )

        log_no_pay_terms = rail.PythonOperator(
            task_id='log_no_pay_terms',
            python_callable=log_no_pay_terms_present
        )

        catch_vendor_dag_error = rail.PythonOperator(
            task_id='catch_vendor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_create_error,
            op_args=[
                '{{ dag_run.conf.Id }}',
                (
                    "{{ dag_run.conf.get('CompanyName') or "
                    "dag_run.conf.get('DisplayName') or '' }}"
                ),
                '{{ get_error_message() }}'
            ]
        )

        (
            create_firm_in_vp >>
            capture_client_id >>
            update_firm_map_lookup >>
            get_firm_after_create >>
            has_billing_address
        )

        (
            has_billing_address >>
            rail.Label('Billing address present') >>
            create_firm_address_in_vp >>
            has_contact_name
        )
        (
            has_billing_address >>
            rail.Label('No billing address') >>
            log_no_billing_address >>
            has_contact_name
        )

        (
            has_contact_name >>
            rail.Label('Contact name present') >>
            check_has_suffix
        )
        (
            has_contact_name >>
            rail.Label('No contact name') >>
            log_no_contact >>
            create_vendor_accounting_in_vp
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
            create_firm_contact_in_vp
        )

        (
            check_suffix_in_codetable >>
            rail.Label('Suffix already in CFGSuffix') >>
            create_firm_contact_in_vp
        )
        (
            check_suffix_in_codetable >>
            rail.Label('Suffix missing — add it') >>
            create_suffix_in_vp >>
            create_firm_contact_in_vp
        )

        create_firm_contact_in_vp >> create_vendor_accounting_in_vp

        create_vendor_accounting_in_vp >> has_pay_terms

        (
            has_pay_terms >>
            rail.Label('PayTerms present') >>
            finalize_ready_for_processing >>
            catch_vendor_dag_error
        )
        (
            has_pay_terms >>
            rail.Label('No PayTerms') >>
            log_no_pay_terms >>
            catch_vendor_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
