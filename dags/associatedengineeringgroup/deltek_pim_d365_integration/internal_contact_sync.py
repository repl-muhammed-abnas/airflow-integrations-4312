"""
Entity sync DAG: D365 vs360_employee -> PIM Contact (classID=1, internal).

Triggered by the router DAG when a D365 ``vs360_employee`` entity webhook
fires.  Fetches the employee from D365, resolves the Company mapping via
``ae_groupprofitcentre.ae_company`` to derive the internal organisation ID,
checks ExternalIntegrationMapping for an existing PIM Contact, then creates
or updates accordingly via the PIM Standard API (``/XWeb/api/v1/contacts``).

PIM token is read from Airflow Variable (refreshed by pim_token_refresh DAG).

Flow
----
can_run_batch_task
  -> Yes: batch_task (create_log … update/create) >> catch_and_log_errors
  -> No:  create_log >> … >> update/create >> catch_and_log_errors
Inside batch: fetch_d365_employee >> get_d365_entity
  >> get_company_mapping >> check_company_mapping
  >> get_entity_mapping >> is_entity_exists (IfOperator)
    -> YES: build_update_body -> update_contact_in_pim
    -> NO:  build_create_body -> create_entity -> prepare_mapping_body
            -> add_contact_mapping
Outside batch: catch_and_log_errors (one_failed) >> trigger_error_log
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
from urllib.parse import quote

from airflow.models import Variable
import rail

from associatedengineeringgroup.deltek_pim_d365_integration.config import (
    D365_API_VERSION,
    D365_ODATA_HEADERS,
    MAPPING_TYPE_NAMES,
    PIM_CUSTOM_API,
    PIM_STANDARD_API_BASE,
)
from associatedengineeringgroup.deltek_pim_d365_integration.utils.python_methods import (
    build_internal_contact_body,
    build_add_mapping_body,
    check_mapping_exists,
    extract_d365_entity,
    extract_pim_entity_id,
    filter_mapping_by_type,
    safe_json_response,
    validate_company_mapping,
)


def create_dag(config):
    """Create the D365 vs360_employee -> PIM Internal Contact sync DAG."""
    with rail.create_airflow_dag(
        dag_id=config.internal_contact_dag_id,
        description='Sync D365 Employee to PIM Internal Contact',
        integration_type='generic',
        company_key=config.company_key,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['pim_d365', 'internal_contact', 'sync'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        # ── 1. Batch task toggle (default: disabled) ─────────────────
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                f'{config.internal_contact_dag_id}_can_run_batch_task',
                default_var='true',
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='create_log',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ── 1b. Create log for sync tracking ─────────────────────────
        create_log = rail.CreateLogOperator(task_id='create_log')

        # ── 2. View dag_run conf ────────────────────────────────────────
        view_conf = rail.ViewDagRunConfOperator(
            task_id='view_conf'
        )

        # ── 3. Fetch D365 employee ─────────────────────────────────────
        fetch_d365_employee = rail.SimpleHttpOperator(
            task_id='fetch_d365_employee',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}vs360_employees"
                "({{ dag_run.conf.entity_guid }})"
                "?$select=vs360_employeeid,vs360_firstname,vs360_lastname,"
                "vs360_knownasname,emailaddress,statecode,"
                "_ae_groupprofitcentre_value"
                "&$expand=ae_GroupProfitCentre($select=_ae_company_value)"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: r.json()
        )

        # ── 5. Extract entity data ──────────────────────────────────────
        get_d365_entity = rail.PythonOperator(
            task_id='get_d365_entity',
            python_callable=extract_d365_entity('fetch_d365_employee')
        )

        # ── 5b. Guard: employee must have a group profit centre with company ──
        check_has_company_guid = rail.IfOperator(
            task_id='check_has_company_guid',
            test=lambda: bool(
                (rail.result('get_d365_entity').get('ae_GroupProfitCentre') or {})
                .get('_ae_company_value')
            ),
            yes_task='get_company_mapping',
            no_task='catch_and_log_errors',
        )

        # ── 6. Get InternalContact mapping (GET with query params) ─────
        get_entity_mapping = rail.SimpleHttpOperator(
            task_id='get_entity_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['INTERNAL_CONTACT'])}"
                "&source={{ dag_run.conf.entity_guid }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=safe_json_response
        )

        # ── 7. Resolve Internal Org mapping from ae_groupprofitcentre ──
        resolve_company_mapping = rail.SimpleHttpOperator(
            task_id='get_company_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['INTERNAL_ORG'])}"
                "&source={{ result('get_d365_entity')['ae_GroupProfitCentre']['_ae_company_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=filter_mapping_by_type(MAPPING_TYPE_NAMES['INTERNAL_ORG'])
        )

        # ── 7b. Validate company mapping exists ─────────────────────────
        check_company_mapping = rail.IfOperator(
            task_id='check_company_mapping',
            test=validate_company_mapping,
            yes_task='get_entity_mapping',
            no_task='catch_and_log_errors',
        )

        # ── 8. Branch: contact exists? ─────────────────────────────────
        is_entity_exists = rail.IfOperator(
            task_id='is_entity_exists',
            test=check_mapping_exists,
            yes_task='build_update_body',
            no_task='build_create_body'
        )

        # ── 9a. UPDATE branch ─────────────────────────────────────────
        build_update_body = rail.PythonOperator(
            task_id='build_update_body',
            python_callable=build_internal_contact_body('update')
        )

        update_contact_in_pim = rail.SimpleHttpOperator(
            task_id='update_contact_in_pim',
            method='PUT',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"{PIM_STANDARD_API_BASE}contacts/"
                "{{ result('get_entity_mapping')[0]['destinationId'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_update_body') }}",
            response_filter=safe_json_response
        )

        # ── 9b. CREATE branch ────────────────────────────────────────
        build_create_body = rail.PythonOperator(
            task_id='build_create_body',
            python_callable=build_internal_contact_body('create')
        )

        create_contact_in_pim = rail.SimpleHttpOperator(
            task_id='create_entity',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=f'{PIM_STANDARD_API_BASE}contacts',
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_create_body') }}",
            response_filter=extract_pim_entity_id
        )

        # ── 10. Add mapping after create ─────────────────────────────
        prepare_mapping_body = rail.PythonOperator(
            task_id='prepare_mapping_body',
            python_callable=build_add_mapping_body(MAPPING_TYPE_NAMES['INTERNAL_CONTACT'])
        )

        add_contact_mapping = rail.SimpleHttpOperator(
            task_id='add_contact_mapping',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/"
                f"{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=AddMapping&name={quote(MAPPING_TYPE_NAMES['INTERNAL_CONTACT'])}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('prepare_mapping_body') }}",
            response_filter=safe_json_response
        )

        # ── 11. Logging ──────────────────────────────────────────────
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'entity_type': 'InternalContact',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ (result('get_d365_entity').get('vs360_firstname', '') ~ ' ' ~ result('get_d365_entity').get('vs360_lastname', '')) | trim if result('get_d365_entity') else '' }}",
                'action': 'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}',
            },
        )

        should_log_error = rail.IfOperator(
            task_id='should_log_error',
            test=lambda: not bool(
                rail.get_current_context()['dag_run'].conf.get('triggered_by')),
            yes_task='trigger_error_log',
            no_task=None,
        )

        trigger_error_log = rail.TriggerDagRunOperator(
            task_id='trigger_error_log',
            trigger_dag_id=config.error_log_dag_id,
            conf={
                'entity_type': 'InternalContact',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ (result('get_d365_entity').get('vs360_firstname', '') ~ ' ' ~ result('get_d365_entity').get('vs360_lastname', '')) | trim if result('get_d365_entity') else '' }}",
                'action': 'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}',
            },
            wait_for_completion=False,
        )

        # ── Task wiring ───────────────────────────────────────────────
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> fetch_d365_employee >> get_d365_entity
        get_d365_entity >> check_has_company_guid
        check_has_company_guid >> rail.Label('Yes') >> resolve_company_mapping >> check_company_mapping
        check_has_company_guid >> rail.Label('No') >> catch_and_log_errors

        (
            check_company_mapping >> rail.Label('Mapping found') >>
            get_entity_mapping >> is_entity_exists
        )
        (
            check_company_mapping >> rail.Label('No mapping') >>
            catch_and_log_errors
        )

        # Update branch
        (
            is_entity_exists >> rail.Label('Entity exists') >>
            build_update_body >> update_contact_in_pim >> catch_and_log_errors
        )

        # Create branch
        (
            is_entity_exists >> rail.Label('Entity does not exist') >>
            build_create_body >> create_contact_in_pim >>
            prepare_mapping_body >> add_contact_mapping >> catch_and_log_errors
        )

        catch_and_log_errors >> should_log_error
        should_log_error >> rail.Label('Yes') >> trigger_error_log

        return dag


rail.for_each_instance(create_dag)
