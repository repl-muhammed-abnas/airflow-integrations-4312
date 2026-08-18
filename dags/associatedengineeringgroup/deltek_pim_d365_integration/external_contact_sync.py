"""
Entity sync DAG: D365 Contact -> PIM Contact (classID=1, external).

Triggered by the router DAG when a D365 ``contact`` entity webhook fires.
Fetches the contact from D365, resolves the parent account's ExternalOrg
mapping to derive the positive PIM organisation ID, checks
ExternalIntegrationMapping for an existing PIM Contact, then creates or
updates accordingly via the PIM Standard API
(``/XWeb/api/v1/contacts``).

Flow
----
ViewDagRunConf -> get_d365_token -> get_pim_token
  -> fetch_d365_contact
  -> get_entity_mapping
  -> resolve_org_mapping (ExternalOrg mapping from parentaccountid)
  -> is_entity_exists (IfOperator)
    -> YES: build_update_body -> update_contact_in_pim -> catch_error
    -> NO:  build_create_body -> create_contact_in_pim -> add_contact_mapping -> catch_error
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
    build_external_contact_body,
    build_add_mapping_body,
    check_mapping_exists,
    extract_d365_entity,
    extract_pim_entity_id,
    safe_json_response,
    is_org_mapping_present
)


def create_dag(config):
    """Create the D365 Contact -> PIM External Contact sync DAG."""
    with rail.create_airflow_dag(
        dag_id=config.external_contact_dag_id,
        description='Sync D365 Contact to PIM External Contact',
        integration_type='generic',
        company_key=config.company_key,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['pim_d365', 'external_contact', 'sync'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        # ── 1. Batch task toggle (default: enabled) ──────────────────────
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                f'{config.external_contact_dag_id}_can_run_batch_task',
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

        # ── 1b. Create log for sync tracking ─────────────────────────────
        create_log = rail.CreateLogOperator(task_id='create_log')

        # ── 2. View dag_run conf ────────────────────────────────────────
        view_conf = rail.ViewDagRunConfOperator(
            task_id='view_conf'
        )

        # ── 4. Fetch D365 contact ──────────────────────────────────────
        fetch_d365_contact = rail.SimpleHttpOperator(
            task_id='fetch_d365_contact',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}contacts"
                "({{ dag_run.conf.entity_guid }})"
                "?$select=contactid,firstname,lastname,"
                "emailaddress1,jobtitle,_parentcustomerid_value"
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
            python_callable=extract_d365_entity('fetch_d365_contact')
        )

        fetch_organization_mapping = rail.SimpleHttpOperator(
            task_id='fetch_organization_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['EXTERNAL_ORG'])}"
                "&source={{ result('get_d365_entity')['_parentcustomerid_value'] }}"
                
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                # 'Content-Type': 'application/json',
            },
            response_filter=safe_json_response
        )

        check_if_org_mapping_is_present =  rail.IfOperator(
            task_id='check_if_org_mapping_is_present',
            test=is_org_mapping_present,
            yes_task='get_entity_mapping',
            no_task='log_org_entity_guid'
        )

        log_org_entity_guid = rail.PythonOperator(
            task_id='log_org_entity_guid',
            python_callable=lambda: rail.result('get_d365_entity').get('_parentcustomerid_value'),
        )

        trigger_external_org_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_external_org_sync',
            items=[1],
            trigger_dag_id=config.external_org_dag_id,
            conf=lambda: {
                'entity_guid': rail.result('log_org_entity_guid'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
                'root_triggered_by': rail.get_current_context()['dag_run'].conf.get(
                    'triggered_by', rail.get_current_context()['dag_run'].run_id),
            },
            retries=0,
            execution_timeout=timedelta(hours=1),
        )

        wait_for_external_org_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_external_org_sync',
            dag_runs="{{ result('trigger_external_org_sync') }}",
            poke_interval=15,
            execution_timeout=timedelta(hours=1),
        )

        

        fetch_organization_mapping2 = rail.SimpleHttpOperator(
            task_id='fetch_organization_mapping2',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['EXTERNAL_ORG'])}"
                "&source={{ result('get_d365_entity')['_parentcustomerid_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                # 'Content-Type': 'application/json',
            },
            response_filter=safe_json_response
        )

        # ── 6. Get ExternalContact mapping ─────────────────────────────
        get_entity_mapping = rail.SimpleHttpOperator(
            task_id='get_entity_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/"
                f"{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['EXTERNAL_CONTACT'])}"
                "&source={{ result('get_d365_entity')['contactid'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                # 'Content-Type': 'application/json',
            },
            response_filter=safe_json_response
        )

        # ── 7. Resolve ExternalOrg mapping from parentaccountid ────────
        resolve_org_mapping = rail.SimpleHttpOperator(
            task_id='get_parent_account_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/"
                f"{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['EXTERNAL_ORG'])}"
                "&source={{ result('get_d365_entity')"
                "['_parentcustomerid_value'] }}"
                # "&source={{ result('get_d365_entity').get('_parentaccountid_value', dag_run.conf.get('parent_account_id', '')) }}"
                # "['_parentaccountid_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                # 'Content-Type': 'application/json',
            },
            response_filter=safe_json_response
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
            python_callable=build_external_contact_body('update')
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
            python_callable=build_external_contact_body('create')
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

        # ── 10. Add mapping after create ──────────────────────────────
        prepare_mapping_body = rail.PythonOperator(
            task_id='prepare_mapping_body',
            python_callable=build_add_mapping_body(MAPPING_TYPE_NAMES['EXTERNAL_CONTACT'])
        )

        add_contact_mapping = rail.SimpleHttpOperator(
            task_id='add_contact_mapping',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/"
                f"{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=AddMapping&name={quote(MAPPING_TYPE_NAMES['EXTERNAL_CONTACT'])}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('prepare_mapping_body') }}",
            response_filter=safe_json_response
        )

        # ── 11. Logging ───────────────────────────────────────────────
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'entity_type': 'ExternalContact',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ (result('get_d365_entity').get('firstname', '') ~ ' ' ~ result('get_d365_entity').get('lastname', '')) | trim if result('get_d365_entity') else '' }}",
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
                'entity_type': 'ExternalContact',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ (result('get_d365_entity').get('firstname', '') ~ ' ' ~ result('get_d365_entity').get('lastname', '')) | trim if result('get_d365_entity') else '' }}",
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

        create_log >> fetch_d365_contact >> get_d365_entity >> fetch_organization_mapping
        fetch_organization_mapping >> check_if_org_mapping_is_present 
        check_if_org_mapping_is_present >> rail.Label("Org mapping not present") >> log_org_entity_guid >> trigger_external_org_sync >> wait_for_external_org_sync >> fetch_organization_mapping2 >> get_entity_mapping
        check_if_org_mapping_is_present >> rail.Label("Org mapping is present") >> get_entity_mapping
        
        get_entity_mapping >> resolve_org_mapping >> is_entity_exists

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
