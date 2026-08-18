"""
Enquiry Sync DAG — D365 OpportunityProduct -> PIM Enquiry (classID=4)

Webhook-triggered DAG that syncs a single D365 opportunityproduct entity
to PIM as an Enquiry. Uses Standard API for create/update and Custom API
(UpdateEnquiryUDF) for UDF fields. Links organisation on create.

PIM and D365 tokens are read from Airflow Variables, refreshed by the
token refresh DAG every 30 minutes.

Schedule: None (triggered by router DAG)

Flow
----
can_run_batch_task
  -> Yes: batch_task (create_log ... sync_entity_contacts) >> catch_and_log_errors
  -> No:  create_log >> view_conf >> fetch >> ...          >> catch_and_log_errors
Inside batch:
  get_d365_entity >> fetch_d365_segment
                  >> get_entity_mapping
                  >> resolve_parent_org
  >> is_enquiry_exists (IfOperator)
    -> YES: build_update_body -> update_enquiry -> prepare_udf_body -> update_enquiry_udf -> converge
    -> NO:  build_create_body -> create_entity  -> prepare_udf_body -> update_enquiry_udf -> converge
            create_entity -> prepare_link_org_body -> link_org_to_enquiry
            link_org_to_enquiry -> prepare_mapping_body -> add_enquiry_mapping -> converge
  >> fetch_enquiry_contacts >> fetch_enquiry_team
  >> get_entity_contacts >> prepare_contacts_body >> sync_entity_contacts
Outside batch: catch_and_log_errors (one_failed) >> trigger_error_log
"""
# pylint: disable=line-too-long,pointless-statement,expression-not-assigned
from datetime import timedelta
from urllib.parse import quote

from airflow.models import Variable
import rail

from associatedengineeringgroup.deltek_pim_d365_integration.config import (
    D365_API_VERSION,
    D365_ODATA_HEADERS,
    ENTITY_CLASS_IDS,
    MAPPING_TYPE_NAMES,
    PIM_CUSTOM_API,
    PIM_STANDARD_API_BASE,
)
from associatedengineeringgroup.deltek_pim_d365_integration.utils.python_methods import (
    build_add_mapping_body,
    build_enquiry_body,
    build_enquiry_udf_body,
    build_lead_contacts_body,
    build_link_org_body,
    check_entity_field,
    check_mapping_exists,
    check_segment_field,
    validate_company_mapping,
    validate_ref_data_mapping,
    extract_pim_entity_id,
    filter_mapping_by_type,
    get_unmapped_external_contacts,
    get_unmapped_team_members,
    has_unmapped_external_contacts,
    has_unmapped_team_members,
    is_org_mapping_present,
    resolve_enquiry_contacts,
    safe_json_response,
)


def create_dag(config):
    """Create the Enquiry sync DAG for a given instance."""

    mapping_type = MAPPING_TYPE_NAMES['ENQUIRY']
    class_id = ENTITY_CLASS_IDS['ENQUIRY']

    with rail.create_airflow_dag(
        dag_id=config.enquiry_dag_id,
        description='Sync D365 OpportunityProduct to PIM Enquiry',
        integration_type='generic',
        company_key=config.company_key,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['pim_d365', 'enquiry', 'sync'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        # ── 1. Batch task toggle ──────────────────────────────────────
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                f'{config.enquiry_dag_id}_can_run_batch_task',
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

        # ── 2. Log + view conf ────────────────────────────────────────
        create_log = rail.CreateLogOperator(task_id='create_log')

        view_conf = rail.ViewDagRunConfOperator(task_id='view_conf')

        # ── 3. Fetch D365 OpportunityProduct ──────────────────────────
        fetch_d365_opportunityproduct = rail.SimpleHttpOperator(
            task_id='get_d365_entity',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}opportunityproducts"
                "({{ dag_run.conf.entity_guid }})"
                "?$select=opportunityproductid,ae_opportunitylineid,"
                "productdescription,vs360_status,ae_statusreason,"
                "vs360_scheduledstart,ae_descriptionscope,"
                "ae_capitalvalueofproject,"
                "_vs360_projectmanager_value,_vs360_company_value,"
                "_ae_groupprofitcentre_value,"
                "_opportunityid_value"
                "&$expand="
                "opportunityid($select=opportunityid,vs360_confidential,"
                "_vs360_primaryclientid_value,_parentcontactid_value)"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: r.json(),
        )

        # ── 4. Fetch segment (Group/Profit Centre) ────────────────────
        fetch_d365_segment = rail.SimpleHttpOperator(
            task_id='fetch_d365_segment',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}vs360_segments("
                "{{ result('get_d365_entity')['_ae_groupprofitcentre_value'] }}"
                ")?$select=vs360_segmentid,vs360_name,"
                "_vs360_marketid_value,_ae_office_value,_ae_company_value"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: r.json(),
        )

        # ── 4b. Division — resolve or create via process_udfs child DAG ──
        check_has_division = rail.IfOperator(
            task_id='check_has_division',
            test=check_segment_field('_vs360_marketid_value'),
            yes_task='process_division_udf',
            no_task='check_has_office',
        )

        process_division_udf = rail.TriggerDagRunForEachItemOperator(
            task_id='process_division_udf',
            items=[1],
            trigger_dag_id=config.process_udfs_dag_id,
            conf=lambda: {
                'source_guid': rail.result('fetch_d365_segment').get('_vs360_marketid_value'),
                'mapping_type_name': MAPPING_TYPE_NAMES['DIVISION'],
                'pim_add_function': 'AddDivision',
                'name': rail.result('fetch_d365_segment').get(
                    '_vs360_marketid_value@OData.Community.Display.V1.FormattedValue'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
            retries=0,
            execution_timeout=timedelta(hours=1),
        )

        wait_for_division_udf = rail.WaitForDagRunsSensor(
            task_id='wait_for_division_udf',
            dag_runs="{{ result('process_division_udf') }}",
            poke_interval=15,
            execution_timeout=timedelta(hours=1),
        )

        get_pim_division_id = rail.GatherResultsFromDagRunsOperator(
            task_id='get_pim_division_id',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_division_udf') }}",
            dagrun_task_id='get_destination_id',
            flatten=True,
        )

        # ── 4c. Office — resolve or create via process_udfs child DAG ──
        check_has_office = rail.IfOperator(
            task_id='check_has_office',
            test=check_segment_field('_ae_office_value'),
            yes_task='process_office_udf',
            no_task='check_has_group',
        )

        process_office_udf = rail.TriggerDagRunForEachItemOperator(
            task_id='process_office_udf',
            items=[1],
            trigger_dag_id=config.process_udfs_dag_id,
            conf=lambda: {
                'source_guid': rail.result('fetch_d365_segment').get('_ae_office_value'),
                'mapping_type_name': MAPPING_TYPE_NAMES['OFFICE'],
                'pim_add_function': 'AddOffice',
                'name': rail.result('fetch_d365_segment').get(
                    '_ae_office_value@OData.Community.Display.V1.FormattedValue'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
            retries=0,
            execution_timeout=timedelta(hours=1),
        )

        wait_for_office_udf = rail.WaitForDagRunsSensor(
            task_id='wait_for_office_udf',
            dag_runs="{{ result('process_office_udf') }}",
            poke_interval=15,
            execution_timeout=timedelta(hours=1),
        )

        get_pim_office_id = rail.GatherResultsFromDagRunsOperator(
            task_id='get_pim_office_id',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_office_udf') }}",
            dagrun_task_id='get_destination_id',
            flatten=True,
        )

        # ── 4d. Group — resolve or create via process_udfs child DAG ──
        check_has_group = rail.IfOperator(
            task_id='check_has_group',
            test=check_entity_field('_ae_groupprofitcentre_value'),
            yes_task='process_group_udf',
            no_task='check_has_company',
        )

        process_group_udf = rail.TriggerDagRunForEachItemOperator(
            task_id='process_group_udf',
            items=[1],
            trigger_dag_id=config.process_udfs_dag_id,
            conf=lambda: {
                'source_guid': rail.result('get_d365_entity').get('_ae_groupprofitcentre_value'),
                'mapping_type_name': MAPPING_TYPE_NAMES['GROUP'],
                'pim_add_function': 'AddGroup',
                'name': rail.result('fetch_d365_segment').get('vs360_name'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
            retries=0,
            execution_timeout=timedelta(hours=1),
        )

        wait_for_group_udf = rail.WaitForDagRunsSensor(
            task_id='wait_for_group_udf',
            dag_runs="{{ result('process_group_udf') }}",
            poke_interval=15,
            execution_timeout=timedelta(hours=1),
        )

        get_pim_group_id = rail.GatherResultsFromDagRunsOperator(
            task_id='get_pim_group_id',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_group_udf') }}",
            dagrun_task_id='get_destination_id',
            flatten=True,
        )

        # ── 4e. Company mapping (read-only, hardcoded by client — error if missing) ──
        check_has_company = rail.IfOperator(
            task_id='check_has_company',
            test=check_segment_field('_ae_company_value'),
            yes_task='get_company_mapping',
            no_task='catch_and_log_errors',
        )

        get_company_mapping = rail.SimpleHttpOperator(
            task_id='get_company_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['COMPANY'])}"
                "&source={{ result('fetch_d365_segment')['_ae_company_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=filter_mapping_by_type(MAPPING_TYPE_NAMES['COMPANY']),
        )

        check_company_mapping_exists = rail.IfOperator(
            task_id='check_company_mapping_exists',
            test=validate_company_mapping,
            yes_task='get_entity_mapping',
            no_task='catch_and_log_errors',
        )

        # ── 5. Check existing mapping ─────────────────────────────────
        get_enquiry_mapping = rail.SimpleHttpOperator(
            task_id='get_entity_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(mapping_type)}"
                "&source={{ dag_run.conf.entity_guid }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=safe_json_response,
        )

        # ── 6. Resolve parent org mapping (with auto-sync fallback) ──────
        fetch_organization_mapping = rail.SimpleHttpOperator(
            task_id='fetch_organization_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['EXTERNAL_ORG'])}"
                "&source={{ result('get_d365_entity')"
                "['opportunityid']['_vs360_primaryclientid_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=safe_json_response,
        )

        check_if_org_mapping_is_present = rail.IfOperator(
            task_id='check_if_org_mapping_is_present',
            test=is_org_mapping_present,
            yes_task='is_enquiry_exists',
            no_task='log_org_entity_guid',
        )

        log_org_entity_guid = rail.PythonOperator(
            task_id='log_org_entity_guid',
            python_callable=lambda: rail.result('get_d365_entity')['opportunityid']['_vs360_primaryclientid_value'],
        )

        trigger_external_org_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_external_org_sync',
            items=[1],
            trigger_dag_id=config.external_org_dag_id,
            conf=lambda: {
                'entity_guid': rail.result('log_org_entity_guid'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
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
                "&source={{ result('get_d365_entity')"
                "['opportunityid']['_vs360_primaryclientid_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=safe_json_response,
        )

        # ── 7. Branch: create or update ───────────────────────────────
        is_enquiry_exists = rail.IfOperator(
            task_id='is_enquiry_exists',
            test=check_mapping_exists,
            yes_task='build_update_body',
            no_task='build_create_body',
        )

        # ── 8a. UPDATE branch ─────────────────────────────────────────
        build_update_body = rail.PythonOperator(
            task_id='build_update_body',
            python_callable=build_enquiry_body('update'),
        )

        update_enquiry = rail.SimpleHttpOperator(
            task_id='update_enquiry',
            method='PUT',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"{PIM_STANDARD_API_BASE}enquiries/"
                "{{ result('get_entity_mapping')[0]['destinationId'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_update_body') }}",
            response_filter=safe_json_response,
        )

        # ── 8b. CREATE branch ─────────────────────────────────────────
        build_create_body = rail.PythonOperator(
            task_id='build_create_body',
            python_callable=build_enquiry_body('create'),
        )

        create_enquiry = rail.SimpleHttpOperator(
            task_id='create_entity',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=f'{PIM_STANDARD_API_BASE}enquiries',
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_create_body') }}",
            response_filter=extract_pim_entity_id,
        )

        # ─ 9. UDF update (mandatory after both create and update) ──
        prepare_udf_body = rail.PythonOperator(
            task_id='prepare_udf_body',
            python_callable=build_enquiry_udf_body(),
        )

        update_enquiry_udf = rail.SimpleHttpOperator(
            task_id='update_enquiry_udf',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['ENQUIRY']}"
                "?function=UpdateEnquiryUDF"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('prepare_udf_body') }}",
            response_filter=safe_json_response,
        )

        # ── 10. Link org (create-only) ────────────────────────────────
        prepare_link_org_body = rail.PythonOperator(
            task_id='prepare_link_org_body',
            python_callable=build_link_org_body(),
        )

        link_org_to_enquiry = rail.SimpleHttpOperator(
            task_id='link_org_to_enquiry',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"{PIM_STANDARD_API_BASE}enquiries/"
                "{{ result('create_entity') }}"
                "/organizations"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('prepare_link_org_body') }}",
            response_filter=safe_json_response,
        )

        # ── 11. Add mapping (create-only) ─────────────────────────────
        prepare_mapping_body = rail.PythonOperator(
            task_id='prepare_mapping_body',
            python_callable=build_add_mapping_body(mapping_type),
        )

        add_enquiry_mapping = rail.SimpleHttpOperator(
            task_id='add_enquiry_mapping',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=AddMapping&name={quote(mapping_type)}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('prepare_mapping_body') }}",
            response_filter=safe_json_response,
        )

        # ── 12. Convergence ───────────────────────────────────────────
        converge = rail.PythonOperator(
            task_id='converge',
            trigger_rule='none_failed_min_one_success',
            python_callable=lambda: True,
        )

        # ── 13. PM resolution (mirrors opportunity pattern) ──────────
        check_pm_assigned = rail.IfOperator(
            task_id='check_pm_assigned',
            test=check_entity_field('_vs360_projectmanager_value'),
            yes_task='resolve_pm_employee',
            no_task='fetch_enquiry_contacts',
        )

        resolve_pm_employee = rail.SimpleHttpOperator(
            task_id='resolve_pm_employee',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}vs360_employees"
                "?$filter=_vs360_systemuserid_value eq "
                "'{{ result('get_d365_entity')['_vs360_projectmanager_value'] }}'"
                "&$select=vs360_employeeid"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: (
                r.json().get('value', [{}])[0] or {}
            ).get('vs360_employeeid'),
        )

        check_pm_employee_found = rail.IfOperator(
            task_id='check_pm_employee_found',
            test=lambda: bool(rail.result('resolve_pm_employee')),
            yes_task='get_pm_mapping',
            no_task='fetch_enquiry_contacts',
        )

        get_pm_mapping = rail.SimpleHttpOperator(
            task_id='get_pm_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['INTERNAL_CONTACT'])}"
                "&source={{ result('resolve_pm_employee') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=safe_json_response,
        )

        check_pm_exists = rail.IfOperator(
            task_id='check_pm_exists',
            test=lambda: validate_ref_data_mapping('get_pm_mapping'),
            yes_task='fetch_enquiry_contacts',
            no_task='trigger_pm_sync',
        )

        trigger_pm_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_pm_sync',
            trigger_dag_id=config.internal_contact_dag_id,
            items=lambda: [rail.result('resolve_pm_employee')],
            conf=lambda item: {
                'entity_guid': item,
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
            retries=0,
            execution_timeout=timedelta(hours=1),
        )

        wait_for_pm_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_pm_sync',
            dag_runs="{{ result('trigger_pm_sync') }}",
            poke_interval=15,
            execution_timeout=timedelta(hours=1),
        )

        get_pm_mapping_after_sync = rail.SimpleHttpOperator(
            task_id='get_pm_mapping_after_sync',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['INTERNAL_CONTACT'])}"
                "&source={{ result('resolve_pm_employee') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=safe_json_response,
        )

        # ── 14. Contacts sync ─────────────────────────────────────────
        fetch_enquiry_contacts = rail.SimpleHttpOperator(
            task_id='fetch_enquiry_contacts',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}vs360_opportunitycontacts"
                "?$filter=_ae_opportunityproduct_value eq "
                "'{{ dag_run.conf.entity_guid }}'"
                "&$select=vs360_opportunitycontactid,_vs360_contactid_value"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: r.json().get('value', []),
        )

        fetch_enquiry_team = rail.SimpleHttpOperator(
            task_id='fetch_enquiry_team',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}vs360_opportunityteams"
                "?$filter=_ae_opportunityproduct_value eq "
                "'{{ dag_run.conf.entity_guid }}'"
                "&$select=vs360_opportunityteamid,_vs360_employeeid_value"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: r.json().get('value', []),
        )

        # ── 13b. Pre-sync unmapped external contacts ─────────────────
        check_unmapped_ext_contacts = rail.PythonOperator(
            task_id='get_unmapped_external_contacts',
            python_callable=get_unmapped_external_contacts(
                config.pim_conn_id, config.instance,
                contacts_task_id='fetch_enquiry_contacts',
                primary_contact_getter=lambda: (
                    (rail.result('get_d365_entity') or {})
                    .get('opportunityid', {})
                    .get('_parentcontactid_value')
                ),
            ),
        )

        has_unmapped_ext = rail.IfOperator(
            task_id='check_unmapped_ext_contacts',
            test=has_unmapped_external_contacts,
            yes_task='trigger_external_contact_sync',
            no_task='get_unmapped_team_members',
        )

        trigger_ext_contact_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_external_contact_sync',
            trigger_dag_id=config.external_contact_dag_id,
            items=lambda: rail.result('get_unmapped_external_contacts'),
            conf=lambda item: {'entity_guid': item, 'triggered_by': rail.get_current_context()['dag_run'].run_id},
            retries=0,
            execution_timeout=timedelta(hours=1),
        )

        wait_for_ext_contact_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_external_contact_sync',
            dag_runs="{{ result('trigger_external_contact_sync') }}",
            poke_interval=15,
            execution_timeout=timedelta(hours=1),
        )

        # ── 13c. Internal team sync (trigger if any unmapped) ─────────
        check_unmapped_team = rail.PythonOperator(
            task_id='get_unmapped_team_members',
            python_callable=get_unmapped_team_members(
                config.pim_conn_id, config.instance,
            ),
        )

        check_team_members_unmapped = rail.IfOperator(
            task_id='check_team_members_unmapped',
            test=has_unmapped_team_members,
            yes_task='trigger_internal_contact_sync',
            no_task='get_entity_contacts',
        )

        trigger_internal_contact_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_internal_contact_sync',
            trigger_dag_id=config.internal_contact_dag_id,
            items=lambda: rail.result('get_unmapped_team_members'),
            conf=lambda item: {'entity_guid': item, 'triggered_by': rail.get_current_context()['dag_run'].run_id},
            retries=0,
            execution_timeout=timedelta(hours=1),
        )

        wait_for_internal_contact_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_internal_contact_sync',
            dag_runs="{{ result('trigger_internal_contact_sync') }}",
            poke_interval=15,
            execution_timeout=timedelta(hours=1),
        )

        resolve_contacts = rail.PythonOperator(
            task_id='get_entity_contacts',
            python_callable=resolve_enquiry_contacts(
                config.pim_conn_id, config.d365_conn_id, config.instance,
            ),
        )

        prepare_contacts_body = rail.PythonOperator(
            task_id='prepare_contacts_body',
            python_callable=build_lead_contacts_body(class_id),
        )

        sync_entity_contacts = rail.SimpleHttpOperator(
            task_id='sync_entity_contacts',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['ENTITY_CONTACTS']}"
                "?function=UpdateEntityContacts"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('prepare_contacts_body') }}",
            response_filter=safe_json_response,
        )

        # ── 14. Error handling ────────────────────────────────────────
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'entity_type': 'Enquiry',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ result('get_d365_entity').get('productdescription', '') if result('get_d365_entity') else '' }}",
                'action': 'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}',
            },
        )

        trigger_error_log = rail.TriggerDagRunOperator(
            task_id='trigger_error_log',
            trigger_dag_id=config.error_log_dag_id,
            conf={
                'entity_type': 'Enquiry',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ result('get_d365_entity').get('productdescription', '') if result('get_d365_entity') else '' }}",
                'action': 'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}',
            },
            wait_for_completion=False,
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> fetch_d365_opportunityproduct >> fetch_d365_segment

        fetch_d365_segment >> check_has_division

        # Division — trigger process_udfs child DAG
        (
            check_has_division >> rail.Label('Yes') >>
            process_division_udf >> wait_for_division_udf >>
            get_pim_division_id >> check_has_office
        )
        check_has_division >> rail.Label('No') >> check_has_office

        # Office — trigger process_udfs child DAG
        (
            check_has_office >> rail.Label('Yes') >>
            process_office_udf >> wait_for_office_udf >>
            get_pim_office_id >> check_has_group
        )
        check_has_office >> rail.Label('No') >> check_has_group

        # Group — trigger process_udfs child DAG
        (
            check_has_group >> rail.Label('Yes') >>
            process_group_udf >> wait_for_group_udf >>
            get_pim_group_id >> check_has_company
        )
        check_has_group >> rail.Label('No') >> check_has_company

        (
            check_has_company >> rail.Label('Has company') >>
            get_company_mapping >> check_company_mapping_exists
        )
        check_company_mapping_exists >> rail.Label('Company mapped') >> get_enquiry_mapping
        check_company_mapping_exists >> rail.Label('Company not mapped') >> catch_and_log_errors
        check_has_company >> rail.Label('No company') >> catch_and_log_errors

        get_enquiry_mapping >> fetch_organization_mapping >> check_if_org_mapping_is_present

        check_if_org_mapping_is_present >> rail.Label('Org mapping is present') >> is_enquiry_exists
        (
            check_if_org_mapping_is_present
            >> rail.Label('Org mapping not present')
            >> log_org_entity_guid
            >> trigger_external_org_sync
            >> wait_for_external_org_sync
            >> fetch_organization_mapping2
            >> is_enquiry_exists
        )

        # Update branch
        (
            is_enquiry_exists
            >> rail.Label('Enquiry exists in PIM')
            >> build_update_body
            >> update_enquiry
            >> converge
        )

        # Create branch
        (
            is_enquiry_exists
            >> rail.Label('New enquiry')
            >> build_create_body
            >> create_enquiry
            >> prepare_link_org_body
            >> link_org_to_enquiry
            >> prepare_mapping_body
            >> add_enquiry_mapping
            >> converge
        )

        converge >> prepare_udf_body >> update_enquiry_udf >> check_pm_assigned

        # PM resolution
        check_pm_assigned >> rail.Label('PM assigned') >> resolve_pm_employee >> check_pm_employee_found
        check_pm_employee_found >> rail.Label('Employee found') >> get_pm_mapping >> check_pm_exists
        check_pm_employee_found >> rail.Label('Employee not found') >> fetch_enquiry_contacts
        check_pm_exists >> rail.Label('PM mapped') >> fetch_enquiry_contacts
        (
            check_pm_exists >> rail.Label('PM not mapped')
            >> trigger_pm_sync >> wait_for_pm_sync >> get_pm_mapping_after_sync
            >> fetch_enquiry_contacts
        )
        check_pm_assigned >> rail.Label('No PM') >> fetch_enquiry_contacts

        fetch_enquiry_contacts >> fetch_enquiry_team

        # External contact pre-sync
        fetch_enquiry_team >> check_unmapped_ext_contacts >> has_unmapped_ext
        (
            has_unmapped_ext >> rail.Label('Yes') >>
            trigger_ext_contact_sync >> wait_for_ext_contact_sync >>
            check_unmapped_team
        )
        has_unmapped_ext >> rail.Label('No') >> check_unmapped_team

        # Internal team pre-sync
        check_unmapped_team >> check_team_members_unmapped

        check_team_members_unmapped >> rail.Label('All mapped') >> resolve_contacts
        (
            check_team_members_unmapped
            >> rail.Label('Unmapped members')
            >> trigger_internal_contact_sync
            >> wait_for_internal_contact_sync
            >> resolve_contacts
        )

        resolve_contacts >> prepare_contacts_body >> sync_entity_contacts >> catch_and_log_errors

        catch_and_log_errors >> trigger_error_log

        return dag

rail.for_each_instance(create_dag)
